//! Test-only host for paired receipt-cleanup measurements. Not a deployment entry point.
use std::{io::Write, sync::Arc, time::Duration};
use tiangz_dbproxy_core::{
    AsyncSnapshotStore, RecordKey, SnapshotEnvelope, SnapshotWrite, SnapshotWriteOutcome,
    TransactionReceipt, TransactionalWrite, TransactionalWriteOutcome,
};
use tiangz_dbproxy_server::{
    BackendError, DbProxyBackend, DbProxyMetrics, DbProxyServer, RetryWorkerPolicy, ServerConfig,
    StorageBackend, StorageBackendConfig, run_backlog_worker_observed,
    run_cache_repair_worker_observed, run_outbox_worker_observed, run_receipt_cleanup_worker,
};
use tokio::sync::watch;

// Diagnostic control only: keep writes unchanged and give authority reads one extra connection.
// This is not a production backend and intentionally supports only the ordinary probe surface.
struct DedicatedReadProbe {
    writes: Arc<StorageBackend>,
    reads: tiangz_dbproxy_storage::PostgresSnapshotStore,
}
#[async_trait::async_trait]
impl DbProxyBackend for DedicatedReadProbe {
    async fn load(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, BackendError> {
        Ok(self.reads.load(record).await?)
    }
    async fn save(&self, request: SnapshotWrite) -> Result<SnapshotWriteOutcome, BackendError> {
        self.writes.save(request).await
    }
    async fn enqueue_snapshot(&self, request: SnapshotWrite) -> Result<(), BackendError> {
        self.writes.enqueue_snapshot(request).await
    }
    async fn apply_transaction(
        &self,
        request: TransactionalWrite,
    ) -> Result<TransactionalWriteOutcome, BackendError> {
        self.writes.apply_transaction(request).await
    }
    async fn load_transaction(
        &self,
        operation: &str,
        record: &RecordKey,
    ) -> Result<Option<TransactionReceipt>, BackendError> {
        self.writes.load_transaction(operation, record).await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(),
        Ok("1")
    );
    let mode = std::env::var("ACCEPT_CLEANUP")?;
    assert!(mode == "on" || mode == "off");
    let read_mode = std::env::var("ACCEPT_READ_CONNECTION").unwrap_or_else(|_| "shared".into());
    let backend = Arc::new(
        StorageBackend::connect_with_redis_urls(
            &std::env::var("DBPROXY_TEST_POSTGRES_URL")?,
            &std::env::var("DBPROXY_REDIS_URL")?,
            &std::env::var("DBPROXY_CACHE_REDIS_URL")?,
            StorageBackendConfig {
                shard_count: 4,
                read_connection_count: if read_mode == "pooled" { 2 } else { 0 },
                tiered: Default::default(),
                enqueue: Default::default(),
            },
        )
        .await?,
    );
    let request_backend: Arc<dyn DbProxyBackend> = match read_mode.as_str() {
        "shared" | "pooled" => backend.clone(),
        "dedicated" => Arc::new(DedicatedReadProbe {
            writes: backend.clone(),
            reads: tiangz_dbproxy_storage::PostgresSnapshotStore::connect_existing(&std::env::var(
                "DBPROXY_TEST_POSTGRES_URL",
            )?)
            .await?,
        }),
        _ => return Err("invalid ACCEPT_READ_CONNECTION".into()),
    };
    let request_metrics = Arc::new(DbProxyMetrics::default());
    let mut server_config = ServerConfig::new(
        std::env::var("ACCEPT_LISTEN")?.parse()?,
        "acceptance-performance-token",
    );
    server_config.metrics = request_metrics.clone();
    let server = DbProxyServer::bind(server_config, request_backend).await?;
    let (stop, shutdown) = watch::channel(false);
    let heartbeat_delay_us = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let heartbeat_value = heartbeat_delay_us.clone();
    let mut heartbeat_stop = shutdown.clone();
    let heartbeat = tokio::spawn(async move {
        loop {
            let deadline = tokio::time::Instant::now() + Duration::from_millis(10);
            tokio::select! {
                _ = heartbeat_stop.changed() => break,
                _ = tokio::time::sleep_until(deadline) => {
                    heartbeat_value.fetch_max(deadline.elapsed().as_micros() as u64, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    });
    // Same default as the server config; ACCEPT_RECEIPT_RETENTION_HOURS overrides it for fixtures.
    let retention = match std::env::var("ACCEPT_RECEIPT_RETENTION_HOURS") {
        Ok(hours) => Duration::from_secs(hours.parse::<u64>()? * 3600),
        Err(_) => tiangz_dbproxy_storage::DEFAULT_RECEIPT_RETENTION,
    };
    tiangz_dbproxy_storage::validate_receipt_retention(retention)?;
    let cleanup = (mode == "on").then(|| {
        tokio::spawn(run_receipt_cleanup_worker(
            backend.clone(),
            "acceptance".into(),
            retention,
            Arc::new(DbProxyMetrics::default()),
            shutdown.clone(),
        ))
    });
    // ACCEPT_BACKGROUND=all also runs the production background workers (one each, with the
    // server config defaults); `cleanup` (default) keeps the historical receipt-cleanup-only host
    // so older runs stay comparable. Without the cache repair worker, dbproxy_cache_repairs only
    // grows, and its PostgreSQL load is missing from the measurement (long_p08_a).
    let background = std::env::var("ACCEPT_BACKGROUND").unwrap_or_else(|_| "cleanup".into());
    if background != "cleanup" && background != "all" {
        return Err("ACCEPT_BACKGROUND must be cleanup or all".into());
    }
    let mut background_workers = Vec::new();
    if background == "all" {
        // Same values as config.rs default_retry_queue_* and default_backlog_* (1 worker each).
        let queue_policy = RetryWorkerPolicy {
            lease_ms: 30_000,
            base_retry_delay_ms: 1_000,
            max_retry_delay_ms: 60_000,
            max_attempts: 20,
        };
        let queue_idle = Duration::from_millis(250);
        background_workers.push(tokio::spawn(run_backlog_worker_observed(
            backend.clone(),
            30_000,
            Duration::from_millis(20),
            Duration::from_secs(1),
            shutdown.clone(),
            None,
        )));
        background_workers.push(tokio::spawn(run_cache_repair_worker_observed(
            backend.clone(),
            "acceptance-cache-repair-0".into(),
            queue_policy,
            queue_idle,
            shutdown.clone(),
            None,
        )));
        background_workers.push(tokio::spawn(run_outbox_worker_observed(
            backend.clone(),
            "acceptance-outbox-0".into(),
            queue_policy,
            queue_idle,
            shutdown.clone(),
            None,
        )));
    }
    println!(
        "READY cleanup={mode} reads={read_mode} background={background} workers={} endpoint={}",
        tokio::runtime::Handle::current().metrics().num_workers(),
        server.local_addr()?
    );
    let serving = tokio::spawn(server.serve(shutdown));
    // An owned stop file permits graceful shutdown on Windows, without killing unrelated processes.
    let stop_file = std::env::var("ACCEPT_STOP_FILE")?;
    let mut pg_config: tokio_postgres::Config =
        std::env::var("DBPROXY_TEST_POSTGRES_URL")?.parse()?;
    pg_config.application_name("acceptance-sampler");
    let mut sampler = ActivitySampler::default();
    let path = std::path::Path::new(&stop_file).with_file_name("storage-stages.jsonl");
    let mut stages = std::io::BufWriter::new(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?,
    );
    while !std::path::Path::new(&stop_file).exists() {
        if serving.is_finished() {
            break;
        }
        let sample: Vec<_> = backend.metrics().latency_snapshot().into_iter().map(|s| serde_json::json!({"stage":s.stage,"buckets":s.buckets,"sum_micros":s.sum_micros,"in_flight":s.in_flight})).collect();
        writeln!(
            stages,
            "{}",
            serde_json::json!({"unix_ms":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis(),"runtime_heartbeat_max_delay_us":heartbeat_delay_us.swap(0,std::sync::atomic::Ordering::Relaxed),"bounds_ms":tiangz_dbproxy_storage::STORAGE_LATENCY_BOUNDS_MS,"stages":sample,"request_stages":request_metrics.request_stage_snapshot(),"read_pool":backend.read_pool_usage().map(|u| serde_json::json!({"capacity":u.capacity,"in_use":u.in_use}))})
        )?;
        stages.flush()?;
        let started = std::time::Instant::now();
        let activity = sampler.sample(&pg_config).await;
        writeln!(
            stages,
            "{}",
            serde_json::json!({"kind":"pg_activity","unix_ms":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis(),"sample_us":started.elapsed().as_micros(),"activity":activity})
        )?;
        stages.flush()?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stop.send(true)?;
    heartbeat.await?;
    tokio::time::timeout(Duration::from_secs(15), serving).await???;
    if let Some(cleanup) = cleanup {
        tokio::time::timeout(Duration::from_secs(5), cleanup).await??;
    }
    for worker in background_workers {
        tokio::time::timeout(Duration::from_secs(35), worker).await??;
    }
    Ok(())
}

// 采样独立限时；超时后销毁连接，下次重新解析地址，不能阻塞停止文件的检查。
// Diagnostics have their own deadline and discard the socket on failure, independently of
// the production network guards being compared. The next sample resolves the address again.
#[derive(Default)]
struct ActivitySampler {
    connection: Option<(tokio_postgres::Client, tokio::task::JoinHandle<()>)>,
}

impl Drop for ActivitySampler {
    fn drop(&mut self) {
        self.disconnect();
    }
}

impl ActivitySampler {
    fn disconnect(&mut self) {
        if let Some((_, driver)) = self.connection.take() {
            driver.abort();
        }
    }

    async fn sample(&mut self, config: &tokio_postgres::Config) -> serde_json::Value {
        let result = tokio::time::timeout(Duration::from_secs(1), async {
            if self.connection.is_none() {
                let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
                self.connection = Some((client, tokio::spawn(async move {
                    let _ = connection.await;
                })));
                self.connection.as_ref().unwrap().0
                    .batch_execute("SET statement_timeout='500ms'").await?;
            }
            self.connection.as_ref().unwrap().0.query(
                "SELECT pid,state,wait_event_type,wait_event FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()", &[]
            ).await
        }).await;
        match result {
            Ok(Ok(rows)) => {
                serde_json::json!({"backends": rows.into_iter().map(|r| serde_json::json!({
                "pid":r.get::<_,i32>(0), "state":r.get::<_,Option<String>>(1),
                "wait_type":r.get::<_,Option<String>>(2), "wait":r.get::<_,Option<String>>(3)
            })).collect::<Vec<_>>() })
            }
            result => {
                self.disconnect();
                let error = match result {
                    Ok(Err(error)) => error.to_string(),
                    Err(_) => "activity sample exceeded 1 second; connection discarded".to_owned(),
                    Ok(Ok(_)) => unreachable!(),
                };
                serde_json::json!({"error": error})
            }
        }
    }
}

#[cfg(test)]
mod sampler_tests {
    use super::*;

    #[tokio::test]
    async fn silent_peer_does_not_block_sampling_or_shutdown() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });
        let config = format!(
            "host=127.0.0.1 port={} user=test sslmode=disable",
            address.port()
        )
        .parse()
        .unwrap();
        let mut sampler = ActivitySampler::default();
        let sample = tokio::time::timeout(Duration::from_secs(3), sampler.sample(&config))
            .await
            .unwrap();
        assert!(
            sample["error"]
                .as_str()
                .unwrap()
                .contains("exceeded 1 second")
        );
        assert!(sampler.connection.is_none());
        drop(sampler);
        peer.abort();
    }
}
