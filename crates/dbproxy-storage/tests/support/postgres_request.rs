//! 独立真实 PG：连接排队、执行取消以及 COMMIT 回包丢失的恢复契约。
//! Dedicated real PostgreSQL: queue expiry, execution cancellation and lost COMMIT replies.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tiangz_dbproxy_core::{
    AppendRecord, AsyncMultiRecordTransactionStore, AsyncSnapshotStore, CommitEffects,
    MultiRecordTransactionalWrite, MultiRecordTransactionalWriteOutcome, OutboxEvent, RecordKey,
    Revision, SnapshotWrite, SnapshotWriteOutcome, TransactionalRecordWrite,
};
use tiangz_dbproxy_storage::{
    CacheRepairAcknowledgements, PostgresRequestConfig, PostgresSnapshotStore, RedisSnapshotCache,
    STORAGE_LATENCY_BOUNDS_MS, StorageError, StorageMetrics, TieredSnapshotStore,
    TieredSnapshotStoreConfig,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::{sleep, timeout},
};

struct Peer(JoinHandle<()>);
impl Drop for Peer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn test_url() -> String {
    assert_eq!(
        std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(),
        Ok("1")
    );
    std::env::var("DBPROXY_TEST_POSTGRES_URL").expect("dedicated PostgreSQL test URL required")
}

fn unique() -> String {
    format!(
        "pg-request-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

async fn sql(url: &str) -> (tokio_postgres::Client, Peer) {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .unwrap();
    (
        client,
        Peer(tokio::spawn(async move {
            let _ = connection.await;
        })),
    )
}

fn write(id: &str) -> SnapshotWrite {
    SnapshotWrite {
        request_id: id.into(),
        record: RecordKey::new("pg-request", id).unwrap(),
        schema: "request.v1".into(),
        schema_version: 1,
        expected_revision: Some(Revision::ZERO),
        payload: b"durable".to_vec(),
        updated_at_unix_ms: 1,
    }
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL/Redis; deferred cleanup recovery and revision safety"]
async fn deferred_cache_cleanup_preserves_newer_targets_and_survives_lost_hints() {
    timeout(Duration::from_secs(30), async {
        let url = test_url();
        let cache_url = std::env::var("DBPROXY_CACHE_REDIS_URL").unwrap();
        let mut store = TieredSnapshotStore::connect(&url, &cache_url).await.unwrap();
        let hints = CacheRepairAcknowledgements::default();
        store.defer_cache_acknowledgements(hints.clone());
        let mut postgres = PostgresSnapshotStore::connect(&url).await.unwrap();
        let queue = postgres.cache_repair_queue();
        let cache = RedisSnapshotCache::connect(&cache_url).await.unwrap();
        let request = write(&unique());
        let key = request.record.clone();
        store.save(request.clone()).await.unwrap();
        assert_eq!(cache.get(&key).await.unwrap().unwrap().revision, Revision(1));
        let (observer, _peer) = sql(&url).await;
        let target = || async {
            observer.query_opt("SELECT target_revision FROM dbproxy_cache_repairs WHERE namespace=$1 AND record_key=$2", &[&key.namespace, &key.key]).await.unwrap().map(|r| r.get::<_, i64>(0))
        };
        assert_eq!(target().await, Some(1), "response leaves a durable recovery target");

        // 新提交尚未刷新缓存时，延迟到达的旧 ACK 绝不能删除新目标。
        // A delayed older ACK must not remove a newer commit whose cache is not refreshed.
        let mut newer = request.clone();
        newer.request_id.push_str("-newer");
        newer.expected_revision = Some(Revision(1));
        newer.payload = b"newer".to_vec();
        postgres.save(newer).await.unwrap();
        assert_eq!(hints.flush(&queue, &StorageMetrics::default()).await.unwrap(), 0);
        assert_eq!(target().await, Some(2));
        assert_eq!(store.repair_cache(&key).await.unwrap(), Some(Revision(2)));
        assert_eq!(cache.get(&key).await.unwrap().unwrap().revision, Revision(2));
        queue.acknowledge_cached(&key, Revision(2)).await.unwrap();
        assert_eq!(target().await, None);

        // 进程丢失未刷新的内存提示后，持久修复仍可完成；原请求重试不重复写。
        // Losing in-memory hints still permits durable repair; original-id retry is duplicate.
        let lost = write(&unique());
        store.save(lost.clone()).await.unwrap();
        drop(store);
        drop(hints);
        assert!(matches!(postgres.save(lost.clone()).await.unwrap(), SnapshotWriteOutcome::Duplicate { revision: Revision(1) }));
        let replacement = TieredSnapshotStore::connect(&url, &cache_url).await.unwrap();
        assert_eq!(replacement.repair_cache(&lost.record).await.unwrap(), Some(Revision(1)));
        assert!(queue.acknowledge_cached(&lost.record, Revision(1)).await.unwrap());
    }).await.expect("deferred cleanup recovery deadline");
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL/Redis; short healthy concurrency baseline"]
async fn healthy_request_shard_completes_concurrent_writes_with_default_policy() {
    timeout(Duration::from_secs(45), async {
        let url = test_url();
        let cache = std::env::var("DBPROXY_CACHE_REDIS_URL")
            .or_else(|_| std::env::var("DBPROXY_REDIS_URL"))
            .unwrap();
        let metrics = Arc::new(StorageMetrics::default());
        let store = TieredSnapshotStore::connect_with_config(
            &url,
            &cache,
            TieredSnapshotStoreConfig::default(),
            metrics.clone(),
        )
        .await
        .unwrap();
        let id = unique();
        let mut tasks = tokio::task::JoinSet::new();
        for worker in 0..16 {
            let mut store = store.clone();
            let id = id.clone();
            tasks.spawn(async move {
                for item in 0..4 {
                    assert!(matches!(
                        store
                            .save(write(&format!("{id}-{worker}-{item}")))
                            .await
                            .unwrap(),
                        SnapshotWriteOutcome::Applied {
                            revision: Revision(1)
                        }
                    ));
                }
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        let queue = metrics
            .latency_snapshot()
            .into_iter()
            .find(|s| s.stage == "postgres_connection_wait")
            .unwrap();
        let count: u64 = queue.buckets.iter().sum();
        assert_eq!(count, 64);
        assert_eq!(queue.in_flight, 0);
        let mut cumulative = 0;
        let p99 = queue
            .buckets
            .iter()
            .position(|n| {
                cumulative += n;
                cumulative * 100 >= count * 99
            })
            .unwrap();
        println!(
            "HEALTHY_PG_QUEUE samples={count} mean_us={} p99_upper_ms={:?}",
            queue.sum_micros / count,
            STORAGE_LATENCY_BOUNDS_MS.get(p99)
        );
        for sample in metrics.latency_snapshot() {
            println!(
                "BASELINE_STAGE {} sum_us={} buckets={:?}",
                sample.stage, sample.sum_micros, sample.buckets
            );
        }
    })
    .await
    .expect("short healthy request-shard baseline deadline");
}

async fn wait_for_sql_lock(monitor: &tokio_postgres::Client, application: &str) {
    timeout(Duration::from_secs(5), async {
        loop {
            let blocked: bool = monitor.query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND state='active')",
                &[&application]).await.unwrap().get(0);
            if blocked { return; }
            sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("server must observe the first write waiting on a SQL lock");
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL and migration opt-in; serial execution only"]
async fn queued_operations_expire_but_sent_transactions_complete_or_roll_back() {
    timeout(Duration::from_secs(45), async {
        let url = test_url();
        let (mut admin, _driver) = sql(&url).await;
        let (monitor, _monitor_driver) = sql(&url).await;
        for cancel in [false, true] {
            let id = unique();
            let separator = if url.contains('?') { '&' } else { '?' };
            let request_url = format!("{url}{separator}application_name={id}");
            let mut store = PostgresSnapshotStore::connect_with_request_config(&request_url, PostgresRequestConfig {
                connection_wait_timeout: Duration::from_millis(40), ..Default::default()
            }).await.unwrap();
            let blocker = admin.transaction().await.unwrap();
            blocker.batch_execute("LOCK TABLE dbproxy_snapshots IN ACCESS EXCLUSIVE MODE").await.unwrap();
            let first = write(&id);
            let mut writer = store.clone();
            let request = first.clone();
            let task = tokio::spawn(async move { writer.save(request).await });
            wait_for_sql_lock(&monitor, &id).await;
            let queued = write(&format!("{id}-queued"));
            let result = timeout(Duration::from_secs(1), store.save(queued.clone())).await;
            let ack = timeout(Duration::from_secs(1), store.cache_repair_queue().acknowledge_cached(&first.record, Revision(1))).await;
            // Server lock observation proves the first operation sent SQL and still owns the client.
            wait_for_sql_lock(&monitor, &id).await;
            if cancel { task.abort(); }
            blocker.commit().await.unwrap();
            assert!(matches!(result.unwrap(), Err(StorageError::PostgresConnectionWaitTimeout { timeout_ms: 40 })));
            assert!(matches!(ack.unwrap(), Err(StorageError::PostgresConnectionWaitTimeout { timeout_ms: 40 })));
            if cancel {
                assert!(task.await.unwrap_err().is_cancelled());
                assert!(store.load(&first.record).await.unwrap().is_none());
            } else {
                assert!(matches!(task.await.unwrap().unwrap(), SnapshotWriteOutcome::Applied { revision: Revision(1) }));
                assert_eq!(store.load(&first.record).await.unwrap().unwrap().revision, Revision(1));
            }
            assert!(store.load(&queued.record).await.unwrap().is_none(), "expired waiter sent no write");
            let pending: i64 = monitor.query_one("SELECT count(*) FROM dbproxy_cache_repairs WHERE namespace=$1 AND record_key=$2",
                &[&first.record.namespace, &first.record.key]).await.unwrap().get(0);
            assert_eq!(pending, i64::from(!cancel));
            assert!(matches!(store.save(queued).await.unwrap(), SnapshotWriteOutcome::Applied { revision: Revision(1) }));
        }
    }).await.expect("real PostgreSQL queue/cancellation test deadline");
}

/// 转发真实 PG 协议，只在服务器已返回 COMMIT 成功时丢弃一次回包。
/// Forward real PostgreSQL, dropping one response only after the server confirms COMMIT.
async fn commit_reply_proxy(url: &str) -> (String, Arc<AtomicBool>, Peer) {
    let config: tokio_postgres::Config = url.parse().unwrap();
    let host = match &config.get_hosts()[0] {
        tokio_postgres::config::Host::Tcp(host) => host.clone(),
        #[cfg(unix)]
        tokio_postgres::config::Host::Unix(_) => panic!("test proxy requires a TCP PostgreSQL URL"),
    };
    let port = config.get_ports().first().copied().unwrap_or(5432);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (credentials, suffix) = url
        .rsplit_once('@')
        .expect("test URL must contain user credentials");
    let (_, database) = suffix.split_once('/').unwrap();
    let proxy_url = format!(
        "{credentials}@{}/{database}",
        listener.local_addr().unwrap()
    );
    let armed = Arc::new(AtomicBool::new(false));
    let drop_reply = armed.clone();
    let peer = Peer(tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                incoming = listener.accept() => {
                    let (client, _) = incoming.unwrap();
                    let upstream = TcpStream::connect((host.as_str(), port)).await.unwrap();
                    let armed = drop_reply.clone();
                    connections.spawn(async move {
                        let (mut from_client, mut to_client) = client.into_split();
                        let (mut from_db, mut to_db) = upstream.into_split();
                        tokio::select! {
                            _ = tokio::io::copy(&mut from_client, &mut to_db) => {},
                            _ = async {
                                loop {
                                    let kind = from_db.read_u8().await?;
                                    let length = from_db.read_u32().await?;
                                    if !(4..=32 * 1024 * 1024).contains(&length) { return Err(std::io::Error::other("invalid PG frame")); }
                                    let mut body = vec![0; length as usize - 4];
                                    from_db.read_exact(&mut body).await?;
                                    if kind == b'C' && body == b"COMMIT\0" && armed.swap(false, Ordering::SeqCst) {
                                        return Ok::<_, std::io::Error>(());
                                    }
                                    to_client.write_u8(kind).await?;
                                    to_client.write_u32(length).await?;
                                    to_client.write_all(&body).await?;
                                }
                            } => {},
                        }
                    });
                },
                _ = connections.join_next(), if !connections.is_empty() => {},
            }
        }
    }));
    (proxy_url, armed, peer)
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL and migration opt-in; real COMMIT response loss"]
async fn lost_commit_reply_recovers_the_same_receipt_and_exactly_once_effects() {
    timeout(Duration::from_secs(45), async {
        let url = test_url();
        let (proxy_url, armed, _proxy) = commit_reply_proxy(&url).await;
        let mut store = PostgresSnapshotStore::connect_with_request_config(
            &proxy_url,
            PostgresRequestConfig::default(),
        )
        .await
        .unwrap();
        let (monitor, _driver) = sql(&url).await;
        let id = unique();
        let record = RecordKey::new("pg-request", &id).unwrap();
        let request = MultiRecordTransactionalWrite {
            operation_id: id.clone(),
            result: b"receipt".to_vec(),
            writes: vec![TransactionalRecordWrite {
                record: record.clone(),
                schema: "request.v1".into(),
                schema_version: 1,
                expected_revision: Revision::ZERO,
                payload: b"state".to_vec(),
                updated_at_unix_ms: 1,
            }],
        };
        let effects = CommitEffects {
            appends: vec![AppendRecord {
                record: RecordKey::new("pg-request-fact", &id).unwrap(),
                schema: "fact.v1".into(),
                schema_version: 1,
                payload: b"fact".to_vec(),
                occurred_at_unix_ms: 1,
            }],
            outbox_events: vec![OutboxEvent {
                event_id: id.clone(),
                topic: "pg.request.changed".into(),
                partition_key: id.clone(),
                payload: b"event".to_vec(),
                occurred_at_unix_ms: 1,
            }],
        };
        armed.store(true, Ordering::SeqCst);
        assert!(matches!(
            store.commit_records(request.clone(), effects.clone()).await,
            Err(StorageError::Postgres(_))
        ));
        assert!(
            !armed.load(Ordering::SeqCst),
            "server must have committed before reply loss"
        );
        let receipt = store
            .load_multi_receipt(&id, std::slice::from_ref(&record))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipt.result, b"receipt");
        assert_eq!(receipt.records[0].new_revision, Revision(1));
        assert!(matches!(
            store.commit_records(request, effects).await.unwrap(),
            MultiRecordTransactionalWriteOutcome::Duplicate { .. }
        ));
        assert_eq!(
            store.load(&record).await.unwrap().unwrap().revision,
            Revision(1)
        );
        for table in [
            "dbproxy_multi_transactions",
            "dbproxy_append_records",
            "dbproxy_outbox",
        ] {
            let count: i64 = monitor
                .query_one(
                    &format!("SELECT count(*) FROM {table} WHERE operation_id=$1"),
                    &[&id],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(
                count, 1,
                "{table} must contain exactly one committed effect"
            );
        }
    })
    .await
    .expect("real PostgreSQL lost COMMIT reply test deadline");
}
