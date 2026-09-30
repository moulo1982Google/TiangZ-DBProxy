//! Test-only supplemental stats on the actual backend maintenance connection.
#[path = "../src/server_process.rs"]
#[allow(dead_code)]
mod server_process;
use std::{io::Write, sync::Arc, time::Duration};
use tiangz_dbproxy_server::{DbProxyMetrics, StorageBackend};
use tokio::sync::watch;

fn probe(
    backend: Arc<StorageBackend>,
    metrics: Arc<DbProxyMetrics>,
    mut shutdown: watch::Receiver<bool>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    Box::pin(async move {
        let enabled = std::env::var("MIX_OUTBOX_STATS").unwrap() == "on";
        let path = std::env::var("MIX_OUTBOX_STATS_LOG").unwrap();
        let mut file = std::fs::File::create(path).unwrap();
        let mut stages = (std::env::var("MIX_STAGE_AUDIT").as_deref() == Ok("1"))
            .then(|| std::fs::File::create(std::env::var("MIX_STAGE_LOG").unwrap()).unwrap());
        let start = tokio::time::Instant::now();
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = interval.tick() => {},
            }
            if let Some(stages) = &mut stages {
                // Read existing in-memory counters only; no extra SQL, worker or metrics poll.
                let captured = tokio::time::Instant::now();
                let unix_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis();
                let elapsed_us = start.elapsed().as_micros();
                let storage: Vec<_> = backend
                    .metrics()
                    .latency_snapshot()
                    .into_iter()
                    .map(|s| serde_json::json!({"stage":s.stage,"buckets":s.buckets,"sum_us":s.sum_micros,"in_flight":s.in_flight}))
                    .collect();
                let requests = metrics.request_stage_snapshot();
                let read_pool = backend
                    .read_pool_usage()
                    .map(|u| serde_json::json!({"capacity":u.capacity,"in_use":u.in_use}));
                let row = serde_json::json!({
                    "kind":"stage_snapshot", "schema_version":1, "process_id":std::process::id(),
                    "unix_ms":unix_ms, "elapsed_us":elapsed_us,
                    "capture_us":captured.elapsed().as_micros(),
                    "storage_bounds_us":tiangz_dbproxy_storage::STORAGE_LATENCY_BOUNDS_MS.map(|ms| ms * 1_000),
                    "storage_stages":storage, "request_stages":requests, "read_pool":read_pool
                });
                writeln!(stages, "{row}").unwrap();
                stages.flush().unwrap();
            }
            let call = tokio::time::Instant::now();
            let result = if enabled {
                match tokio::time::timeout(Duration::from_secs(5), backend.outbox_stats()).await {
                    Ok(Ok(s)) => {
                        serde_json::json!({"pending":s.pending,"processing":s.processing,"dead_lettered":s.dead_lettered})
                    }
                    other => serde_json::json!({"error":format!("{other:?}")}),
                }
            } else {
                serde_json::Value::Null
            };
            writeln!(file,"{}",serde_json::json!({"unix_ms":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis(),"elapsed_ms":start.elapsed().as_millis(),"enabled":enabled,"call_us":call.elapsed().as_micros(),"result":result})).unwrap();
            file.flush().unwrap();
        }
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref() != Ok("1") {
        return Err("acceptance requires explicit test opt-in".into());
    }
    if std::env::var("MIX_TX_AUDIT").as_deref() == Ok("1") {
        #[cfg(feature = "acceptance-trace")]
        tiangz_dbproxy_storage::acceptance_trace::enable();
        #[cfg(not(feature = "acceptance-trace"))]
        return Err("transaction audit requires acceptance-trace build".into());
    }
    let mode = std::env::var("MIX_OUTBOX_STATS")?;
    if mode != "on" && mode != "off" {
        return Err("stats mode must be on/off".into());
    }
    server_process::main_with_acceptance_worker(true, Some(probe))
}
