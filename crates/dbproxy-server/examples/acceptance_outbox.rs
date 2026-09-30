//! Test-only supplemental stats on the actual backend maintenance connection.
#[path = "../src/server_process.rs"]
#[allow(dead_code)]
mod server_process;
use std::{io::Write, sync::Arc, time::Duration};
use tiangz_dbproxy_server::StorageBackend;
use tokio::sync::watch;

fn probe(
    backend: Arc<StorageBackend>,
    mut shutdown: watch::Receiver<bool>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    Box::pin(async move {
        let enabled = std::env::var("MIX_OUTBOX_STATS").unwrap() == "on";
        let path = std::env::var("MIX_OUTBOX_STATS_LOG").unwrap();
        let mut file = std::fs::File::create(path).unwrap();
        let start = tokio::time::Instant::now();
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = interval.tick() => {},
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
    let mode = std::env::var("MIX_OUTBOX_STATS")?;
    if mode != "on" && mode != "off" {
        return Err("stats mode must be on/off".into());
    }
    server_process::main_with_acceptance_worker(true, Some(probe))
}
