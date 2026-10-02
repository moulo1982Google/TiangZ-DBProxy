use super::*;
#[path = "../tests/support/diagnostic_trace.rs"]
mod diagnostic_trace;
use tiangz_dbproxy_core::{AsyncSnapshotStore, RecordKey};
#[test]
fn bounded_waiters_survive_cancellation_and_overflow() {
    let diagnostics = Diagnostics::default();
    let waiters: Vec<_> = (0..140)
        .map(|_| diagnostics.wait("load", None, 1))
        .collect();
    let view = diagnostics.inspect();
    assert_eq!(view["waiter_count"], 140);
    assert_eq!(view["untracked_waiters"], 12);
    drop(waiters);
    assert_eq!(diagnostics.inspect()["waiter_count"], 0);
}

#[tokio::test]
async fn cancelled_waiter_never_becomes_a_timeout_or_holder() {
    let (store, _arrived, _peer) =
        crate::latency_path_tests::postgres(Arc::new(crate::StorageMetrics::default())).await;
    let holder = store
        .request_client("controlled_holder", Some("private-id"), 4)
        .await
        .unwrap();
    let mut future = Box::pin(store.request_client("load", None, 1));
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(future.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    assert_eq!(store.client.diagnostics.inspect()["waiter_count"], 1);
    drop(future);
    let evidence = store.client.diagnostics.inspect();
    assert_eq!(evidence["waiter_count"], 0);
    assert_eq!(
        evidence["holder"]["request"]["operation"],
        "controlled_holder"
    );
    assert_eq!(
        store
            .metrics
            .latency_snapshot()
            .iter()
            .find(|s| s.stage == "postgres_connection_wait")
            .unwrap()
            .timeouts,
        0
    );
    drop(holder);
    for _ in 0..20 {
        drop(store.request_client("load", None, 1).await.unwrap());
    }
    assert_eq!(
        store.client.diagnostics.inspect()["recent_holds"]
            .as_array()
            .unwrap()
            .len(),
        HISTORY
    );
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL and schema migration opt-in"]
async fn real_postgres_identity_reconnect_and_contention() {
    tokio::time::timeout(Duration::from_secs(30), async {
        assert_eq!(std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(), Ok("1"));
        let trace = diagnostic_trace::DiagnosticTrace::default();
        let _tracing = tracing::subscriber::set_default(trace.clone());
        let _dump = diagnostic_trace::TraceOnDrop(trace.clone());
        let url = std::env::var("DBPROXY_TEST_POSTGRES_URL").unwrap();
        let store = crate::PostgresSnapshotStore::connect_with_request_config(&url, crate::PostgresRequestConfig {
            connection_wait_timeout: Duration::from_millis(40), reconnect_cooldown: Duration::from_millis(50) }).await.unwrap();
        store.identify_connection(Some(3)).await.unwrap();
        let (monitor, driver) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await.unwrap();
        let driver = tokio::spawn(driver);
        let before = store.client.diagnostics.inspect();
        let pid = before["backend_pid"].as_i64().unwrap() as i32;
        let application: String = monitor.query_one("SELECT application_name FROM pg_stat_activity WHERE pid=$1", &[&pid]).await.unwrap().get(0);
        assert_eq!(application, format!("tzdb:{}:request:3", std::process::id()));
        let holder_store = store.clone();
        let holder = tokio::spawn(async move {
            let client = holder_store.request_client("backlog_save_batch", Some("controlled-operation"), 100).await.unwrap();
            client.simple_query("SELECT pg_sleep(0.6)").await.unwrap();
        });
        loop {
            let sleeping: bool = monitor.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event='PgSleep')", &[&pid]).await.unwrap().get(0);
            if sleeping { break; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let record = RecordKey::new("diagnostics", "missing").unwrap();
        assert!(matches!(store.load(&record).await, Err(StorageError::PostgresConnectionWaitTimeout { timeout_ms: 40 })));
        let blocked = store.client.diagnostics.inspect();
        assert_eq!(blocked["holder"]["request"]["operation"], "backlog_save_batch");
        assert_eq!(blocked["holder"]["request"]["batch_size"], 100);
        assert_eq!(blocked["backend_pid"], pid);
        assert_eq!(blocked["waiter_count"], 0);
        assert_eq!(store.metrics.latency_snapshot().iter().find(|s| s.stage=="postgres_connection_wait").unwrap().timeouts, 1);
        holder.await.unwrap();
        let released = store.client.diagnostics.inspect();
        assert!(released["recent_holds"].as_array().unwrap().iter().any(|h| h["request"]["operation"]=="backlog_save_batch" && h["held_ms"].as_u64().unwrap()>=600));
        monitor.query_one("SELECT pg_terminate_backend($1)", &[&pid]).await.unwrap();
        while !store.client.lock().await.is_closed() { tokio::task::yield_now().await; }
        assert!(store.load(&record).await.unwrap().is_none());
        let after = store.client.diagnostics.inspect();
        assert_eq!(after["connection_generation"], 2);
        let new_pid = after["backend_pid"].as_i64().unwrap() as i32;
        assert_ne!(pid, new_pid);
        let new_application: String = monitor.query_one("SELECT application_name FROM pg_stat_activity WHERE pid=$1", &[&new_pid]).await.unwrap().get(0);
        assert_eq!(new_application, application);
        let actual_logs = trace.0.lock().unwrap().clone();
        let queued_log = actual_logs.iter().find(|e| e["reason"] == "queue_timeout").expect("actual queue warning");
        let queued_context: serde_json::Value = serde_json::from_str(queued_log["context"].as_str().unwrap()).unwrap();
        assert_eq!(queued_context["backend_pid"], pid);
        assert_eq!(queued_context["holder"]["request"]["operation"], "backlog_save_batch");
        let released_log = actual_logs.iter().filter(|e| e["reason"] == "slow_hold_released").find(|e| {
            let context: serde_json::Value = serde_json::from_str(e["context"].as_str().unwrap()).unwrap();
            context["recent_holds"].as_array().unwrap().iter().any(|h| h["request"]["operation"] == "backlog_save_batch" && h["held_ms"].as_u64().unwrap() >= 600)
        }).expect("actual full-duration release warning cannot be consumed by startup logging");
        println!("PG_DIAGNOSTICS_ACTUAL_LOGS {}", serde_json::json!([queued_log, released_log]));
        println!("PG_DIAGNOSTICS_REAL {}", serde_json::json!({"blocked":blocked,"released":released,"reconnected":after}));
        driver.abort();
    }).await.expect("real diagnostic fixture deadline");
}
