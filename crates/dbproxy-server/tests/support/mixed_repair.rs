//! Bounded, unique repair targets alongside the normal mixed application workload.
use super::*;
use tiangz_dbproxy_core::AsyncSnapshotStore;
use tiangz_dbproxy_storage::{PostgresSnapshotStore, RedisSnapshotCache, SnapshotCacheConfig};

pub(super) async fn prepare(
    url: &str,
    cache_url: &str,
    run: &str,
    seconds: u64,
    mode: &str,
) -> (
    u64,
    Option<tokio::task::JoinHandle<Value>>,
    Option<tokio::sync::oneshot::Sender<()>>,
) {
    if mode == "none" {
        return (0, None, None);
    }
    assert!(mode == "control" || mode == "repair");
    let mut store = PostgresSnapshotStore::connect(url).await.unwrap();
    // Match the explicit experimental server policy: fixture entries must outlive
    // the seven-minute sample and final reconciliation, not expire at five minutes.
    let cache = RedisSnapshotCache::connect_with_metrics_and_policy(
        cache_url,
        Default::default(),
        SnapshotCacheConfig {
            ttl: Duration::from_secs(1800),
            ttl_jitter: Duration::ZERO,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let queue = store.cache_repair_queue();
    let namespace = format!("mixed-repair-{run}");
    let count = seconds * 2;
    let mut records = Vec::new();
    for n in 0..count {
        let mut w = write(&namespace, &format!("r-{n}"), 1);
        store.save(w.clone()).await.unwrap();
        cache
            .put(&store.load(&w.record).await.unwrap().unwrap())
            .await
            .unwrap();
        w.request_id.push_str("-new");
        w.expected_revision = Some(Revision(1));
        w.payload = vec![2; 1024];
        store.save(w.clone()).await.unwrap();
        if mode == "control" {
            cache
                .put(&store.load(&w.record).await.unwrap().unwrap())
                .await
                .unwrap();
        }
        records.push(w.record);
    }
    let mode = mode.to_string();
    let pg = sql(url).await;
    let (tx, rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        rx.await.unwrap();
        let started = tokio::time::Instant::now();
        let mut observations = Vec::new();
        for (n, record) in records.iter().enumerate() {
            tokio::time::sleep_until(started + Duration::from_millis(n as u64 * 500)).await;
            let sent = Instant::now();
            if mode == "repair" {
                queue.enqueue(record, Revision(2)).await.unwrap();
            }
            let pending: i64 = pg
                .query_one(
                    "SELECT count(*) FROM dbproxy_cache_repairs WHERE namespace=$1",
                    &[&namespace],
                )
                .await
                .unwrap()
                .get(0);
            observations.push(json!({"n":n,"elapsed_ms":started.elapsed().as_millis(),"enqueue_and_probe_us":sent.elapsed().as_micros(),"pending":pending}));
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        let remaining = loop {
            let pending: i64 = pg
                .query_one(
                    "SELECT count(*) FROM dbproxy_cache_repairs WHERE namespace=$1",
                    &[&namespace],
                )
                .await
                .unwrap()
                .get(0);
            if pending == 0 || Instant::now() >= deadline {
                break pending;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        let mut mismatches = 0;
        for record in &records {
            let authoritative = store.load(record).await.unwrap().unwrap();
            mismatches +=
                u64::from(cache.get(record).await.unwrap().as_ref() != Some(&authoritative));
        }
        json!({"mode":mode,"targets":count,"remaining":remaining,"mismatches":mismatches,"seconds":started.elapsed().as_secs_f64(),"observations":observations})
    });
    (count, Some(task), Some(tx))
}
