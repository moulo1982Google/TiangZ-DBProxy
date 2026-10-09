//! Real production repair path: PG claim, authoritative read, Redis update and PG acknowledgement.
use super::*;
use tiangz_dbproxy_core::AsyncSnapshotStore;
use tiangz_dbproxy_server::{
    DurableQueueProcessOutcome, RetryWorkerPolicy, StorageBackend, StorageBackendConfig,
};
use tiangz_dbproxy_storage::{PostgresSnapshotStore, RedisSnapshotCache};

#[tokio::test]
#[ignore = "P06: new PG database, real Redis repair with half locked rows and concurrent hot updates"]
async fn p06_repairs_reach_redis_and_preserve_latest_revision() {
    let env = env();
    let db = format!("{}_p06", env.run_id);
    let owner: String = sql(&format!("{}/postgres", env.admin_base))
        .await
        .query_one("SELECT current_user::text", &[])
        .await
        .unwrap()
        .get(0);
    create_database(&env.admin_base, &db, &owner).await;
    let url = format!("{}/{db}", env.admin_base);
    let backend = StorageBackend::connect_with_redis_urls(
        &url,
        &env.redis[0],
        &env.cache[0],
        StorageBackendConfig {
            shard_count: 2,
            read_connection_count: 2,
            tiered: Default::default(),
            enqueue: Default::default(),
        },
    )
    .await
    .unwrap();
    let mut store = PostgresSnapshotStore::connect(&url).await.unwrap();
    let queue = store.cache_repair_queue();
    let cache = RedisSnapshotCache::connect(&env.cache[0]).await.unwrap();
    let namespace = format!("repair-{}", env.run_id);
    let mut requests = Vec::new();
    for n in 0..256 {
        let original = write(&namespace, &format!("key-{n:03}"), 1);
        store.save(original.clone()).await.unwrap();
        cache
            .put(&store.load(&original.record).await.unwrap().unwrap())
            .await
            .unwrap();
        let mut newer = original;
        newer.request_id.push_str("-newer");
        newer.expected_revision = Some(Revision(1));
        newer.payload = vec![2, n as u8];
        store.save(newer.clone()).await.unwrap();
        queue.enqueue(&newer.record, Revision(2)).await.unwrap();
        requests.push(newer);
    }
    let mut admin = sql(&url).await;
    admin.execute("UPDATE dbproxy_cache_repairs SET requested_at='2020-01-01'::timestamptz,available_at='2020-01-01'::timestamptz WHERE namespace=$1",&[&namespace]).await.unwrap();
    let lock = admin.transaction().await.unwrap();
    assert_eq!(lock.query("SELECT record_key FROM dbproxy_cache_repairs WHERE namespace=$1 AND record_key<'key-128' FOR UPDATE",&[&namespace]).await.unwrap().len(),128);
    let policy = RetryWorkerPolicy {
        lease_ms: 30000,
        base_retry_delay_ms: 1000,
        max_retry_delay_ms: 60000,
        max_attempts: 20,
    };
    let mut durations = Vec::new();
    for _ in 0..128 {
        let started = Instant::now();
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(5),
                backend.process_cache_repair_once("p06", policy)
            )
            .await
            .unwrap()
            .unwrap(),
            DurableQueueProcessOutcome::Committed
        );
        durations.push(started.elapsed().as_secs_f64());
    }
    assert_eq!(
        backend
            .process_cache_repair_once("p06", policy)
            .await
            .unwrap(),
        DurableQueueProcessOutcome::Empty
    );
    for (n, request) in requests.iter().enumerate() {
        let value = cache.get(&request.record).await.unwrap().unwrap();
        assert_eq!(value.revision, Revision(if n < 128 { 1 } else { 2 }));
        assert_eq!(
            value.payload,
            if n < 128 {
                vec![1]
            } else {
                request.payload.clone()
            }
        );
    }
    lock.rollback().await.unwrap();
    for _ in 0..128 {
        let started = Instant::now();
        assert_eq!(
            backend
                .process_cache_repair_once("p06", policy)
                .await
                .unwrap(),
            DurableQueueProcessOutcome::Committed
        );
        durations.push(started.elapsed().as_secs_f64());
    }
    assert_eq!(
        count(&admin, "SELECT count(*) FROM dbproxy_cache_repairs").await,
        0
    );
    for request in &requests {
        assert_eq!(
            cache.get(&request.record).await.unwrap().unwrap(),
            store.load(&request.record).await.unwrap().unwrap()
        );
    }
    // A repair target for a deleted authority must remove the old Redis payload, not resurrect it.
    let removed = &requests[0].record;
    admin
        .execute(
            "DELETE FROM dbproxy_snapshots WHERE namespace=$1 AND record_key=$2",
            &[&removed.namespace, &removed.key],
        )
        .await
        .unwrap();
    queue.enqueue(removed, Revision(3)).await.unwrap();
    assert_eq!(
        backend
            .process_cache_repair_once("p06", policy)
            .await
            .unwrap(),
        DurableQueueProcessOutcome::Committed
    );
    assert!(cache.get(removed).await.unwrap().is_none());
    let hot = requests[1].clone();
    let started = Instant::now();
    let producer = async {
        for n in 0..64u64 {
            let mut write = hot.clone();
            write.request_id = format!("{}-hot-{n}", env.run_id);
            write.expected_revision = Some(Revision(n + 2));
            write.payload = vec![3, n as u8];
            store.save(write.clone()).await.unwrap();
            queue.enqueue(&write.record, Revision(n + 3)).await.unwrap();
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    };
    let consumer = async {
        let mut committed = 0;
        for _ in 0..100 {
            let outcome = backend
                .process_cache_repair_once("p06-hot", policy)
                .await
                .unwrap();
            assert!(matches!(
                outcome,
                DurableQueueProcessOutcome::Committed
                    | DurableQueueProcessOutcome::Empty
                    | DurableQueueProcessOutcome::LeaseLost
            ));
            if outcome == DurableQueueProcessOutcome::Committed {
                committed += 1;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        committed
    };
    let (_, concurrent_commits) = tokio::join!(producer, consumer);
    for _ in 0..3 {
        if backend
            .process_cache_repair_once("p06-final", policy)
            .await
            .unwrap()
            == DurableQueueProcessOutcome::Empty
        {
            break;
        }
    }
    assert_eq!(
        count(&admin, "SELECT count(*) FROM dbproxy_cache_repairs").await,
        0
    );
    let cached = cache.get(&hot.record).await.unwrap().unwrap();
    assert_eq!(cached.revision, Revision(66));
    assert_eq!(cached.payload, vec![3, 63]);
    assert_eq!(cached, store.load(&hot.record).await.unwrap().unwrap());
    durations.sort_by(f64::total_cmp);
    let result = serde_json::json!({"rows":256,"locked_rows":128,"repair_p50_ms":durations[127]*1000.0,"repair_p99_ms":durations[253]*1000.0,"repair_max_ms":durations[255]*1000.0,"missing_cache_removed":true,"hot_updates":64,"hot_concurrent_commits":concurrent_commits,"hot_seconds":started.elapsed().as_secs_f64(),"final_revision":66,"queue_remaining":0,"scope":"actual production repair method, finite functional/cost sample; not timed application capacity"});
    let dir = env.artifacts.join("p06-e2e");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("result.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    std::fs::write(
        dir.join("repair-seconds.json"),
        serde_json::to_vec(&durations).unwrap(),
    )
    .unwrap();
    println!("P06_E2E_RESULT {result}");
}
