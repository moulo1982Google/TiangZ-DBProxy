use super::*;
use std::{future::Future, task::Poll};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::oneshot,
    task::JoinHandle,
};

struct Peer(JoinHandle<()>);
impl Drop for Peer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

// Minimal private PostgreSQL wire peer: authenticate, signal the first command, then stall.
// No real database, schema migration or SQL execution is involved.
async fn postgres(
    metrics: Arc<StorageMetrics>,
) -> (PostgresSnapshotStore, oneshot::Receiver<()>, Peer) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "postgresql://test@{}/test?sslmode=disable",
        listener.local_addr().unwrap()
    );
    let (sent, arrived) = oneshot::channel();
    let peer = Peer(tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let length = stream.read_u32().await.unwrap();
        assert!((8..4096).contains(&length));
        let mut startup = vec![0; length as usize - 4];
        stream.read_exact(&mut startup).await.unwrap();
        assert_eq!(&startup[..4], &[0, 3, 0, 0]);
        // AuthenticationOk followed by ReadyForQuery (idle).
        stream
            .write_all(b"R\0\0\0\x08\0\0\0\0Z\0\0\0\x05I")
            .await
            .unwrap();
        stream.read_u8().await.unwrap();
        let _ = sent.send(());
        std::future::pending::<()>().await;
    }));
    let mut store = PostgresSnapshotStore::connect_existing(&url).await.unwrap();
    store.metrics = metrics;
    (store, arrived, peer)
}

async fn cache(metrics: Arc<StorageMetrics>, fail: bool) -> (RedisSnapshotCache, Peer) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("redis://{}/", listener.local_addr().unwrap());
    let peer = Peer(tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).await.unwrap() == 0 {
                break;
            }
            let count: usize = line.trim().strip_prefix('*').unwrap().parse().unwrap();
            assert!(count <= 128);
            let mut args = Vec::new();
            for _ in 0..count {
                line.clear();
                reader.read_line(&mut line).await.unwrap();
                let size: usize = line.trim().strip_prefix('$').unwrap().parse().unwrap();
                assert!(size <= 65536);
                let mut value = vec![0; size + 2];
                reader.read_exact(&mut value).await.unwrap();
                value.truncate(size);
                args.push(value);
            }
            let script = matches!(args[0].as_slice(), b"EVAL" | b"EVALSHA");
            let response: &[u8] = if script && fail {
                b"-ERR injected cache failure\r\n"
            } else if script {
                b":1\r\n"
            } else {
                b"+OK\r\n"
            };
            writer.write_all(response).await.unwrap();
        }
    }));
    let cache = RedisSnapshotCache::connect_with_metrics(&url, metrics)
        .await
        .unwrap();
    (cache, peer)
}

fn stage(metrics: &StorageMetrics, name: &str) -> StorageStageSnapshot {
    metrics
        .latency_snapshot()
        .into_iter()
        .find(|sample| sample.stage == name)
        .unwrap()
}

fn count(metrics: &StorageMetrics, name: &str) -> u64 {
    stage(metrics, name).buckets.iter().sum()
}

async fn tiered(fail_cache: bool) -> (TieredSnapshotStore, oneshot::Receiver<()>, Peer, Peer) {
    let metrics = Arc::new(StorageMetrics::default());
    let (postgres, arrived, pg_peer) = postgres(metrics.clone()).await;
    let (cache, cache_peer) = cache(metrics.clone(), fail_cache).await;
    let mut coordinator = CacheReadCoordinator::new_with_circuit_and_lock(
        CacheFallbackConfig::default(),
        CacheFallbackCircuitConfig::default(),
        CacheFallbackLockConfig::default(),
    )
    .unwrap();
    coordinator.metrics = metrics.clone();
    (
        TieredSnapshotStore {
            postgres,
            cache,
            read_coordinator: Arc::new(coordinator),
            metrics,
            refreshing: Arc::new(StdMutex::new(HashSet::new())),
        },
        arrived,
        pg_peer,
        cache_peer,
    )
}

fn snapshot() -> SnapshotEnvelope {
    SnapshotEnvelope {
        record: RecordKey::new("timing", "one").unwrap(),
        schema: "timing.v1".into(),
        schema_version: 1,
        revision: Revision(1),
        payload: vec![1],
        updated_at_unix_ms: 1,
    }
}

// 固定住缓存连接锁，验证预算覆盖排队，而不是仅覆盖已经发出的 Redis 命令。
// Hold the cache mutex: the independent budget must include queueing, not just network I/O.
#[tokio::test]
async fn cache_queue_timeouts_use_cache_budget_without_attempting_repair_ack() {
    timeout(Duration::from_secs(1), async {
        let (mut store, mut arrived, _pg, _redis) = tiered(false).await;
        let coordinator = Arc::get_mut(&mut store.read_coordinator).unwrap();
        coordinator.cache_operation_timeout = Duration::from_millis(20);
        assert_eq!(coordinator.timeout(), Duration::from_secs(2));
        let held = store.cache.connection.lock().await;
        let snapshot = snapshot();
        let record = &snapshot.record;
        let records = [record.clone()];
        for result in [
            store.lookup_cache(record, true).await.map(|_| ()),
            store.lookup_cache_multi(&records, true).await.map(|_| ()),
            store.put_cache(&snapshot).await,
            store.put_cache_multi(std::slice::from_ref(&snapshot)).await,
            store.put_negative_cache(record, None).await,
            store.delete_cache(record).await,
        ] {
            assert!(matches!(
                result,
                Err(StorageError::CacheOperationTimeout { timeout_ms: 20, .. })
            ));
        }
        store.synchronize_committed_cache(&snapshot).await;
        store
            .synchronize_committed_cache_multi(std::slice::from_ref(&snapshot))
            .await;
        assert_eq!(count(&store.metrics, "cache_repair_ack"), 0);
        assert_eq!(count(&store.metrics, "committed_cache_sync"), 2);
        assert!(matches!(
            store.acquire_fallback_lock(record).await,
            CacheFallbackLockOutcome::Unavailable
        ));
        store
            .release_fallback_lock(CacheFallbackLock {
                key: RedisSnapshotCache::fallback_lock_key(record),
                token: "test".into(),
            })
            .await;
        assert_eq!(store.metrics.snapshot().cache_fallback_lock_errors, 1);
        assert_eq!(
            store.metrics.snapshot().cache_fallback_lock_release_errors,
            1
        );
        assert!(matches!(
            arrived.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(
            store
                .metrics
                .latency_snapshot()
                .iter()
                .all(|s| s.in_flight == 0)
        );
        drop(held);
    })
    .await
    .expect("cache queue must not wait for the 2-second PG budget");
}

#[tokio::test]
async fn shorter_cache_budget_does_not_cancel_pg_fallback() {
    timeout(Duration::from_secs(1), async {
        let (mut store, arrived, _pg, _redis) = tiered(false).await;
        Arc::get_mut(&mut store.read_coordinator)
            .unwrap()
            .cache_operation_timeout = Duration::from_millis(20);
        let record = snapshot().record;
        let mut load = Box::pin(store.load(&record));
        tokio::select! {
            result = &mut load => panic!("PG fixture must stall: {result:?}"),
            arrival = arrived => arrival.unwrap(),
        }
        assert!(timeout(Duration::from_millis(80), &mut load).await.is_err());
        assert_eq!(stage(&store.metrics, "postgres_operation").in_flight, 1);
        drop(load);
    })
    .await
    .expect("PG fallback retains its own budget");
}

#[tokio::test]
async fn invalid_cache_budget_is_rejected_before_connecting() {
    for budget in [Duration::ZERO, Duration::from_micros(999)] {
        let result = TieredSnapshotStore::connect_with_config(
            "invalid-pg",
            "invalid-redis",
            TieredSnapshotStoreConfig {
                cache_operation_timeout: budget,
                ..Default::default()
            },
            Arc::new(StorageMetrics::default()),
        )
        .await;
        assert!(matches!(
            result,
            Err(StorageError::InvalidCacheOperationTimeout)
        ));
    }
    let defaults = TieredSnapshotStoreConfig::default();
    assert_eq!(defaults.cache_operation_timeout, Duration::from_millis(200));
    assert_eq!(defaults.fallback.timeout, Duration::from_secs(2));
}

#[tokio::test]
async fn postgres_mutex_wait_and_cancelled_query_are_distinct_real_call_scopes() {
    timeout(Duration::from_secs(5), async {
        let metrics = Arc::new(StorageMetrics::default());
        let (store, arrived, _peer) = postgres(metrics.clone()).await;
        let record = RecordKey::new("timing", "one").unwrap();
        let held = store.client.lock().await;
        let mut queued = Box::pin(store.load(&record));
        std::future::poll_fn(|cx| {
            assert!(queued.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_eq!(stage(&metrics, "postgres_connection_wait").in_flight, 1);
        assert_eq!(count(&metrics, "postgres_operation"), 0);
        drop(queued);
        assert_eq!(count(&metrics, "postgres_connection_wait"), 1);
        assert_eq!(stage(&metrics, "postgres_connection_wait").in_flight, 0);
        drop(held);

        let mut query = Box::pin(store.load(&record));
        tokio::select! {
            _ = &mut query => panic!("fixture must not complete a query"),
            arrival = arrived => arrival.unwrap(),
        }
        assert_eq!(count(&metrics, "postgres_connection_wait"), 2);
        assert_eq!(stage(&metrics, "postgres_operation").in_flight, 1);
        sleep(Duration::from_millis(20)).await;
        drop(query);
        assert_eq!(stage(&metrics, "postgres_operation").in_flight, 0);
        assert_eq!(count(&metrics, "postgres_operation"), 1);
        assert!(stage(&metrics, "postgres_operation").sum_micros >= 20_000);
    })
    .await
    .expect("PostgreSQL timing fixture deadline");
}

#[tokio::test]
async fn cache_failure_records_sync_but_does_not_attempt_repair_ack() {
    timeout(Duration::from_secs(5), async {
        let (store, mut arrived, _pg, _redis) = tiered(true).await;
        let snapshot = snapshot();
        let record = snapshot.record.clone();
        store.synchronize_committed_cache(&snapshot).await;
        store.synchronize_committed_cache_multi(&[snapshot]).await;
        assert_eq!(count(&store.metrics, "committed_cache_sync"), 2);
        assert_eq!(count(&store.metrics, "cache_write"), 2);
        assert_eq!(count(&store.metrics, "cache_repair_ack"), 0);
        assert_eq!(store.metrics.snapshot().cache_write_errors, 2);
        assert!(store.lookup_cache(&record, true).await.is_err());
        assert_eq!(count(&store.metrics, "cache_lookup"), 1);
        let CacheFallbackLockOutcome::Acquired(lock) = store.acquire_fallback_lock(&record).await
        else {
            panic!("fixture accepts the Redis SET lease command");
        };
        store.release_fallback_lock(lock).await;
        assert_eq!(count(&store.metrics, "fallback_distributed_lease"), 1);
        assert_eq!(count(&store.metrics, "fallback_lease_release"), 1);
        assert!(matches!(
            arrived.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(
            store
                .metrics
                .latency_snapshot()
                .iter()
                .all(|sample| sample.in_flight == 0)
        );
    })
    .await
    .expect("cache failure timing fixture deadline");
}

#[tokio::test]
async fn successful_cache_write_then_stalled_ack_remains_visible_on_cancellation() {
    timeout(Duration::from_secs(5), async {
        let (store, arrived, _pg, _redis) = tiered(false).await;
        let snapshots = [snapshot()];
        let mut sync = Box::pin(store.synchronize_committed_cache_multi(&snapshots));
        tokio::select! {
            _ = &mut sync => panic!("fixture must stall the PostgreSQL acknowledgement"),
            arrival = arrived => arrival.unwrap(),
        }
        assert_eq!(count(&store.metrics, "cache_write"), 1);
        assert_eq!(stage(&store.metrics, "cache_repair_ack").in_flight, 1);
        assert_eq!(stage(&store.metrics, "committed_cache_sync").in_flight, 1);
        drop(sync);
        assert_eq!(count(&store.metrics, "cache_repair_ack"), 1);
        assert_eq!(count(&store.metrics, "committed_cache_sync"), 1);
        assert_eq!(
            count(&store.metrics, "postgres_operation"),
            0,
            "repair ACK has its own stage"
        );
        assert!(
            store
                .metrics
                .latency_snapshot()
                .iter()
                .all(|sample| sample.in_flight == 0)
        );
    })
    .await
    .expect("acknowledgement timing fixture deadline");
}
