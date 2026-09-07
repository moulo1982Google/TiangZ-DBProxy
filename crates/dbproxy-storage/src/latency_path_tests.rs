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
