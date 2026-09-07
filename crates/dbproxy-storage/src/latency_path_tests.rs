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
    let (mut postgres, arrived, pg_peer) = postgres(metrics.clone()).await;
    postgres
        .configure_requests(PostgresRequestConfig::default())
        .await;
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
            cache_acknowledgements: None,
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
async fn request_postgres_queue_expires_without_sending_sql() {
    let (mut store, mut arrived, _pg, _redis) = tiered(false).await;
    store.postgres.connection_wait_timeout = Some(Duration::from_millis(20));
    let held = store.postgres.client.lock().await;
    let record = snapshot().record;
    let mut pending = Box::pin(store.postgres.load(&record));
    std::future::poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(
        stage(&store.metrics, "postgres_connection_wait").in_flight,
        1
    );
    let result = timeout(Duration::from_millis(900), pending).await;
    assert!(result.is_ok(), "request queue must have its own deadline");
    assert!(matches!(
        result.unwrap(),
        Err(StorageError::PostgresConnectionWaitTimeout { timeout_ms: 20 })
    ));
    assert_eq!(count(&store.metrics, "postgres_operation"), 0);
    assert_eq!(
        stage(&store.metrics, "postgres_connection_wait").in_flight,
        0
    );
    assert!(matches!(
        arrived.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    drop(held);
}

#[tokio::test]
async fn request_postgres_all_entries_and_repair_ack_obey_queue_budget() {
    use tiangz_dbproxy_core::{AsyncTradeStore, TradeState, TradeTransaction, TradeTransition};
    timeout(Duration::from_secs(3), async {
        let (mut store, mut arrived, _pg, _redis) = tiered(false).await;
        store.postgres.connection_wait_timeout = Some(Duration::from_millis(15));
        let client = store.postgres.client.clone();
        let held = client.lock().await;
        let snapshot = snapshot();
        let record = snapshot.record.clone();
        let writes = vec![TransactionalRecordWrite {
            record: record.clone(),
            schema: "timing".into(),
            schema_version: 1,
            expected_revision: Revision::ZERO,
            payload: vec![1],
            updated_at_unix_ms: 1,
        }];
        let multi = MultiRecordTransactionalWrite {
            operation_id: "op".into(),
            writes: writes.clone(),
            result: vec![],
        };
        let single = SnapshotWrite {
            request_id: "request".into(),
            record: record.clone(),
            schema: "timing".into(),
            schema_version: 1,
            expected_revision: Some(Revision::ZERO),
            payload: vec![1],
            updated_at_unix_ms: 1,
        };
        let trade = TradeTransaction {
            operation_id: "trade-op".into(),
            transition: TradeTransition {
                trade_id: "trade".into(),
                expected_version: Revision::ZERO,
                expected_state: None,
                next_state: TradeState::Proposed,
                payload: vec![],
                updated_at_unix_ms: 1,
            },
            writes,
            ledger_postings: vec![],
            outbox_events: vec![],
            result: vec![],
        };
        let pg = &mut store.postgres;
        for result in [
            pg.load(&record).await.map(|_| ()),
            pg.load_multi(std::slice::from_ref(&record))
                .await
                .map(|_| ()),
            pg.save(single.clone()).await.map(|_| ()),
            pg.save_batch(&[single]).await.map(|_| ()),
            pg.load_receipt("op", &record).await.map(|_| ()),
            pg.apply(TransactionalWrite {
                operation_id: "op".into(),
                record: record.clone(),
                schema: "timing".into(),
                schema_version: 1,
                expected_revision: Revision::ZERO,
                payload: vec![1],
                result: vec![],
                updated_at_unix_ms: 1,
            })
            .await
            .map(|_| ()),
            pg.load_multi_receipt("op", std::slice::from_ref(&record))
                .await
                .map(|_| ()),
            pg.apply_multi(multi.clone()).await.map(|_| ()),
            pg.commit_records(multi, CommitEffects::default())
                .await
                .map(|_| ()),
            pg.load_trade("trade").await.map(|_| ()),
            pg.load_trade_receipt("trade-op", "trade").await.map(|_| ()),
            pg.apply_trade(trade).await.map(|_| ()),
            pg.cache_repair_queue()
                .acknowledge_cached(&record, Revision(1))
                .await
                .map(|_| ()),
            pg.cache_repair_queue()
                .acknowledge_cached_multi(std::slice::from_ref(&snapshot))
                .await
                .map(|_| ()),
        ] {
            assert!(
                matches!(
                    result,
                    Err(StorageError::PostgresConnectionWaitTimeout { timeout_ms: 15 })
                ),
                "{result:?}"
            );
        }
        store.synchronize_committed_cache(&snapshot).await;
        store
            .synchronize_committed_cache_multi(std::slice::from_ref(&snapshot))
            .await;
        assert_eq!(count(&store.metrics, "cache_repair_ack"), 2);
        assert_eq!(count(&store.metrics, "postgres_operation"), 0);
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
        // Expired waiters no longer occupy the queue: the next request reaches the peer.
        let mut load = Box::pin(store.postgres.load(&record));
        tokio::select! { _ = &mut load => panic!("peer stalls SQL"), r = arrived => r.unwrap() }
    })
    .await
    .expect("all entry points must use the queue budget");
}

#[tokio::test]
async fn request_queue_budget_does_not_bound_sql_or_maintenance_wait() {
    timeout(Duration::from_secs(2), async {
        let (mut store, arrived, _pg, _redis) = tiered(false).await;
        store.postgres.connection_wait_timeout = Some(Duration::from_millis(15));
        let record = snapshot().record;
        let mut load = Box::pin(store.postgres.load(&record));
        tokio::select! { _ = &mut load => panic!("peer stalls SQL"), r = arrived => r.unwrap() }
        assert!(timeout(Duration::from_millis(60), &mut load).await.is_err());
        assert_eq!(stage(&store.metrics, "postgres_operation").in_flight, 1);
        drop(load);

        let metrics = Arc::new(StorageMetrics::default());
        let (maintenance, arrived, _peer) = postgres(metrics.clone()).await;
        assert_eq!(maintenance.connection_wait_timeout, None);
        let held = maintenance.client.lock().await;
        assert!(held.reconnect_cooldown.is_zero());
        let queue = maintenance.cache_repair_queue();
        let mut ack = Box::pin(queue.acknowledge_cached(&record, Revision(1)));
        std::future::poll_fn(|cx| {
            assert!(ack.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(timeout(Duration::from_millis(60), &mut ack).await.is_err());
        drop(held);
        tokio::select! { _ = &mut ack => panic!("peer stalls ACK"), r = arrived => r.unwrap() }
    })
    .await
    .expect("execution and maintenance retain separate budgets");
}

#[tokio::test]
async fn request_postgres_invalid_policy_is_rejected_before_connecting() {
    for duration in [Duration::ZERO, Duration::from_micros(999)] {
        for config in [
            PostgresRequestConfig {
                connection_wait_timeout: duration,
                ..Default::default()
            },
            PostgresRequestConfig {
                reconnect_cooldown: duration,
                ..Default::default()
            },
        ] {
            let result =
                PostgresSnapshotStore::connect_with_request_config("invalid-pg", config).await;
            assert!(matches!(
                result,
                Err(StorageError::InvalidPostgresConnectionWaitTimeout
                    | StorageError::InvalidPostgresReconnectCooldown)
            ));
        }
    }
}

#[tokio::test]
async fn request_postgres_cancelled_reconnect_cools_down_then_recovers() {
    timeout(Duration::from_secs(3), async {
        let (store, _arrival, pg, _redis) = tiered(false).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("postgresql://test@{}/test?sslmode=disable", listener.local_addr().unwrap());
        let attempts = Arc::new(AtomicU64::new(0));
        let observed = attempts.clone();
        let (sent, mut arrivals) = tokio::sync::mpsc::unbounded_channel();
        let _peer = Peer(tokio::spawn(async move {
            let mut streams = Vec::new();
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let length = stream.read_u32().await.unwrap();
                stream.read_exact(&mut vec![0; length as usize - 4]).await.unwrap();
                let index = observed.fetch_add(1, Ordering::SeqCst);
                if index > 0 { stream.write_all(b"R\0\0\0\x08\0\0\0\0Z\0\0\0\x05I").await.unwrap(); }
                sent.send(()).unwrap();
                streams.push(stream);
            }
        }));
        {
            let mut client = store.postgres.client.lock().await;
            client.url = Arc::from(url);
            client.reconnect_cooldown = Duration::from_millis(80);
        }
        drop(pg);
        while !store.postgres.client.lock().await.is_closed() { tokio::task::yield_now().await; }
        let record = snapshot().record;
        let mut reconnect = Box::pin(store.postgres.load(&record));
        tokio::select! { _ = &mut reconnect => panic!("first reconnect stalls"), _ = arrivals.recv() => {} }
        drop(reconnect);
        assert!(matches!(store.postgres.load(&record).await, Err(StorageError::PostgresReconnectCooldown { .. })));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(store.metrics.latency_snapshot().iter().all(|s| s.in_flight == 0));
        sleep(Duration::from_millis(90)).await;
        let mut client = store.postgres.client.lock().await;
        client.ensure_connected().await.unwrap();
        assert!(client.retry_at.is_none());
        assert!(!client.is_closed());
        client.ensure_connected().await.unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }).await.expect("cancelled reconnect must permit one later recovery");
}

#[tokio::test]
async fn request_postgres_reconnect_failure_is_shared_by_waiters() {
    timeout(Duration::from_secs(5), async {
        let (store, _arrived, pg, _redis) = tiered(false).await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let attempts = Arc::new(AtomicU64::new(0));
        let observed = attempts.clone();
        let url = format!(
            "postgresql://test@{}/test?sslmode=disable",
            listener.local_addr().unwrap()
        );
        let _failed_server = Peer(tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let length = stream.read_u32().await.unwrap();
                let mut startup = vec![0; length as usize - 4];
                stream.read_exact(&mut startup).await.unwrap();
                observed.fetch_add(1, Ordering::SeqCst);
                // Reject after observing a complete startup packet, without accepting any SQL.
                drop(stream);
            }
        }));
        store.postgres.client.lock().await.url = Arc::from(url);
        drop(pg);
        loop {
            if store.postgres.client.lock().await.is_closed() {
                break;
            }
            tokio::task::yield_now().await;
        }
        let record = snapshot().record;
        let (a, b, c, d) = tokio::join!(
            store.postgres.load(&record),
            store.postgres.load(&record),
            store.postgres.load(&record),
            store.postgres.load(&record)
        );
        assert!([a, b, c, d].iter().all(Result::is_err));
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "waiters must reuse one failed reconnect"
        );
    })
    .await
    .expect("reconnect fixture deadline");
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
async fn committed_cache_should_not_wait_for_repair_cleanup() {
    let (mut store, _arrived, _pg, _redis) = tiered(false).await;
    store.defer_cache_acknowledgements(CacheRepairAcknowledgements::default());
    let held = store.postgres.client.lock().await;
    let value = snapshot();
    timeout(
        Duration::from_millis(200),
        store.synchronize_committed_cache(&value),
    )
    .await
    .expect("a committed and cached write must not wait for repair cleanup");
    timeout(
        Duration::from_millis(200),
        store.synchronize_committed_cache_multi(&[value]),
    )
    .await
    .expect("batch cleanup must also be deferred");
    assert_eq!(count(&store.metrics, "cache_write"), 2);
    assert_eq!(count(&store.metrics, "cache_repair_ack"), 0);
    drop(held);
}

#[tokio::test]
async fn failed_cache_write_does_not_offer_deferred_cleanup() {
    let (mut store, _arrived, _pg, _redis) = tiered(true).await;
    let hints = CacheRepairAcknowledgements::default();
    store.defer_cache_acknowledgements(hints.clone());
    store.synchronize_committed_cache(&snapshot()).await;
    store.synchronize_committed_cache_multi(&[snapshot()]).await;
    let held = store.postgres.client.lock().await;
    assert_eq!(
        timeout(
            Duration::from_millis(200),
            hints.flush(&store.cache_repair_queue(), &store.metrics,)
        )
        .await
        .expect("failed cache writes must never generate cleanup hints")
        .unwrap(),
        0
    );
    drop(held);
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
