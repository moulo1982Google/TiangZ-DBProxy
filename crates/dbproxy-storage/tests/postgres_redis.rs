use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use tiangz_dbproxy_core::{
    AsyncMultiRecordTransactionStore, AsyncSnapshotStore, AsyncTradeStore, AsyncTransactionalStore,
    LedgerPosting, MultiRecordTransactionalWrite, MultiRecordTransactionalWriteOutcome,
    OutboxEvent, RecordKey, Revision, SnapshotEnvelope, SnapshotWrite, SnapshotWriteOutcome,
    StoreError, TradeState, TradeTransaction, TradeTransactionOutcome, TradeTransition,
    TransactionalRecordWrite, TransactionalWrite, TransactionalWriteOutcome,
};
use tiangz_dbproxy_storage::{
    CacheFallbackConfig, CacheFallbackLockConfig, DEFAULT_OUTBOX_STREAM_PREFIX,
    PostgresSnapshotStore, RedisOutboxPublisher, RedisSnapshotBacklog, RedisSnapshotCache,
    SnapshotBacklogAck, SnapshotCacheConfig, StorageError, StorageMetrics, TieredSnapshotStore,
    TieredSnapshotStoreConfig,
};

fn test_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after Unix epoch")
        .as_nanos();
    format!("{}-{}", std::process::id(), nanos)
}

#[tokio::test]
#[ignore = "需要本机 Redis；使用 --ignored 显式运行"]
async fn redis_backlog_stats_report_depth_and_oldest_age() {
    let redis_url = std::env::var("DBPROXY_REDIS_URL")
        .expect("DBPROXY_REDIS_URL must be set for the integration test");
    let backlog = RedisSnapshotBacklog::connect(&redis_url).await.unwrap();
    let request = SnapshotWrite {
        request_id: format!("stats-{}", test_suffix()),
        record: RecordKey::new("backlog-stats", test_suffix()).unwrap(),
        schema: "integration.snapshot".to_string(),
        schema_version: 1,
        payload: b"stats".to_vec(),
        expected_revision: None,
        updated_at_unix_ms: 1,
    };
    backlog.enqueue(request).await.unwrap();
    let queued = backlog.stats().await.unwrap();
    assert!(queued.pending >= 1);
    assert!(queued.oldest_pending_age_ms.is_some());

    let lease = backlog.claim(5_000).await.unwrap().unwrap();
    let processing = backlog.stats().await.unwrap();
    assert!(processing.processing >= 1);
    assert_eq!(
        backlog.ack(&lease).await.unwrap(),
        SnapshotBacklogAck::Removed
    );
}

#[tokio::test]
#[ignore = "需要本机 PostgreSQL 和 Redis；使用 --ignored 显式运行"]
async fn postgres_and_redis_preserve_snapshot_semantics() {
    let postgres_url = std::env::var("DBPROXY_POSTGRES_URL")
        .expect("DBPROXY_POSTGRES_URL must be set for the integration test");
    let redis_url = std::env::var("DBPROXY_REDIS_URL")
        .expect("DBPROXY_REDIS_URL must be set for the integration test");
    let mut store = TieredSnapshotStore::connect(&postgres_url, &redis_url)
        .await
        .expect("PostgreSQL and Redis must be available");

    let key = RecordKey::new("integration", test_suffix()).unwrap();
    let first = SnapshotWrite {
        request_id: format!("first-{}", test_suffix()),
        record: key.clone(),
        schema: "integration.snapshot".to_string(),
        schema_version: 1,
        payload: b"v1".to_vec(),
        expected_revision: Some(Revision::ZERO),
        updated_at_unix_ms: 1,
    };

    assert_eq!(
        store.save(first.clone()).await.unwrap(),
        SnapshotWriteOutcome::Applied {
            revision: Revision(1)
        }
    );
    assert_eq!(
        store.save(first.clone()).await.unwrap(),
        SnapshotWriteOutcome::Duplicate {
            revision: Revision(1)
        }
    );
    assert_eq!(store.load(&key).await.unwrap().unwrap().payload, b"v1");

    let missing = RecordKey::new("integration", format!("missing-{}", test_suffix())).unwrap();
    let cache = RedisSnapshotCache::connect(&redis_url).await.unwrap();
    cache.delete(&key).await.unwrap();
    let loaded = store
        .load_multi(&[missing.clone(), key.clone()])
        .await
        .unwrap();
    assert_eq!(loaded.len(), 2);
    assert!(
        loaded[0].is_none(),
        "missing records must preserve their slot"
    );
    assert_eq!(loaded[1].as_ref().unwrap().record, key);
    assert_eq!(loaded[1].as_ref().unwrap().payload, b"v1");
    assert!(
        cache.get(&key).await.unwrap().is_some(),
        "PostgreSQL batch fallback must warm Redis"
    );

    let mut changed_request = first.clone();
    changed_request.payload = b"tampered-retry".to_vec();
    assert!(matches!(
        store.save(changed_request).await,
        Err(StorageError::Core(StoreError::IdempotencyConflict { .. }))
    ));
    let mut changed_timestamp = first.clone();
    changed_timestamp.updated_at_unix_ms = 999;
    assert!(matches!(
        store.save(changed_timestamp).await,
        Err(StorageError::Core(StoreError::IdempotencyConflict { .. }))
    ));

    let stale = SnapshotWrite {
        request_id: format!("stale-{}", test_suffix()),
        record: key.clone(),
        schema: "integration.snapshot".to_string(),
        schema_version: 1,
        payload: b"stale".to_vec(),
        expected_revision: Some(Revision::ZERO),
        updated_at_unix_ms: 2,
    };
    assert!(matches!(
        store.save(stale).await,
        Err(StorageError::Core(StoreError::RevisionConflict {
            actual: Revision(1),
            ..
        }))
    ));

    let cached = RedisSnapshotCache::connect(&redis_url)
        .await
        .unwrap()
        .get(&key)
        .await
        .unwrap()
        .expect("successful durable write must warm Redis");
    assert_eq!(cached.revision, Revision(1));
    assert_eq!(cached.payload, b"v1");

    let newer = SnapshotEnvelope {
        record: key.clone(),
        schema: "integration.snapshot".to_string(),
        schema_version: 1,
        revision: Revision(2),
        payload: b"newer-cache-value".to_vec(),
        updated_at_unix_ms: 3,
    };
    let older = SnapshotEnvelope {
        revision: Revision(1),
        payload: b"older-cache-value".to_vec(),
        ..newer.clone()
    };
    cache.put(&newer).await.unwrap();
    cache.put(&older).await.unwrap();
    let guarded = cache
        .get(&key)
        .await
        .unwrap()
        .expect("revision-aware cache entry must remain present");
    assert_eq!(guarded.revision, Revision(2));
    assert_eq!(guarded.payload, b"newer-cache-value");
    cache.delete(&key).await.unwrap();
    cache.put(&older).await.unwrap();
    let reset = cache
        .get(&key)
        .await
        .unwrap()
        .expect("deleting a cache entry must also clear its revision guard");
    assert_eq!(reset.revision, Revision(1));
    assert_eq!(reset.payload, b"older-cache-value");
}

#[tokio::test]
#[ignore = "requires local PostgreSQL and Redis; run explicitly with --ignored"]
async fn distributed_fallback_lock_rechecks_cache_before_postgres() {
    let postgres_url = std::env::var("DBPROXY_POSTGRES_URL")
        .expect("DBPROXY_POSTGRES_URL must be set for the integration test");
    let redis_url = std::env::var("DBPROXY_REDIS_URL")
        .expect("DBPROXY_REDIS_URL must be set for the integration test");
    let metrics = Arc::new(StorageMetrics::default());
    let mut store = TieredSnapshotStore::connect_with_config(
        &postgres_url,
        &redis_url,
        TieredSnapshotStoreConfig {
            fallback: CacheFallbackConfig {
                max_concurrent: 1,
                timeout: Duration::from_secs(1),
            },
            lock: CacheFallbackLockConfig {
                lease: Duration::from_secs(1),
                wait: Duration::from_millis(500),
                poll_interval: Duration::from_millis(10),
            },
            ..TieredSnapshotStoreConfig::default()
        },
        Arc::clone(&metrics),
    )
    .await
    .expect("PostgreSQL and Redis must be available");

    let key = RecordKey::new("distributed-lock", test_suffix()).unwrap();
    let request = SnapshotWrite {
        request_id: format!("seed-{}", test_suffix()),
        record: key.clone(),
        schema: "integration.snapshot".to_string(),
        schema_version: 1,
        payload: b"durable".to_vec(),
        expected_revision: Some(Revision::ZERO),
        updated_at_unix_ms: 1,
    };
    assert_eq!(
        store.save(request).await.unwrap(),
        SnapshotWriteOutcome::Applied {
            revision: Revision(1)
        }
    );

    let cache = RedisSnapshotCache::connect(&redis_url).await.unwrap();
    cache.delete(&key).await.unwrap();
    let lock_key = format!("{}:fallback-lock", RedisSnapshotCache::cache_key(&key));
    let token = format!("test-holder-{}", test_suffix());
    let redis_client = redis::Client::open(redis_url.clone()).unwrap();
    let mut lock_connection = redis_client
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let acquired: Option<String> = redis::cmd("SET")
        .arg(&lock_key)
        .arg(&token)
        .arg("NX")
        .arg("PX")
        .arg(1_000)
        .query_async(&mut lock_connection)
        .await
        .unwrap();
    assert_eq!(acquired.as_deref(), Some("OK"));

    let cached = SnapshotEnvelope {
        record: key.clone(),
        schema: "integration.snapshot".to_string(),
        schema_version: 1,
        revision: Revision(1),
        payload: b"durable".to_vec(),
        updated_at_unix_ms: 1,
    };
    let encoded = bincode::serde::encode_to_vec(&cached, bincode::config::standard()).unwrap();
    let cache_key = RedisSnapshotCache::cache_key(&key);
    let filler = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let client = redis::Client::open(redis_url).unwrap();
        let mut connection = client.get_multiplexed_async_connection().await.unwrap();
        let _: String = redis::cmd("SET")
            .arg(cache_key)
            .arg(encoded)
            .query_async(&mut connection)
            .await
            .unwrap();
    });

    let loaded = tokio::time::timeout(Duration::from_secs(2), store.load(&key))
        .await
        .expect("distributed lock wait must not hang")
        .unwrap()
        .expect("the filler must make the cache visible");
    filler.await.unwrap();
    assert_eq!(loaded.payload, b"durable");
    let snapshot = metrics.snapshot();
    assert!(snapshot.cache_fallback_lock_contention >= 1);
    assert_eq!(snapshot.cache_fallback_lock_timeouts, 0);
    assert_eq!(snapshot.postgres_fallbacks, 0);

    let _: i64 = redis::cmd("DEL")
        .arg(&lock_key)
        .query_async(&mut lock_connection)
        .await
        .unwrap();
    cache.delete(&key).await.unwrap();

    let loaded = store.load(&key).await.unwrap().unwrap();
    assert_eq!(loaded.payload, b"durable");
    let snapshot = metrics.snapshot();
    assert!(snapshot.cache_fallback_lock_acquired >= 1);
    assert_eq!(snapshot.postgres_fallbacks, 1);
    let lock_exists: i64 = redis::cmd("EXISTS")
        .arg(&lock_key)
        .query_async(&mut lock_connection)
        .await
        .unwrap();
    assert_eq!(
        lock_exists, 0,
        "the owner must release its lock after warming cache"
    );
    cache.delete(&key).await.unwrap();
}

#[tokio::test]
#[ignore = "requires local PostgreSQL and Redis; run explicitly with --ignored"]
async fn cache_lifecycle_renews_serves_stale_and_negative_caches() {
    let postgres_url = std::env::var("DBPROXY_POSTGRES_URL")
        .expect("DBPROXY_POSTGRES_URL must be set for the integration test");
    let redis_url = std::env::var("DBPROXY_REDIS_URL")
        .expect("DBPROXY_REDIS_URL must be set for the integration test");
    let metrics = Arc::new(StorageMetrics::default());
    let cache_policy = SnapshotCacheConfig {
        ttl: Duration::from_millis(150),
        ttl_jitter: Duration::ZERO,
        negative_ttl: Duration::from_secs(1),
        stale_while_revalidate: Duration::from_secs(1),
    };
    let mut store = TieredSnapshotStore::connect_with_config(
        &postgres_url,
        &redis_url,
        TieredSnapshotStoreConfig {
            fallback: CacheFallbackConfig {
                max_concurrent: 2,
                timeout: Duration::from_secs(1),
            },
            lock: CacheFallbackLockConfig {
                lease: Duration::from_secs(2),
                wait: Duration::from_millis(250),
                poll_interval: Duration::from_millis(10),
            },
            cache: cache_policy,
            ..TieredSnapshotStoreConfig::default()
        },
        Arc::clone(&metrics),
    )
    .await
    .expect("PostgreSQL and Redis must be available");
    let cache = RedisSnapshotCache::connect_with_metrics_and_policy(
        &redis_url,
        Arc::new(StorageMetrics::default()),
        cache_policy,
    )
    .await
    .unwrap();
    let redis_client = redis::Client::open(redis_url).unwrap();
    let mut redis_connection = redis_client
        .get_multiplexed_async_connection()
        .await
        .unwrap();

    let key = RecordKey::new("cache-lifecycle", test_suffix()).unwrap();
    store
        .save(SnapshotWrite {
            request_id: format!("lifecycle-seed-{}", test_suffix()),
            record: key.clone(),
            schema: "integration.snapshot".to_string(),
            schema_version: 1,
            payload: b"v1".to_vec(),
            expected_revision: Some(Revision::ZERO),
            updated_at_unix_ms: 1,
        })
        .await
        .unwrap();
    let freshness_key = format!("{}:fresh-until", RedisSnapshotCache::cache_key(&key));
    tokio::time::sleep(Duration::from_millis(90)).await;
    assert_eq!(store.repair_cache(&key).await.unwrap(), Some(Revision(1)));
    let renewed_ttl: i64 = redis::cmd("PTTL")
        .arg(&freshness_key)
        .query_async(&mut redis_connection)
        .await
        .unwrap();
    assert!(
        renewed_ttl > 70,
        "same-revision refresh must renew freshness TTL, got {renewed_ttl}ms"
    );

    tokio::time::sleep(Duration::from_millis(220)).await;
    let refreshes_before = metrics.snapshot().cache_refresh_completed;
    let stale = tokio::time::timeout(Duration::from_millis(500), store.load(&key))
        .await
        .expect("stale reads must return without waiting for PostgreSQL")
        .unwrap()
        .unwrap();
    assert_eq!(stale.payload, b"v1");
    assert!(metrics.snapshot().cache_stale_hits >= 1);
    let refresh_deadline = Instant::now() + Duration::from_secs(2);
    while metrics.snapshot().cache_refresh_completed == refreshes_before {
        assert!(
            Instant::now() < refresh_deadline,
            "background stale refresh did not complete"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let freshness_exists: i64 = redis::cmd("EXISTS")
        .arg(&freshness_key)
        .query_async(&mut redis_connection)
        .await
        .unwrap();
    assert_eq!(
        freshness_exists, 1,
        "background refresh must restore freshness"
    );

    let missing = RecordKey::new("cache-lifecycle", format!("missing-{}", test_suffix())).unwrap();
    cache.delete(&missing).await.unwrap();
    let fallbacks_before = metrics.snapshot().postgres_fallbacks;
    assert!(store.load(&missing).await.unwrap().is_none());
    let fallbacks_after_first = metrics.snapshot().postgres_fallbacks;
    assert_eq!(fallbacks_after_first, fallbacks_before + 1);
    assert!(store.load(&missing).await.unwrap().is_none());
    assert!(
        store
            .load_multi(std::slice::from_ref(&missing))
            .await
            .unwrap()[0]
            .is_none()
    );
    assert_eq!(
        metrics.snapshot().postgres_fallbacks,
        fallbacks_after_first,
        "negative cache hits must not query PostgreSQL again"
    );
    assert!(metrics.snapshot().cache_negative_hits >= 2);

    let negative_key = format!("{}:negative", RedisSnapshotCache::cache_key(&missing));
    let negative_exists: i64 = redis::cmd("EXISTS")
        .arg(&negative_key)
        .query_async(&mut redis_connection)
        .await
        .unwrap();
    assert_eq!(negative_exists, 1);
    store
        .save(SnapshotWrite {
            request_id: format!("negative-invalidate-{}", test_suffix()),
            record: missing.clone(),
            schema: "integration.snapshot".to_string(),
            schema_version: 1,
            payload: b"created".to_vec(),
            expected_revision: Some(Revision::ZERO),
            updated_at_unix_ms: 2,
        })
        .await
        .unwrap();
    let negative_exists: i64 = redis::cmd("EXISTS")
        .arg(&negative_key)
        .query_async(&mut redis_connection)
        .await
        .unwrap();
    assert_eq!(
        negative_exists, 0,
        "a committed write must clear negative cache"
    );
    assert_eq!(
        store.load(&missing).await.unwrap().unwrap().payload,
        b"created"
    );

    cache.delete(&key).await.unwrap();
    cache.delete(&missing).await.unwrap();
}

#[tokio::test]
#[ignore = "需要本机 PostgreSQL 和 Redis；使用 --ignored 显式运行"]
async fn postgres_and_redis_preserve_transactional_semantics() {
    let postgres_url = std::env::var("DBPROXY_POSTGRES_URL")
        .expect("DBPROXY_POSTGRES_URL must be set for the integration test");
    let redis_url = std::env::var("DBPROXY_REDIS_URL")
        .expect("DBPROXY_REDIS_URL must be set for the integration test");
    let mut store = TieredSnapshotStore::connect(&postgres_url, &redis_url)
        .await
        .expect("PostgreSQL and Redis must be available");

    let key = RecordKey::new("transactional-integration", test_suffix()).unwrap();
    let first = TransactionalWrite {
        operation_id: format!("grant-{}", test_suffix()),
        record: key.clone(),
        schema: "player.wallet-inventory".to_string(),
        schema_version: 1,
        expected_revision: Revision::ZERO,
        payload: b"wallet=100;item=1001:51".to_vec(),
        result: b"granted_gold=100;granted_item=1".to_vec(),
        updated_at_unix_ms: 1,
    };

    assert_eq!(
        store.apply(first.clone()).await.unwrap(),
        TransactionalWriteOutcome::Applied {
            new_revision: Revision(1),
            result: b"granted_gold=100;granted_item=1".to_vec(),
        }
    );
    assert_eq!(
        store.apply(first.clone()).await.unwrap(),
        TransactionalWriteOutcome::Duplicate {
            new_revision: Revision(1),
            result: b"granted_gold=100;granted_item=1".to_vec(),
        }
    );
    let receipt = store
        .load_receipt(&first.operation_id, &key)
        .await
        .unwrap()
        .expect("committed transaction receipt must be durable");
    assert_eq!(receipt.new_revision, Revision(1));
    assert_eq!(receipt.result, b"granted_gold=100;granted_item=1");

    let mut tampered_retry = first.clone();
    tampered_retry.result = b"granted_gold=999".to_vec();
    assert!(matches!(
        store.apply(tampered_retry).await,
        Err(StorageError::Core(StoreError::OperationIdConflict { .. }))
    ));

    let stale = TransactionalWrite {
        operation_id: format!("stale-{}", test_suffix()),
        record: key.clone(),
        schema: "player.wallet-inventory".to_string(),
        schema_version: 1,
        expected_revision: Revision::ZERO,
        payload: b"stale".to_vec(),
        result: b"must-not-commit".to_vec(),
        updated_at_unix_ms: 2,
    };
    assert!(matches!(
        store.apply(stale).await,
        Err(StorageError::Core(StoreError::RevisionConflict {
            actual: Revision(1),
            ..
        }))
    ));

    let durable = store.load(&key).await.unwrap().unwrap();
    assert_eq!(durable.revision, Revision(1));
    assert_eq!(durable.payload, b"wallet=100;item=1001:51");
    let cached = RedisSnapshotCache::connect(&redis_url)
        .await
        .unwrap()
        .get(&key)
        .await
        .unwrap()
        .expect("successful transactional write must warm Redis");
    assert_eq!(cached.revision, Revision(1));
    assert_eq!(cached.payload, b"wallet=100;item=1001:51");

    assert_eq!(store.repair_cache(&key).await.unwrap(), Some(Revision(1)));
    let missing = RecordKey::new(
        "transactional-integration",
        format!("missing-{}", test_suffix()),
    )
    .unwrap();
    assert_eq!(store.repair_cache(&missing).await.unwrap(), None);
}

#[tokio::test]
#[ignore = "需要本机 PostgreSQL；使用 --ignored 显式运行"]
async fn single_and_multi_transaction_creation_share_one_atomic_revision_boundary() {
    let postgres_url = std::env::var("DBPROXY_POSTGRES_URL")
        .expect("DBPROXY_POSTGRES_URL must be set for the integration test");
    let record = RecordKey::new("cross-api-cas", test_suffix()).unwrap();
    let contenders = 8;
    let barrier = Arc::new(tokio::sync::Barrier::new(contenders + 1));
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..contenders {
        let postgres_url = postgres_url.clone();
        let record = record.clone();
        let barrier = Arc::clone(&barrier);
        tasks.spawn(async move {
            let mut store = PostgresSnapshotStore::connect(&postgres_url).await.unwrap();
            barrier.wait().await;
            let write = TransactionalRecordWrite {
                record: record.clone(),
                schema: "cross-api.snapshot".to_string(),
                schema_version: 1,
                expected_revision: Revision::ZERO,
                payload: format!("contender-{index}").into_bytes(),
                updated_at_unix_ms: index as u64,
            };
            if index % 2 == 0 {
                store
                    .apply(TransactionalWrite {
                        operation_id: format!("single-{index}-{}", test_suffix()),
                        record: write.record,
                        schema: write.schema,
                        schema_version: write.schema_version,
                        expected_revision: write.expected_revision,
                        payload: write.payload,
                        result: Vec::new(),
                        updated_at_unix_ms: write.updated_at_unix_ms,
                    })
                    .await
                    .map(|outcome| matches!(outcome, TransactionalWriteOutcome::Applied { .. }))
            } else {
                store
                    .apply_multi(MultiRecordTransactionalWrite {
                        operation_id: format!("multi-{index}-{}", test_suffix()),
                        writes: vec![write],
                        result: Vec::new(),
                    })
                    .await
                    .map(|outcome| {
                        matches!(
                            outcome,
                            MultiRecordTransactionalWriteOutcome::Applied { .. }
                        )
                    })
            }
        });
    }
    barrier.wait().await;
    let mut applied = 0;
    while let Some(result) = tasks.join_next().await {
        match result.unwrap() {
            Ok(true) => applied += 1,
            Ok(false) => panic!("a fresh operation cannot be reported as duplicate"),
            Err(StorageError::Core(StoreError::RevisionConflict {
                actual: Revision(1),
                ..
            })) => {}
            Err(error) => panic!("unexpected concurrent write result: {error}"),
        }
    }
    assert_eq!(
        applied, 1,
        "exactly one expected-revision-zero write may commit"
    );
    let store = PostgresSnapshotStore::connect(&postgres_url).await.unwrap();
    assert_eq!(
        store.load(&record).await.unwrap().unwrap().revision,
        Revision(1)
    );
}

#[tokio::test]
#[ignore = "需要本机 PostgreSQL 和 Redis；使用 --ignored 显式运行"]
async fn durable_cache_repair_queue_keeps_the_newest_revision() {
    let postgres_url = std::env::var("DBPROXY_POSTGRES_URL")
        .expect("DBPROXY_POSTGRES_URL must be set for the integration test");
    let redis_url = std::env::var("DBPROXY_REDIS_URL")
        .expect("DBPROXY_REDIS_URL must be set for the integration test");
    let mut postgres = PostgresSnapshotStore::connect(&postgres_url).await.unwrap();
    let (sql, connection) = tokio_postgres::connect(&postgres_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    sql.execute(
        "DELETE FROM dbproxy_cache_repairs WHERE namespace LIKE 'repair-integration-%'",
        &[],
    )
    .await
    .unwrap();
    let record = RecordKey::new(format!("repair-integration-{}", test_suffix()), "record").unwrap();
    let first = SnapshotWrite {
        request_id: format!("repair-first-{}", test_suffix()),
        record: record.clone(),
        schema: "repair.snapshot".to_string(),
        schema_version: 1,
        payload: b"v1".to_vec(),
        expected_revision: Some(Revision::ZERO),
        updated_at_unix_ms: 1,
    };
    assert_eq!(
        postgres.save(first).await.unwrap(),
        SnapshotWriteOutcome::Applied {
            revision: Revision(1)
        }
    );
    let queue = postgres.cache_repair_queue();
    sql.execute(
        "UPDATE dbproxy_cache_repairs SET requested_at = to_timestamp(0) WHERE namespace = $1 AND record_key = $2",
        &[&record.namespace, &record.key],
    )
    .await
    .unwrap();
    let old_lease = queue
        .claim("repair-test-old", 30_000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old_lease.record, record);
    assert_eq!(old_lease.target_revision, Revision(1));

    let second = SnapshotWrite {
        request_id: format!("repair-second-{}", test_suffix()),
        record: record.clone(),
        schema: "repair.snapshot".to_string(),
        schema_version: 1,
        payload: b"v2".to_vec(),
        expected_revision: Some(Revision(1)),
        updated_at_unix_ms: 2,
    };
    assert_eq!(
        postgres.save(second).await.unwrap(),
        SnapshotWriteOutcome::Applied {
            revision: Revision(2)
        }
    );
    assert!(
        !queue.acknowledge(&old_lease).await.unwrap(),
        "an old lease must not delete a newer repair target"
    );
    sql.execute(
        "UPDATE dbproxy_cache_repairs SET requested_at = to_timestamp(0) WHERE namespace = $1 AND record_key = $2",
        &[&record.namespace, &record.key],
    )
    .await
    .unwrap();
    let dead_letter = queue
        .claim("repair-test-current", 30_000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(dead_letter.record, record);
    assert_eq!(dead_letter.target_revision, Revision(2));
    assert!(
        queue
            .fail(&dead_letter, "intentional integration-test failure", 0, 1)
            .await
            .unwrap()
    );
    assert!(queue.stats().await.unwrap().dead_lettered >= 1);
    assert!(queue.requeue_dead_letter(&record).await.unwrap());
    let current = queue
        .claim("repair-test-requeued", 30_000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.record, record);
    assert_eq!(current.attempt_count, 0);

    let store = TieredSnapshotStore::connect(&postgres_url, &redis_url)
        .await
        .unwrap();
    assert_eq!(
        store.repair_cache(&record).await.unwrap(),
        Some(Revision(2))
    );
    assert!(queue.acknowledge(&current).await.unwrap());
    let cached = RedisSnapshotCache::connect(&redis_url)
        .await
        .unwrap()
        .get(&record)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cached.revision, Revision(2));
    assert_eq!(cached.payload, b"v2");
}

#[tokio::test]
#[ignore = "需要本机 PostgreSQL 和 Redis；使用 --ignored 显式运行"]
async fn trade_state_ledger_and_outbox_commit_atomically() {
    let postgres_url = std::env::var("DBPROXY_POSTGRES_URL")
        .expect("DBPROXY_POSTGRES_URL must be set for the integration test");
    let redis_url = std::env::var("DBPROXY_REDIS_URL")
        .expect("DBPROXY_REDIS_URL must be set for the integration test");
    let mut store = TieredSnapshotStore::connect(&postgres_url, &redis_url)
        .await
        .expect("PostgreSQL and Redis must be available");
    let (sql, connection) = tokio_postgres::connect(&postgres_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    sql.execute(
        "UPDATE dbproxy_outbox SET published_at = COALESCE(published_at, clock_timestamp()), attempt_count = 0, lease_owner = NULL, lease_until = NULL, last_error = NULL, dead_lettered_at = NULL WHERE event_id LIKE '000-event-%' OR event_id LIKE '001-event-%'",
        &[],
    )
    .await
    .unwrap();
    let suffix = test_suffix();
    let trade_id = format!("trade-{suffix}");
    let operation_id = format!("escrow-{suffix}");
    let buyer = RecordKey::new("trade-player", format!("buyer-{suffix}")).unwrap();
    let seller = RecordKey::new("trade-player", format!("seller-{suffix}")).unwrap();
    let topic = format!("trade.escrowed-{suffix}");
    let event_id = format!("000-event-{suffix}");
    let request = TradeTransaction {
        operation_id: operation_id.clone(),
        transition: TradeTransition {
            trade_id: trade_id.clone(),
            expected_version: Revision::ZERO,
            expected_state: None,
            next_state: TradeState::Escrowed,
            payload: b"item=1001;price=100".to_vec(),
            updated_at_unix_ms: 1,
        },
        writes: vec![
            TransactionalRecordWrite {
                record: seller.clone(),
                schema: "player.inventory".to_string(),
                schema_version: 1,
                expected_revision: Revision::ZERO,
                payload: b"item=escrowed".to_vec(),
                updated_at_unix_ms: 1,
            },
            TransactionalRecordWrite {
                record: buyer.clone(),
                schema: "player.wallet".to_string(),
                schema_version: 1,
                expected_revision: Revision::ZERO,
                payload: b"gold=900".to_vec(),
                updated_at_unix_ms: 1,
            },
        ],
        ledger_postings: vec![
            LedgerPosting {
                posting_id: format!("buyer-debit-{suffix}"),
                account_id: buyer.key.clone(),
                asset: "gold".to_string(),
                amount: -100,
                metadata: Vec::new(),
            },
            LedgerPosting {
                posting_id: format!("escrow-credit-{suffix}"),
                account_id: format!("escrow:{trade_id}"),
                asset: "gold".to_string(),
                amount: 100,
                metadata: Vec::new(),
            },
        ],
        outbox_events: vec![OutboxEvent {
            event_id: event_id.clone(),
            topic: topic.clone(),
            partition_key: trade_id.clone(),
            payload: b"escrowed".to_vec(),
            occurred_at_unix_ms: 0,
        }],
        result: b"escrowed".to_vec(),
    };

    let applied = store.apply_trade(request.clone()).await.unwrap();
    assert!(matches!(
        &applied,
        TradeTransactionOutcome::Applied(receipt)
            if receipt.new_trade_version == Revision(1)
                && receipt.records.len() == 2
                && receipt.ledger_posting_ids.len() == 2
                && receipt.outbox_event_ids == [event_id.clone()]
    ));
    assert!(matches!(
        store.apply_trade(request.clone()).await.unwrap(),
        TradeTransactionOutcome::Duplicate(_)
    ));
    let trade = store.load_trade(&trade_id).await.unwrap().unwrap();
    assert_eq!(trade.version, Revision(1));
    assert_eq!(trade.state, TradeState::Escrowed);
    let receipt = store
        .load_trade_receipt(&operation_id, &trade_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt, applied.receipt().clone());

    let mut tampered = request;
    tampered.result = b"tampered".to_vec();
    assert!(matches!(
        store.apply_trade(tampered).await,
        Err(StorageError::Core(StoreError::OperationIdConflict { .. }))
    ));

    let settled_event_id = format!("001-event-{suffix}");
    let settled = TradeTransaction {
        operation_id: format!("settle-{suffix}"),
        transition: TradeTransition {
            trade_id: trade_id.clone(),
            expected_version: Revision(1),
            expected_state: Some(TradeState::Escrowed),
            next_state: TradeState::Settled,
            payload: b"settled".to_vec(),
            updated_at_unix_ms: 2,
        },
        writes: vec![
            TransactionalRecordWrite {
                record: seller.clone(),
                schema: "player.inventory".to_string(),
                schema_version: 1,
                expected_revision: Revision(1),
                payload: b"item=transferred".to_vec(),
                updated_at_unix_ms: 2,
            },
            TransactionalRecordWrite {
                record: buyer.clone(),
                schema: "player.wallet".to_string(),
                schema_version: 1,
                expected_revision: Revision(1),
                payload: b"gold=900;item=1001".to_vec(),
                updated_at_unix_ms: 2,
            },
        ],
        ledger_postings: Vec::new(),
        outbox_events: vec![OutboxEvent {
            event_id: settled_event_id.clone(),
            topic: topic.clone(),
            partition_key: trade_id.clone(),
            payload: b"settled".to_vec(),
            occurred_at_unix_ms: 2,
        }],
        result: b"settled".to_vec(),
    };
    assert!(matches!(
        store.apply_trade(settled).await.unwrap(),
        TradeTransactionOutcome::Applied(receipt)
            if receipt.new_trade_version == Revision(2)
                && receipt.outbox_event_ids == [settled_event_id.clone()]
    ));

    let immutable = sql
        .execute(
            "UPDATE dbproxy_ledger_postings SET amount = amount + 1 WHERE posting_id = $1",
            &[&receipt.ledger_posting_ids[0]],
        )
        .await;
    assert!(immutable.is_err(), "ledger trigger must reject updates");
    let immutable_order = sql
        .execute(
            "UPDATE dbproxy_outbox SET created_at = created_at + interval '1 second' WHERE event_id = $1",
            &[&event_id],
        )
        .await;
    assert!(
        immutable_order.is_err(),
        "outbox ordering metadata must be immutable"
    );
    let unpublished_delete = sql
        .execute(
            "DELETE FROM dbproxy_outbox WHERE event_id = $1",
            &[&event_id],
        )
        .await;
    assert!(
        unpublished_delete.is_err(),
        "an unpublished outbox event must not be deletable"
    );

    let outbox = store.outbox_queue();
    let lease = outbox
        .claim("outbox-integration", 30_000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lease.event.event_id, event_id);
    assert!(
        outbox
            .fail(&lease, "intentional integration-test failure", 0, 1)
            .await
            .unwrap()
    );
    assert!(outbox.stats().await.unwrap().dead_lettered >= 1);
    assert!(
        outbox
            .claim("outbox-integration-blocked", 30_000)
            .await
            .unwrap()
            .is_none(),
        "a dead-letter predecessor must block later events in its partition"
    );
    assert!(outbox.requeue_dead_letter(&event_id).await.unwrap());
    let lease = outbox
        .claim("outbox-integration-requeued", 30_000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lease.event.event_id, event_id);
    assert_eq!(lease.attempt_count, 0);
    let publisher = RedisOutboxPublisher::connect(&redis_url, DEFAULT_OUTBOX_STREAM_PREFIX)
        .await
        .unwrap();
    publisher.publish(&lease).await.unwrap();
    assert!(outbox.acknowledge(&lease).await.unwrap());
    let settled_lease = outbox
        .claim("outbox-integration-settled", 30_000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(settled_lease.event.event_id, settled_event_id);
    publisher.publish(&settled_lease).await.unwrap();
    assert!(outbox.acknowledge(&settled_lease).await.unwrap());
    let redis_client = redis::Client::open(redis_url).unwrap();
    let mut redis = redis_client
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let stream_len: u64 = redis::cmd("XLEN")
        .arg(format!("{DEFAULT_OUTBOX_STREAM_PREFIX}{topic}"))
        .query_async(&mut redis)
        .await
        .unwrap();
    assert!(stream_len >= 2);
}
