use std::time::{SystemTime, UNIX_EPOCH};

use tiangz_dbproxy_core::{
    AsyncSnapshotStore, AsyncTransactionalStore, RecordKey, Revision, SnapshotWrite,
    SnapshotWriteOutcome, StoreError, TransactionalWrite, TransactionalWriteOutcome,
};
use tiangz_dbproxy_storage::{RedisSnapshotCache, StorageError, TieredSnapshotStore};

fn test_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after Unix epoch")
        .as_nanos();
    format!("{}-{}", std::process::id(), nanos)
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

    let mut changed_request = first.clone();
    changed_request.payload = b"tampered-retry".to_vec();
    assert!(matches!(
        store.save(changed_request).await,
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
