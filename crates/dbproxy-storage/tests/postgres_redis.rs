use std::time::{SystemTime, UNIX_EPOCH};

use tiangz_dbproxy_core::{
    AsyncSnapshotStore, RecordKey, Revision, SnapshotWrite, SnapshotWriteOutcome, StoreError,
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
