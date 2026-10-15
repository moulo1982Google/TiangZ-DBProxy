//! Isolated PostgreSQL regressions for multi-record receipt retirement. Never use a business
//! database: every test works in its own namespace and ages claims with SQL, not the clock.
use std::time::Duration;

use tiangz_dbproxy_core::{
    AppendRecord, AsyncMultiRecordTransactionStore, CommitEffects, MultiRecordTransactionalWrite,
    MultiRecordTransactionalWriteOutcome, RecordKey, Revision, StoreError,
    TransactionalRecordWrite,
};
use tiangz_dbproxy_storage::{
    DEFAULT_OPERATION_RECEIPT_RETENTION, OPERATION_RETIREMENT_BATCH_SIZE, PostgresSnapshotStore,
    StorageError,
};
use tokio_postgres::{Client, NoTls};

const RETENTION: Duration = DEFAULT_OPERATION_RECEIPT_RETENTION;

async fn fixture(namespace: &str) -> (PostgresSnapshotStore, PostgresSnapshotStore, Client) {
    assert_eq!(
        std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(),
        Ok("1")
    );
    let url = std::env::var("DBPROXY_TEST_POSTGRES_URL").unwrap();
    let store = PostgresSnapshotStore::connect(&url).await.unwrap();
    let maintenance = PostgresSnapshotStore::connect_existing(&url).await.unwrap();
    let (sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    let existing: i64 = sql
        .query_one(
            "SELECT count(*) FROM dbproxy_multi_transaction_records WHERE namespace=$1",
            &[&namespace],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        existing, 0,
        "use a fresh database; namespace {namespace} is not empty"
    );
    (store, maintenance, sql)
}

fn write(
    namespace: &str,
    operation: &str,
    key: &str,
    expected: u64,
) -> MultiRecordTransactionalWrite {
    MultiRecordTransactionalWrite {
        operation_id: format!("{namespace}:{operation}"),
        writes: vec![TransactionalRecordWrite {
            record: RecordKey::new(namespace, key).unwrap(),
            schema: "test".into(),
            schema_version: 1,
            expected_revision: Revision(expected),
            payload: format!("payload-{operation}").into_bytes(),
            updated_at_unix_ms: 1,
        }],
        result: format!("result-{operation}").into_bytes(),
    }
}

async fn age(sql: &Client, operation_id: &str, interval: &str) {
    let changed = sql
        .execute(
            &format!("UPDATE dbproxy_operation_claims SET claimed_at = clock_timestamp() - interval '{interval}' WHERE operation_id = $1"),
            &[&operation_id],
        )
        .await
        .unwrap();
    assert_eq!(changed, 1);
}

async fn count(sql: &Client, query: &str, operation_id: &str) -> i64 {
    sql.query_one(query, &[&operation_id]).await.unwrap().get(0)
}

fn is_expired(error: &StorageError) -> bool {
    matches!(
        error,
        StorageError::Core(StoreError::OperationExpired { .. })
    )
}

#[tokio::test]
#[ignore = "fresh isolated PG; multi-record receipt retirement boundaries and expired answers"]
async fn retired_receipts_answer_expired_and_recent_ones_stay() {
    let ns = "operation-retirement";
    let (mut store, maintenance, sql) = fixture(ns).await;
    let old = write(ns, "old", "a", 0);
    let recent = write(ns, "recent", "b", 0);
    for request in [&old, &recent] {
        assert!(matches!(
            store.apply_multi(request.clone()).await.unwrap(),
            MultiRecordTransactionalWriteOutcome::Applied { .. }
        ));
    }
    // One second past the retention retires; one hour inside it does not.
    age(&sql, &old.operation_id, "30 days 1 second").await;
    age(&sql, &recent.operation_id, "29 days 23 hours").await;
    assert_eq!(
        maintenance
            .retire_expired_operations(RETENTION)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        maintenance
            .retire_expired_operations(RETENTION)
            .await
            .unwrap(),
        0
    );

    // Header and records are gone, the claim stays as a retired marker.
    assert_eq!(
        count(
            &sql,
            "SELECT count(*) FROM dbproxy_multi_transactions WHERE operation_id=$1",
            &old.operation_id
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &sql,
            "SELECT count(*) FROM dbproxy_multi_transaction_records WHERE operation_id=$1",
            &old.operation_id
        )
        .await,
        0
    );
    assert_eq!(count(&sql, "SELECT count(*) FROM dbproxy_operation_claims WHERE operation_id=$1 AND retired_at IS NOT NULL", &old.operation_id).await, 1);
    // The snapshot itself is business state and is untouched.
    assert_eq!(count(&sql, "SELECT count(*) FROM dbproxy_snapshots WHERE namespace='operation-retirement' AND record_key='a' AND revision=1 AND $1<>''", &old.operation_id).await, 1);

    // Late lookup: "expired", never None ("not executed").
    let lookup = store
        .load_multi_receipt(&old.operation_id, &[old.writes[0].record.clone()])
        .await
        .unwrap_err();
    assert!(is_expired(&lookup), "{lookup:?}");
    // Late replay with the original bytes: "expired", not re-applied and not a revision conflict.
    let replay = store.apply_multi(old.clone()).await.unwrap_err();
    assert!(is_expired(&replay), "{replay:?}");
    // Reusing the retired id for a different, otherwise valid write is also refused.
    let mut reuse = write(ns, "old", "a", 1);
    reuse.result = b"different".to_vec();
    let reused = store.apply_multi(reuse).await.unwrap_err();
    assert!(is_expired(&reused), "{reused:?}");
    assert_eq!(count(&sql, "SELECT count(*) FROM dbproxy_snapshots WHERE namespace='operation-retirement' AND record_key='a' AND revision=1 AND $1<>''", &old.operation_id).await, 1);

    // The recent receipt is intact: lookup and original-bytes replay return the original result.
    let receipt = store
        .load_multi_receipt(&recent.operation_id, &[recent.writes[0].record.clone()])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.result, recent.result);
    match store.apply_multi(recent.clone()).await.unwrap() {
        MultiRecordTransactionalWriteOutcome::Duplicate { result, records } => {
            assert_eq!(result, recent.result);
            assert_eq!(records[0].new_revision, Revision(1));
        }
        other => panic!("recent replay was {other:?}"),
    }
    // A never-used id is still plainly absent.
    assert!(
        store
            .load_multi_receipt(&format!("{ns}:never"), &[old.writes[0].record.clone()])
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
#[ignore = "fresh isolated PG; receipts with commit effects are never retired"]
async fn receipts_with_commit_effects_are_kept() {
    let ns = "operation-retirement-effects";
    let (mut store, maintenance, sql) = fixture(ns).await;
    let request = write(ns, "with-append", "a", 0);
    let effects = CommitEffects {
        appends: vec![AppendRecord {
            record: RecordKey::new(ns, "fact-1").unwrap(),
            schema: "fact".into(),
            schema_version: 1,
            payload: b"fact".to_vec(),
            occurred_at_unix_ms: 1,
        }],
        outbox_events: vec![],
    };
    store
        .commit_records(request.clone(), effects)
        .await
        .unwrap();
    age(&sql, &request.operation_id, "400 days").await;
    assert_eq!(
        maintenance
            .retire_expired_operations(RETENTION)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        count(
            &sql,
            "SELECT count(*) FROM dbproxy_multi_transactions WHERE operation_id=$1",
            &request.operation_id
        )
        .await,
        1
    );
    assert_eq!(count(&sql, "SELECT count(*) FROM dbproxy_operation_claims WHERE operation_id=$1 AND retired_at IS NULL", &request.operation_id).await, 1);
    assert!(
        store
            .load_multi_receipt(&request.operation_id, &[request.writes[0].record.clone()])
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
#[ignore = "fresh isolated PG; full batches, a replay holding the claim, and batch pacing"]
async fn batches_are_bounded_and_skip_a_claim_held_by_a_replay() {
    let ns = "operation-retirement-batch";
    let (mut store, maintenance, sql) = fixture(ns).await;
    let total = OPERATION_RETIREMENT_BATCH_SIZE as usize + 3;
    for n in 0..total {
        let request = write(ns, &format!("op-{n}"), &format!("k-{n}"), 0);
        store.apply_multi(request).await.unwrap();
    }
    sql.execute(
        "UPDATE dbproxy_operation_claims SET claimed_at = clock_timestamp() - interval '31 days' WHERE operation_id LIKE 'operation-retirement-batch:%'",
        &[],
    )
    .await
    .unwrap();
    // A concurrent replay of op-0 holds FOR SHARE on its claim (as claim_operation does).
    let (holder, connection) =
        tokio_postgres::connect(&std::env::var("DBPROXY_TEST_POSTGRES_URL").unwrap(), NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    holder.batch_execute("BEGIN").await.unwrap();
    holder
        .execute(
            "SELECT 1 FROM dbproxy_operation_claims WHERE operation_id=$1 FOR SHARE",
            &[&format!("{ns}:op-0")],
        )
        .await
        .unwrap();
    assert_eq!(
        maintenance
            .retire_expired_operations(RETENTION)
            .await
            .unwrap(),
        OPERATION_RETIREMENT_BATCH_SIZE
    );
    assert_eq!(
        maintenance
            .retire_expired_operations(RETENTION)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        maintenance
            .retire_expired_operations(RETENTION)
            .await
            .unwrap(),
        0
    );
    let held = format!("{ns}:op-0");
    assert_eq!(
        count(
            &sql,
            "SELECT count(*) FROM dbproxy_multi_transactions WHERE operation_id=$1",
            &held
        )
        .await,
        1
    );
    holder.batch_execute("COMMIT").await.unwrap();
    assert_eq!(
        maintenance
            .retire_expired_operations(RETENTION)
            .await
            .unwrap(),
        1
    );
    let left: i64 = sql
        .query_one(
            "SELECT count(*) FROM dbproxy_multi_transactions WHERE operation_id LIKE 'operation-retirement-batch:%'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(left, 0);
    let retired: i64 = sql
        .query_one(
            "SELECT count(*) FROM dbproxy_operation_claims WHERE operation_id LIKE 'operation-retirement-batch:%' AND retired_at IS NOT NULL",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(retired, total as i64);
}
