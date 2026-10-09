//! A05/A15: compare every column of every protected root table, not row counts.
use std::collections::BTreeMap;
use tiangz_dbproxy_core::*;
use tiangz_dbproxy_storage::{DEFAULT_RECEIPT_RETENTION, PostgresSnapshotStore};

async fn contents(sql: &tokio_postgres::Client) -> BTreeMap<String, Vec<String>> {
    let tables = sql.query("SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename<>'dbproxy_idempotency' AND tablename NOT IN (SELECT c.relname FROM pg_class c WHERE c.relispartition) ORDER BY tablename", &[]).await.unwrap();
    let mut result = BTreeMap::new();
    for table in tables {
        let name: String = table.get(0);
        assert!(name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
        let rows = sql
            .query(
                &format!("SELECT to_jsonb(t)::text FROM {name} t ORDER BY to_jsonb(t)::text"),
                &[],
            )
            .await
            .unwrap();
        result.insert(name, rows.iter().map(|r| r.get(0)).collect());
    }
    result
}

fn record(id: &str) -> TransactionalRecordWrite {
    TransactionalRecordWrite {
        record: RecordKey::new("content-ledger", id).unwrap(),
        schema: "opaque".into(),
        schema_version: 1,
        expected_revision: Revision::ZERO,
        payload: id.as_bytes().to_vec(),
        updated_at_unix_ms: 1,
    }
}

fn event(id: &str) -> OutboxEvent {
    OutboxEvent {
        event_id: id.into(),
        topic: "content-ledger".into(),
        partition_key: id.into(),
        payload: id.as_bytes().to_vec(),
        occurred_at_unix_ms: 1,
    }
}

fn same_multi_receipt(
    applied: MultiRecordTransactionalWriteOutcome,
    duplicate: MultiRecordTransactionalWriteOutcome,
) {
    match (applied, duplicate) {
        (
            MultiRecordTransactionalWriteOutcome::Applied { records, result },
            MultiRecordTransactionalWriteOutcome::Duplicate {
                records: replayed,
                result: replayed_result,
            },
        ) => {
            assert_eq!(records, replayed);
            assert_eq!(result, replayed_result);
        }
        other => panic!("expected Applied then identical Duplicate: {other:?}"),
    }
}

async fn populate(store: &mut PostgresSnapshotStore, prefix: &str) {
    let one = record(&format!("{prefix}-single"));
    let single = TransactionalWrite {
        operation_id: format!("{prefix}-single"),
        record: one.record,
        schema: one.schema,
        schema_version: 1,
        expected_revision: Revision::ZERO,
        payload: one.payload,
        result: prefix.as_bytes().to_vec(),
        updated_at_unix_ms: 1,
    };
    assert_eq!(
        store.apply(single.clone()).await.unwrap(),
        TransactionalWriteOutcome::Applied {
            new_revision: Revision(1),
            result: prefix.as_bytes().to_vec()
        }
    );
    assert_eq!(
        store.apply(single).await.unwrap(),
        TransactionalWriteOutcome::Duplicate {
            new_revision: Revision(1),
            result: prefix.as_bytes().to_vec()
        }
    );
    let multi = MultiRecordTransactionalWrite {
        operation_id: format!("{prefix}-multi"),
        writes: vec![
            record(&format!("{prefix}-m0")),
            record(&format!("{prefix}-m1")),
        ],
        result: prefix.as_bytes().to_vec(),
    };
    let applied = store.apply_multi(multi.clone()).await.unwrap();
    let duplicate = store.apply_multi(multi).await.unwrap();
    assert!(matches!(
        applied,
        MultiRecordTransactionalWriteOutcome::Applied { .. }
    ));
    assert!(matches!(
        duplicate,
        MultiRecordTransactionalWriteOutcome::Duplicate { .. }
    ));
    same_multi_receipt(applied, duplicate);
    let generic = MultiRecordTransactionalWrite {
        operation_id: format!("{prefix}-generic"),
        writes: vec![
            record(&format!("{prefix}-g0")),
            record(&format!("{prefix}-g1")),
        ],
        result: prefix.as_bytes().to_vec(),
    };
    let effects = CommitEffects {
        appends: vec![AppendRecord {
            record: RecordKey::new("content-facts", prefix).unwrap(),
            schema: "fact".into(),
            schema_version: 1,
            payload: prefix.as_bytes().to_vec(),
            occurred_at_unix_ms: 1,
        }],
        outbox_events: vec![event(&format!("{prefix}-generic-event"))],
    };
    let applied = store
        .commit_records(generic.clone(), effects.clone())
        .await
        .unwrap();
    let duplicate = store
        .commit_records(generic.clone(), effects.clone())
        .await
        .unwrap();
    assert!(matches!(
        applied,
        MultiRecordTransactionalWriteOutcome::Applied { .. }
    ));
    assert!(matches!(
        duplicate,
        MultiRecordTransactionalWriteOutcome::Duplicate { .. }
    ));
    same_multi_receipt(applied, duplicate);
    let trade = TradeTransaction {
        operation_id: format!("{prefix}-trade"),
        transition: TradeTransition {
            trade_id: format!("{prefix}-trade"),
            expected_version: Revision::ZERO,
            expected_state: None,
            next_state: TradeState::Escrowed,
            payload: prefix.as_bytes().to_vec(),
            updated_at_unix_ms: 1,
        },
        writes: vec![
            record(&format!("{prefix}-t0")),
            record(&format!("{prefix}-t1")),
        ],
        ledger_postings: [-10, 10]
            .into_iter()
            .enumerate()
            .map(|(n, amount)| LedgerPosting {
                posting_id: format!("{prefix}-posting-{n}"),
                account_id: format!("{prefix}-account-{n}"),
                asset: "test".into(),
                amount,
                metadata: prefix.as_bytes().to_vec(),
            })
            .collect(),
        outbox_events: vec![event(&format!("{prefix}-trade-event"))],
        result: prefix.as_bytes().to_vec(),
    };
    let applied = store.apply_trade(trade.clone()).await.unwrap();
    let duplicate = store.apply_trade(trade).await.unwrap();
    assert!(matches!(applied, TradeTransactionOutcome::Applied(_)));
    assert!(matches!(duplicate, TradeTransactionOutcome::Duplicate(_)));
    assert_eq!(applied.receipt(), duplicate.receipt());
    // A conflicting append occurs after planning two new snapshot writes. The failed
    // transaction must leave no records, receipts or effects; caller compares all tables.
    let mut rejected = generic;
    rejected.operation_id = format!("{prefix}-rejected");
    rejected.writes = vec![
        record(&format!("{prefix}-bad0")),
        record(&format!("{prefix}-bad1")),
    ];
    let mut conflicting = effects;
    conflicting.appends[0].payload = b"conflicting immutable fact".to_vec();
    // The direct observer below is independent of the store connection.
    let url = std::env::var("DBPROXY_TEST_POSTGRES_URL").unwrap();
    let (sql, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });
    let before = contents(&sql).await;
    assert!(store.commit_records(rejected, conflicting).await.is_err());
    assert_eq!(
        contents(&sql).await,
        before,
        "failed commit changed protected content"
    );
    driver.abort();
}

#[tokio::test]
#[ignore = "A05/A15: fresh isolated PG, full protected content ledger and concurrent receipt cleanup"]
async fn cleanup_preserves_full_content_and_concurrent_transactions() {
    assert_eq!(
        std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(),
        Ok("1")
    );
    let url = std::env::var("DBPROXY_TEST_POSTGRES_URL").unwrap();
    let out = std::path::PathBuf::from(std::env::var("DBPROXY_ACCEPTANCE_ARTIFACTS").unwrap());
    let mut store = PostgresSnapshotStore::connect(&url).await.unwrap();
    let cleaner = PostgresSnapshotStore::connect_existing(&url).await.unwrap();
    let (sql, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });
    populate(&mut store, "baseline").await;
    let queue = store.outbox_queue();
    let published = queue.claim("content-test", 30000).await.unwrap().unwrap();
    assert!(queue.acknowledge(&published).await.unwrap());
    sql.batch_execute("INSERT INTO dbproxy_idempotency(request_id,namespace,record_key,schema_name,schema_version,payload,revision,recorded_at)
        SELECT 'ledger-expired-'||n,'ledger-expired',n::text,'test',1,'',1,clock_timestamp()-interval '25 hours' FROM generate_series(1,6000) n;
        INSERT INTO dbproxy_idempotency(request_id,namespace,record_key,schema_name,schema_version,payload,revision,recorded_at)
        SELECT 'ledger-recent-'||n,'ledger-recent',n::text,'test',1,'',1,clock_timestamp() FROM generate_series(1,100) n").await.unwrap();
    let before = contents(&sql).await;
    let mut deleted = 0;
    let recent_before: Vec<String> = sql.query("SELECT to_jsonb(t)::text FROM dbproxy_idempotency t WHERE namespace='ledger-recent' ORDER BY request_id", &[]).await.unwrap().iter().map(|r| r.get(0)).collect();
    // A15: exact whole-table snapshots around deletion, including published outbox rows.
    for _ in 0..2 {
        deleted += cleaner
            .cleanup_expired_receipts(DEFAULT_RECEIPT_RETENTION)
            .await
            .unwrap();
    }
    let after = contents(&sql).await;
    assert_eq!(before, after, "cleanup changed protected table contents");
    std::fs::write(
        out.join("protected-before.json"),
        serde_json::to_vec_pretty(&before).unwrap(),
    )
    .unwrap();
    std::fs::write(
        out.join("protected-after.json"),
        serde_json::to_vec_pretty(&after).unwrap(),
    )
    .unwrap();
    // A05: each business batch is run concurrently with a nonempty cleanup batch on a
    // separate PG connection. No worker consumes outbox/cache repair rows behind the ledger.
    for n in 0..10 {
        let prefix = format!("concurrent-{n}");
        let ((), cleaned) = tokio::join!(
            populate(&mut store, &prefix),
            cleaner.cleanup_expired_receipts(DEFAULT_RECEIPT_RETENTION)
        );
        assert_eq!(cleaned.as_ref().unwrap(), &500);
        deleted += cleaned.unwrap();
    }
    assert_eq!(deleted, 6000);
    assert_eq!(
        cleaner
            .cleanup_expired_receipts(DEFAULT_RECEIPT_RETENTION)
            .await
            .unwrap(),
        0
    );
    let final_content = contents(&sql).await;
    let facts = sql.query("SELECT record_key,operation_id,schema_name,schema_version,payload,occurred_at_unix_ms FROM dbproxy_append_records WHERE namespace='content-facts'", &[]).await.unwrap();
    assert_eq!(facts.len(), 11);
    for row in facts {
        let key: String = row.get(0);
        assert_eq!(row.get::<_, String>(1), format!("{key}-generic"));
        assert_eq!(row.get::<_, String>(2), "fact");
        assert_eq!(row.get::<_, i64>(3), 1);
        assert_eq!(row.get::<_, Vec<u8>>(4), key.as_bytes());
        assert_eq!(row.get::<_, i64>(5), 1);
    }
    let postings = sql.query("SELECT posting_id,account_id,asset,amount,metadata FROM dbproxy_ledger_postings ORDER BY posting_id", &[]).await.unwrap();
    assert_eq!(postings.len(), 22);
    for row in postings {
        let id: String = row.get(0);
        let (prefix, n) = id.rsplit_once("-posting-").unwrap();
        assert_eq!(row.get::<_, String>(1), format!("{prefix}-account-{n}"));
        assert_eq!(row.get::<_, String>(2), "test");
        assert_eq!(row.get::<_, i64>(3), if n == "0" { -10 } else { 10 });
        assert_eq!(row.get::<_, Vec<u8>>(4), prefix.as_bytes());
    }
    let events = sql
        .query(
            "SELECT event_id,topic,partition_key,payload FROM dbproxy_outbox ORDER BY event_id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(events.len(), 22);
    for row in events {
        let id: String = row.get(0);
        assert_eq!(row.get::<_, String>(1), "content-ledger");
        assert_eq!(row.get::<_, String>(2), id);
        assert_eq!(row.get::<_, Vec<u8>>(3), id.as_bytes());
    }
    let snapshots = sql.query("SELECT record_key,payload,revision FROM dbproxy_snapshots WHERE namespace='content-ledger' ORDER BY record_key", &[]).await.unwrap();
    assert_eq!(snapshots.len(), 77); // 11 batches × (single + multi 2 + generic 2 + trade 2)
    for row in snapshots {
        let key: String = row.get(0);
        assert_eq!(row.get::<_, Vec<u8>>(1), key.as_bytes());
        assert_eq!(row.get::<_, i64>(2), 1);
    }
    assert_eq!(
        sql.query_one(
            "SELECT count(*) FROM dbproxy_idempotency WHERE namespace='ledger-recent'",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        100
    );
    assert_eq!(
        sql.query_one(
            "SELECT count(*) FROM dbproxy_idempotency WHERE namespace='ledger-expired'",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    std::fs::write(
        out.join("protected-final.json"),
        serde_json::to_vec_pretty(&final_content).unwrap(),
    )
    .unwrap();
    sql.batch_execute("INSERT INTO dbproxy_idempotency(request_id,namespace,record_key,schema_name,schema_version,payload,revision,recorded_at)
        SELECT 'ledger-final-expired-'||n,'ledger-final-expired',n::text,'test',1,'',1,clock_timestamp()-interval '25 hours' FROM generate_series(1,1001) n").await.unwrap();
    let mut final_deleted = 0;
    loop {
        let count = cleaner
            .cleanup_expired_receipts(DEFAULT_RECEIPT_RETENTION)
            .await
            .unwrap();
        final_deleted += count;
        if count == 0 {
            break;
        }
    }
    assert_eq!(final_deleted, 1001);
    assert_eq!(
        contents(&sql).await,
        final_content,
        "cleanup changed any concurrently generated protected content"
    );
    let recent_after: Vec<String> = sql.query("SELECT to_jsonb(t)::text FROM dbproxy_idempotency t WHERE namespace='ledger-recent' ORDER BY request_id", &[]).await.unwrap().iter().map(|r| r.get(0)).collect();
    assert_eq!(recent_before, recent_after);
    std::fs::write(
        out.join("recent-before.json"),
        serde_json::to_vec_pretty(&recent_before).unwrap(),
    )
    .unwrap();
    std::fs::write(
        out.join("recent-after.json"),
        serde_json::to_vec_pretty(&recent_after).unwrap(),
    )
    .unwrap();
    println!(
        "CONTENT_LEDGER_RESULT protected_tables={} snapshots=77 transaction_batches=11 rejected_batches=11 deleted=7001 recent_content_verified=100 facts=11 postings=22 outbox=22",
        before.len()
    );
    driver.abort();
}
