//! 重复请求与回执查询的键顺序回归：SQL 读回的行必须与请求一样按字节序排列。
//! Key-order regressions for duplicate requests and receipt lookups: rows read back from SQL must
//! use the same byte order as the normalized request.
//!
//! 数据库默认排序规则（如 en_US.utf8）下 `a_1`/`a1`、`B`/`a`、`aa`/`a-b` 的先后与 Rust 字节序相反；
//! 若按位置比对，合法重试会被判成 OperationIdConflict。需要独立库：
//! DBPROXY_TEST_POSTGRES_URL、DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION=1、--ignored --test-threads=1。
//! Under a linguistic default collation such as en_US.utf8 these pairs sort opposite to Rust byte
//! order; positional comparison then rejected valid retries as OperationIdConflict.
use std::time::{SystemTime, UNIX_EPOCH};
use tiangz_dbproxy_core::{
    AsyncMultiRecordTransactionStore, AsyncTradeStore, CommitEffects, LedgerPosting,
    MultiRecordTransactionalWrite, MultiRecordTransactionalWriteOutcome, OutboxEvent, RecordKey,
    Revision, TradeState, TradeTransaction, TradeTransactionOutcome, TradeTransition,
    TransactionalRecordWrite,
};
use tiangz_dbproxy_storage::PostgresSnapshotStore;

/// 在常见的语言排序规则下与字节序先后不同的键。`1:x`/`10` 是对照项：2026-10-17 在 PostgreSQL 18.6
/// （libc 提供的 en_US.utf8）上实测，默认排序与 "C" 排序都是 `10` 在前；其他平台或版本的结果不影响用例。
/// Keys whose linguistic order differs from byte order. `1:x`/`10` is a control: on PostgreSQL 18.6
/// with libc en_US.utf8 (measured 2026-10-17) both the default and "C" collations put `10` first;
/// whether another platform agrees does not affect the test.
const KEYS: [&str; 8] = ["a_1", "a1", "B", "a", "1:x", "10", "a-b", "aa"];

fn suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{}-{nanos}", std::process::id())
}

async fn store() -> PostgresSnapshotStore {
    assert_eq!(
        std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(),
        Ok("1")
    );
    let url = std::env::var("DBPROXY_TEST_POSTGRES_URL").expect("dedicated test database required");
    let store = PostgresSnapshotStore::connect(&url).await.unwrap();
    let (sql, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let task = tokio::spawn(async move { connection.await.unwrap() });
    // 排序规则本来就是字节序时，用例照常通过但不构成反例。CI 设
    // DBPROXY_TEST_REQUIRE_LINGUISTIC_COLLATION=1 时直接失败，避免回归静默失去意义；本地未设置时只提示。
    // Under a byte-order collation the test passes but proves nothing. CI sets
    // DBPROXY_TEST_REQUIRE_LINGUISTIC_COLLATION=1 to fail instead of silently losing the regression.
    let differs: bool = sql
        .query_one("SELECT NOT ('a1' < 'a_1')", &[])
        .await
        .unwrap()
        .get(0);
    if !differs {
        let message = "database default collation already sorts by bytes (e.g. C/POSIX); the key-order regression cannot detect anything here, use a database whose default collation is linguistic such as en_US.utf8";
        if std::env::var("DBPROXY_TEST_REQUIRE_LINGUISTIC_COLLATION").as_deref() == Ok("1") {
            panic!("{message}");
        }
        eprintln!("{message}");
    }
    drop(sql);
    task.await.unwrap();
    store
}

/// 两个只差大小写的命名空间乘以全部键，请求里故意打乱顺序。
/// Two namespaces differing only by case times every key, deliberately shuffled in the request.
fn records(suffix: &str, prefix: &str) -> Vec<RecordKey> {
    let mut records = Vec::new();
    for namespace in [
        format!("{prefix}-{suffix}"),
        format!("{}-{suffix}", prefix.to_uppercase()),
    ] {
        for key in KEYS.iter().rev() {
            records.push(RecordKey::new(&namespace, *key).unwrap());
        }
    }
    records
}

fn write(record: &RecordKey) -> TransactionalRecordWrite {
    TransactionalRecordWrite {
        record: record.clone(),
        schema: "key-order".into(),
        schema_version: 1,
        expected_revision: Revision::ZERO,
        payload: record.key.as_bytes().to_vec(),
        updated_at_unix_ms: 1,
    }
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL; run serially"]
async fn commit_records_retry_and_receipt_lookup_keep_byte_order() {
    let mut store = store().await;
    let suffix = suffix();
    let records = records(&suffix, "key-order");
    let request = MultiRecordTransactionalWrite {
        operation_id: format!("key-order-multi-{suffix}"),
        writes: records.iter().map(write).collect(),
        result: b"ok".to_vec(),
    };
    let MultiRecordTransactionalWriteOutcome::Applied {
        records: applied, ..
    } = store
        .commit_records(request.clone(), CommitEffects::default())
        .await
        .unwrap()
    else {
        panic!("first commit must apply");
    };
    let mut expected = records.clone();
    expected.sort_by(|a, b| (&a.namespace, &a.key).cmp(&(&b.namespace, &b.key)));
    assert_eq!(
        applied.iter().map(|r| r.record.clone()).collect::<Vec<_>>(),
        expected
    );

    // 各路径分别核对、最后统一报告，反例能看到每条路径的结果。
    // Check every path and report together so a counterexample shows each one.
    let mut problems = Vec::new();
    // 合法重试必须得到 Duplicate 与同一份记录回执。 / A valid retry is a duplicate with the same receipts.
    match store
        .commit_records(request.clone(), CommitEffects::default())
        .await
    {
        Ok(MultiRecordTransactionalWriteOutcome::Duplicate { records, .. })
            if records == applied => {}
        other => problems.push(format!("commit_records retry: {other:?}")),
    }
    // 旧接口 apply_multi 走同一条重复路径。 / The legacy entry point shares the duplicate path.
    match store.apply_multi(request.clone()).await {
        Ok(MultiRecordTransactionalWriteOutcome::Duplicate { records, .. })
            if records == applied => {}
        other => problems.push(format!("apply_multi retry: {other:?}")),
    }
    // 回执查询（LoadMultiTransaction）同样按位置比对。 / Receipt lookup also compares by position.
    match store
        .load_multi_receipt(&request.operation_id, &records)
        .await
    {
        Ok(Some(receipt)) if receipt.records == applied => {}
        other => problems.push(format!("load_multi_receipt: {other:?}")),
    }
    assert!(problems.is_empty(), "{problems:#?}");
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL; run serially"]
async fn trade_retry_and_receipt_lookup_keep_byte_order() {
    let mut store = store().await;
    let suffix = suffix();
    let records = records(&suffix, "trade-key-order");
    let trade_id = format!("key-order-trade-{suffix}");
    let amounts = [-100_i64, 10, 20, 30, 40];
    let ids = ["a_1", "a1", "B", "a", "aa"];
    let request = TradeTransaction {
        operation_id: format!("key-order-trade-op-{suffix}"),
        transition: TradeTransition {
            trade_id: trade_id.clone(),
            expected_version: Revision::ZERO,
            expected_state: None,
            next_state: TradeState::Escrowed,
            payload: b"escrowed".to_vec(),
            updated_at_unix_ms: 1,
        },
        writes: records.iter().map(write).collect(),
        // 记录、分录号、事件号都含排序规则敏感的后缀。 / Records, posting and event IDs all differ.
        ledger_postings: ids
            .iter()
            .zip(amounts)
            .map(|(id, amount)| LedgerPosting {
                posting_id: format!("{suffix}-{id}"),
                account_id: format!("account-{id}"),
                asset: "gold".into(),
                amount,
                metadata: Vec::new(),
            })
            .collect(),
        outbox_events: ids
            .iter()
            .map(|id| OutboxEvent {
                event_id: format!("{suffix}-{id}"),
                topic: format!("key-order.{suffix}"),
                partition_key: trade_id.clone(),
                payload: id.as_bytes().to_vec(),
                occurred_at_unix_ms: 1,
            })
            .collect(),
        result: b"escrowed".to_vec(),
    };
    let applied = match store.apply_trade(request.clone()).await.unwrap() {
        TradeTransactionOutcome::Applied(receipt) => receipt,
        other => panic!("first trade must apply: {other:?}"),
    };
    let mut posting_ids: Vec<_> = ids.iter().map(|id| format!("{suffix}-{id}")).collect();
    posting_ids.sort();
    assert_eq!(applied.ledger_posting_ids, posting_ids);
    assert_eq!(applied.outbox_event_ids, posting_ids);

    let mut problems = Vec::new();
    match store.apply_trade(request.clone()).await {
        Ok(TradeTransactionOutcome::Duplicate(receipt)) if receipt == applied => {}
        other => problems.push(format!("apply_trade retry: {other:?}")),
    }
    // 回执查询不做比对，但返回顺序必须与提交时一致。 / Lookup must return the applied order.
    match store
        .load_trade_receipt(&request.operation_id, &trade_id)
        .await
    {
        Ok(Some(receipt)) if receipt == applied => {}
        other => problems.push(format!("load_trade_receipt: {other:?}")),
    }
    assert!(problems.is_empty(), "{problems:#?}");
}
