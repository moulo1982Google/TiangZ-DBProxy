//! 独立测试库验证建库、索引漂移与历史数据查询成本；不得指向业务库。
//! Disposable-database checks for initialization, index drift and history-heavy queries.

use tiangz_dbproxy_storage::{PostgresSnapshotStore, StorageError};
use tokio_postgres::{Client, NoTls};

async fn fixture() -> (String, PostgresSnapshotStore, Client) {
    assert_eq!(
        std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(),
        Ok("1")
    );
    let url = std::env::var("DBPROXY_TEST_POSTGRES_URL")
        .expect("disposable PostgreSQL database required");
    let store = PostgresSnapshotStore::connect(&url).await.unwrap();
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    (url, store, client)
}

#[tokio::test]
#[ignore = "需要独立 PostgreSQL 测试库；修改索引，串行执行"]
async fn initialization_and_drift_checks_cover_primary_secondary_and_partition_indexes() {
    let (url, store, sql) = fixture().await;
    store.validate_schema_indexes().await.unwrap();
    // 重复连接不会重跑索引 DDL，但仍验证已登记迁移的真实结构。
    // A repeated startup validates the actual schema even when migrations are registered.
    PostgresSnapshotStore::connect(&url).await.unwrap();
    let cases = [
        (
            "dbproxy_idempotency_retention",
            "DROP INDEX dbproxy_idempotency_retention",
            "CREATE INDEX dbproxy_idempotency_retention ON dbproxy_idempotency(recorded_at,request_id)",
        ),
        (
            "dbproxy_cache_repairs_unleased_order",
            "DROP INDEX dbproxy_cache_repairs_unleased_order",
            "CREATE INDEX dbproxy_cache_repairs_unleased_order ON dbproxy_cache_repairs(requested_at,namespace,record_key,available_at) WHERE dead_lettered_at IS NULL AND lease_until IS NULL",
        ),
        (
            "dbproxy_cache_repairs_leased_order",
            "DROP INDEX dbproxy_cache_repairs_leased_order; CREATE INDEX dbproxy_cache_repairs_leased_order ON dbproxy_cache_repairs(requested_at,namespace,record_key,lease_until,available_at) WHERE dead_lettered_at IS NULL",
            "DROP INDEX dbproxy_cache_repairs_leased_order; CREATE INDEX dbproxy_cache_repairs_leased_order ON dbproxy_cache_repairs(requested_at,namespace,record_key,lease_until,available_at) WHERE dead_lettered_at IS NULL AND lease_until IS NOT NULL",
        ),
        (
            "dbproxy_ledger_postings_operation",
            "DROP INDEX dbproxy_ledger_postings_operation",
            "CREATE INDEX dbproxy_ledger_postings_operation ON dbproxy_ledger_postings(operation_id,posting_id)",
        ),
        (
            "dbproxy_outbox_operation",
            "DROP INDEX dbproxy_outbox_operation; CREATE INDEX dbproxy_outbox_operation ON dbproxy_outbox(event_id,operation_id)",
            "DROP INDEX dbproxy_outbox_operation; CREATE INDEX dbproxy_outbox_operation ON dbproxy_outbox(operation_id,event_id)",
        ),
        (
            "dbproxy_outbox_order",
            "DROP INDEX dbproxy_outbox_order; CREATE INDEX dbproxy_outbox_order ON dbproxy_outbox(publisher_id,destination,partition_key,enqueue_order) WHERE published_at IS NULL AND dead_lettered_at IS NULL",
            "DROP INDEX dbproxy_outbox_order; CREATE INDEX dbproxy_outbox_order ON dbproxy_outbox(publisher_id,destination,partition_key,enqueue_order) WHERE published_at IS NULL",
        ),
        (
            "dbproxy_idempotency",
            "ALTER TABLE dbproxy_idempotency DROP CONSTRAINT dbproxy_idempotency_pkey",
            "ALTER TABLE dbproxy_idempotency ADD PRIMARY KEY(request_id)",
        ),
        (
            "dbproxy_snapshots",
            "ALTER TABLE dbproxy_snapshots DROP CONSTRAINT dbproxy_snapshots_pkey; ALTER TABLE dbproxy_snapshots ADD PRIMARY KEY(record_key,namespace)",
            "ALTER TABLE dbproxy_snapshots DROP CONSTRAINT dbproxy_snapshots_pkey; ALTER TABLE dbproxy_snapshots ADD PRIMARY KEY(namespace,record_key)",
        ),
    ];
    for (object, damage, repair) in cases {
        sql.batch_execute(damage).await.unwrap();
        let checked = store.validate_schema_indexes().await;
        let restarted = PostgresSnapshotStore::connect(&url).await;
        sql.batch_execute(repair).await.unwrap();
        for result in [checked, restarted.map(|_| ())] {
            assert!(
                matches!(&result, Err(StorageError::InvalidSchemaIndex(message)) if message.contains(object)),
                "{object}: {result:?}"
            );
        }
        store.validate_schema_indexes().await.unwrap();
    }
    sql.batch_execute("ALTER TABLE dbproxy_snapshots DETACH PARTITION dbproxy_snapshots_p00")
        .await
        .unwrap();
    let detached = store.validate_schema_indexes().await;
    sql.batch_execute("ALTER TABLE dbproxy_snapshots ATTACH PARTITION dbproxy_snapshots_p00 FOR VALUES WITH (MODULUS 32, REMAINDER 0)").await.unwrap();
    assert!(
        matches!(detached, Err(StorageError::InvalidSchemaIndex(message)) if message.contains("dbproxy_snapshots_p00"))
    );
    store.validate_schema_indexes().await.unwrap();
}

#[tokio::test]
#[ignore = "需要独立 PostgreSQL 测试库；注入失败的并发建索引，串行执行"]
async fn invalid_concurrent_index_is_rejected_and_can_be_repaired() {
    let (_, store, sql) = fixture().await;
    // 违反 CHECK 的数据不需要；相同版本的两个合法操作即可让唯一索引构建失败。
    // Two valid operations with the same timestamp make concurrent unique indexing fail.
    sql.batch_execute("INSERT INTO dbproxy_operation_claims(operation_id,operation_kind) VALUES('invalid-index-a','multi'),('invalid-index-b','multi') ON CONFLICT DO NOTHING;
        INSERT INTO dbproxy_outbox(event_id,operation_id,topic,partition_key,payload,occurred_at_unix_ms) VALUES('invalid-index-a','invalid-index-a','test','a','',0),('invalid-index-b','invalid-index-b','test','b','',0) ON CONFLICT DO NOTHING;
        DROP INDEX dbproxy_outbox_operation").await.unwrap();
    let failed = sql.batch_execute("CREATE UNIQUE INDEX CONCURRENTLY dbproxy_outbox_operation ON dbproxy_outbox(occurred_at_unix_ms)").await;
    let valid: bool = sql
        .query_one(
            "SELECT indisvalid FROM pg_index WHERE indexrelid='dbproxy_outbox_operation'::regclass",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let checked = store.validate_schema_indexes().await;
    sql.batch_execute("DROP INDEX dbproxy_outbox_operation; CREATE INDEX dbproxy_outbox_operation ON dbproxy_outbox(operation_id,event_id);
        UPDATE dbproxy_outbox SET published_at=clock_timestamp() WHERE event_id IN ('invalid-index-a','invalid-index-b');
        DELETE FROM dbproxy_outbox WHERE event_id IN ('invalid-index-a','invalid-index-b')").await.unwrap();
    assert!(failed.is_err());
    assert!(!valid);
    assert!(matches!(checked, Err(StorageError::InvalidSchemaIndex(_))));
    store.validate_schema_indexes().await.unwrap();
}

// 测试读取生产 SQL 文件，避免测试副本与实际统计查询漂移。
// Explain the production SQL, rather than a similar but independently maintained query.
const STATS: &str = include_str!("../src/outbox_stats.sql");

#[derive(Debug)]
struct JsonPlan(String);

impl<'a> tokio_postgres::types::FromSql<'a> for JsonPlan {
    fn from_sql(
        _: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(std::str::from_utf8(raw)?.to_owned()))
    }
    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        *ty == tokio_postgres::types::Type::JSON
    }
}

async fn explain(
    client: &(impl tokio_postgres::GenericClient + Sync),
    query: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> serde_json::Value {
    let plan = client
        .query_one(
            &format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"),
            params,
        )
        .await
        .unwrap()
        .get::<_, JsonPlan>(0);
    serde_json::from_str(&plan.0).unwrap()
}

#[tokio::test]
#[ignore = "需要独立 PostgreSQL 测试库；生成十万条历史，串行执行"]
async fn history_heavy_queries_use_bounded_index_access_and_preserve_stats() {
    let (_, store, mut sql) = fixture().await;
    sql.batch_execute(r#"
INSERT INTO dbproxy_trades(trade_id,version,state,payload,updated_at_unix_ms)
VALUES ('index-audit-trade',1,1,'',0);
INSERT INTO dbproxy_trade_operations(operation_id,trade_id,expected_version,next_state,trade_payload,result,record_count,ledger_count,outbox_count,updated_at_unix_ms,new_trade_version)
SELECT id,'index-audit-trade',0,1,'','',1,1,1,0,1 FROM (VALUES('index-background'),('index-target')) v(id);
INSERT INTO dbproxy_ledger_postings(posting_id,operation_id,trade_id,account_id,asset,amount,metadata,created_at_unix_ms)
SELECT 'index-posting-'||n,CASE WHEN n>100000 THEN 'index-target' ELSE 'index-background' END,
       'index-audit-trade','account','gold',1,convert_to(repeat('x',128),'UTF8'),0 FROM generate_series(1,100002) n;
INSERT INTO dbproxy_operation_claims(operation_id,operation_kind) VALUES('index-background','trade'),('index-target','trade');
INSERT INTO dbproxy_outbox(event_id,operation_id,topic,partition_key,payload,occurred_at_unix_ms,published_at)
SELECT 'index-event-'||n,CASE WHEN n>100000 THEN 'index-target' ELSE 'index-background' END,
       'audit','group-'||n,convert_to(repeat('y',128),'UTF8'),0,
       CASE WHEN n<=100000 THEN clock_timestamp() ELSE NULL END FROM generate_series(1,100002) n;
UPDATE dbproxy_outbox SET lease_until=clock_timestamp()+interval '1 hour' WHERE event_id='index-event-100002';
INSERT INTO dbproxy_outbox(event_id,operation_id,topic,partition_key,payload,occurred_at_unix_ms,published_at,dead_lettered_at)
VALUES('index-dead','index-background','audit','dead','',0,NULL,clock_timestamp()),
      ('index-published-dead','index-background','audit','published-dead','',0,clock_timestamp(),clock_timestamp());
ANALYZE dbproxy_ledger_postings;
ANALYZE dbproxy_outbox;
"#).await.unwrap();
    let queries = [
        (
            "ledger",
            "SELECT posting_id,account_id,asset,amount,metadata FROM dbproxy_ledger_postings WHERE operation_id=$1 ORDER BY posting_id",
            "dbproxy_ledger_postings_operation",
        ),
        (
            "outbox",
            "SELECT event_id,topic,partition_key,payload,occurred_at_unix_ms FROM dbproxy_outbox WHERE operation_id=$1 ORDER BY event_id",
            "dbproxy_outbox_operation",
        ),
        ("stats", STATS, "dbproxy_outbox_"),
    ];
    for (name, query, index) in queries {
        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = if name == "stats" {
            &[]
        } else {
            &[&"index-target"]
        };
        let plan = explain(&sql, query, params).await;
        let text = plan.to_string();
        assert!(text.contains(index), "{name}: {plan}");
        assert!(!text.contains("Seq Scan"), "{name} scanned history: {plan}");
        let buffers = plan[0]["Plan"]["Shared Hit Blocks"].as_u64().unwrap_or(0)
            + plan[0]["Plan"]["Shared Read Blocks"].as_u64().unwrap_or(0);
        assert!(buffers < 500, "{name} read too many pages: {plan}");
        println!("INDEX_AUDIT after {name}: {plan}");
    }
    // 同一数据集临时恢复旧索引布局，保存对照计划后回滚，不改变测试库最终结构。
    // Measure the previous layout on the same data, then roll back all index changes.
    let baseline = sql.transaction().await.unwrap();
    baseline
        .batch_execute(
            "DROP INDEX dbproxy_ledger_postings_operation; DROP INDEX dbproxy_outbox_operation",
        )
        .await
        .unwrap();
    for (name, query, _) in &queries[..2] {
        println!(
            "INDEX_AUDIT before {name}: {}",
            explain(&baseline, query, &[&"index-target"]).await
        );
    }
    let old_stats = "SELECT COUNT(*) FILTER(WHERE published_at IS NULL AND dead_lettered_at IS NULL AND (lease_until IS NULL OR lease_until<=clock_timestamp())), COUNT(*) FILTER(WHERE published_at IS NULL AND dead_lettered_at IS NULL AND lease_until>clock_timestamp()), COUNT(*) FILTER(WHERE dead_lettered_at IS NOT NULL), (EXTRACT(EPOCH FROM(clock_timestamp()-MIN(created_at) FILTER(WHERE published_at IS NULL AND dead_lettered_at IS NULL)))*1000)::DOUBLE PRECISION FROM dbproxy_outbox";
    println!(
        "INDEX_AUDIT before stats: {}",
        explain(&baseline, old_stats, &[]).await
    );
    baseline.rollback().await.unwrap();
    let stats = store.outbox_queue().stats().await.unwrap();
    assert_eq!(
        (stats.pending, stats.processing, stats.dead_lettered),
        (1, 1, 2)
    );
    assert!(stats.oldest_age_ms.is_some());
}
