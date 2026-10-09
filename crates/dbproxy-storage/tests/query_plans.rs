//! 独立测试库中的批量读取与队列积压查询验证；使用生产 SQL。
//! Exercise production queries with representative backlog distributions in a disposable DB.
use tiangz_dbproxy_core::RecordKey;
use tiangz_dbproxy_storage::PostgresSnapshotStore;
use tokio_postgres::{Client, GenericClient, NoTls, types::ToSql};

const LOAD: &str = include_str!("../src/snapshot_load_multi.sql");
const REPAIR: &str = include_str!("../src/cache_repair_claim.sql");
const OUTBOX: &str = include_str!("../src/outbox_claim.sql");
const OLD_LOAD: &str = "SELECT namespace, record_key, schema_name, schema_version, revision, payload, updated_at_unix_ms FROM dbproxy_snapshots WHERE (namespace, record_key) IN (SELECT * FROM unnest($1::TEXT[], $2::TEXT[]))";

fn shared_pages(plan: &serde_json::Value) -> u64 {
    plan[0]["Plan"]["Shared Hit Blocks"].as_u64().unwrap_or(0)
        + plan[0]["Plan"]["Shared Read Blocks"].as_u64().unwrap_or(0)
}

async fn fixture() -> Client {
    assert_eq!(
        std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(),
        Ok("1")
    );
    let url = std::env::var("DBPROXY_TEST_POSTGRES_URL").expect("disposable database required");
    PostgresSnapshotStore::connect(&url).await.unwrap();
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    client
}

#[derive(Debug)]
struct PlanJson(String);
impl<'a> tokio_postgres::types::FromSql<'a> for PlanJson {
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
    client: &(impl GenericClient + Sync),
    label: &str,
    query: &str,
    params: &[&(dyn ToSql + Sync)],
) -> serde_json::Value {
    let raw = client
        .query_one(
            &format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"),
            params,
        )
        .await
        .unwrap()
        .get::<_, PlanJson>(0);
    let plan: serde_json::Value = serde_json::from_str(&raw.0).unwrap();
    println!("QUERY_PLAN {label}: {plan}");
    plan
}

#[tokio::test]
#[ignore = "需要独立 PostgreSQL 测试库；十万条记录，串行执行"]
async fn batch_snapshot_plans() {
    let mut sql = fixture().await;
    let tx = sql.transaction().await.unwrap();
    tx.batch_execute("INSERT INTO dbproxy_snapshots(namespace,record_key,schema_name,schema_version,revision,payload,updated_at_unix_ms) SELECT 'query-plan',n::text,'test',1,1,decode(repeat('ab',128),'hex'),0 FROM generate_series(1,100000) n; ANALYZE dbproxy_snapshots").await.unwrap();
    for count in [1, 30, 64] {
        let namespaces: Vec<_> = (0..count).map(|_| "query-plan".to_string()).collect();
        let keys: Vec<_> = (1..=count).map(|n| (n * 1000).to_string()).collect();
        for (label, query) in [("before", OLD_LOAD), ("after", LOAD)] {
            let plan = explain(
                &tx,
                &format!("snapshots-{count}-{label}"),
                query,
                &[&namespaces, &keys],
            )
            .await;
            assert_eq!(plan[0]["Plan"]["Actual Rows"].as_f64(), Some(count as f64));
            if label == "after" {
                assert!(
                    shared_pages(&plan) < count * 8 + 32,
                    "batch read scanned unrelated history: {plan}"
                );
                assert!(!plan.to_string().contains("Seq Scan"), "{plan}");
            }
        }
    }
    let namespaces = vec![
        "query-plan",
        "query-plan",
        "another-namespace",
        "query-plan",
    ];
    let keys = vec!["64000", "missing", "64000", "1000"];
    let rows = tx.query(LOAD, &[&namespaces, &keys]).await.unwrap();
    let mut actual: Vec<String> = rows
        .iter()
        .map(|row| {
            assert_eq!(row.get::<_, String>(0), "query-plan");
            assert_eq!(row.get::<_, Vec<u8>>(5), vec![0xab; 128]);
            row.get(1)
        })
        .collect();
    actual.sort();
    assert_eq!(actual, ["1000", "64000"]);
    tx.commit().await.unwrap();
    let store = PostgresSnapshotStore::connect_existing(
        &std::env::var("DBPROXY_TEST_POSTGRES_URL").unwrap(),
    )
    .await
    .unwrap();
    let requested: Vec<_> = namespaces
        .iter()
        .zip(&keys)
        .map(|(namespace, key)| RecordKey::new(*namespace, *key).unwrap())
        .collect();
    let loaded = store.load_multi(&requested).await.unwrap();
    assert_eq!(loaded.len(), requested.len());
    assert_eq!(loaded[0].as_ref().unwrap().record, requested[0]);
    assert!(loaded[1].is_none());
    assert!(loaded[2].is_none());
    assert_eq!(loaded[3].as_ref().unwrap().record, requested[3]);
    assert_eq!(loaded[3].as_ref().unwrap().payload, vec![0xab; 128]);
}

#[tokio::test]
#[ignore = "需要独立 PostgreSQL 测试库；积压分布对照，串行执行"]
async fn outbox_claim_plans() {
    let mut sql = fixture().await;
    for mode in ["ready", "backoff", "leased", "dead-heads"] {
        let tx = sql.transaction().await.unwrap();
        tx.batch_execute("INSERT INTO dbproxy_operation_claims(operation_id,operation_kind) VALUES('query-plan','multi')").await.unwrap();
        // Fixed retry time makes the rare ready row estimate reproducible.
        // Varying retry times and their full-query cost are covered by outbox_candidate_plans.
        tx.execute("INSERT INTO dbproxy_outbox(event_id,operation_id,topic,partition_key,payload,occurred_at_unix_ms,available_at,lease_until,dead_lettered_at)
            SELECT 'query-event-'||n,'query-plan','test',
            CASE WHEN $1='dead-heads' AND n<=100000 THEN 'group-'||((n-1)%1000) ELSE 'group-'||n END,'',0,
            CASE WHEN $1='backoff' AND n<=100000 THEN statement_timestamp()+interval '1 hour' ELSE '2020-01-01'::timestamptz END,
            CASE WHEN $1='leased' AND n<=100000 THEN clock_timestamp()+interval '1 hour' ELSE NULL END,
            CASE WHEN $1='dead-heads' AND n<=1000 THEN clock_timestamp() ELSE NULL END FROM generate_series(1,100001) n", &[&mode]).await.unwrap();
        tx.batch_execute("ANALYZE dbproxy_outbox").await.unwrap();
        for (label, query) in [
            (
                "before",
                include_str!("fixtures/outbox_claim_v14.sql").to_string(),
            ),
            ("after", OUTBOX.to_string()),
        ] {
            tx.batch_execute("SAVEPOINT probe").await.unwrap();
            // 正式 SQL 多一个批量上限参数 $4；旧版对照保持 3 个参数。
            // The production query takes the batch maximum as $4; the old baseline keeps 3 parameters.
            let batch_params: &[&(dyn ToSql + Sync)] =
                &[&"query-worker", &30000_i64, &None::<String>, &1_i64];
            let single_params: &[&(dyn ToSql + Sync)] =
                &[&"query-worker", &30000_i64, &None::<String>];
            let params = if label == "after" {
                batch_params
            } else {
                single_params
            };
            let plan = explain(&tx, &format!("outbox-{mode}-{label}"), &query, params).await;
            assert_eq!(plan[0]["Plan"]["Actual Rows"].as_f64(), Some(1.0));
            if mode == "backoff" && label == "after" {
                assert!(
                    shared_pages(&plan) < 100,
                    "outbox backoff scan read unrelated tasks: {plan}"
                );
            }
            tx.batch_execute("ROLLBACK TO SAVEPOINT probe")
                .await
                .unwrap();
            let row = tx.query_one(&query, params).await.unwrap();
            assert_eq!(
                row.get::<_, String>(0),
                if mode == "ready" {
                    "query-event-1"
                } else {
                    "query-event-100001"
                }
            );
            tx.batch_execute("ROLLBACK TO SAVEPOINT probe; RELEASE SAVEPOINT probe")
                .await
                .unwrap();
        }
        let scoped = tx
            .query_one(
                OUTBOX,
                &[&"query-worker", &30000_i64, &Some("legacy"), &1_i64],
            )
            .await
            .unwrap();
        assert_eq!(
            scoped.get::<_, String>(0),
            if mode == "ready" {
                "query-event-1"
            } else {
                "query-event-100001"
            }
        );
        assert!(
            tx.query_opt(
                OUTBOX,
                &[
                    &"query-worker",
                    &30000_i64,
                    &Some("unknown-publisher"),
                    &1_i64
                ]
            )
            .await
            .unwrap()
            .is_none()
        );
        tx.rollback().await.unwrap();
        sql.batch_execute("VACUUM (ANALYZE) dbproxy_outbox")
            .await
            .unwrap();
    }
}

#[tokio::test]
#[ignore = "需要独立 PostgreSQL 测试库；检查到期边界使用本条语句而非事务开始时间"]
async fn claim_uses_statement_time_inside_an_older_transaction() {
    let mut sql = fixture().await;
    let tx = sql.transaction().await.unwrap();
    // 固定事务时间后，让任务在事务内到期；NOW() 会错误地一直看不到它。
    // Fix transaction time first; NOW() would miss a task that becomes eligible later.
    tx.batch_execute("SELECT transaction_timestamp(); SELECT pg_sleep(0.03);
        INSERT INTO dbproxy_cache_repairs(namespace,record_key,target_revision,available_at) VALUES('query-time','ready',1,clock_timestamp()),('query-time','later',1,clock_timestamp()+interval '1 hour');
        INSERT INTO dbproxy_operation_claims(operation_id,operation_kind) VALUES('query-time','multi');
        INSERT INTO dbproxy_outbox(event_id,operation_id,topic,partition_key,payload,occurred_at_unix_ms,available_at) VALUES('query-time-ready','query-time','test','time-ready','',0,clock_timestamp()),('query-time-later','query-time','test','time-later','',0,clock_timestamp()+interval '1 hour')").await.unwrap();
    let repair = tx
        .query_one(REPAIR, &[&"time-worker", &30000_i64])
        .await
        .unwrap();
    assert_eq!(repair.get::<_, String>(1), "ready");
    // A lease expiring after transaction start is eligible in the next statement.
    tx.batch_execute("UPDATE dbproxy_cache_repairs SET lease_until=clock_timestamp() WHERE namespace='query-time' AND record_key='ready'").await.unwrap();
    let reclaimed = tx
        .query_one(REPAIR, &[&"time-worker", &30000_i64])
        .await
        .unwrap();
    assert_eq!(reclaimed.get::<_, String>(1), "ready");
    assert_ne!(reclaimed.get::<_, i64>(4), repair.get::<_, i64>(4));
    assert!(
        tx.query_opt(REPAIR, &[&"time-worker", &30000_i64])
            .await
            .unwrap()
            .is_none()
    );
    let event = tx
        .query_one(
            OUTBOX,
            &[&"time-worker", &30000_i64, &None::<String>, &1_i64],
        )
        .await
        .unwrap();
    assert_eq!(event.get::<_, String>(0), "query-time-ready");
    assert!(
        tx.query_opt(
            OUTBOX,
            &[&"time-worker", &30000_i64, &None::<String>, &1_i64]
        )
        .await
        .unwrap()
        .is_none()
    );
    tx.rollback().await.unwrap();
}
