//! 缓存修复领取索引的独立数据库分布对照。
//! Compare repair claim layouts in a disposable database.
use tiangz_dbproxy_storage::PostgresSnapshotStore;
use tokio_postgres::{GenericClient, NoTls};

const CLAIM: &str = include_str!("../src/cache_repair_claim.sql");
const BEFORE: &str = include_str!("fixtures/cache_repair_claim_v13.sql");

#[tokio::test]
#[ignore = "requires an isolated PostgreSQL database; run serially"]
async fn mixed_states_preserve_order_and_skip_locked_heads() {
    assert_eq!(
        std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(),
        Ok("1")
    );
    let url = std::env::var("DBPROXY_TEST_POSTGRES_URL").unwrap();
    let store = PostgresSnapshotStore::connect(&url).await.unwrap();
    let queue = store.cache_repair_queue();
    let (mut sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    sql.batch_execute("INSERT INTO dbproxy_cache_repairs(namespace,record_key,target_revision,requested_at,available_at,lease_until)
        SELECT 'repair-order',lpad(n::text,3,'0'),1,'2020-01-01'::timestamptz+((n-1)/2)*interval '1 second','2020-01-01',
        CASE WHEN n%2=0 THEN '2020-01-02'::timestamptz ELSE NULL END FROM generate_series(1,60) n").await.unwrap();
    let lock = sql.transaction().await.unwrap();
    lock.query_one("SELECT record_key FROM dbproxy_cache_repairs WHERE namespace='repair-order' AND record_key='001' FOR UPDATE", &[]).await.unwrap();
    let first = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        queue.claim("order-worker", 300_000),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(first.record.key, "002");
    // The other candidate was locked, but must not have been leased or changed.
    let untouched: bool = lock.query_one("SELECT lease_until IS NULL FROM dbproxy_cache_repairs WHERE namespace='repair-order' AND record_key='003'", &[]).await.unwrap().get(0);
    assert!(untouched);
    lock.rollback().await.unwrap();
    for n in (1..=60).filter(|n| *n != 2) {
        let lease = queue.claim("order-worker", 300_000).await.unwrap().unwrap();
        assert_eq!(lease.record.key, format!("{n:03}"));
    }
    assert!(
        queue
            .claim("order-worker", 300_000)
            .await
            .unwrap()
            .is_none()
    );
    sql.execute(
        "DELETE FROM dbproxy_cache_repairs WHERE namespace='repair-order'",
        &[],
    )
    .await
    .unwrap();
}

#[derive(Debug)]
struct Json(String);
impl<'a> tokio_postgres::types::FromSql<'a> for Json {
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
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> serde_json::Value {
    let raw = client
        .query_one(
            &format!("EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON) {query}"),
            params,
        )
        .await
        .unwrap()
        .get::<_, Json>(0);
    let plan = serde_json::from_str(&raw.0).unwrap();
    println!("REPAIR_PLAN {label}: {plan}");
    plan
}

#[tokio::test]
#[ignore = "需要全新独立 PG 测试库；十万条积压和索引变更对照，串行执行"]
async fn repair_claim_distribution_matrix() {
    assert_eq!(
        std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(),
        Ok("1")
    );
    let url = std::env::var("DBPROXY_TEST_POSTGRES_URL").unwrap();
    PostgresSnapshotStore::connect(&url).await.unwrap();
    let (mut sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    for mode in [
        "ready",
        "backoff",
        "leased",
        "expired",
        "mixed",
        "dead",
        "none",
        "backoff-varied",
        "leased-varied",
    ] {
        let tx = sql.transaction().await.unwrap();
        tx.execute("INSERT INTO dbproxy_cache_repairs(namespace,record_key,target_revision,requested_at,available_at,lease_until,dead_lettered_at)
            SELECT 'repair-plan',n::text,1,'2020-01-01'::timestamptz+n*interval '1 millisecond',
            CASE WHEN ($1 IN ('backoff','backoff-varied') OR ($1='mixed' AND n%2=0)) AND n<=100000 THEN (CASE WHEN $1='backoff-varied' THEN clock_timestamp() ELSE statement_timestamp() END)+interval '1 hour' ELSE '2020-01-01'::timestamptz END,
            CASE WHEN ($1 IN ('leased','leased-varied') OR ($1='mixed' AND n%2=1)) AND n<=100000 OR $1='none' THEN (CASE WHEN $1='leased-varied' THEN clock_timestamp() ELSE statement_timestamp() END)+interval '1 hour' WHEN $1='expired' THEN '2020-01-02'::timestamptz ELSE NULL END,
            CASE WHEN $1='dead' AND n<=100000 THEN clock_timestamp() ELSE NULL END FROM generate_series(1,100001) n", &[&mode]).await.unwrap();
        for layout in ["before", "after"] {
            tx.batch_execute("SAVEPOINT layout").await.unwrap();
            if layout == "before" {
                tx.batch_execute("DROP INDEX dbproxy_cache_repairs_unleased_order; DROP INDEX dbproxy_cache_repairs_expired; DROP INDEX dbproxy_cache_repairs_leased_order").await.unwrap();
            }
            tx.batch_execute("ANALYZE dbproxy_cache_repairs; SAVEPOINT claim")
                .await
                .unwrap();
            let query = if layout == "before" { BEFORE } else { CLAIM };
            let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] =
                &[&"repair-worker", &30000_i64];
            let plan = explain(&tx, &format!("{mode}-{layout}"), query, params).await;
            assert_eq!(
                plan[0]["Plan"]["Actual Rows"].as_f64(),
                Some(if mode == "none" { 0.0 } else { 1.0 })
            );
            // Fixed deadline distributions have reproducible estimates; varied deadlines
            // are diagnostic because sampled estimates can select a larger index scan.
            if layout == "after" && !mode.ends_with("-varied") {
                let root = &plan[0]["Plan"];
                let pages = root["Shared Hit Blocks"].as_u64().unwrap()
                    + root["Shared Read Blocks"].as_u64().unwrap();
                assert!(
                    pages < if mode == "mixed" { 600 } else { 100 },
                    "unexpected claim reads: {plan}"
                );
            }
            tx.batch_execute("ROLLBACK TO SAVEPOINT claim")
                .await
                .unwrap();
            let row = tx.query_opt(query, params).await.unwrap();
            if mode == "none" {
                assert!(row.is_none());
            } else {
                assert_eq!(
                    row.unwrap().get::<_, String>(1),
                    if mode == "ready" || mode == "expired" {
                        "1"
                    } else {
                        "100001"
                    }
                );
            }
            tx.batch_execute("ROLLBACK TO SAVEPOINT claim")
                .await
                .unwrap();
            if mode == "ready" {
                explain(&tx,&format!("write-1000-{layout}"),"UPDATE dbproxy_cache_repairs SET lease_owner='write-probe',lease_token=nextval('dbproxy_cache_repair_lease_seq'),lease_until=clock_timestamp()+interval '30 seconds' WHERE namespace='repair-plan' AND record_key=ANY(SELECT n::text FROM generate_series(1,1000) n)",&[]).await;
            }
            tx.batch_execute("ROLLBACK TO SAVEPOINT layout; RELEASE SAVEPOINT layout")
                .await
                .unwrap();
        }
        tx.rollback().await.unwrap();
        sql.batch_execute("VACUUM (ANALYZE) dbproxy_cache_repairs")
            .await
            .unwrap();
    }
}
