//! Diagnostic comparison of rejected alternatives; these queries are NOT production choices.
//! Requires an empty isolated database, explicit migration opt-in and serial execution.
use tiangz_dbproxy_storage::PostgresSnapshotStore;
use tokio_postgres::{Client, GenericClient, NoTls, types::ToSql};
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
#[ignore = "diagnostic only: isolated PG with 100001-row backlogs"]
async fn compare_outbox_candidates_without_changing_schema() {
    let mut sql = fixture().await;
    for mode in ["ready", "backoff", "leased", "dead-heads"] {
        let tx = sql.transaction().await.unwrap();
        tx.batch_execute("INSERT INTO dbproxy_operation_claims(operation_id,operation_kind) VALUES('query-plan','multi')").await.unwrap();
        tx.execute("INSERT INTO dbproxy_outbox(event_id,operation_id,topic,partition_key,payload,occurred_at_unix_ms,available_at,lease_until,dead_lettered_at)
            SELECT 'query-event-'||n,'query-plan','test',
            CASE WHEN $1='dead-heads' AND n<=100000 THEN 'group-'||((n-1)%1000) ELSE 'group-'||n END,'',0,
            CASE WHEN $1='backoff' AND n<=100000 THEN clock_timestamp()+interval '1 hour' ELSE '2020-01-01'::timestamptz END,
            CASE WHEN $1='leased' AND n<=100000 THEN clock_timestamp()+interval '1 hour' ELSE NULL END,
            CASE WHEN $1='dead-heads' AND n<=1000 THEN clock_timestamp() ELSE NULL END FROM generate_series(1,100001) n", &[&mode]).await.unwrap();
        tx.batch_execute("ANALYZE dbproxy_outbox").await.unwrap();
        for (label, query) in [
            ("v14", include_str!("fixtures/outbox_claim_v14.sql")),
            ("hybrid", include_str!("../src/outbox_claim.sql")),
            (
                "lateral",
                include_str!("fixtures/outbox_candidate_lateral.sql"),
            ),
            (
                "materialized",
                include_str!("fixtures/outbox_candidate_materialized.sql"),
            ),
            (
                "grouped",
                include_str!("fixtures/outbox_candidate_grouped.sql"),
            ),
        ] {
            tx.batch_execute("SAVEPOINT probe").await.unwrap();
            let params: &[&(dyn ToSql + Sync)] = &[&"query-worker", &30000_i64, &None::<String>];
            let plan = explain(&tx, &format!("outbox-{mode}-{label}"), query, params).await;
            assert_eq!(plan[0]["Plan"]["Actual Rows"].as_f64(), Some(1.0));
            tx.batch_execute("ROLLBACK TO SAVEPOINT probe")
                .await
                .unwrap();
            let row = tx.query_one(query, params).await.unwrap();
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
        tx.rollback().await.unwrap();
        sql.batch_execute("VACUUM (ANALYZE) dbproxy_outbox")
            .await
            .unwrap();
    }
}
