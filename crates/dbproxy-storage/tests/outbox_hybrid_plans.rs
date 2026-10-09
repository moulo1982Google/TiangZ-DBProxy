//! Before/after comparison of production claim SQL with nine backlog distributions.
//! Requires an empty isolated database, explicit migration opt-in and serial execution.
use tiangz_dbproxy_storage::PostgresSnapshotStore;
use tokio_postgres::{Client, GenericClient, NoTls, types::ToSql};

fn aggregate_executed(node: &serde_json::Value) -> bool {
    (node["Node Type"] == "Aggregate" && node["Actual Loops"].as_u64().unwrap_or(0) > 0)
        || node["Plans"]
            .as_array()
            .is_some_and(|children| children.iter().any(aggregate_executed))
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
#[ignore = "requires isolated PG with 100001-row backlogs"]
async fn hybrid_handles_dense_blocked_and_empty_queues() {
    let mut sql = fixture().await;
    for mode in [
        "ready",
        "dense-ready",
        "backoff",
        "leased",
        "dead-heads",
        "backoff-heads",
        "leased-heads",
        "all-blocked",
        "none",
    ] {
        let tx = sql.transaction().await.unwrap();
        tx.batch_execute("INSERT INTO dbproxy_operation_claims(operation_id,operation_kind) VALUES('query-plan','multi')").await.unwrap();
        tx.execute("INSERT INTO dbproxy_outbox(event_id,operation_id,topic,partition_key,payload,occurred_at_unix_ms,available_at,lease_until,dead_lettered_at)
            SELECT 'query-event-'||n,'query-plan','test',
            CASE WHEN ($1 IN ('dense-ready','dead-heads','backoff-heads','leased-heads') AND n<=100000 OR $1='all-blocked') THEN 'group-'||((n-1)%1000) ELSE 'group-'||n END,'',0,
            CASE WHEN ($1='backoff' AND n<=100000 OR $1='backoff-heads' AND n<=1000) THEN clock_timestamp()+interval '1 hour' ELSE '2020-01-01'::timestamptz END,
            CASE WHEN ($1='leased' AND n<=100000 OR $1='leased-heads' AND n<=1000 OR $1='none') THEN clock_timestamp()+interval '1 hour' ELSE NULL END,
            CASE WHEN $1 IN ('dead-heads','all-blocked') AND n<=1000 THEN clock_timestamp() ELSE NULL END FROM generate_series(1,100001) n", &[&mode]).await.unwrap();
        tx.batch_execute("ANALYZE dbproxy_outbox").await.unwrap();
        for (label, query) in [
            ("before", include_str!("fixtures/outbox_claim_v14.sql")),
            ("after", include_str!("../src/outbox_claim.sql")),
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
            let plan = explain(&tx, &format!("outbox-{mode}-{label}"), query, params).await;
            if label == "after"
                && matches!(
                    mode,
                    "ready" | "dense-ready" | "backoff" | "leased" | "none"
                )
            {
                assert!(
                    !aggregate_executed(&plan[0]["Plan"]),
                    "fast/empty path must not aggregate the queue: {plan}"
                );
            }
            assert_eq!(
                plan[0]["Plan"]["Actual Rows"].as_f64(),
                Some(if mode == "none" || mode == "all-blocked" {
                    0.0
                } else {
                    1.0
                })
            );
            tx.batch_execute("ROLLBACK TO SAVEPOINT probe")
                .await
                .unwrap();
            let row = tx.query_opt(query, params).await.unwrap();
            if mode == "none" || mode == "all-blocked" {
                assert!(row.is_none());
            } else {
                assert_eq!(
                    row.unwrap().get::<_, String>(0),
                    if mode == "ready" || mode == "dense-ready" {
                        "query-event-1"
                    } else {
                        "query-event-100001"
                    }
                );
            }
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
