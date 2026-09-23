//! SQL scale evidence, not a TCP throughput benchmark.
/// Default ordinary receipt retention (24 hours); fixtures use 169-hour-old and fresh receipts.
const RETENTION: std::time::Duration = tiangz_dbproxy_storage::DEFAULT_RECEIPT_RETENTION;
use tiangz_dbproxy_storage::PostgresSnapshotStore;
use tokio_postgres::NoTls;

#[derive(Debug)]
struct Json(String);
impl<'a> tokio_postgres::types::FromSql<'a> for Json {
    fn from_sql(
        _: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self(std::str::from_utf8(raw)?.into()))
    }
    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        *ty == tokio_postgres::types::Type::JSON
    }
}

#[tokio::test]
#[ignore = "isolated PG; creates up to one million recent receipts, no old-schema upgrade"]
async fn recent_history_scale_keeps_cleanup_indexed_and_bounded() {
    assert_eq!(
        std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(),
        Ok("1")
    );
    let url = std::env::var("DBPROXY_TEST_POSTGRES_URL").unwrap();
    let store = PostgresSnapshotStore::connect(&url).await.unwrap();
    let (mut sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    let initial: i64 = sql
        .query_one("SELECT count(*) FROM dbproxy_idempotency", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(initial, 0, "use a fresh isolated database");
    let mut previous = 0;
    for size in [10_000, 100_000, 1_000_000] {
        sql.batch_execute(&format!("INSERT INTO dbproxy_idempotency(request_id,namespace,record_key,schema_name,schema_version,payload,revision)
          SELECT 'scale-'||n, 'receipt-scale', n::text,'test',1,decode(repeat('07',1024),'hex'),1 FROM generate_series({}, {size}) n;
          ANALYZE dbproxy_idempotency", previous+1)).await.unwrap();
        previous = size;
        for expired in [0, 1, 500, 501, 1101] {
            sql.batch_execute(&format!("INSERT INTO dbproxy_idempotency(request_id,namespace,record_key,schema_name,schema_version,payload,revision,recorded_at)
              SELECT 'expired-'||n,'receipt-scale',n::text,'test',1,'',1,clock_timestamp()-interval '169 hours' FROM generate_series(1,{expired}) n;
              ANALYZE dbproxy_idempotency")).await.unwrap();
            let tx = sql.transaction().await.unwrap();
            let raw: Json = tx
                .query_one(
                    &format!(
                        "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) {}",
                        include_str!("../src/receipt_cleanup.sql")
                    ),
                    &[&RETENTION.as_secs_f64()],
                )
                .await
                .unwrap()
                .get(0);
            println!("SCALE_PLAN recent={size} expired={expired} {}", raw.0);
            assert!(raw.0.contains("dbproxy_idempotency_retention"));
            assert!(!raw.0.contains("Seq Scan"), "recent history was scanned");
            tx.rollback().await.unwrap();
            let started = std::time::Instant::now();
            let mut deleted = 0;
            loop {
                let batch = store.cleanup_expired_receipts(RETENTION).await.unwrap();
                assert!(batch <= 500);
                deleted += batch;
                if batch == 0 {
                    break;
                }
            }
            let cleanup_elapsed_ms = started.elapsed().as_millis();
            assert_eq!(deleted, expired);
            let remain: i64 = sql
                .query_one("SELECT count(*) FROM dbproxy_idempotency", &[])
                .await
                .unwrap()
                .get(0);
            assert_eq!(remain, size);
            println!(
                "SCALE_RESULT recent={size} expired={expired} deleted={deleted} cleanup_elapsed_ms={cleanup_elapsed_ms}"
            );
        }
        let bytes: i64 = sql
            .query_one("SELECT pg_total_relation_size('dbproxy_idempotency')", &[])
            .await
            .unwrap()
            .get(0);
        println!("SCALE_SIZE recent={size} total_bytes={bytes}");
    }
}
