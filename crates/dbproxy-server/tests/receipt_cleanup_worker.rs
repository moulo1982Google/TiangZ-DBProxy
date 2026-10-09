//! Real server cleanup worker: a dedicated PG connection, bounded batches and prompt shutdown.
use std::{sync::Arc, time::Duration};
use tiangz_dbproxy_server::{DbProxyMetrics, StorageBackend, run_receipt_cleanup_worker};
use tokio::sync::watch;

#[tokio::test]
#[ignore = "requires an isolated PostgreSQL database and authorized Redis"]
async fn automatic_cleanup_drains_batches_and_stops_while_idle() {
    assert_eq!(
        std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(),
        Ok("1")
    );
    let pg = std::env::var("DBPROXY_TEST_POSTGRES_URL").unwrap();
    let redis = std::env::var("DBPROXY_REDIS_URL").unwrap();
    let backend = Arc::new(StorageBackend::connect(&pg, &redis, 1).await.unwrap());
    let (sql, connection) = tokio_postgres::connect(&pg, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    sql.batch_execute("INSERT INTO dbproxy_idempotency(request_id,namespace,record_key,schema_name,schema_version,payload,revision,recorded_at)
        SELECT 'worker-retention-'||n,'receipt-worker','test','test',1,'',1,
        CASE WHEN n<=1101 THEN statement_timestamp()-interval '169 hours' ELSE statement_timestamp() END FROM generate_series(1,1102) n;
        -- The default 24-hour retention: 25 hours old is expired, 23 hours old is kept.
        INSERT INTO dbproxy_idempotency(request_id,namespace,record_key,schema_name,schema_version,payload,revision,recorded_at) VALUES
          ('worker-retention-25h','receipt-worker','test','test',1,'',1,statement_timestamp()-interval '25 hours'),
          ('worker-retention-23h','receipt-worker','test','test',1,'',1,statement_timestamp()-interval '23 hours');
        INSERT INTO dbproxy_operation_claims(operation_id,operation_kind) VALUES('retention-kept-single','single'),('retention-kept-multi','multi');
        INSERT INTO dbproxy_transactions(operation_id,namespace,record_key,schema_name,schema_version,expected_revision,payload,result,new_revision,updated_at_unix_ms) VALUES('retention-kept-single','receipt-worker','single','test',1,0,'','',1,1);
        INSERT INTO dbproxy_multi_transactions(operation_id,result,record_count,updated_at_unix_ms) VALUES('retention-kept-multi','',1,1);
        INSERT INTO dbproxy_multi_transaction_records(operation_id,namespace,record_key,schema_name,schema_version,expected_revision,payload,updated_at_unix_ms,new_revision) VALUES('retention-kept-multi','receipt-worker','multi','test',1,0,'',1,1)").await.unwrap();
    let (stop, shutdown) = watch::channel(false);
    let worker = tokio::spawn(run_receipt_cleanup_worker(
        backend,
        "retention-test".into(),
        tiangz_dbproxy_storage::DEFAULT_RECEIPT_RETENTION,
        Arc::new(DbProxyMetrics::default()),
        shutdown,
    ));
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let remaining: i64 = sql
                .query_one(
                    "SELECT count(*) FROM dbproxy_idempotency WHERE namespace='receipt-worker'",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            // One fresh receipt plus the 23-hour one survive; the 25-hour one is deleted.
            if remaining == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap();
    let kept: bool = sql
        .query_one("SELECT EXISTS(SELECT 1 FROM dbproxy_idempotency WHERE request_id='worker-retention-23h') AND NOT EXISTS(SELECT 1 FROM dbproxy_idempotency WHERE request_id='worker-retention-25h')", &[])
        .await
        .unwrap()
        .get(0);
    assert!(
        kept,
        "default retention keeps 23 hours and deletes 25 hours"
    );
    for table in [
        "dbproxy_transactions",
        "dbproxy_multi_transactions",
        "dbproxy_multi_transaction_records",
    ] {
        let count: i64 = sql
            .query_one(&format!("SELECT count(*) FROM {table}"), &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, 1, "ordinary cleanup must not touch {table}");
    }
}
