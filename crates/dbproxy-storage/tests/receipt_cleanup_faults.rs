//! Deterministic cleanup interruption; only an explicitly supplied disposable database.
/// Default ordinary receipt retention (24 hours); fixtures use 169-hour-old and fresh receipts.
const RETENTION: std::time::Duration = tiangz_dbproxy_storage::DEFAULT_RECEIPT_RETENTION;
use std::time::Duration;
use tiangz_dbproxy_storage::PostgresSnapshotStore;
use tokio_postgres::{Client, NoTls};

async fn connect(url: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

#[tokio::test]
#[ignore = "isolated current-schema PG; terminates only the named cleanup backend"]
async fn cleanup_deleted_but_not_committed_rolls_back_after_connection_kill() {
    assert_eq!(
        std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(),
        Ok("1")
    );
    let url = std::env::var("DBPROXY_TEST_POSTGRES_URL").unwrap();
    let store = PostgresSnapshotStore::connect(&url).await.unwrap();
    let sql = connect(&url).await;
    sql.batch_execute(
        "CREATE FUNCTION acceptance_pause_delete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      IF current_setting('application_name')='acceptance-cleanup-f03' THEN
        PERFORM pg_advisory_xact_lock(928341);
      END IF; RETURN NULL; END $$;
      CREATE TRIGGER acceptance_pause_delete AFTER DELETE ON dbproxy_idempotency
      FOR EACH STATEMENT EXECUTE FUNCTION acceptance_pause_delete();
      SELECT pg_advisory_lock(928341)",
    )
    .await
    .unwrap();
    for round in 0..3 {
        sql.batch_execute("INSERT INTO dbproxy_idempotency(request_id,namespace,record_key,schema_name,schema_version,payload,revision,recorded_at)
          SELECT 'f03-'||n, 'f03', n::text, 'test',1,'',1,clock_timestamp()-interval '169 hours' FROM generate_series(1,501) n;
          INSERT INTO dbproxy_idempotency(request_id,namespace,record_key,schema_name,schema_version,payload,revision)
          VALUES('f03-recent','f03','recent','test',1,'',1)").await.unwrap();
        let tagged = format!(
            "{url}{}application_name=acceptance-cleanup-f03",
            if url.contains('?') { "&" } else { "?" }
        );
        let worker = PostgresSnapshotStore::connect_existing(&tagged)
            .await
            .unwrap();
        let task = tokio::spawn(async move { worker.cleanup_expired_receipts(RETENTION).await });
        let pid: i32 = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(row) = sql.query_opt("SELECT pid FROM pg_stat_activity WHERE application_name='acceptance-cleanup-f03' AND datname=current_database() AND wait_event='advisory'", &[]).await.unwrap() {
                    break row.get(0);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }).await.unwrap();
        // AFTER DELETE runs after all candidate deletions, while the transaction is uncommitted.
        println!("FAULT_EVENT F03 round={round} barrier=after_delete_before_commit pid={pid}");
        assert!(
            sql.query_one("SELECT pg_terminate_backend($1)", &[&pid])
                .await
                .unwrap()
                .get::<_, bool>(0)
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        let count: i64 = sql
            .query_one(
                "SELECT count(*) FROM dbproxy_idempotency WHERE namespace='f03'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, 502, "uncommitted deletes must all roll back");
        assert_eq!(
            store.cleanup_expired_receipts(RETENTION).await.unwrap(),
            500
        );
        assert_eq!(store.cleanup_expired_receipts(RETENTION).await.unwrap(), 1);
        assert_eq!(store.cleanup_expired_receipts(RETENTION).await.unwrap(), 0);
        assert!(
            sql.query_one(
                "SELECT EXISTS(SELECT 1 FROM dbproxy_idempotency WHERE request_id='f03-recent')",
                &[]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
        );
        sql.batch_execute("DELETE FROM dbproxy_idempotency WHERE request_id='f03-recent'")
            .await
            .unwrap();
        println!("FAULT_EVENT F03 round={round} recovery=501_expired_deleted_recent_preserved");
    }
    sql.batch_execute("SELECT pg_advisory_unlock(928341); DROP TRIGGER acceptance_pause_delete ON dbproxy_idempotency; DROP FUNCTION acceptance_pause_delete()").await.unwrap();
}
