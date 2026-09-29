//! Isolated PostgreSQL receipt-retention regressions. Never use a business database.
/// Default ordinary receipt retention (24 hours); fixtures use 169-hour-old and fresh receipts.
const RETENTION: std::time::Duration = tiangz_dbproxy_storage::DEFAULT_RECEIPT_RETENTION;
use tiangz_dbproxy_core::{
    AsyncSnapshotStore, RecordKey, Revision, SnapshotWrite, SnapshotWriteOutcome,
};
use tiangz_dbproxy_storage::PostgresSnapshotStore;
use tokio_postgres::{Client, NoTls};

async fn fixture() -> (String, PostgresSnapshotStore, Client) {
    assert_eq!(
        std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(),
        Ok("1")
    );
    let url = std::env::var("DBPROXY_TEST_POSTGRES_URL").unwrap();
    let store = PostgresSnapshotStore::connect(&url).await.unwrap();
    let (sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    sql.execute(
        "DELETE FROM dbproxy_idempotency WHERE namespace='receipt-retention'",
        &[],
    )
    .await
    .unwrap();
    (url, store, sql)
}
fn request(id: &str) -> SnapshotWrite {
    SnapshotWrite {
        request_id: format!("retention-{id}"),
        record: RecordKey::new("receipt-retention", id).unwrap(),
        schema: "test".into(),
        schema_version: 1,
        payload: vec![7],
        expected_revision: None,
        updated_at_unix_ms: 1,
    }
}

#[tokio::test]
#[ignore = "A06: fresh isolated PG; past/future business time and receipt retry age"]
async fn business_time_never_controls_receipt_age_or_renews_it() {
    let (_, mut store, sql) = fixture().await;
    // Unix epoch, historical business time, and 2100; never alter the server clock.
    for (n, business_ms) in [0, 1, 4_102_444_800_000u64].into_iter().enumerate() {
        let mut saved = request(&format!("business-time-{n}"));
        saved.updated_at_unix_ms = business_ms;
        assert_eq!(
            store.save(saved.clone()).await.unwrap(),
            SnapshotWriteOutcome::Applied {
                revision: Revision(1)
            }
        );
        let original: String = sql
            .query_one(
                "SELECT recorded_at::text FROM dbproxy_idempotency WHERE request_id=$1",
                &[&saved.request_id],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(store.cleanup_expired_receipts(RETENTION).await.unwrap(), 0);
        assert_eq!(
            store.save(saved.clone()).await.unwrap(),
            SnapshotWriteOutcome::Duplicate {
                revision: Revision(1)
            }
        );
        let unchanged: bool = sql.query_one(
            "SELECT recorded_at=$2::text::timestamptz FROM dbproxy_idempotency WHERE request_id=$1", &[&saved.request_id, &original]
        ).await.unwrap().get(0);
        assert!(unchanged, "retry renewed a fresh receipt");
        sql.execute("UPDATE dbproxy_idempotency SET recorded_at=clock_timestamp()-interval '23 hours' WHERE request_id=$1", &[&saved.request_id]).await.unwrap();
        let aged: String = sql
            .query_one(
                "SELECT recorded_at::text FROM dbproxy_idempotency WHERE request_id=$1",
                &[&saved.request_id],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            store.save(saved.clone()).await.unwrap(),
            SnapshotWriteOutcome::Duplicate {
                revision: Revision(1)
            }
        );
        let unchanged: bool = sql.query_one(
            "SELECT recorded_at=$2::text::timestamptz FROM dbproxy_idempotency WHERE request_id=$1", &[&saved.request_id, &aged]
        ).await.unwrap().get(0);
        assert!(unchanged, "retry renewed an aged receipt");
        assert_eq!(store.cleanup_expired_receipts(RETENTION).await.unwrap(), 0);
        sql.execute("UPDATE dbproxy_idempotency SET recorded_at=clock_timestamp()-interval '25 hours' WHERE request_id=$1", &[&saved.request_id]).await.unwrap();
        assert_eq!(store.cleanup_expired_receipts(RETENTION).await.unwrap(), 1);
        let snapshot = store.load(&saved.record).await.unwrap().unwrap();
        assert_eq!(snapshot.revision, Revision(1));
        assert_eq!(snapshot.payload, saved.payload);
        assert_eq!(snapshot.updated_at_unix_ms, business_ms);
        println!(
            "A06_BUSINESS_TIME business_ms={business_ms} fresh_kept=true retry_age_unchanged=true aged_deleted=true snapshot_kept=true"
        );
    }
}

#[tokio::test]
#[ignore = "isolated PG; changes migration shape to test old-schema upgrade"]
async fn migration_grants_old_receipts_full_retention() {
    let (url, mut store, sql) = fixture().await;
    store.save(request("upgrade")).await.unwrap();
    sql.batch_execute("DROP INDEX dbproxy_idempotency_retention; ALTER TABLE dbproxy_idempotency DROP COLUMN recorded_at; DELETE FROM dbproxy_schema_migrations WHERE version=15").await.unwrap();
    let started: String = sql
        .query_one("SELECT clock_timestamp()::TEXT", &[])
        .await
        .unwrap()
        .get(0);
    let upgraded = PostgresSnapshotStore::connect(&url).await.unwrap();
    let grace: bool = sql.query_one("SELECT recorded_at >= $1::TEXT::TIMESTAMPTZ AND recorded_at <= clock_timestamp() FROM dbproxy_idempotency WHERE request_id='retention-upgrade'", &[&started]).await.unwrap().get(0);
    assert!(grace);
    assert_eq!(
        upgraded.cleanup_expired_receipts(RETENTION).await.unwrap(),
        0
    );
    upgraded.validate_schema_indexes().await.unwrap();
}

#[tokio::test]
#[ignore = "isolated PG; ages test receipts and holds row locks"]
async fn cleanup_respects_age_locks_replay_and_snapshot() {
    let (_, mut store, mut sql) = fixture().await;
    let saved = request("boundary");
    let first = store.save(saved.clone()).await.unwrap();
    let revision = match first {
        SnapshotWriteOutcome::Applied { revision } => revision,
        _ => panic!("fresh receipt"),
    };
    assert_eq!(
        store.save(saved.clone()).await.unwrap(),
        SnapshotWriteOutcome::Duplicate { revision }
    );
    assert_eq!(
        store.cleanup_expired_receipts(RETENTION).await.unwrap(),
        0,
        "business time 1 must not expire a new receipt"
    );
    // Default retention is 24 hours: 23 hours is kept, 25 hours is eligible.
    sql.batch_execute("UPDATE dbproxy_idempotency SET recorded_at=clock_timestamp()-interval '23 hours' WHERE request_id='retention-boundary'").await.unwrap();
    assert_eq!(store.cleanup_expired_receipts(RETENTION).await.unwrap(), 0);
    // A longer configured retention keeps what the default would delete.
    sql.batch_execute("UPDATE dbproxy_idempotency SET recorded_at=clock_timestamp()-interval '25 hours' WHERE request_id='retention-boundary'").await.unwrap();
    assert_eq!(
        store
            .cleanup_expired_receipts(std::time::Duration::from_secs(48 * 3600))
            .await
            .unwrap(),
        0
    );
    // Out-of-range retention is rejected before touching the database.
    assert!(matches!(
        store
            .cleanup_expired_receipts(std::time::Duration::from_secs(1800))
            .await,
        Err(tiangz_dbproxy_storage::StorageError::InvalidReceiptRetention { .. })
    ));
    let tx = sql.transaction().await.unwrap();
    tx.query_one("SELECT request_id FROM dbproxy_idempotency WHERE request_id='retention-boundary' FOR KEY SHARE", &[]).await.unwrap();
    assert_eq!(
        store.cleanup_expired_receipts(RETENTION).await.unwrap(),
        0,
        "retry read lock must protect receipt"
    );
    tx.rollback().await.unwrap();
    assert_eq!(store.cleanup_expired_receipts(RETENTION).await.unwrap(), 1);
    assert_eq!(
        store.load(&saved.record).await.unwrap().unwrap().revision,
        revision
    );
    // Beyond retention an unconditional write is a new request, not an old Duplicate.
    assert_eq!(
        store.save(saved).await.unwrap(),
        SnapshotWriteOutcome::Applied {
            revision: Revision(revision.0 + 1)
        }
    );
    let tx = sql.transaction().await.unwrap();
    tx.batch_execute("LOCK TABLE dbproxy_idempotency IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    assert!(
        store.cleanup_expired_receipts(RETENTION).await.is_err(),
        "DDL lock must hit the short lock timeout"
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        store.cleanup_expired_receipts(RETENTION).await.unwrap(),
        0,
        "connection recovers after timeout rollback"
    );
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
#[tokio::test]
#[ignore = "isolated PG; creates 100601 test receipts"]
async fn cleanup_is_indexed_and_bounded_with_large_recent_history() {
    let (_, store, mut sql) = fixture().await;
    sql.batch_execute("INSERT INTO dbproxy_idempotency(request_id,namespace,record_key,schema_name,schema_version,payload,revision,recorded_at)
        SELECT 'retention-plan-'||n,'receipt-retention','plan','test',1,'',1,
        CASE WHEN n<=601 THEN statement_timestamp()-interval '169 hours' ELSE statement_timestamp() END FROM generate_series(1,100601) n;
        ANALYZE dbproxy_idempotency").await.unwrap();
    let tx = sql.transaction().await.unwrap();
    let plan = tx
        .query_one(
            &format!(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {}",
                include_str!("../src/receipt_cleanup.sql")
            ),
            &[&RETENTION.as_secs_f64()],
        )
        .await
        .unwrap()
        .get::<_, Json>(0)
        .0;
    println!("RECEIPT_CLEANUP_PLAN {plan}");
    assert!(plan.contains("dbproxy_idempotency_retention"));
    assert!(
        !plan.contains("Seq Scan"),
        "large recent history must not be scanned: {plan}"
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        store.cleanup_expired_receipts(RETENTION).await.unwrap(),
        500
    );
    assert_eq!(
        store.cleanup_expired_receipts(RETENTION).await.unwrap(),
        101
    );
    assert_eq!(store.cleanup_expired_receipts(RETENTION).await.unwrap(), 0);
    let remaining: i64 = sql
        .query_one(
            "SELECT count(*) FROM dbproxy_idempotency WHERE namespace='receipt-retention'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(remaining, 100000);
}

#[tokio::test]
#[ignore = "isolated PG; races receipt cleanup and eight retries"]
async fn concurrent_retries_and_cleanup_keep_atomic_writes() {
    let (url, mut store, sql) = fixture().await;
    let saved = request("race");
    store.save(saved.clone()).await.unwrap();
    sql.batch_execute("UPDATE dbproxy_idempotency SET recorded_at=clock_timestamp()-interval '169 hours' WHERE request_id='retention-race'").await.unwrap();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(9));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let mut writer = PostgresSnapshotStore::connect_existing(&url).await.unwrap();
        let barrier = barrier.clone();
        let saved = saved.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            writer.save(saved).await.unwrap()
        });
    }
    barrier.wait().await;
    store.cleanup_expired_receipts(RETENTION).await.unwrap();
    let mut applied = 0;
    while let Some(result) = tasks.join_next().await {
        if matches!(result.unwrap(), SnapshotWriteOutcome::Applied { .. }) {
            applied += 1;
        }
    }
    assert!(applied <= 1);
    assert_eq!(
        store.load(&saved.record).await.unwrap().unwrap().revision,
        Revision(1 + applied)
    );
}
#[tokio::test]
#[ignore = "isolated PG; statement trigger pauses a retry between conflict detection and receipt read"]
async fn receipt_deleted_between_conflict_and_read_is_reclaimed() {
    let (url, mut store, sql) = fixture().await;
    let saved = request("gap");
    store.save(saved.clone()).await.unwrap();
    sql.batch_execute("UPDATE dbproxy_idempotency SET recorded_at=clock_timestamp()-interval '169 hours' WHERE request_id='retention-gap';
        CREATE FUNCTION retention_pause_retry() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
          IF current_setting('application_name')='retention-gap' THEN PERFORM pg_advisory_xact_lock(87423); END IF;
          RETURN NULL; END $$;
        CREATE TRIGGER retention_pause_retry AFTER INSERT ON dbproxy_idempotency FOR EACH STATEMENT EXECUTE FUNCTION retention_pause_retry();
        SELECT pg_advisory_lock(87423)").await.unwrap();
    let writer_url = format!(
        "{url}{}application_name=retention-gap",
        if url.contains('?') { "&" } else { "?" }
    );
    let mut writer = PostgresSnapshotStore::connect_existing(&writer_url)
        .await
        .unwrap();
    let task = tokio::spawn(async move { writer.save(saved).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let waiting: bool = sql.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='retention-gap' AND wait_event='advisory')", &[]).await.unwrap().get(0);
            if waiting { break; }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    assert_eq!(store.cleanup_expired_receipts(RETENTION).await.unwrap(), 1);
    sql.batch_execute("SELECT pg_advisory_unlock(87423)")
        .await
        .unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        result,
        SnapshotWriteOutcome::Applied {
            revision: Revision(2)
        }
    );
    sql.batch_execute("DROP TRIGGER retention_pause_retry ON dbproxy_idempotency; DROP FUNCTION retention_pause_retry()").await.unwrap();
}

#[tokio::test]
#[ignore = "isolated PG; 32 callers over eight connections, concurrent with cleanup"]
async fn thirty_two_retries_share_bounded_connections_with_cleanup() {
    let (url, mut store, sql) = fixture().await;
    let saved = request("race32");
    store.save(saved.clone()).await.unwrap();
    let mut writers = Vec::new();
    for _ in 0..8 {
        writers.push(std::sync::Arc::new(tokio::sync::Mutex::new(
            PostgresSnapshotStore::connect_existing(&url).await.unwrap(),
        )));
    }
    for expired in [false, true] {
        if expired {
            sql.batch_execute("UPDATE dbproxy_idempotency SET recorded_at=clock_timestamp()-interval '169 hours' WHERE request_id='retention-race32'").await.unwrap();
        }
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(33));
        let mut tasks = tokio::task::JoinSet::new();
        for n in 0..32 {
            let writer = writers[n % writers.len()].clone();
            let request = saved.clone();
            let barrier = barrier.clone();
            tasks.spawn(async move {
                barrier.wait().await;
                writer.lock().await.save(request).await.unwrap()
            });
        }
        barrier.wait().await;
        store.cleanup_expired_receipts(RETENTION).await.unwrap();
        let mut applied = 0;
        while let Some(result) = tasks.join_next().await {
            applied += u64::from(matches!(
                result.unwrap(),
                SnapshotWriteOutcome::Applied { .. }
            ));
        }
        assert!(applied <= u64::from(expired));
        assert_eq!(
            store.load(&saved.record).await.unwrap().unwrap().revision,
            Revision(1 + applied)
        );
        println!("RETRY32 expired={expired} applied={applied} connections=8 callers=32");
    }
}
#[tokio::test]
#[ignore = "isolated PG; exact cutoff and post-retention CAS rejection"]
async fn cutoff_is_strict_and_expired_receipts_do_not_bypass_cas() {
    let (_, mut store, mut sql) = fixture().await;
    // The default (24 h) and the configurable floor (1 h). A single simple-query message shares
    // statement_timestamp across its commands, so the parameter is inlined for this check only.
    for seconds in [
        RETENTION.as_secs(),
        tiangz_dbproxy_storage::MIN_RECEIPT_RETENTION.as_secs(),
    ] {
        let tx = sql.transaction().await.unwrap();
        let cleanup = include_str!("../src/receipt_cleanup.sql").replace(
            "$1::double precision",
            &format!("{seconds}::double precision"),
        );
        tx.batch_execute(&format!("INSERT INTO dbproxy_idempotency(request_id,namespace,record_key,schema_name,schema_version,payload,revision,recorded_at)
            SELECT 'retention-'||label,'receipt-retention',label,'test',1,'',1,statement_timestamp()-make_interval(secs => {seconds}+delta)
            FROM (VALUES ('before',-1),('exact',0),('after',1)) AS cases(label,delta); {cleanup}")).await.unwrap();
        let remains: bool = tx
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM dbproxy_idempotency WHERE request_id='retention-exact')",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert!(
            remains,
            "a receipt exactly {seconds} seconds old is not strictly older than the retention"
        );
        let retained: Vec<String> = tx.query(
            "SELECT request_id FROM dbproxy_idempotency WHERE namespace='receipt-retention' ORDER BY request_id", &[]
        ).await.unwrap().into_iter().map(|row| row.get(0)).collect();
        assert_eq!(
            retained,
            ["retention-before", "retention-exact"],
            "only strictly expired receipts may be removed"
        );
        println!(
            "A06_CUTOFF retention_seconds={seconds} before_kept=true exact_kept=true after_deleted=true"
        );
        tx.rollback().await.unwrap();
    }
    let mut saved = request("cas");
    saved.expected_revision = Some(Revision::ZERO);
    store.save(saved.clone()).await.unwrap();
    sql.batch_execute("UPDATE dbproxy_idempotency SET recorded_at=clock_timestamp()-interval '169 hours' WHERE request_id='retention-cas'").await.unwrap();
    assert_eq!(store.cleanup_expired_receipts(RETENTION).await.unwrap(), 1);
    assert!(matches!(
        store.save(saved.clone()).await,
        Err(tiangz_dbproxy_storage::StorageError::Core(
            tiangz_dbproxy_core::StoreError::RevisionConflict { .. }
        ))
    ));
    assert_eq!(
        store.load(&saved.record).await.unwrap().unwrap().revision,
        Revision(1)
    );
}
