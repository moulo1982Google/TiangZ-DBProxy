//! Independent negative probe. Never selected by the normal parallel smoke runner.
use super::*;

fn isolated_database(url: &str, run: &str) -> bool {
    run.starts_with("p7ct_")
        && run.len() <= 24
        && run
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
        && url
            .parse::<tokio_postgres::Config>()
            .is_ok_and(|c| c.get_dbname() == Some(run))
}

struct ConnectionGuard(tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>);
impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[test]
fn requires_exact_dedicated_database() {
    assert!(isolated_database(
        "postgres://localhost/p7ct_local",
        "p7ct_local"
    ));
    for (url, run) in [
        ("postgres://localhost/postgres", "p7ct_local"),
        ("postgres://localhost/p7ct_other", "p7ct_local"),
        ("postgres://localhost/production", "production"),
        ("postgres://localhost/p7ct_local", "p7ct_X"),
        ("invalid", "p7ct_local"),
    ] {
        assert!(!isolated_database(url, run));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dedicated fresh p7ct_ database only; separate negative acceptance"]
async fn real_claim_timeout() {
    let url = std::env::var("DBPROXY_TEST_POSTGRES_URL").unwrap();
    let run = std::env::var("P07_TIMEOUT_RUN_ID").unwrap();
    assert!(
        isolated_database(&url, &run),
        "refuse non-dedicated database"
    );
    let output = std::path::PathBuf::from(std::env::var("P07_OUTPUT").unwrap());
    std::fs::create_dir(&output).unwrap();
    let journal = Journal(Mutex::new(
        File::options()
            .create_new(true)
            .write(true)
            .open(output.join("journal.jsonl"))
            .unwrap(),
    ));
    let (sql, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let _monitor_connection = ConnectionGuard(tokio::spawn(connection));
    // Must be an empty fresh database before schema initialization, not an old acceptance DB.
    assert_eq!(
        sql.query_one(
            "SELECT count(*) FROM pg_tables WHERE schemaname='public'",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    let _setup = PostgresSnapshotStore::connect(&url).await.unwrap();
    sql.batch_execute("SET statement_timeout='2s'")
        .await
        .unwrap();
    let suffix = if url.contains('?') { '&' } else { '?' };
    let first = PostgresSnapshotStore::connect_existing(&format!(
        "{url}{suffix}application_name=p7ct_worker0"
    ))
    .await
    .unwrap()
    .outbox_queue();
    let second = PostgresSnapshotStore::connect_existing(&format!(
        "{url}{suffix}application_name=p7ct_worker1"
    ))
    .await
    .unwrap()
    .outbox_queue();
    let (blocker, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let blocker_connection = ConnectionGuard(tokio::spawn(connection));
    let pid: i32 = blocker
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    // Server-side idle timeout also releases this transaction if this test stops progressing.
    blocker.batch_execute("SET lock_timeout='2s'; SET idle_in_transaction_session_timeout='12s'; BEGIN; LOCK TABLE dbproxy_outbox IN ACCESS EXCLUSIVE MODE").await.unwrap();
    let start = Instant::now();
    journal.record(json!({"kind":"timeout_fixture","schema":1,"run_id":run,"rows":0,"workers":2,"claims":2,"timeout_ms":5000,"blocker_pid":pid,"scope":"empty_outbox_relation_lock"}));
    journal.record(json!({"kind":"lock_acquired","at_us":start.elapsed().as_micros()}));
    let monitor = async {
        timeout(Duration::from_secs(2), async {
            loop {
                let rows = sql.query("SELECT pid,application_name FROM pg_stat_activity WHERE datname=current_database() AND application_name IN ('p7ct_worker0','p7ct_worker1') AND state='active' AND wait_event_type='Lock' AND $1=ANY(pg_blocking_pids(pid)) ORDER BY application_name", &[&pid]).await.unwrap();
                if rows.len() == 2 {
                    journal.record(json!({"kind":"blocked_peers","at_us":start.elapsed().as_micros(),"peers":rows.iter().map(|r|json!({"pid":r.get::<_,i32>(0),"application":r.get::<_,String>(1)})).collect::<Vec<_>>()}));
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }).await.unwrap_or(false)
    };
    let (a, b, blocked) = tokio::join!(
        observed(
            &journal,
            start,
            0,
            0,
            "claim",
            first.claim("p7ct_worker0", 30000)
        ),
        observed(
            &journal,
            start,
            0,
            1,
            "claim",
            second.claim("p7ct_worker1", 30000)
        ),
        monitor
    );
    // Always release before asserting outcomes; neither timed-out request is retried or acked.
    let release = timeout(Duration::from_secs(2), blocker.batch_execute("ROLLBACK")).await;
    let released = matches!(release, Ok(Ok(())));
    journal.record(json!({"kind":"lock_release","at_us":start.elapsed().as_micros(),"rollback_confirmed":released}));
    drop(blocker);
    drop(blocker_connection);
    assert!(
        released && blocked,
        "missing bounded release or actual PG blocking proof"
    );
    assert_eq!(
        a.unwrap_err(),
        "unknown operation timeout; journal retained"
    );
    assert_eq!(
        b.unwrap_err(),
        "unknown operation timeout; journal retained"
    );
    // Dropping a Rust future does not prove server cancellation. Wait for both PG queries to
    // finish after release; only the empty-table final state is asserted, never 'not injected'.
    timeout(Duration::from_secs(5), async {
        loop {
            let active:i64=sql.query_one("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name IN ('p7ct_worker0','p7ct_worker1') AND state='active'",&[]).await.unwrap().get(0);
            if active==0 { break; }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }).await.expect("peer server queries still active after release");
    let remaining: i64 = sql
        .query_one("SELECT count(*) FROM dbproxy_outbox", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(remaining, 0);
    journal.record(json!({"kind":"timeout_result","at_us":start.elapsed().as_micros(),"status":"EXPECTED_CLAIM_TIMEOUT_ONLY","unknown":2,"retries":0,"acks":0,"remaining":remaining}));
    journal.seal(&output);
}
