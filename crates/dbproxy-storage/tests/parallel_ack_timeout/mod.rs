//! Dedicated row-lock ack timeout acceptance; never a normal load entry point.
use super::*;
struct ConnectionGuard(tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>);
impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}
fn isolated(url: &str, run: &str) -> bool {
    run.starts_with("p7at_")
        && (6..=19).contains(&run.len())
        && run
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        && url
            .parse::<tokio_postgres::Config>()
            .is_ok_and(|c| c.get_dbname() == Some(run))
}
#[test]
fn dedicated_target() {
    assert!(isolated("postgres://localhost/p7at_local", "p7at_local"));
    for (url, run) in [
        ("postgres://localhost/postgres", "p7at_local"),
        ("postgres://localhost/p7at_other", "p7at_local"),
        ("postgres://localhost/p7ct_local", "p7ct_local"),
        ("invalid", "p7at_local"),
    ] {
        assert!(!isolated(url, run));
    }
}
async fn rows(sql: &tokio_postgres::Client) -> Vec<Value> {
    sql.query("SELECT event_id,publisher_id,partition_key,lease_token,lease_owner,published_at IS NOT NULL,lease_until IS NOT NULL,coalesce(lease_until>clock_timestamp(),false),attempt_count FROM dbproxy_outbox ORDER BY event_id", &[]).await.unwrap().iter().map(|r|json!({"event_id":r.get::<_,String>(0),"publisher":r.get::<_,String>(1),"partition_key":r.get::<_,String>(2),"token":r.get::<_,i64>(3),"owner":r.get::<_,Option<String>>(4),"published":r.get::<_,bool>(5),"lease_present":r.get::<_,bool>(6),"lease_valid":r.get::<_,bool>(7),"attempt_count":r.get::<_,i64>(8)})).collect()
}
async fn proofs(sql: &tokio_postgres::Client, blocker: i32) -> Vec<Value> {
    sql.query("SELECT datname,pid,application_name,state,wait_event_type,pg_blocking_pids(pid) FROM pg_stat_activity WHERE datname=current_database() AND application_name IN ('p7at_worker0','p7at_worker1') AND state='active' AND wait_event_type='Lock' AND $1=ANY(pg_blocking_pids(pid)) ORDER BY application_name", &[&blocker]).await.unwrap().iter().map(|r|json!({"database":r.get::<_,String>(0),"pid":r.get::<_,i32>(1),"application":r.get::<_,String>(2),"state":r.get::<_,String>(3),"wait_event_type":r.get::<_,String>(4),"blocking_pids":r.get::<_,Vec<i32>>(5)})).collect()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dedicated fresh p7at_ database only"]
async fn real_ack_timeout() {
    let url = std::env::var("DBPROXY_TEST_POSTGRES_URL").unwrap();
    let run = std::env::var("P07_TIMEOUT_RUN_ID").unwrap();
    assert!(isolated(&url, &run));
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
    let _monitor = ConnectionGuard(tokio::spawn(connection));
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
    let setup = PostgresSnapshotStore::connect(&url)
        .await
        .unwrap()
        .outbox_queue();
    sql.batch_execute("SET statement_timeout='2s'; INSERT INTO dbproxy_operation_claims(operation_id,operation_kind) VALUES('ack-timeout','multi')").await.unwrap();
    setup
        .register_publisher("p7at_publisher", "isolated-no-mq-test")
        .await
        .unwrap();
    let route = OutboxRoute {
        producer: "p7at_publisher".into(),
        publisher: "p7at_publisher".into(),
        version: 1,
        destination: "ack-timeout".into(),
    };
    setup.register_route(&route).await.unwrap();
    for worker in 0..2 {
        sql.execute("INSERT INTO dbproxy_outbox(event_id,operation_id,topic,partition_key,payload,occurred_at_unix_ms,attempt_count) VALUES($1,'ack-timeout',$2,$3,'',0,0)", &[&format!("{run}_event{worker}"),&route.key(),&format!("p7at_partition{worker}")]).await.unwrap();
    }
    let suffix = if url.contains('?') { '&' } else { '?' };
    let first = PostgresSnapshotStore::connect_existing(&format!(
        "{url}{suffix}application_name=p7at_worker0"
    ))
    .await
    .unwrap()
    .outbox_queue();
    let second = PostgresSnapshotStore::connect_existing(&format!(
        "{url}{suffix}application_name=p7at_worker1"
    ))
    .await
    .unwrap()
    .outbox_queue();
    let start = Instant::now();
    let a = timeout(Duration::from_secs(5), first.claim("p7at_worker0", 30000))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let b = timeout(Duration::from_secs(5), second.claim("p7at_worker1", 30000))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(a.event.event_id, format!("{run}_event0"));
    assert_eq!(b.event.event_id, format!("{run}_event1"));
    let baseline = rows(&sql).await;
    journal.record(json!({"kind":"ack_fixture","schema":1,"run_id":run,"database":run,"leases":baseline,"claim_end_us":start.elapsed().as_micros()}));
    let (blocker, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let blocker_guard = ConnectionGuard(tokio::spawn(connection));
    let pid: i32 = blocker
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    blocker
        .batch_execute(
            "SET lock_timeout='2s'; SET idle_in_transaction_session_timeout='12s'; BEGIN",
        )
        .await
        .unwrap();
    let locked = blocker
        .query(
            "SELECT event_id FROM dbproxy_outbox ORDER BY event_id FOR UPDATE",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(locked.len(), 2);
    journal.record(
        json!({"kind":"lock_acquired","at_us":start.elapsed().as_micros(),"blocker_pid":pid}),
    );
    let monitor = async {
        timeout(Duration::from_secs(2),async {
            loop {
                let p=proofs(&sql,pid).await;
                if p.len()==2 { journal.record(json!({"kind":"blocked_peers","at_us":start.elapsed().as_micros(),"proofs":p})); return true; }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await.unwrap_or(false)
    };
    let (x, y, blocked) = tokio::join!(
        observed(&journal, start, 0, 0, "ack", first.acknowledge(&a)),
        observed(&journal, start, 0, 1, "ack", second.acknowledge(&b)),
        monitor
    );
    // Inspect with MVCC while both UPDATEs remain blocked. Bound the combined reads.
    let before_release = timeout(Duration::from_secs(2), async {
        (proofs(&sql, pid).await, rows(&sql).await)
    })
    .await;
    if let Ok((ref p, ref r)) = before_release {
        journal.record(
            json!({"kind":"after_timeout","at_us":start.elapsed().as_micros(),"proofs":p,"rows":r}),
        );
    }
    let released = matches!(
        timeout(Duration::from_secs(2), blocker.batch_execute("ROLLBACK")).await,
        Ok(Ok(()))
    );
    journal.record(json!({"kind":"lock_release","at_us":start.elapsed().as_micros(),"rollback_confirmed":released}));
    drop(blocker);
    drop(blocker_guard);
    assert!(released && blocked);
    assert_eq!(
        x.unwrap_err(),
        "unknown operation timeout; journal retained"
    );
    assert_eq!(
        y.unwrap_err(),
        "unknown operation timeout; journal retained"
    );
    let (p, r) = before_release.unwrap();
    assert_eq!(p.len(), 2);
    assert_eq!(r, baseline);
    let final_rows=timeout(Duration::from_secs(5),async {
        loop {
            let active:i64=sql.query_one("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name IN ('p7at_worker0','p7at_worker1') AND state='active'",&[]).await.unwrap().get(0);
            if active==0 { return rows(&sql).await; }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await.unwrap();
    journal.record(json!({"kind":"final_state","at_us":start.elapsed().as_micros(),"rows":final_rows,"total_rows":final_rows.len(),"active_peers":0,"retries":0,"extra_claims":0}));
    // Seal observations even when final reconciliation fails; never retry unknown acknowledgements.
    journal.seal(&output);
    assert_eq!(final_rows.len(), 2);
    for (old, new) in baseline.iter().zip(&final_rows) {
        let mut expected = old.clone();
        expected["owner"] = Value::Null;
        expected["published"] = json!(true);
        expected["lease_present"] = json!(false);
        expected["lease_valid"] = json!(false);
        assert_eq!(&expected, new);
    }
}
