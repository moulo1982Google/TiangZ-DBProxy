//! Small, fixed-budget two-connection claim acceptance; never a capacity test.
use serde_json::{Value, json};
use std::{collections::HashSet, fs::File, future::Future, io::Write, sync::Mutex, time::Duration};
use tiangz_dbproxy_storage::{OutboxRoute, PostgresSnapshotStore};
use tokio::time::{Instant, sleep_until, timeout};

struct Journal(Mutex<File>);
impl Journal {
    fn record(&self, value: Value) {
        let mut file = self.0.lock().unwrap();
        writeln!(file, "{value}").unwrap();
        file.sync_data().unwrap();
    }
}

#[derive(Default)]
struct WaveGuard {
    stopped: bool,
}
impl WaveGuard {
    fn admit(&mut self, journal: &Journal, wave: u64, scheduled: u64, dispatch: u64) -> bool {
        if self.stopped {
            return false;
        }
        if dispatch.saturating_sub(scheduled) > 100_000 {
            self.stopped = true;
            journal.record(json!({"kind":"guard","wave":wave,"reason":"dispatch_lag"}));
            return false;
        }
        true
    }
}

// A timeout can follow a committed database operation. Preserve unknown, fail,
// and never retry it or silently turn it into an empty claim.
async fn observed<T, E: std::fmt::Debug>(
    journal: &Journal,
    start: Instant,
    wave: u64,
    worker: usize,
    operation: &str,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, &'static str> {
    journal.record(json!({"kind":"started","wave":wave,"worker":worker,"operation":operation,"at_us":start.elapsed().as_micros()}));
    let begin = start.elapsed().as_micros();
    let result = timeout(Duration::from_secs(5), future).await;
    let end = start.elapsed().as_micros();
    let outcome = if matches!(&result, Ok(Ok(_))) {
        "completed"
    } else {
        "unknown"
    };
    journal.record(json!({"kind":"operation","wave":wave,"worker":worker,"operation":operation,"begin_us":begin,"end_us":end,"outcome":outcome}));
    result
        .map_err(|_| "unknown operation timeout; journal retained")?
        .map_err(|_| "unknown operation error; journal retained")
}

fn local_journal(name: &str) -> (Journal, std::path::PathBuf) {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/parallel-local-tests");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{name}-{}-{nonce}.jsonl", std::process::id()));
    let file = File::options()
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    (Journal(Mutex::new(file)), path)
}

fn journal_rows(path: &std::path::Path) -> Vec<Value> {
    let raw = std::fs::read_to_string(path).unwrap();
    assert!(raw.ends_with('\n'));
    raw.lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test]
async fn guard_rejects_late_wave_and_remains_stopped() {
    let (journal, path) = local_journal("guard");
    let mut guard = WaveGuard::default();
    assert!(guard.admit(&journal, 0, 0, 100_000)); // Exact original boundary.
    let start = Instant::now();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!guard.admit(&journal, 1, 0, start.elapsed().as_micros() as u64));
    assert!(!guard.admit(&journal, 2, 500_000, 500_000)); // No recovery/retry.
    let rows = journal_rows(&path);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["kind"], "guard");
    assert_eq!(rows[0]["wave"], 1);
}

#[tokio::test]
async fn both_unknown_timeouts_are_retained_before_rejection() {
    for operation in ["claim", "ack"] {
        let (journal, path) = local_journal(operation);
        let start = Instant::now();
        let (first, second) = tokio::join!(
            observed(
                &journal,
                start,
                0,
                0,
                operation,
                std::future::pending::<Result<(), ()>>()
            ),
            observed(
                &journal,
                start,
                0,
                1,
                operation,
                std::future::pending::<Result<(), ()>>()
            )
        );
        assert!(first.is_err() && second.is_err());
        assert!(start.elapsed() >= Duration::from_secs(5));
        let rows = journal_rows(&path);
        assert_eq!(rows.len(), 4);
        for worker in [0, 1] {
            assert_eq!(
                rows.iter()
                    .filter(|r| r["worker"] == worker && r["kind"] == "started")
                    .count(),
                1
            );
            let result = rows
                .iter()
                .find(|r| r["worker"] == worker && r["kind"] == "operation")
                .unwrap();
            assert_eq!(result["outcome"], "unknown");
            assert_eq!(result["operation"], operation);
        }
    }
}

#[tokio::test]
async fn failed_worker_does_not_cancel_delayed_peer() {
    let (journal, path) = local_journal("peer");
    let start = Instant::now();
    let (first, second) = tokio::join!(
        observed(&journal, start, 0, 0, "claim", async {
            Err::<(), _>("simulated error")
        }),
        observed(&journal, start, 0, 1, "claim", async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok::<_, ()>(42)
        })
    );
    assert!(first.is_err());
    assert_eq!(second.unwrap(), 42);
    let rows = journal_rows(&path);
    assert_eq!(rows.len(), 4);
    assert!(
        rows.iter()
            .any(|v| v["worker"] == 0 && v["outcome"] == "unknown")
    );
    assert!(
        rows.iter()
            .any(|v| v["worker"] == 1 && v["outcome"] == "completed")
    );
}

#[tokio::test]
#[ignore = "requires a fresh dedicated PostgreSQL database"]
async fn two_workers_fixed_budget() {
    assert_eq!(
        std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(),
        Ok("1")
    );
    let url = std::env::var("DBPROXY_TEST_POSTGRES_URL").unwrap();
    let output = std::path::PathBuf::from(std::env::var("P07_OUTPUT").unwrap());
    std::fs::create_dir(&output).unwrap();
    let journal = Journal(Mutex::new(
        File::create(output.join("journal.jsonl")).unwrap(),
    ));
    // Intentionally smoke-only: formal timing/distributions require a separate review.
    let warmup = 2_u64;
    let sample = 5_u64;
    let waves = (warmup + sample) * 2;
    let store = PostgresSnapshotStore::connect(&url).await.unwrap();
    let setup = store.outbox_queue();
    let (sql, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    assert_eq!(
        sql.query_one("SELECT count(*) FROM dbproxy_outbox", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    sql.batch_execute("SET statement_timeout='5s'; INSERT INTO dbproxy_operation_claims(operation_id,operation_kind) VALUES('parallel','multi')").await.unwrap();
    let publishers = ["parallel-a", "parallel-b"];
    for publisher in publishers {
        setup
            .register_publisher(publisher, "isolated-no-mq-test")
            .await
            .unwrap();
        let route = OutboxRoute {
            producer: publisher.into(),
            publisher: publisher.into(),
            version: 1,
            destination: "parallel-destination".into(),
        };
        setup.register_route(&route).await.unwrap();
        // Two FIFO partitions per publisher; names deliberately shared across publishers.
        sql.execute("INSERT INTO dbproxy_outbox(event_id,operation_id,topic,partition_key,payload,occurred_at_unix_ms) SELECT $1::text||'-'||n,'parallel',$2,'key-'||(n%2),'',0 FROM generate_series(0,499) n ORDER BY n", &[&publisher, &route.key()]).await.unwrap();
    }
    sql.batch_execute("ANALYZE dbproxy_outbox").await.unwrap();
    let first = PostgresSnapshotStore::connect_existing(&url)
        .await
        .unwrap()
        .outbox_queue();
    let second = PostgresSnapshotStore::connect_existing(&url)
        .await
        .unwrap()
        .outbox_queue();
    let start = Instant::now();
    let mut seen = HashSet::new();
    let mut next = [[0_u64, 1_u64]; 2];
    let mut overlaps = 0;
    let mut guard = WaveGuard::default();
    for wave in 0..waves {
        let scheduled = wave * 500_000;
        sleep_until(start + Duration::from_micros(scheduled)).await;
        let dispatch = start.elapsed().as_micros() as u64;
        journal.record(json!({"kind":"wave","wave":wave,"scheduled_us":scheduled,"dispatch_us":dispatch,"publisher":publishers[(wave%2) as usize],"sample":wave>=warmup*2}));
        if !guard.admit(&journal, wave, scheduled, dispatch) {
            panic!("guard stopped new wave; evidence retained");
        }
        let publisher = publishers[(wave % 2) as usize];
        let before = start.elapsed().as_micros();
        let (a, b) = tokio::join!(
            observed(
                &journal,
                start,
                wave,
                0,
                "claim",
                first.claim_for_publisher("parallel-worker-0", 30_000, Some(publisher))
            ),
            observed(
                &journal,
                start,
                wave,
                1,
                "claim",
                second.claim_for_publisher("parallel-worker-1", 30_000, Some(publisher))
            )
        );
        let leases = [
            a.expect("claim outcome unknown")
                .expect("ready partition required"),
            b.expect("claim outcome unknown")
                .expect("second ready partition required"),
        ];
        assert_ne!(leases[0].event.partition_key, leases[1].event.partition_key);
        for (worker, lease) in leases.iter().enumerate() {
            assert_eq!(lease.publisher_id, publisher);
            assert_eq!(lease.destination, "parallel-destination");
            assert!(seen.insert(lease.event.event_id.clone()));
            let key: usize = lease
                .event
                .partition_key
                .strip_prefix("key-")
                .unwrap()
                .parse()
                .unwrap();
            assert!(key < 2);
            let expected = &mut next[(wave % 2) as usize][key];
            assert_eq!(lease.event.event_id, format!("{publisher}-{expected}"));
            *expected += 2;
            journal.record(json!({"kind":"lease","wave":wave,"worker":worker,"publisher":lease.publisher_id,"event":lease.event.event_id,"partition":lease.event.partition_key,"token":lease.lease_token,"destination":lease.destination}));
        }
        // No acknowledge starts until both claims returned. Next wave waits for both acknowledgements.
        let (a, b) = tokio::join!(
            observed(
                &journal,
                start,
                wave,
                0,
                "ack",
                first.acknowledge(&leases[0])
            ),
            observed(
                &journal,
                start,
                wave,
                1,
                "ack",
                second.acknowledge(&leases[1])
            )
        );
        assert!(a.expect("ack outcome unknown") && b.expect("ack outcome unknown"));
        journal.record(json!({"kind":"wave_completed","wave":wave,"begin_us":before,"end_us":start.elapsed().as_micros()}));
    }
    sleep_until(start + Duration::from_secs(warmup + sample)).await;
    let rows = sql
        .query(
            "SELECT event_id,published_at IS NOT NULL FROM dbproxy_outbox ORDER BY enqueue_order",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1000);
    for row in rows {
        let event: String = row.get(0);
        let published: bool = row.get(1);
        assert_eq!(published, seen.contains(&event));
        journal.record(json!({"kind":"final","event":event,"published":published}));
    }
    // Actual overlap is checked using operation boundaries, not join!/barrier presence.
    let raw = std::fs::read_to_string(output.join("journal.jsonl")).unwrap();
    let values: Vec<Value> = raw
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    for wave in 0..waves {
        let claims: Vec<_> = values
            .iter()
            .filter(|v| v["kind"] == "operation" && v["operation"] == "claim" && v["wave"] == wave)
            .collect();
        if claims[0]["begin_us"]
            .as_u64()
            .unwrap()
            .max(claims[1]["begin_us"].as_u64().unwrap())
            < claims[0]["end_us"]
                .as_u64()
                .unwrap()
                .min(claims[1]["end_us"].as_u64().unwrap())
        {
            overlaps += 1;
        }
    }
    assert!(overlaps > 0, "no observed concurrent claim intervals");
    journal.record(json!({"kind":"result","status":"SMOKE_ONLY","workers":2,"publishers":2,"rows":1000,"waves":waves,"claims":seen.len(),"overlap_waves":overlaps,"warmup_seconds":warmup,"sample_seconds":sample,"stats":false}));
    connection.abort();
}
