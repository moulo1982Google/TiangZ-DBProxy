//! Bounded, unique repair targets alongside the normal mixed application workload.
use super::*;
use tiangz_dbproxy_core::AsyncSnapshotStore;
use tiangz_dbproxy_storage::{PostgresSnapshotStore, RedisSnapshotCache, SnapshotCacheConfig};

pub(super) async fn prepare(
    url: &str,
    cache_url: &str,
    run: &str,
    seconds: u64,
    evidence: &std::path::Path,
    mode: &str,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> (
    u64,
    Option<tokio::task::JoinHandle<Value>>,
    Option<tokio::sync::oneshot::Sender<()>>,
) {
    if mode == "none" {
        return (0, None, None);
    }
    assert!(mode == "control" || mode == "repair");
    let mut store = PostgresSnapshotStore::connect(url).await.unwrap();
    // Match the explicit experimental server policy: fixture entries must outlive
    // the seven-minute sample and final reconciliation, not expire at five minutes.
    let cache = RedisSnapshotCache::connect_with_metrics_and_policy(
        cache_url,
        Default::default(),
        SnapshotCacheConfig {
            ttl: Duration::from_secs(1800),
            ttl_jitter: Duration::ZERO,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let queue = store.cache_repair_queue();
    let namespace = format!("mixed-repair-{run}");
    let count = seconds * 2;
    let mut records = Vec::new();
    let mut initial_cache = Vec::new();
    for n in 0..count {
        let mut w = write(&namespace, &format!("r-{n}"), 1);
        store.save(w.clone()).await.unwrap();
        cache
            .put(&store.load(&w.record).await.unwrap().unwrap())
            .await
            .unwrap();
        w.request_id.push_str("-new");
        w.expected_revision = Some(Revision(1));
        w.payload = vec![2; 1024];
        store.save(w.clone()).await.unwrap();
        if mode == "control" {
            cache
                .put(&store.load(&w.record).await.unwrap().unwrap())
                .await
                .unwrap();
        }
        initial_cache.push(cache.get(&w.record).await.unwrap());
        records.push(w.record);
    }
    let journal_path = evidence.join("repair-operations.jsonl");
    let mut journal = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(journal_path)
        .unwrap();
    let mode = mode.to_string();
    let pg = sql(url).await;
    let (tx, rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        rx.await.unwrap();
        let started = tokio::time::Instant::now();
        let mut observations = Vec::new();
        let mut stopped = false;
        for (n, record) in records.iter().enumerate() {
            if !admit(&mut stop, started + Duration::from_millis(n as u64 * 500)).await {
                stopped = true;
                break;
            }
            // Admission precedes any await: a stop lets this one admitted item finish.
            let sent = Instant::now();
            if mode == "repair" {
                recorded(
                    &mut journal,
                    n,
                    "enqueue",
                    tokio::time::Instant::now() + Duration::from_secs(5),
                    queue.enqueue(record, Revision(2)),
                )
                .await;
            }
            let pending: i64 = tokio::time::timeout(
                Duration::from_secs(5),
                pg.query_one(
                    "SELECT count(*) FROM dbproxy_cache_repairs WHERE namespace=$1",
                    &[&namespace],
                ),
            )
            .await
            .expect("repair pending query timed out; evidence incomplete")
            .unwrap()
            .get(0);
            observations.push(json!({"n":n,"elapsed_ms":started.elapsed().as_millis(),"enqueue_and_probe_us":sent.elapsed().as_micros(),"pending":pending}));
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        let remaining = loop {
            let pending: i64 = tokio::time::timeout(
                Duration::from_secs(5),
                pg.query_one(
                    "SELECT count(*) FROM dbproxy_cache_repairs WHERE namespace=$1",
                    &[&namespace],
                ),
            )
            .await
            .expect("repair drain query timed out; evidence incomplete")
            .unwrap()
            .get(0);
            if pending == 0 || Instant::now() >= deadline {
                break pending;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        let mut mismatches = 0;
        let admitted = observations.len();
        let mut untouched_mismatches = 0;
        let reconcile_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        for (n, record) in records.iter().enumerate() {
            let authoritative = recorded(
                &mut journal,
                n,
                "read_authoritative",
                reconcile_deadline,
                store.load(record),
            )
            .await
            .unwrap();
            let actual = recorded(
                &mut journal,
                n,
                "read_cache",
                reconcile_deadline,
                cache.get(record),
            )
            .await;
            if n < admitted || mode == "control" {
                mismatches += u64::from(actual.as_ref() != Some(&authoritative));
            } else {
                untouched_mismatches += u64::from(actual != initial_cache[n]);
            }
        }
        json!({"schema_version":2,"admitted":admitted,"not_injected":count-admitted as u64,"stopped":stopped,"untouched_mismatches":untouched_mismatches,"mode":mode,"targets":count,"remaining":remaining,"mismatches":mismatches,"seconds":started.elapsed().as_secs_f64(),"observations":observations})
    });
    (count, Some(task), Some(tx))
}

// A successful return is admission, not completion; do not cancel an admitted enqueue.
async fn admit(stop: &mut tokio::sync::watch::Receiver<bool>, due: tokio::time::Instant) -> bool {
    if *stop.borrow() {
        return false;
    }
    tokio::select! {
        biased;
        _ = stop.changed() => false,
        _ = tokio::time::sleep_until(due) => !*stop.borrow(),
    }
}

#[tokio::test]
async fn repair_admission_stops_before_due_and_never_resumes() {
    let (tx, mut rx) = tokio::sync::watch::channel(false);
    assert!(admit(&mut rx, tokio::time::Instant::now()).await);
    tx.send(true).unwrap();
    assert!(!admit(&mut rx, tokio::time::Instant::now()).await);
    assert!(!admit(&mut rx, tokio::time::Instant::now()).await);
    let (tx, mut rx) = tokio::sync::watch::channel(false);
    let pending = tokio::spawn(async move {
        admit(
            &mut rx,
            tokio::time::Instant::now() + Duration::from_secs(60),
        )
        .await
    });
    tokio::task::yield_now().await;
    tx.send(true).unwrap();
    assert!(
        !tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .unwrap()
            .unwrap()
    );
}

// Persist intent before polling a possibly committing operation. Timeouts are unknown,
// never proof of no commit. A missing terminal record after process loss is unknown too.
async fn recorded<T, E: std::fmt::Debug>(
    journal: &mut std::fs::File,
    n: usize,
    phase: &str,
    deadline: tokio::time::Instant,
    operation: impl std::future::Future<Output = Result<T, E>>,
) -> T {
    append(journal, &json!({"n":n,"phase":phase,"outcome":"started"}));
    journal.sync_data().unwrap();
    match tokio::time::timeout_at(deadline, operation).await {
        Ok(Ok(value)) => {
            append(journal, &json!({"n":n,"phase":phase,"outcome":"completed"}));
            journal.sync_data().unwrap();
            value
        }
        other => {
            let outcome = if other.is_err() {
                "timeout_unknown"
            } else {
                "error_unknown"
            };
            append(journal, &json!({"n":n,"phase":phase,"outcome":outcome}));
            journal.sync_data().unwrap();
            panic!("repair {phase} {outcome}: inspect retained operation journal; do not retry");
        }
    }
}

#[tokio::test]
async fn repair_timeout_keeps_started_item_unknown() {
    let dir = std::env::temp_dir().join(format!(
        "dbproxy-repair-timeout-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("operations.jsonl");
    let mut file = std::fs::File::create(&path).unwrap();
    let task = tokio::spawn(async move {
        recorded(
            &mut file,
            0,
            "enqueue",
            tokio::time::Instant::now() + Duration::from_millis(1),
            std::future::pending::<Result<(), ()>>(),
        )
        .await;
    });
    assert!(task.await.unwrap_err().is_panic());
    let rows: Vec<Value> = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["outcome"], "started");
    assert_eq!(rows[1]["outcome"], "timeout_unknown");
}
