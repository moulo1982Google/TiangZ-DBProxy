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
    // No acceptance worker exists yet. Hold only this fresh RunId's automatic rows.
    let pg = sql(url).await;
    let held = pg.execute("UPDATE dbproxy_cache_repairs SET available_at=clock_timestamp()+interval '1 day' WHERE namespace=$1 AND target_revision=2 AND lease_until IS NULL AND lease_owner IS NULL AND dead_lettered_at IS NULL AND attempt_count=0", &[&namespace]).await.unwrap();
    assert_eq!(held, count);
    let snapshots = evidence.join("repair-fixture.json");
    std::fs::write(&snapshots, serde_json::to_vec(&initial_cache).unwrap()).unwrap();
    verify_baseline(url, cache_url, run, evidence, mode, "prepared").await;
    let journal_path = evidence.join("repair-operations.jsonl");
    let mut journal = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(journal_path)
        .unwrap();
    let mode = mode.to_string();
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
            // Both groups perform the same conditional fixture release. Ordinary enqueue
            // preserves available_at, so must NOT be used as a release mechanism.
            let released = recorded(&mut journal, n, "release", tokio::time::Instant::now()+Duration::from_secs(5),
                pg.execute("UPDATE dbproxy_cache_repairs SET available_at=clock_timestamp() WHERE namespace=$1 AND record_key=$2 AND target_revision=2 AND available_at>statement_timestamp() AND lease_until IS NULL AND lease_owner IS NULL AND dead_lettered_at IS NULL AND attempt_count=0", &[&namespace, &record.key])).await;
            assert_eq!(
                released, 1,
                "held repair release did not affect exactly one row"
            );
            let state = queue_state(&pg, &namespace).await;
            observations.push(json!({"n":n,"elapsed_ms":started.elapsed().as_millis(),"release_and_probe_us":sent.elapsed().as_micros(),"pending":state["total"],"queue":state}));
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        let final_queue = loop {
            let state = queue_state(&pg, &namespace).await;
            let active = state["eligible"].as_u64().unwrap()
                + state["leased"].as_u64().unwrap()
                + state["dead"].as_u64().unwrap();
            if active == 0 || Instant::now() >= deadline {
                break state;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        let remaining = final_queue["eligible"].as_u64().unwrap()
            + final_queue["leased"].as_u64().unwrap()
            + final_queue["dead"].as_u64().unwrap();
        let mut mismatches = 0;
        let admitted = observations.len();
        let mut untouched_mismatches = 0;
        let mut queue_mismatches = 0;
        let mut final_items = Vec::new();
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
            let held_row = recorded(&mut journal, n, "read_queue", reconcile_deadline,
                pg.query_opt("SELECT target_revision=2 AND available_at>statement_timestamp() AND lease_until IS NULL AND lease_owner IS NULL AND dead_lettered_at IS NULL AND attempt_count=0 FROM dbproxy_cache_repairs WHERE namespace=$1 AND record_key=$2", &[&namespace,&record.key])).await;
            let queue_valid = if n < admitted {
                held_row.is_none()
            } else {
                held_row.as_ref().is_some_and(|r| r.get::<_, bool>(0))
            };
            queue_mismatches += u64::from(!queue_valid);
            final_items.push(json!({"n":n,"released":n<admitted,"queue_valid":queue_valid,"cache_revision":actual.as_ref().map(|v|v.revision.0),"authority_revision":authoritative.revision.0}));
            if n < admitted {
                mismatches += u64::from(actual.as_ref() != Some(&authoritative));
            } else {
                untouched_mismatches += u64::from(actual != initial_cache[n]);
            }
        }
        json!({"baseline_scope":"held_stale_release","schema_version":3,"final_queue":final_queue,"queue_mismatches":queue_mismatches,"final_items":final_items,"admitted":admitted,"not_injected":count-admitted as u64,"stopped":stopped,"untouched_mismatches":untouched_mismatches,"mode":mode,"targets":count,"remaining":remaining,"mismatches":mismatches,"seconds":started.elapsed().as_secs_f64(),"observations":observations})
    });
    (count, Some(task), Some(tx))
}

// These counters are exclusive: expired leases remain leased until consumed/reset.
async fn queue_state(pg: &tokio_postgres::Client, namespace: &str) -> Value {
    let row = tokio::time::timeout(Duration::from_secs(5), pg.query_one(
        "SELECT count(*), count(*) FILTER (WHERE dead_lettered_at IS NOT NULL), count(*) FILTER (WHERE dead_lettered_at IS NULL AND lease_until IS NOT NULL), count(*) FILTER (WHERE dead_lettered_at IS NULL AND lease_until IS NULL AND available_at>statement_timestamp()), count(*) FILTER (WHERE dead_lettered_at IS NULL AND lease_until IS NULL AND available_at<=statement_timestamp()) FROM dbproxy_cache_repairs WHERE namespace=$1", &[&namespace])).await.expect("repair queue query timed out").unwrap();
    json!({"total":row.get::<_,i64>(0),"dead":row.get::<_,i64>(1),"leased":row.get::<_,i64>(2),"held":row.get::<_,i64>(3),"eligible":row.get::<_,i64>(4)})
}

pub(super) async fn verify_started(
    url: &str,
    cache_url: &str,
    run: &str,
    evidence: &std::path::Path,
    mode: &str,
) {
    if mode != "none" {
        verify_baseline(url, cache_url, run, evidence, mode, "started").await;
    }
}

async fn verify_baseline(
    url: &str,
    cache_url: &str,
    run: &str,
    evidence: &std::path::Path,
    mode: &str,
    phase: &str,
) {
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(evidence.join("repair-baseline.jsonl"))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        let snapshots: Vec<Option<tiangz_dbproxy_core::SnapshotEnvelope>>=serde_json::from_slice(&std::fs::read(evidence.join("repair-fixture.json")).unwrap()).unwrap();
        let store=PostgresSnapshotStore::connect(url).await.unwrap();
        let cache=RedisSnapshotCache::connect(cache_url).await.unwrap();
        let pg=sql(url).await;
        let namespace=format!("mixed-repair-{run}");
        for (n, expected) in snapshots.iter().enumerate() {
            let expected=expected.as_ref().unwrap();
            let authoritative=store.load(&expected.record).await.unwrap().unwrap();
            let actual=cache.get(&expected.record).await.unwrap();
            let matches=actual.as_ref()==Some(expected);
            let queue=pg.query_one("SELECT target_revision=2 AND available_at>statement_timestamp() AND lease_until IS NULL AND lease_owner IS NULL AND dead_lettered_at IS NULL AND attempt_count=0 FROM dbproxy_cache_repairs WHERE namespace=$1 AND record_key=$2", &[&namespace,&expected.record.key]).await.unwrap().get::<_,bool>(0);
            append(&mut file,&json!({"kind":"cache_baseline","phase":phase,"n":n,"revision":authoritative.revision.0,"cache_revision":actual.as_ref().map(|v|v.revision.0),"matches":matches,"held":queue}));
            file.sync_data().unwrap();
            assert_eq!(authoritative.revision,Revision(2));
            assert_eq!(expected.revision,Revision(if mode=="repair" {1} else {2}));
            assert!(matches && queue,"fixture changed before release");
        }
        let state=queue_state(&pg,&namespace).await;
        append(&mut file,&json!({"kind":"baseline_ready","phase":phase,"targets":snapshots.len(),"scope":"held_stale_release","queue":state}));
        file.sync_data().unwrap();
        assert_eq!(state,json!({"total":snapshots.len(),"held":snapshots.len(),"eligible":0,"leased":0,"dead":0}));
    }).await.expect("held repair baseline timed out; retain evidence");
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
