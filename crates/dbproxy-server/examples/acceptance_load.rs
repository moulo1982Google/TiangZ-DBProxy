//! Fixed-rate ordinary read/write probe with bounded in-flight requests and durable evidence.
//! Deliberately not the full six-operation acceptance workload.
#[path = "support/client_timing.rs"]
mod client_timing;
use serde_json::json;
use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
    sync::Arc,
    time::Duration,
};
use tiangz_dbproxy_client::{ClientConfig, DbProxyClientPool};
use tiangz_dbproxy_core::{RecordKey, Revision, SnapshotWrite, SnapshotWriteOutcome};
use tokio::{sync::Semaphore, task::JoinSet, time::Instant};

fn number(name: &str) -> u64 {
    std::env::var(name).unwrap().parse().unwrap()
}
fn request(run: &str, n: u64, bytes: usize) -> SnapshotWrite {
    SnapshotWrite {
        request_id: format!("{run}-{n}"),
        record: RecordKey::new("accept-load", format!("{run}-{n}")).unwrap(),
        schema: "accept".into(),
        schema_version: 1,
        payload: vec![(n % 251) as u8; bytes],
        expected_revision: Some(Revision::ZERO),
        updated_at_unix_ms: 1,
    }
}
fn percentile(values: &mut [u64], percent: usize) -> u64 {
    values.sort_unstable();
    if values.is_empty() {
        return 0;
    }
    values[((values.len() * percent).div_ceil(100) - 1).min(values.len() - 1)]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::path::PathBuf::from(std::env::var("ACCEPT_ARTIFACTS")?);
    std::fs::create_dir_all(&dir)?;
    let mut ledger = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dir.join("requests.jsonl"))?,
    );
    let run = std::env::var("ACCEPT_RUN")?;
    let rate = number("ACCEPT_RATE");
    let seconds = number("ACCEPT_SECONDS");
    let warmup = number("ACCEPT_WARMUP_SECONDS");
    let bytes = number("ACCEPT_PAYLOAD_BYTES") as usize;
    let concurrency = number("ACCEPT_CONCURRENCY") as usize;
    let pacing = std::env::var("ACCEPT_PACING_TIMER").unwrap_or_else(|_| "tokio".into());
    if pacing != "tokio" && pacing != "std" {
        return Err("ACCEPT_PACING_TIMER must be tokio or std".into());
    }
    // Fault mode only moves the error/not-sent verdict to the per-phase analysis of the fault
    // run; data mismatches and lost diagnostics still fail here. Normal runs are unchanged.
    let fault_mode = std::env::var("ACCEPT_FAULT_MODE").as_deref() == Ok("1");
    assert!(rate > 0 && seconds > 0 && concurrency > 0 && (1..=16384).contains(&bytes));
    let mut config = ClientConfig::new(
        std::env::var("ACCEPT_LISTEN")?,
        "acceptance-performance-token",
        "fixed-rate-acceptance",
    );
    config.request_timeout = Duration::from_secs(10);
    let (timing_observer, timing_events) = client_timing::Timings::new();
    // Diagnostic events are written continuously from the start, not buffered until the end.
    let timing_drain = client_timing::spawn_drain(timing_events, dir.join("client-timings.jsonl"));
    config = config.with_observer(timing_observer.clone());
    let client_connections =
        std::env::var("ACCEPT_CLIENT_CONNECTIONS").unwrap_or_else(|_| "shared4".into());
    let pool = match client_connections.as_str() {
        "shared4" => DbProxyClientPool::connect(config, 4).await?,
        "shared8" => DbProxyClientPool::connect(config, 8).await?,
        "split2" => DbProxyClientPool::connect_split(config, 2, 2).await?,
        "split4" => DbProxyClientPool::connect_split(config, 4, 4).await?,
        _ => {
            return Err(
                "ACCEPT_CLIENT_CONNECTIONS must be shared4, shared8, split2, or split4".into(),
            );
        }
    };
    println!("CLIENT_CONNECTIONS={client_connections}");
    let seed = request(&run, 0, bytes);
    assert_eq!(
        pool.save(seed.clone()).await?,
        SnapshotWriteOutcome::Applied {
            revision: Revision(1)
        }
    );
    writeln!(
        ledger,
        "{}",
        json!({"kind":"manifest","run":run,"client_connections":client_connections,"pacing_timer":pacing,"rate":rate,"warmup_seconds":warmup,"sample_seconds":seconds,"payload_bytes":bytes,"concurrency":concurrency,"mix":"50% immutable reads, 50% distinct-key CAS writes","payload_rule":"n modulo 251 repeated payload_bytes","expected_revision":0})
    )?;
    ledger.flush()?;
    let slots = Arc::new(Semaphore::new(concurrency));
    let start = Instant::now();
    let start_unix_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_micros();
    let mut tasks = JoinSet::new();
    let mut outcomes = Vec::new();
    let mut dropped = 0;
    let mut sample_dropped = 0;
    for n in 1..=(warmup + seconds) * rate {
        let scheduled = start + Duration::from_secs_f64((n - 1) as f64 / rate as f64);
        let before_sleep = Instant::now();
        if pacing == "std" {
            // Only the probe's main thread sleeps; SDK IO and request tasks use runtime workers.
            std::thread::sleep(scheduled.saturating_duration_since(Instant::now()));
        } else {
            tokio::time::sleep_until(scheduled).await;
        }
        let awakened = Instant::now();
        while let Some(result) = tasks.try_join_next() {
            outcomes.push(result?);
        }
        let sample = n > warmup * rate;
        let Ok(permit) = slots.clone().try_acquire_owned() else {
            dropped += 1;
            sample_dropped += u64::from(sample);
            writeln!(
                ledger,
                "{}",
                json!({"kind":"not_sent","n":n,"sample":sample,"reason":"in_flight_limit"})
            )?;
            continue;
        };
        let intent_started = Instant::now();
        // Record intent before dispatch; no response is never classified as a definite failure.
        writeln!(
            ledger,
            "{}",
            json!({"kind":"intent","n":n,"sample":sample,"op":if n%2==0 {"save"} else {"load"},"scheduled_us":scheduled.duration_since(start).as_micros()})
        )?;
        ledger.flush()?;
        let intent_finished = Instant::now();
        let pool = pool.clone();
        let run = run.clone();
        let seed = seed.clone();
        let spawned = Instant::now();
        tasks.spawn(async move {
            let _permit = permit;
            let sent = Instant::now();
            let outcome: Result<bool, String> = if n.is_multiple_of(2) {
                pool.save(request(&run, n, bytes))
                    .await
                    .map(|x| {
                        x == SnapshotWriteOutcome::Applied {
                            revision: Revision(1),
                        }
                    })
                    .map_err(|e| e.to_string())
            } else {
                pool.load(&seed.record)
                    .await
                    .map(|x| {
                        x.is_some_and(|x| x.revision == Revision(1) && x.payload == seed.payload)
                    })
                    .map_err(|e| e.to_string())
            };
            (
                n,
                sample,
                sent.duration_since(scheduled).as_micros() as u64,
                scheduled.elapsed().as_micros() as u64,
                outcome,
                before_sleep
                    .saturating_duration_since(scheduled)
                    .as_micros() as u64,
                awakened.saturating_duration_since(scheduled).as_micros() as u64,
                intent_finished.duration_since(intent_started).as_micros() as u64,
                sent.duration_since(spawned).as_micros() as u64,
            )
        });
        // Bound retained completion records; responses are written throughout the run.
        for row in outcomes.drain(..) {
            writeln!(
                ledger,
                "{}",
                json!({"kind":"response","n":row.0,"sample":row.1,"dispatch_delay_us":row.2,"end_to_end_us":row.3,"outcome":row.4,"pre_sleep_late_us":row.5,"wake_late_us":row.6,"intent_write_us":row.7,"task_start_wait_us":row.8})
            )?;
        }
    }
    while let Some(result) = tasks.join_next().await {
        outcomes.push(result?);
    }
    for row in outcomes {
        writeln!(
            ledger,
            "{}",
            json!({"kind":"response","n":row.0,"sample":row.1,"dispatch_delay_us":row.2,"end_to_end_us":row.3,"outcome":row.4,"pre_sleep_late_us":row.5,"wake_late_us":row.6,"intent_write_us":row.7,"task_start_wait_us":row.8})
        )?;
    }
    ledger.flush()?;
    ledger.get_ref().sync_all()?;
    let actual_elapsed_seconds = start.elapsed().as_secs_f64();
    // Reconcile all dispatched writes, including unknown responses, outside the timed phase.
    let text = std::fs::read_to_string(dir.join("requests.jsonl"))?;
    let mut latencies = Vec::new();
    let mut delays = Vec::new();
    let mut errors = 0;
    let mut checked = 0;
    let mut mismatches = 0;
    let mut checks = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dir.join("reconciliation.jsonl"))?,
    );
    let mut writes = Vec::new();
    for line in text.lines() {
        let row: serde_json::Value = serde_json::from_str(line)?;
        if row["kind"] != "response" {
            continue;
        }
        let n = row["n"].as_u64().unwrap();
        if row["outcome"]["Ok"] != true {
            errors += 1;
        }
        if row["sample"] == true {
            latencies.push(row["end_to_end_us"].as_u64().unwrap());
            delays.push(row["dispatch_delay_us"].as_u64().unwrap());
        }
        if n.is_multiple_of(2) {
            writes.push((n, row["outcome"]["Ok"] == true));
        }
    }
    drop(text);
    // Read back every dispatched write with bounded concurrency, so long runs reconcile in minutes
    // rather than one round trip at a time. Order of the check file does not matter.
    let reconcile_concurrency = std::env::var("ACCEPT_RECONCILE_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(32)
        .max(1);
    let reconcile_started = Instant::now();
    let mut pending = writes.into_iter();
    let mut checking = JoinSet::new();
    loop {
        while checking.len() < reconcile_concurrency {
            let Some((n, ok)) = pending.next() else { break };
            let pool = pool.clone();
            let expected = request(&run, n, bytes);
            checking.spawn(async move {
                let saved = pool.load(&expected.record).await;
                (n, ok, expected, saved)
            });
        }
        let Some(result) = checking.join_next().await else {
            break;
        };
        let (n, ok, expected, saved) = result?;
        let saved = saved?;
        let committed = saved.is_some();
        // Whatever exists must be exactly the intended write; a confirmed write must exist.
        // An errored or unknown write may be absent (rolled back) or committed, never half-written.
        let consistent =
            saved.is_none_or(|s| s.revision == Revision(1) && s.payload == expected.payload);
        let matches = consistent && (committed || !ok);
        checked += 1;
        mismatches += u64::from(!matches);
        writeln!(
            checks,
            "{}",
            json!({"n":n,"matches":matches,"outcome_ok":ok,"committed":committed})
        )?;
    }
    let reconcile_seconds = reconcile_started.elapsed().as_secs_f64();
    checks.flush()?;
    checks.get_ref().sync_all()?;
    let timing_dropped = timing_observer
        .dropped
        .load(std::sync::atomic::Ordering::Relaxed);
    // Stop the drain after every event queued so far; the file has been written throughout.
    let stop_started = Instant::now();
    if !timing_observer.finish().await {
        return Err("client timing channel closed before the run finished".into());
    }
    let drain = tokio::time::timeout(Duration::from_secs(30), timing_drain)
        .await
        .map_err(|_| "client timing drain did not stop within 30 s")?;
    let drain_stats = drain??;
    let timing_stop_us = stop_started.elapsed().as_micros() as u64;
    let summary = json!({"run":run,"target_rate":rate,"sample_seconds":seconds,"actual_elapsed_seconds_including_warmup_and_drain":actual_elapsed_seconds,"sample_responses":latencies.len(),"sample_not_sent":sample_dropped,"all_not_sent":dropped,"all_response_errors_or_wrong_data":errors,"writes_checked":checked,"mismatches":mismatches,"p50_us":percentile(&mut latencies,50),"p95_us":percentile(&mut latencies,95),"p99_us":percentile(&mut latencies,99),"dispatch_p99_us":percentile(&mut delays,99),"full_acceptance":false,"pacing_timer":pacing,"start_unix_us":start_unix_us,"client_timing_dropped":timing_dropped,"client_timing_channel_capacity":client_timing::CHANNEL_CAPACITY,"client_timing_drain":drain_stats,"client_timing_stop_us":timing_stop_us,"fault_mode":fault_mode,"reconcile_seconds":reconcile_seconds,"reconcile_concurrency":reconcile_concurrency});
    std::fs::write(
        dir.join("summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    println!("{summary}");
    if mismatches > 0 || timing_dropped > 0 || !drain_stats.stopped_by_signal {
        return Err("probe did not pass; inspect preserved evidence".into());
    }
    if !fault_mode && (errors > 0 || dropped > 0) {
        return Err("probe did not pass; inspect preserved evidence".into());
    }
    Ok(())
}
