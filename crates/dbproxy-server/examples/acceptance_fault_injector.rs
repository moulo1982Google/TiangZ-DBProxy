//! Single fault injection during a fixed-rate load run, with exact timestamps for phase analysis.
//! Only touches the dedicated `fault_load_*` database named twice in the environment; never a
//! production switch. Faults: `blocked_write` (one ordinary write held in its PostgreSQL
//! transaction by an advisory lock), `kill_connections` (terminate this database's DBProxy
//! backends once), or the relay faults `pg_pause` / `pg_delay` (F06): the host reaches PostgreSQL
//! only through a TCP relay run here, which stops forwarding (connections stay open, like a
//! network blackhole) or adds a fixed delay per direction for the fault window, then resumes.
use serde_json::json;
use std::{
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{mpsc, watch},
};

const BARRIER_LOCK: i64 = 827_616;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Mode {
    Pass,
    Pause,
    Delay(Duration),
}

#[derive(Default)]
struct RelayStats {
    connections: AtomicU64,
    buffered: AtomicI64,
    peak_buffered: AtomicI64,
}

struct Relay {
    mode: watch::Sender<Mode>,
    stats: Arc<RelayStats>,
}

/// Plain byte relay; every accepted connection is counted so reconnect storms are visible.
async fn start_relay(listen: &str, target: String) -> std::io::Result<Relay> {
    let listener = TcpListener::bind(listen).await?;
    let (mode, receiver) = watch::channel(Mode::Pass);
    let stats = Arc::new(RelayStats::default());
    let accept_stats = stats.clone();
    tokio::spawn(async move {
        while let Ok((inbound, _)) = listener.accept().await {
            accept_stats.connections.fetch_add(1, Ordering::Relaxed);
            let (target, mode, stats) = (target.clone(), receiver.clone(), accept_stats.clone());
            tokio::spawn(async move {
                let Ok(outbound) = TcpStream::connect(&target).await else {
                    return;
                };
                let _ = inbound.set_nodelay(true);
                let _ = outbound.set_nodelay(true);
                let (from_client, to_client) = inbound.into_split();
                let (from_server, to_server) = outbound.into_split();
                tokio::join!(
                    pipe(from_client, to_server, mode.clone(), stats.clone()),
                    pipe(from_server, to_client, mode, stats)
                );
            });
        }
    });
    Ok(Relay { mode, stats })
}

/// Reads eagerly and forwards in order; a delay is stamped when bytes arrive, and a pause holds
/// every write until the mode changes. Buffered bytes are tracked to bound memory evidence.
async fn pipe(
    mut from: OwnedReadHalf,
    mut to: OwnedWriteHalf,
    mode: watch::Receiver<Mode>,
    stats: Arc<RelayStats>,
) {
    let (sender, mut queue) = mpsc::unbounded_channel::<(tokio::time::Instant, Vec<u8>)>();
    let reader = {
        let (mode, stats) = (mode.clone(), stats.clone());
        async move {
            let mut buffer = vec![0u8; 64 * 1024];
            while let Ok(read @ 1..) = from.read(&mut buffer).await {
                let delay = match *mode.borrow() {
                    Mode::Delay(delay) => delay,
                    _ => Duration::ZERO,
                };
                let now = stats.buffered.fetch_add(read as i64, Ordering::Relaxed) + read as i64;
                stats.peak_buffered.fetch_max(now, Ordering::Relaxed);
                let due = tokio::time::Instant::now() + delay;
                if sender.send((due, buffer[..read].to_vec())).is_err() {
                    break;
                }
            }
        }
    };
    let writer = async move {
        let mut mode = mode;
        while let Some((due, bytes)) = queue.recv().await {
            // Only a real delay may touch the timer: an unconditional sleep costs about 1 ms per
            // chunk, which added ~7 ms to every multi-round-trip write in the f06_*_a runs.
            if due > tokio::time::Instant::now() {
                tokio::time::sleep_until(due).await;
            }
            loop {
                let paused = *mode.borrow_and_update() == Mode::Pause;
                if !paused || mode.changed().await.is_err() {
                    break;
                }
            }
            stats
                .buffered
                .fetch_sub(bytes.len() as i64, Ordering::Relaxed);
            if to.write_all(&bytes).await.is_err() {
                break;
            }
        }
        let _ = to.shutdown().await;
    };
    tokio::join!(reader, writer);
}

fn unix_us() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
}

fn number(name: &str) -> Result<u64, Box<dyn std::error::Error>> {
    Ok(std::env::var(name)?.parse()?)
}

/// Terminate this database's DBProxy sessions, sparing this one and the host's sampler.
async fn terminate_dbproxy_backends(
    sql: &tokio_postgres::Client,
) -> Result<i64, tokio_postgres::Error> {
    Ok(sql.query_one("SELECT count(*) FILTER (WHERE pg_terminate_backend(pid)) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND backend_type='client backend' AND application_name<>'acceptance-sampler'", &[]).await?.get(0))
}

/// `df -B1` of the ballast directory, kept verbatim as evidence.
fn disk_free(dir: &std::path::Path) -> String {
    std::process::Command::new("df")
        .arg("-B1")
        .arg(dir)
        .output()
        .map(|out| String::from_utf8_lossy(&out.stdout).into_owned())
        .unwrap_or_else(|error| error.to_string())
}

/// Latest intent number the load tool has flushed so far; `None` before the first intent.
fn latest_intent(ledger: &std::path::Path) -> Option<u64> {
    let text = std::fs::read_to_string(ledger).ok()?;
    text.lines()
        .rev()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|row| row["kind"] == "intent")
        .and_then(|row| row["n"].as_u64())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let kind = std::env::var("ACCEPT_FAULT_KIND")?;
    const KINDS: [&str; 7] = [
        "blocked_write",
        "kill_connections",
        "pg_pause",
        "pg_delay",
        "read_only",
        "disk_full",
        "external",
    ];
    if !KINDS.contains(&kind.as_str()) {
        return Err(format!("ACCEPT_FAULT_KIND must be one of {KINDS:?}").into());
    }
    let relayed = kind.starts_with("pg_");
    let start_after = Duration::from_secs(number("ACCEPT_FAULT_START_SECONDS")?);
    let hold = Duration::from_secs(number("ACCEPT_FAULT_DURATION_SECONDS")?);
    let rate = number("ACCEPT_RATE")?;
    let run = std::env::var("ACCEPT_RUN")?;
    let artifacts = std::path::PathBuf::from(std::env::var("ACCEPT_ARTIFACTS")?);
    let ledger = artifacts.join("requests.jsonl");
    let mut events = std::io::BufWriter::new(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(artifacts.join("fault-events.jsonl"))?,
    );
    let url = std::env::var("DBPROXY_TEST_POSTGRES_URL")?;
    let (sql, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    // Two independent guards: the database must be named explicitly and carry the fault prefix.
    let database: String = sql
        .query_one("SELECT current_database()", &[])
        .await?
        .get(0);
    if std::env::var("DBPROXY_FAULT_DATABASE")? != database || !database.starts_with("fault_load_")
    {
        return Err("DBPROXY_FAULT_DATABASE must equal the connected fault_load_* database".into());
    }
    sql.batch_execute("SET statement_timeout='2s'").await?;
    let delay = Duration::from_millis(number("ACCEPT_PROXY_DELAY_MS").unwrap_or(0));
    let relay = if relayed {
        let relay = start_relay(
            &std::env::var("ACCEPT_PROXY_LISTEN")?,
            std::env::var("ACCEPT_PROXY_TARGET")?,
        )
        .await?;
        // The runner starts the host only after this line, so the relay carries every connection.
        println!("PROXY_READY");
        std::io::stdout().flush()?;
        // The fault clock starts with the load, not with this earlier launch.
        let waiting = Instant::now();
        while latest_intent(&ledger).is_none() {
            if waiting.elapsed() > Duration::from_secs(180) {
                return Err("load tool never started".into());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Some(relay)
    } else {
        None
    };
    let launched = Instant::now();
    writeln!(
        events,
        "{}",
        json!({"kind":"plan","unix_us":unix_us(),"fault":kind,"database":database,"start_after_seconds":start_after.as_secs(),"hold_seconds":hold.as_secs(),"rate":rate,"relay_delay_ms":delay.as_millis() as u64})
    )?;
    events.flush()?;
    tokio::time::sleep(start_after.saturating_sub(launched.elapsed())).await;
    match kind.as_str() {
        "blocked_write" => {
            // Target a write about one second ahead of the load tool's current position, so the
            // barrier is in place before that request is sent and no other write is affected.
            let latest =
                latest_intent(&ledger).ok_or("load tool has not written any intent yet")?;
            let mut target = latest + rate.max(1);
            if !target.is_multiple_of(2) {
                target += 1;
            }
            let target_request_id = format!("{run}-{target}");
            sql.execute("SELECT pg_advisory_lock($1)", &[&BARRIER_LOCK])
                .await?;
            sql.batch_execute(&format!(
                "CREATE FUNCTION accept_fault_barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
                 IF NEW.request_id = '{target_request_id}' THEN PERFORM pg_advisory_xact_lock({BARRIER_LOCK}); END IF;
                 RETURN NEW; END $$;
                 CREATE TRIGGER accept_fault_barrier BEFORE INSERT ON dbproxy_idempotency FOR EACH ROW EXECUTE FUNCTION accept_fault_barrier()"
            )).await?;
            let injected = Instant::now();
            writeln!(
                events,
                "{}",
                json!({"kind":"injected","unix_us":unix_us(),"fault":kind,"target_request_id":target_request_id,"latest_intent_n":latest,"target_n":target})
            )?;
            events.flush()?;
            let mut observed_blocked = None;
            while injected.elapsed() < hold {
                if observed_blocked.is_none() {
                    let rows = sql.query("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event='advisory'", &[]).await?;
                    if let Some(row) = rows.first() {
                        observed_blocked = Some(row.get::<_, i32>(0));
                        writeln!(
                            events,
                            "{}",
                            json!({"kind":"observed_blocked","unix_us":unix_us(),"pid":observed_blocked,"waiting_backends":rows.len(),"after_injection_us":injected.elapsed().as_micros()})
                        )?;
                        events.flush()?;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            sql.execute("SELECT pg_advisory_unlock($1)", &[&BARRIER_LOCK])
                .await?;
            let released_unix_us = unix_us();
            let released = Instant::now();
            let mut still_waiting = true;
            while released.elapsed() < Duration::from_secs(5) && still_waiting {
                still_waiting = sql.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event='advisory')", &[]).await?.get(0);
                if still_waiting {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            sql.batch_execute("DROP TRIGGER accept_fault_barrier ON dbproxy_idempotency; DROP FUNCTION accept_fault_barrier()").await?;
            writeln!(
                events,
                "{}",
                json!({"kind":"released","unix_us":released_unix_us,"fault":kind,"target_request_id":target_request_id,"target_was_blocked":observed_blocked.is_some(),"blocked_pid":observed_blocked,"waiter_cleared_after_us":released.elapsed().as_micros(),"waiter_still_waiting":still_waiting})
            )?;
            events.flush()?;
            println!(
                "FAULT_INJECTED fault=blocked_write target={target_request_id} blocked={} hold_seconds={}",
                observed_blocked.is_some(),
                hold.as_secs()
            );
            if observed_blocked.is_none() {
                return Err(
                    "the targeted write never reached the barrier; no fault was injected".into(),
                );
            }
        }
        "pg_pause" | "pg_delay" => {
            let relay = relay.ok_or("relay missing")?;
            let before = relay.stats.connections.load(Ordering::Relaxed);
            let mode = if kind == "pg_pause" {
                Mode::Pause
            } else {
                Mode::Delay(delay)
            };
            relay.mode.send_replace(mode);
            writeln!(
                events,
                "{}",
                json!({"kind":"injected","unix_us":unix_us(),"fault":kind,"relay_connections_before":before,"relay_delay_ms":delay.as_millis() as u64})
            )?;
            events.flush()?;
            tokio::time::sleep(hold).await;
            relay.mode.send_replace(Mode::Pass);
            let released_unix_us = unix_us();
            let peak = relay.stats.peak_buffered.load(Ordering::Relaxed);
            let released = Instant::now();
            while relay.stats.buffered.load(Ordering::Relaxed) > 0
                && released.elapsed() < Duration::from_secs(10)
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let during = relay.stats.connections.load(Ordering::Relaxed) - before;
            writeln!(
                events,
                "{}",
                json!({"kind":"released","unix_us":released_unix_us,"fault":kind,"relay_connections_before":before,
                    "relay_connections_opened_during_fault":during,"peak_buffered_bytes":peak,
                    "buffer_drained_after_us":released.elapsed().as_micros(),"buffered_bytes_left":relay.stats.buffered.load(Ordering::Relaxed)})
            )?;
            events.flush()?;
            println!(
                "FAULT_INJECTED fault={kind} relay_connections_before={before} opened_during_fault={during} peak_buffered_bytes={peak}"
            );
            if before == 0 {
                return Err(
                    "the host never connected through the relay; no fault was injected".into(),
                );
            }
            // Keep relaying through reconciliation and host shutdown until the runner says stop.
            let stop = std::path::PathBuf::from(std::env::var("ACCEPT_PROXY_STOP_FILE")?);
            let waiting = Instant::now();
            while !stop.exists() && waiting.elapsed() < Duration::from_secs(900) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            writeln!(
                events,
                "{}",
                json!({"kind":"relay_stopped","unix_us":unix_us(),"relay_connections_total":relay.stats.connections.load(Ordering::Relaxed),
                    "relay_connections_after_release":relay.stats.connections.load(Ordering::Relaxed) - before - during})
            )?;
        }
        "read_only" => {
            // F10 read-only: new sessions of this database start read-only; cutting the current
            // DBProxy sessions makes them reconnect into it. Reversed the same way.
            sql.batch_execute(&format!(
                "ALTER DATABASE \"{database}\" SET default_transaction_read_only = on"
            ))
            .await?;
            let killed = terminate_dbproxy_backends(&sql).await?;
            writeln!(
                events,
                "{}",
                json!({"kind":"injected","unix_us":unix_us(),"fault":kind,"killed":killed})
            )?;
            events.flush()?;
            tokio::time::sleep(hold).await;
            sql.batch_execute(&format!(
                "ALTER DATABASE \"{database}\" RESET default_transaction_read_only"
            ))
            .await?;
            let killed_on_release = terminate_dbproxy_backends(&sql).await?;
            writeln!(
                events,
                "{}",
                json!({"kind":"released","unix_us":unix_us(),"fault":kind,"killed":killed,"killed_on_release":killed_on_release})
            )?;
            events.flush()?;
            println!(
                "FAULT_INJECTED fault=read_only killed={killed} killed_on_release={killed_on_release}"
            );
            if killed == 0 {
                return Err("no DBProxy backend was connected; no fault was injected".into());
            }
        }
        "external" => {
            // The fault itself is done on the host (e.g. `docker network disconnect` in
            // run_f06_netcut.sh), which this container cannot do. Handshake through two files in
            // the artifact folder: `fault-go` asks the host to inject, `fault-release` reports it
            // has undone the fault. The host writes `unix_us=<n>` first: the instant just before
            // it started the fault and just after it undid it (containers share the host clock).
            // f06_cut_*_a used this side's later observation instead, so a few sends that the
            // cut had already blocked were counted as missed in the normal phase.
            let host_time = |note: &str| {
                note.split_whitespace()
                    .next()
                    .and_then(|first| first.strip_prefix("unix_us="))
                    .and_then(|value| value.parse::<u128>().ok())
            };
            let go = artifacts.join("fault-go");
            let release = artifacts.join("fault-release");
            std::fs::write(&go, b"")?;
            let asked = Instant::now();
            let injected = artifacts.join("fault-injected");
            while !injected.exists() {
                if asked.elapsed() > Duration::from_secs(30) {
                    return Err("the host never injected the external fault".into());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let note = std::fs::read_to_string(&injected).unwrap_or_default();
            let seen = unix_us();
            writeln!(
                events,
                "{}",
                json!({"kind":"injected","unix_us":host_time(&note).unwrap_or(seen),"seen_unix_us":seen,"fault":kind,"host_note":note})
            )?;
            events.flush()?;
            let waiting = Instant::now();
            while !release.exists() {
                if waiting.elapsed() > hold + Duration::from_secs(120) {
                    return Err("the host never released the external fault".into());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let note = std::fs::read_to_string(&release).unwrap_or_default();
            let seen = unix_us();
            writeln!(
                events,
                "{}",
                json!({"kind":"released","unix_us":host_time(&note).unwrap_or(seen),"seen_unix_us":seen,"fault":kind,"host_note":note})
            )?;
            events.flush()?;
            println!(
                "FAULT_INJECTED fault=external held_ms={}",
                waiting.elapsed().as_millis()
            );
        }
        "disk_full" => {
            // F10 disk full: the PostgreSQL data directory lives on a small tmpfs shared with
            // this container; a ballast file takes every free byte, then is deleted. The real
            // disk is never involved: the runner refuses anything but a tmpfs mount.
            let dir = std::path::PathBuf::from(std::env::var("ACCEPT_BALLAST_DIR")?);
            let path = dir.join("ballast");
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?;
            let chunk = vec![0u8; 1 << 20];
            let mut filled = 0u64;
            let full_error = loop {
                match file.write_all(&chunk).and_then(|_| file.flush()) {
                    Ok(()) => filled += chunk.len() as u64,
                    Err(error) => break error.to_string(),
                }
                if filled > 64 << 30 {
                    return Err("ballast passed 64 GiB; refusing to continue".into());
                }
            };
            let df = disk_free(&dir);
            writeln!(
                events,
                "{}",
                json!({"kind":"injected","unix_us":unix_us(),"fault":kind,"ballast_bytes":filled,"full_error":full_error,"df":df})
            )?;
            events.flush()?;
            tokio::time::sleep(hold).await;
            drop(file);
            std::fs::remove_file(&path)?;
            let released_unix_us = unix_us();
            writeln!(
                events,
                "{}",
                json!({"kind":"released","unix_us":released_unix_us,"fault":kind,"ballast_bytes":filled,"df":disk_free(&dir)})
            )?;
            events.flush()?;
            println!("FAULT_INJECTED fault=disk_full ballast_bytes={filled}");
        }
        _ => {
            // Terminate the DBProxy backends of this database once; keep this session and the
            // host's diagnostic sampler so the evidence stream continues.
            let rows = sql.query("SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND backend_type='client backend' AND application_name<>'acceptance-sampler'", &[]).await?;
            let now = unix_us();
            let killed: Vec<i32> = rows
                .iter()
                .filter(|r| r.get::<_, bool>(1))
                .map(|r| r.get::<_, i32>(0))
                .collect();
            writeln!(
                events,
                "{}",
                json!({"kind":"injected","unix_us":now,"fault":kind,"killed_pids":killed,"killed":killed.len()})
            )?;
            writeln!(
                events,
                "{}",
                json!({"kind":"released","unix_us":now,"fault":kind,"killed":killed.len()})
            )?;
            events.flush()?;
            println!(
                "FAULT_INJECTED fault=kill_connections killed={}",
                killed.len()
            );
            if killed.is_empty() {
                return Err("no DBProxy backend was connected; no fault was injected".into());
            }
        }
    }
    events.flush()?;
    events.get_ref().sync_all()?;
    Ok(())
}
