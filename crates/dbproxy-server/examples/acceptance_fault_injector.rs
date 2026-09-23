//! Single fault injection during a fixed-rate load run, with exact timestamps for phase analysis.
//! Only touches the dedicated `fault_load_*` database named twice in the environment; never a
//! production switch. Faults: `blocked_write` (one ordinary write held in its PostgreSQL
//! transaction by an advisory lock) or `kill_connections` (terminate this database's DBProxy
//! backends once).
use serde_json::json;
use std::{
    io::Write,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const BARRIER_LOCK: i64 = 827_616;

fn unix_us() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
}

fn number(name: &str) -> Result<u64, Box<dyn std::error::Error>> {
    Ok(std::env::var(name)?.parse()?)
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
    if kind != "blocked_write" && kind != "kill_connections" {
        return Err("ACCEPT_FAULT_KIND must be blocked_write or kill_connections".into());
    }
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
    let launched = Instant::now();
    writeln!(
        events,
        "{}",
        json!({"kind":"plan","unix_us":unix_us(),"fault":kind,"database":database,"start_after_seconds":start_after.as_secs(),"hold_seconds":hold.as_secs(),"rate":rate})
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
