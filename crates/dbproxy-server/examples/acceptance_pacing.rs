//! Timer-only control: no network, database, task spawning or file IO during measurement.
use serde_json::json;
use std::{fs::OpenOptions, io::Write, time::Duration};
use tokio::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mode = std::env::var("ACCEPT_PACING_TIMER").unwrap_or_else(|_| "tokio".into());
    if mode != "tokio" && mode != "std" {
        return Err("ACCEPT_PACING_TIMER must be tokio or std".into());
    }
    let path = std::env::var("ACCEPT_PACING_OUTPUT")?;
    let mut output = OpenOptions::new().write(true).create_new(true).open(path)?;
    let rate = 200_u64;
    let count = 4000_u64;
    let mut rows = Vec::with_capacity(count as usize);
    let start = Instant::now();
    for n in 0..count {
        let scheduled = start + Duration::from_secs_f64(n as f64 / rate as f64);
        let before = Instant::now();
        if mode == "std" {
            // This control runs alone on main; no server or asynchronous work is blocked.
            std::thread::sleep(scheduled.saturating_duration_since(Instant::now()));
        } else {
            tokio::time::sleep_until(scheduled).await;
        }
        let after = Instant::now();
        rows.push((
            n + 1,
            before.saturating_duration_since(scheduled).as_micros(),
            after.saturating_duration_since(scheduled).as_micros(),
        ));
    }
    for (n, pre_sleep_late_us, wake_late_us) in rows {
        writeln!(
            output,
            "{}",
            json!({"n":n,"pre_sleep_late_us":pre_sleep_late_us,"wake_late_us":wake_late_us})
        )?;
    }
    output.sync_all()?;
    Ok(())
}
