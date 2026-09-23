//! Bounded client timing channel with a continuous drain. Overflow is counted and fails the
//! run; nothing is dropped silently. The SDK callback never blocks on file IO.
use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tiangz_dbproxy_client::{
    ClientConnectionOutcome, ClientObserver, ClientRequestOutcome, ClientRequestTiming,
};
use tokio::sync::mpsc;

/// Queue between SDK callbacks and the writer. At 200 requests/s the drain empties it far
/// faster than it fills; the run fails if it ever overflows.
pub const CHANNEL_CAPACITY: usize = 8_192;
/// Flush the buffered writer after this many events so an aborted run keeps its evidence.
const FLUSH_EVERY: u64 = 1_024;

#[derive(serde::Serialize)]
pub struct Event {
    unix_us: u128,
    operation: &'static str,
    queue_us: u128,
    exchange_us: u128,
}
pub enum Message {
    Event(Event),
    Stop,
}
pub struct Timings {
    sender: mpsc::Sender<Message>,
    pub dropped: AtomicU64,
}
impl Timings {
    pub fn new() -> (Arc<Self>, mpsc::Receiver<Message>) {
        let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
        (
            Arc::new(Self {
                sender,
                dropped: AtomicU64::new(0),
            }),
            receiver,
        )
    }
    /// Stop the drain after everything queued before this call; later events are ignored.
    pub async fn finish(&self) -> bool {
        self.sender.send(Message::Stop).await.is_ok()
    }
}
impl ClientObserver for Timings {
    fn connection_attempt(&self, _: usize, _: Duration, _: ClientConnectionOutcome) {}
    fn endpoint_failover(&self, _: usize, _: usize) {}
    fn request_attempt(&self, _: usize, _: &'static str, _: Duration, _: ClientRequestOutcome) {}
    fn request_attempt_timed(
        &self,
        _: usize,
        operation: &'static str,
        timing: ClientRequestTiming,
        _: ClientRequestOutcome,
    ) {
        let event = Event {
            unix_us: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros(),
            operation,
            queue_us: timing.queue_wait.as_micros(),
            exchange_us: timing.exchange.as_micros(),
        };
        if self.sender.try_send(Message::Event(event)).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Observation overhead of the drain itself, reported next to the load results.
#[derive(serde::Serialize, Clone, Debug, Default)]
pub struct DrainStats {
    pub written: u64,
    pub write_total_us: u64,
    pub write_max_us: u64,
    /// Largest queue depth seen when an event was taken, including that event.
    pub max_queue_len: usize,
    pub flushes: u64,
    pub stopped_by_signal: bool,
}

/// Write every event as it arrives on a blocking thread; the file is created exclusively.
pub fn spawn_drain(
    mut receiver: mpsc::Receiver<Message>,
    path: PathBuf,
) -> tokio::task::JoinHandle<std::io::Result<DrainStats>> {
    tokio::task::spawn_blocking(move || {
        let mut file = BufWriter::new(OpenOptions::new().write(true).create_new(true).open(path)?);
        let mut stats = DrainStats::default();
        let mut since_flush = 0_u64;
        while let Some(message) = receiver.blocking_recv() {
            let event = match message {
                Message::Event(event) => event,
                Message::Stop => {
                    stats.stopped_by_signal = true;
                    break;
                }
            };
            stats.max_queue_len = stats.max_queue_len.max(receiver.len() + 1);
            let started = Instant::now();
            serde_json::to_writer(&mut file, &event)?;
            file.write_all(b"\n")?;
            since_flush += 1;
            if since_flush >= FLUSH_EVERY {
                file.flush()?;
                since_flush = 0;
                stats.flushes += 1;
            }
            let elapsed = started.elapsed().as_micros() as u64;
            stats.written += 1;
            stats.write_total_us += elapsed;
            stats.write_max_us = stats.write_max_us.max(elapsed);
        }
        file.flush()?;
        file.get_ref().sync_all()?;
        Ok(stats)
    })
}
