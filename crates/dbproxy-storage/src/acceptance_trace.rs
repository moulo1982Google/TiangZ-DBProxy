//! Opt-in acceptance build only. No payloads or plain business identifiers.
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    cell::RefCell,
    future::Future,
    sync::atomic::{AtomicBool, Ordering},
    time::Instant,
};

static ENABLED: AtomicBool = AtomicBool::new(false);
tokio::task_local! { static TRACE: RefCell<Trace>; }
#[derive(Serialize)]
struct Span {
    stage: &'static str,
    begin_us: u64,
    end_us: u64,
}
struct Trace {
    start: Instant,
    spans: Vec<Span>,
}

/// Called only by the acceptance example, never by the production entry point.
pub fn enable() {
    ENABLED.store(true, Ordering::Relaxed);
}

pub async fn capture<T>(operation_id: &str, future: impl Future<Output = T>) -> T {
    if !ENABLED.load(Ordering::Relaxed) {
        return future.await;
    }
    let digest = format!("{:x}", Sha256::digest(operation_id.as_bytes()));
    TRACE
        .scope(
            RefCell::new(Trace {
                start: Instant::now(),
                spans: Vec::with_capacity(32),
            }),
            async {
                let result = future.await;
                TRACE.with(|cell| {
                    let t = cell.borrow();
                    let total_us = t.start.elapsed().as_micros();
                    let output_at = Instant::now();
                    let row = serde_json::json!({"schema_version":2,"operation_sha256":digest,
                "total_us":total_us,"spans":t.spans});
                    tracing::info!("ACCEPTANCE_TX_TRACE {}", row);
                    let output_us = output_at.elapsed().as_micros();
                    // This second record's own output remains outside the measured interval.
                    tracing::info!(
                        "ACCEPTANCE_TX_OUTPUT {}",
                        serde_json::json!({
                            "schema_version":1,"operation_sha256":digest,"output_us":output_us
                        })
                    );
                });
                result
            },
        )
        .await
}

pub(crate) fn record(stage: &'static str, started: Instant, ended: Instant) {
    let _ = TRACE.try_with(|cell| {
        let mut t = cell.borrow_mut();
        assert!(t.spans.len() < 64, "acceptance trace span bound exceeded");
        let begin_us = started.duration_since(t.start).as_micros() as u64;
        let end_us = ended.duration_since(t.start).as_micros() as u64;
        t.spans.push(Span {
            stage,
            begin_us,
            end_us,
        });
    });
}

pub(crate) struct CommitTimer(Instant);
impl CommitTimer {
    pub(crate) fn start() -> Self {
        Self(Instant::now())
    }
}
impl Drop for CommitTimer {
    fn drop(&mut self) {
        record("single_transaction_commit", self.0, Instant::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn spans_are_nested_in_the_request_scope() {
        TRACE
            .scope(
                RefCell::new(Trace {
                    start: Instant::now(),
                    spans: vec![],
                }),
                async {
                    let before = Instant::now();
                    tokio::task::yield_now().await;
                    record("phase", before, Instant::now());
                    TRACE.with(|t| {
                        let t = t.borrow();
                        assert_eq!(t.spans.len(), 1);
                        assert!(t.spans[0].end_us >= t.spans[0].begin_us);
                    });
                },
            )
            .await;
        record("outside", Instant::now(), Instant::now());
    }
}
