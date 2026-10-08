//! Bounded connection ownership evidence; does not cancel SQL or change queue policy.
use crate::{ReconnectingPostgresClient, StorageError, duration_millis};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, MutexGuard};

const MAX_WAITERS: usize = 128;
const HISTORY: usize = 8;
const SLOW_HOLD: Duration = Duration::from_millis(500);

/// Shared by all clones and maintenance queues using this exact physical connection.
pub(crate) struct PostgresConnection {
    inner: Mutex<ReconnectingPostgresClient>,
    pub(crate) diagnostics: Arc<Diagnostics>,
}

impl PostgresConnection {
    pub(crate) fn new(client: ReconnectingPostgresClient) -> Self {
        Self {
            diagnostics: client.diagnostics.clone(),
            inner: Mutex::new(client),
        }
    }

    #[cfg(test)]
    pub(crate) async fn lock(&self) -> PostgresGuard<'_> {
        self.lock_for("unclassified", None, 0, None)
            .await
            .expect("unbounded mutex acquisition")
    }

    pub(crate) async fn lock_for(
        &self,
        operation: &'static str,
        correlation: Option<&str>,
        batch_size: usize,
        wait: Option<Duration>,
    ) -> Result<PostgresGuard<'_>, StorageError> {
        let mut waiter = self.diagnostics.wait(operation, correlation, batch_size);
        let inner = match wait {
            Some(budget) => match tokio::time::timeout(budget, self.inner.lock()).await {
                Ok(inner) => inner,
                Err(_) => {
                    self.diagnostics.log("queue_timeout", Some(&waiter.entry));
                    return Err(StorageError::PostgresConnectionWaitTimeout {
                        timeout_ms: duration_millis(budget),
                    });
                }
            },
            None => self.inner.lock().await,
        };
        let entry = waiter.acquire();
        Ok(PostgresGuard {
            inner,
            diagnostics: &self.diagnostics,
            entry,
        })
    }
}

pub(crate) struct PostgresGuard<'a> {
    inner: MutexGuard<'a, ReconnectingPostgresClient>,
    diagnostics: &'a Diagnostics,
    entry: Entry,
}
impl Deref for PostgresGuard<'_> {
    type Target = ReconnectingPostgresClient;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl DerefMut for PostgresGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
impl Drop for PostgresGuard<'_> {
    fn drop(&mut self) {
        // Finish before the actual mutex unlock. A subsequent holder cannot be erased.
        let held = self.entry.started.elapsed();
        {
            let mut state = self
                .diagnostics
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            state.holder = None;
            if state.history.len() == HISTORY {
                state.history.pop_front();
            }
            state.history.push_back(Completed {
                request: self.entry.view(),
                held_ms: duration_millis(held),
            });
        }
        if held >= SLOW_HOLD {
            self.diagnostics
                .log("slow_hold_released", Some(&self.entry));
        }
    }
}

#[derive(Clone)]
struct Entry {
    sequence: u64,
    operation: &'static str,
    correlation: Option<String>,
    batch_size: usize,
    started: Instant,
}
impl Entry {
    fn view(&self) -> RequestView {
        RequestView {
            sequence: self.sequence,
            operation: self.operation,
            correlation: self.correlation.clone(),
            batch_size: self.batch_size,
        }
    }
}
#[derive(Clone, Serialize)]
struct RequestView {
    sequence: u64,
    operation: &'static str,
    correlation: Option<String>,
    batch_size: usize,
}
#[derive(Serialize)]
struct Completed {
    request: RequestView,
    held_ms: u64,
}
#[derive(Serialize)]
struct Active {
    request: RequestView,
    age_ms: u64,
}
#[derive(Serialize)]
pub(crate) struct Snapshot {
    role: &'static str,
    shard: Option<usize>,
    backend_pid: Option<i32>,
    connection_generation: u64,
    holder: Option<Active>,
    waiter_count: usize,
    oldest_tracked_wait_ms: Option<u64>,
    untracked_waiters: usize,
    recent_holds: Vec<Completed>,
    suppressed_logs: u64,
}
struct State {
    role: &'static str,
    shard: Option<usize>,
    backend_pid: Option<i32>,
    generation: u64,
    sequence: u64,
    holder: Option<Entry>,
    waiters: VecDeque<Entry>,
    untracked: usize,
    history: VecDeque<Completed>,
    last_log: [Option<Instant>; 2],
    suppressed: [u64; 2],
}
pub(crate) struct Diagnostics {
    state: StdMutex<State>,
}
impl Default for Diagnostics {
    fn default() -> Self {
        Self {
            state: StdMutex::new(State {
                role: "standalone",
                shard: None,
                backend_pid: None,
                generation: 0,
                sequence: 0,
                holder: None,
                waiters: VecDeque::with_capacity(MAX_WAITERS),
                untracked: 0,
                history: VecDeque::with_capacity(HISTORY),
                last_log: [None; 2],
                suppressed: [0; 2],
            }),
        }
    }
}
impl Diagnostics {
    pub(crate) fn identify(&self, shard: Option<usize>) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.role = if shard.is_some() {
            "request"
        } else {
            "maintenance"
        };
        state.shard = shard;
        // Startup migration observations must not consume the first serving warning.
        state.last_log = [None; 2];
        state.suppressed = [0; 2];
    }
    pub(crate) fn connected(&self, pid: Option<i32>) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.backend_pid = pid;
        state.generation += 1;
        tracing::info!(role = state.role, shard = ?state.shard, backend_pid = ?pid, connection_generation = state.generation, "postgres diagnostic connection identified");
    }
    pub(crate) fn disconnected(&self) {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .backend_pid = None;
    }
    fn wait(
        &self,
        operation: &'static str,
        correlation: Option<&str>,
        batch_size: usize,
    ) -> Waiter<'_> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.sequence += 1;
        let entry = Entry {
            sequence: state.sequence,
            operation,
            correlation: correlation.map(fingerprint),
            batch_size,
            started: Instant::now(),
        };
        let tracked = state.waiters.len() < MAX_WAITERS;
        if tracked {
            state.waiters.push_back(entry.clone());
        } else {
            state.untracked += 1;
        }
        Waiter {
            diagnostics: self,
            entry,
            tracked,
            active: true,
        }
    }
    fn snapshot(state: &State) -> Snapshot {
        Snapshot {
            role: state.role,
            shard: state.shard,
            backend_pid: state.backend_pid,
            connection_generation: state.generation,
            holder: state.holder.as_ref().map(|e| Active {
                request: e.view(),
                age_ms: duration_millis(e.started.elapsed()),
            }),
            waiter_count: state.waiters.len() + state.untracked,
            oldest_tracked_wait_ms: state
                .waiters
                .front()
                .map(|e| duration_millis(e.started.elapsed())),
            untracked_waiters: state.untracked,
            recent_holds: state
                .history
                .iter()
                .map(|e| Completed {
                    request: e.request.clone(),
                    held_ms: e.held_ms,
                })
                .collect(),
            suppressed_logs: state.suppressed.iter().sum(),
        }
    }
    fn log(&self, reason: &'static str, request: Option<&Entry>) {
        // A queue warning must not consume the later full-duration holder-release warning.
        let category = usize::from(reason == "slow_hold_released");
        let snapshot = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.last_log[category].is_some_and(|t| t.elapsed() < Duration::from_secs(1)) {
                state.suppressed[category] += 1;
                return;
            }
            let value = Self::snapshot(&state);
            state.last_log[category] = Some(Instant::now());
            state.suppressed[category] = 0;
            value
        };
        if let Ok(context) = serde_json::to_string(&snapshot) {
            let request = request.and_then(|e| {
                serde_json::to_string(&Active {
                    request: e.view(),
                    age_ms: duration_millis(e.started.elapsed()),
                })
                .ok()
            });
            tracing::warn!(reason, context, request = ?request, "postgres connection contention");
        }
    }
    #[cfg(test)]
    pub(crate) fn inspect(&self) -> serde_json::Value {
        serde_json::to_value(Self::snapshot(
            &self.state.lock().unwrap_or_else(|p| p.into_inner()),
        ))
        .unwrap()
    }
}
struct Waiter<'a> {
    diagnostics: &'a Diagnostics,
    entry: Entry,
    tracked: bool,
    active: bool,
}
impl Waiter<'_> {
    fn remove(&self, state: &mut State) {
        if self.tracked {
            state.waiters.retain(|e| e.sequence != self.entry.sequence);
        } else {
            state.untracked -= 1;
        }
    }
    fn acquire(&mut self) -> Entry {
        let mut state = self
            .diagnostics
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        self.remove(&mut state);
        self.active = false;
        let mut entry = self.entry.clone();
        entry.started = Instant::now();
        state.holder = Some(entry.clone());
        entry
    }
}
impl Drop for Waiter<'_> {
    fn drop(&mut self) {
        if self.active {
            let mut state = self
                .diagnostics
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            self.remove(&mut state);
        }
    }
}

/// Correlate a retry without logging business identifiers or introducing metric labels.
pub fn fingerprint(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    digest[..8].iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
#[path = "postgres_diagnostics_tests.rs"]
mod tests;
