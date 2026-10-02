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
mod tests {
    use super::*;
    use tiangz_dbproxy_core::{AsyncSnapshotStore, RecordKey};
    #[test]
    fn bounded_waiters_survive_cancellation_and_overflow() {
        let diagnostics = Diagnostics::default();
        let waiters: Vec<_> = (0..140)
            .map(|_| diagnostics.wait("load", None, 1))
            .collect();
        let view = diagnostics.inspect();
        assert_eq!(view["waiter_count"], 140);
        assert_eq!(view["untracked_waiters"], 12);
        drop(waiters);
        assert_eq!(diagnostics.inspect()["waiter_count"], 0);
    }

    #[tokio::test]
    async fn cancelled_waiter_never_becomes_a_timeout_or_holder() {
        let (store, _arrived, _peer) =
            crate::latency_path_tests::postgres(Arc::new(crate::StorageMetrics::default())).await;
        let holder = store
            .request_client("controlled_holder", Some("private-id"), 4)
            .await
            .unwrap();
        let mut future = Box::pin(store.request_client("load", None, 1));
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(future.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(store.client.diagnostics.inspect()["waiter_count"], 1);
        drop(future);
        let evidence = store.client.diagnostics.inspect();
        assert_eq!(evidence["waiter_count"], 0);
        assert_eq!(
            evidence["holder"]["request"]["operation"],
            "controlled_holder"
        );
        assert_eq!(
            store
                .metrics
                .latency_snapshot()
                .iter()
                .find(|s| s.stage == "postgres_connection_wait")
                .unwrap()
                .timeouts,
            0
        );
        drop(holder);
        for _ in 0..20 {
            drop(store.request_client("load", None, 1).await.unwrap());
        }
        assert_eq!(
            store.client.diagnostics.inspect()["recent_holds"]
                .as_array()
                .unwrap()
                .len(),
            HISTORY
        );
    }

    #[tokio::test]
    #[ignore = "requires dedicated PostgreSQL and schema migration opt-in"]
    async fn real_postgres_identity_reconnect_and_contention() {
        tokio::time::timeout(Duration::from_secs(30), async {
            assert_eq!(std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(), Ok("1"));
            let url = std::env::var("DBPROXY_TEST_POSTGRES_URL").unwrap();
            let store = crate::PostgresSnapshotStore::connect_with_request_config(&url, crate::PostgresRequestConfig {
                connection_wait_timeout: Duration::from_millis(40), reconnect_cooldown: Duration::from_millis(50) }).await.unwrap();
            store.identify_connection(Some(3)).await.unwrap();
            let (monitor, driver) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await.unwrap();
            let driver = tokio::spawn(driver);
            let before = store.client.diagnostics.inspect();
            let pid = before["backend_pid"].as_i64().unwrap() as i32;
            let application: String = monitor.query_one("SELECT application_name FROM pg_stat_activity WHERE pid=$1", &[&pid]).await.unwrap().get(0);
            assert_eq!(application, format!("tzdb:{}:request:3", std::process::id()));
            let holder_store = store.clone();
            let holder = tokio::spawn(async move {
                let client = holder_store.request_client("backlog_save_batch", Some("controlled-operation"), 100).await.unwrap();
                client.simple_query("SELECT pg_sleep(0.6)").await.unwrap();
            });
            loop {
                let sleeping: bool = monitor.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event='PgSleep')", &[&pid]).await.unwrap().get(0);
                if sleeping { break; }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let record = RecordKey::new("diagnostics", "missing").unwrap();
            assert!(matches!(store.load(&record).await, Err(StorageError::PostgresConnectionWaitTimeout { timeout_ms: 40 })));
            let blocked = store.client.diagnostics.inspect();
            assert_eq!(blocked["holder"]["request"]["operation"], "backlog_save_batch");
            assert_eq!(blocked["holder"]["request"]["batch_size"], 100);
            assert_eq!(blocked["backend_pid"], pid);
            assert_eq!(blocked["waiter_count"], 0);
            assert_eq!(store.metrics.latency_snapshot().iter().find(|s| s.stage=="postgres_connection_wait").unwrap().timeouts, 1);
            holder.await.unwrap();
            let released = store.client.diagnostics.inspect();
            assert!(released["recent_holds"].as_array().unwrap().iter().any(|h| h["request"]["operation"]=="backlog_save_batch" && h["held_ms"].as_u64().unwrap()>=600));
            monitor.query_one("SELECT pg_terminate_backend($1)", &[&pid]).await.unwrap();
            while !store.client.lock().await.is_closed() { tokio::task::yield_now().await; }
            assert!(store.load(&record).await.unwrap().is_none());
            let after = store.client.diagnostics.inspect();
            assert_eq!(after["connection_generation"], 2);
            let new_pid = after["backend_pid"].as_i64().unwrap() as i32;
            assert_ne!(pid, new_pid);
            let new_application: String = monitor.query_one("SELECT application_name FROM pg_stat_activity WHERE pid=$1", &[&new_pid]).await.unwrap().get(0);
            assert_eq!(new_application, application);
            println!("PG_DIAGNOSTICS_REAL {}", serde_json::json!({"blocked":blocked,"released":released,"reconnected":after}));
            driver.abort();
        }).await.expect("real diagnostic fixture deadline");
    }
}
