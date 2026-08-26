//! DBProxy 的低基数 Prometheus 指标与独立 HTTP 探针。 / Low-cardinality Prometheus metrics and independent HTTP probes for DBProxy.

use std::{
    fmt::Write as _,
    io,
    net::SocketAddr,
    sync::Arc,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::{Duration, Instant},
};

use tiangz_dbproxy_protocol::wire;
use tiangz_dbproxy_storage::StorageMetricsSnapshot;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinHandle,
    time::timeout,
};

const MAX_HTTP_REQUEST_BYTES: usize = 8 * 1024;
const DURATION_BUCKETS_SECONDS: [f64; 10] = [
    0.0005, 0.001, 0.0025, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 1.0,
];

#[derive(Clone, Copy, Debug)]
pub(crate) enum RpcOperation {
    LoadSnapshot,
    LoadMultiSnapshot,
    SaveSnapshot,
    SaveMultiSnapshot,
    EnqueueSnapshot,
    EnqueueMultiSnapshot,
    ApplyTransaction,
    LoadTransaction,
    ApplyMultiTransaction,
    LoadMultiTransaction,
    ApplyTradeTransaction,
    LoadTrade,
    LoadTradeTransaction,
    Invalid,
}

impl RpcOperation {
    const ALL: [Self; 14] = [
        Self::LoadSnapshot,
        Self::LoadMultiSnapshot,
        Self::SaveSnapshot,
        Self::SaveMultiSnapshot,
        Self::EnqueueSnapshot,
        Self::EnqueueMultiSnapshot,
        Self::ApplyTransaction,
        Self::LoadTransaction,
        Self::ApplyMultiTransaction,
        Self::LoadMultiTransaction,
        Self::ApplyTradeTransaction,
        Self::LoadTrade,
        Self::LoadTradeTransaction,
        Self::Invalid,
    ];

    const fn index(self) -> usize {
        self as usize
    }

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::LoadSnapshot => "load_snapshot",
            Self::LoadMultiSnapshot => "load_multi_snapshot",
            Self::SaveSnapshot => "save_snapshot",
            Self::SaveMultiSnapshot => "save_multi_snapshot",
            Self::EnqueueSnapshot => "enqueue_snapshot",
            Self::EnqueueMultiSnapshot => "enqueue_multi_snapshot",
            Self::ApplyTransaction => "apply_transaction",
            Self::LoadTransaction => "load_transaction",
            Self::ApplyMultiTransaction => "apply_multi_transaction",
            Self::LoadMultiTransaction => "load_multi_transaction",
            Self::ApplyTradeTransaction => "apply_trade_transaction",
            Self::LoadTrade => "load_trade",
            Self::LoadTradeTransaction => "load_trade_transaction",
            Self::Invalid => "invalid",
        }
    }

    pub(crate) fn from_body(body: Option<&wire::request_envelope::Body>) -> Self {
        match body {
            Some(wire::request_envelope::Body::LoadSnapshot(_)) => Self::LoadSnapshot,
            Some(wire::request_envelope::Body::LoadMultiSnapshot(_)) => Self::LoadMultiSnapshot,
            Some(wire::request_envelope::Body::SaveSnapshot(_)) => Self::SaveSnapshot,
            Some(wire::request_envelope::Body::SaveMultiSnapshot(_)) => Self::SaveMultiSnapshot,
            Some(wire::request_envelope::Body::EnqueueSnapshot(_)) => Self::EnqueueSnapshot,
            Some(wire::request_envelope::Body::EnqueueMultiSnapshot(_)) => {
                Self::EnqueueMultiSnapshot
            }
            Some(wire::request_envelope::Body::ApplyTransaction(_)) => Self::ApplyTransaction,
            Some(wire::request_envelope::Body::LoadTransaction(_)) => Self::LoadTransaction,
            Some(wire::request_envelope::Body::ApplyMultiTransaction(_)) => {
                Self::ApplyMultiTransaction
            }
            Some(wire::request_envelope::Body::LoadMultiTransaction(_)) => {
                Self::LoadMultiTransaction
            }
            Some(wire::request_envelope::Body::ApplyTradeTransaction(_)) => {
                Self::ApplyTradeTransaction
            }
            Some(wire::request_envelope::Body::LoadTrade(_)) => Self::LoadTrade,
            Some(wire::request_envelope::Body::LoadTradeTransaction(_)) => {
                Self::LoadTradeTransaction
            }
            None => Self::Invalid,
        }
    }

    pub(crate) fn record_count(body: Option<&wire::request_envelope::Body>) -> u64 {
        match body {
            Some(wire::request_envelope::Body::LoadMultiSnapshot(request)) => {
                request.records.len() as u64
            }
            Some(wire::request_envelope::Body::SaveMultiSnapshot(request)) => {
                request.writes.len() as u64
            }
            Some(wire::request_envelope::Body::EnqueueMultiSnapshot(request)) => {
                request.writes.len() as u64
            }
            Some(wire::request_envelope::Body::ApplyMultiTransaction(request)) => {
                request.writes.len() as u64
            }
            Some(wire::request_envelope::Body::LoadMultiTransaction(request)) => {
                request.records.len() as u64
            }
            Some(wire::request_envelope::Body::ApplyTradeTransaction(request)) => {
                request.writes.len() as u64
            }
            Some(_) => 1,
            None => 0,
        }
    }

    pub(crate) fn payload_bytes(body: Option<&wire::request_envelope::Body>) -> u64 {
        fn bytes(length: usize) -> u64 {
            u64::try_from(length).unwrap_or(u64::MAX)
        }

        match body {
            Some(wire::request_envelope::Body::SaveSnapshot(request)) => {
                bytes(request.payload.len())
            }
            Some(wire::request_envelope::Body::SaveMultiSnapshot(request)) => request
                .writes
                .iter()
                .map(|write| bytes(write.payload.len()))
                .sum(),
            Some(wire::request_envelope::Body::EnqueueSnapshot(request)) => request
                .write
                .as_ref()
                .map_or(0, |write| bytes(write.payload.len())),
            Some(wire::request_envelope::Body::EnqueueMultiSnapshot(request)) => request
                .writes
                .iter()
                .map(|write| bytes(write.payload.len()))
                .sum(),
            Some(wire::request_envelope::Body::ApplyTransaction(request)) => {
                bytes(request.payload.len().saturating_add(request.result.len()))
            }
            Some(wire::request_envelope::Body::ApplyMultiTransaction(request)) => request
                .writes
                .iter()
                .map(|write| bytes(write.payload.len()))
                .fold(bytes(request.result.len()), u64::saturating_add),
            Some(wire::request_envelope::Body::ApplyTradeTransaction(request)) => {
                let transition = request
                    .transition
                    .as_ref()
                    .map_or(0, |transition| bytes(transition.payload.len()));
                request
                    .writes
                    .iter()
                    .map(|write| bytes(write.payload.len()))
                    .chain(
                        request
                            .ledger_postings
                            .iter()
                            .map(|posting| bytes(posting.metadata.len())),
                    )
                    .chain(
                        request
                            .outbox_events
                            .iter()
                            .map(|event| bytes(event.payload.len())),
                    )
                    .fold(
                        transition.saturating_add(bytes(request.result.len())),
                        u64::saturating_add,
                    )
            }
            _ => 0,
        }
    }
}

#[derive(Default)]
struct OperationMetrics {
    requests: AtomicU64,
    failures: AtomicU64,
    records: AtomicU64,
    payload_bytes: AtomicU64,
    duration_micros: AtomicU64,
    duration_buckets: [AtomicU64; DURATION_BUCKETS_SECONDS.len()],
    error_codes: [AtomicU64; 11],
}

/// 指标只按固定操作名、错误码和实例维度聚合，禁止加入 RecordKey 或业务幂等ID。
/// Metrics aggregate only by bounded operation/error dimensions; never add RecordKey or idempotency IDs.
pub struct DbProxyMetrics {
    started_at: Instant,
    live: AtomicBool,
    ready: AtomicBool,
    accepted_connections: AtomicU64,
    active_connections: AtomicU64,
    connection_errors: AtomicU64,
    handshake_rejections: [AtomicU64; 3],
    requests_in_flight: AtomicU64,
    operations: [OperationMetrics; RpcOperation::ALL.len()],
    backlog_committed: AtomicU64,
    backlog_empty_polls: AtomicU64,
    backlog_failures: AtomicU64,
    backlog_duration_micros: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_read_errors: AtomicU64,
    cache_writes: AtomicU64,
    cache_write_errors: AtomicU64,
    cache_negative_hits: AtomicU64,
    cache_stale_hits: AtomicU64,
    cache_negative_writes: AtomicU64,
    cache_refresh_started: AtomicU64,
    cache_refresh_completed: AtomicU64,
    cache_refresh_errors: AtomicU64,
    postgres_fallbacks: AtomicU64,
    postgres_fallback_errors: AtomicU64,
    postgres_fallback_timeouts: AtomicU64,
    postgres_fallback_circuit_open: AtomicU64,
    cache_fallback_lock_acquired: AtomicU64,
    cache_fallback_lock_contention: AtomicU64,
    cache_fallback_lock_timeouts: AtomicU64,
    cache_fallback_lock_errors: AtomicU64,
    cache_fallback_lock_release_errors: AtomicU64,
    backlog_pending: AtomicU64,
    backlog_processing: AtomicU64,
    backlog_oldest_pending_age_ms: AtomicU64,
    cache_repair_results: [AtomicU64; DURABLE_QUEUE_RESULT_NAMES.len()],
    cache_repair_pending: AtomicU64,
    cache_repair_processing: AtomicU64,
    cache_repair_dead_lettered: AtomicU64,
    cache_repair_oldest_age_ms: AtomicU64,
    outbox_results: [AtomicU64; DURABLE_QUEUE_RESULT_NAMES.len()],
    outbox_pending: AtomicU64,
    outbox_processing: AtomicU64,
    outbox_dead_lettered: AtomicU64,
    outbox_oldest_age_ms: AtomicU64,
}

impl Default for DbProxyMetrics {
    fn default() -> Self {
        Self {
            started_at: Instant::now(),
            live: AtomicBool::new(true),
            ready: AtomicBool::new(false),
            accepted_connections: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            connection_errors: AtomicU64::new(0),
            handshake_rejections: std::array::from_fn(|_| AtomicU64::new(0)),
            requests_in_flight: AtomicU64::new(0),
            operations: std::array::from_fn(|_| OperationMetrics::default()),
            backlog_committed: AtomicU64::new(0),
            backlog_empty_polls: AtomicU64::new(0),
            backlog_failures: AtomicU64::new(0),
            backlog_duration_micros: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            cache_read_errors: AtomicU64::new(0),
            cache_writes: AtomicU64::new(0),
            cache_write_errors: AtomicU64::new(0),
            cache_negative_hits: AtomicU64::new(0),
            cache_stale_hits: AtomicU64::new(0),
            cache_negative_writes: AtomicU64::new(0),
            cache_refresh_started: AtomicU64::new(0),
            cache_refresh_completed: AtomicU64::new(0),
            cache_refresh_errors: AtomicU64::new(0),
            postgres_fallbacks: AtomicU64::new(0),
            postgres_fallback_errors: AtomicU64::new(0),
            postgres_fallback_timeouts: AtomicU64::new(0),
            postgres_fallback_circuit_open: AtomicU64::new(0),
            cache_fallback_lock_acquired: AtomicU64::new(0),
            cache_fallback_lock_contention: AtomicU64::new(0),
            cache_fallback_lock_timeouts: AtomicU64::new(0),
            cache_fallback_lock_errors: AtomicU64::new(0),
            cache_fallback_lock_release_errors: AtomicU64::new(0),
            backlog_pending: AtomicU64::new(0),
            backlog_processing: AtomicU64::new(0),
            backlog_oldest_pending_age_ms: AtomicU64::new(0),
            cache_repair_results: std::array::from_fn(|_| AtomicU64::new(0)),
            cache_repair_pending: AtomicU64::new(0),
            cache_repair_processing: AtomicU64::new(0),
            cache_repair_dead_lettered: AtomicU64::new(0),
            cache_repair_oldest_age_ms: AtomicU64::new(0),
            outbox_results: std::array::from_fn(|_| AtomicU64::new(0)),
            outbox_pending: AtomicU64::new(0),
            outbox_processing: AtomicU64::new(0),
            outbox_dead_lettered: AtomicU64::new(0),
            outbox_oldest_age_ms: AtomicU64::new(0),
        }
    }
}

impl DbProxyMetrics {
    /// 标记业务监听和存储后端已经就绪。 / Mark the business listener and storage backend ready.
    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }

    /// 停机开始后立即撤销ready。 / Withdraw readiness as soon as shutdown begins.
    pub fn mark_stopping(&self) {
        self.ready.store(false, Ordering::Release);
    }

    /// 标记进程服务循环已经停止。 / Mark the server loop stopped.
    pub fn mark_stopped(&self) {
        self.ready.store(false, Ordering::Release);
        self.live.store(false, Ordering::Release);
    }

    pub(crate) fn connection_opened(&self) {
        self.accepted_connections.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn connection_closed(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn connection_failed(&self) {
        self.connection_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn handshake_rejected(&self, reason: HandshakeRejection) {
        self.handshake_rejections[reason as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn request_started(&self) {
        self.requests_in_flight.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn request_finished(
        &self,
        operation: RpcOperation,
        records: u64,
        payload_bytes: u64,
        elapsed: Duration,
        error: Option<wire::ErrorCode>,
    ) {
        self.requests_in_flight.fetch_sub(1, Ordering::Relaxed);
        let metric = &self.operations[operation.index()];
        metric.requests.fetch_add(1, Ordering::Relaxed);
        metric.records.fetch_add(records, Ordering::Relaxed);
        metric
            .payload_bytes
            .fetch_add(payload_bytes, Ordering::Relaxed);
        let micros = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        metric.duration_micros.fetch_add(micros, Ordering::Relaxed);
        for (index, bound) in DURATION_BUCKETS_SECONDS.iter().enumerate() {
            if elapsed.as_secs_f64() <= *bound {
                metric.duration_buckets[index].fetch_add(1, Ordering::Relaxed);
            }
        }
        if let Some(code) = error {
            metric.failures.fetch_add(1, Ordering::Relaxed);
            metric.error_codes[error_code_index(code)].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn backlog_finished(&self, result: BacklogMetricResult, elapsed: Duration) {
        match result {
            BacklogMetricResult::Committed => &self.backlog_committed,
            BacklogMetricResult::Empty => &self.backlog_empty_polls,
            BacklogMetricResult::Failure => &self.backlog_failures,
        }
        .fetch_add(1, Ordering::Relaxed);
        self.backlog_duration_micros.fetch_add(
            elapsed.as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    pub(crate) fn storage_metrics_updated(&self, snapshot: StorageMetricsSnapshot) {
        self.cache_hits
            .store(snapshot.cache_hits, Ordering::Relaxed);
        self.cache_misses
            .store(snapshot.cache_misses, Ordering::Relaxed);
        self.cache_read_errors
            .store(snapshot.cache_read_errors, Ordering::Relaxed);
        self.cache_writes
            .store(snapshot.cache_writes, Ordering::Relaxed);
        self.cache_write_errors
            .store(snapshot.cache_write_errors, Ordering::Relaxed);
        self.cache_negative_hits
            .store(snapshot.cache_negative_hits, Ordering::Relaxed);
        self.cache_stale_hits
            .store(snapshot.cache_stale_hits, Ordering::Relaxed);
        self.cache_negative_writes
            .store(snapshot.cache_negative_writes, Ordering::Relaxed);
        self.cache_refresh_started
            .store(snapshot.cache_refresh_started, Ordering::Relaxed);
        self.cache_refresh_completed
            .store(snapshot.cache_refresh_completed, Ordering::Relaxed);
        self.cache_refresh_errors
            .store(snapshot.cache_refresh_errors, Ordering::Relaxed);
        self.postgres_fallbacks
            .store(snapshot.postgres_fallbacks, Ordering::Relaxed);
        self.postgres_fallback_errors
            .store(snapshot.postgres_fallback_errors, Ordering::Relaxed);
        self.postgres_fallback_timeouts
            .store(snapshot.postgres_fallback_timeouts, Ordering::Relaxed);
        self.postgres_fallback_circuit_open
            .store(snapshot.postgres_fallback_circuit_open, Ordering::Relaxed);
        self.cache_fallback_lock_acquired
            .store(snapshot.cache_fallback_lock_acquired, Ordering::Relaxed);
        self.cache_fallback_lock_contention
            .store(snapshot.cache_fallback_lock_contention, Ordering::Relaxed);
        self.cache_fallback_lock_timeouts
            .store(snapshot.cache_fallback_lock_timeouts, Ordering::Relaxed);
        self.cache_fallback_lock_errors
            .store(snapshot.cache_fallback_lock_errors, Ordering::Relaxed);
        self.cache_fallback_lock_release_errors.store(
            snapshot.cache_fallback_lock_release_errors,
            Ordering::Relaxed,
        );
    }

    pub(crate) fn backlog_depth_updated(
        &self,
        pending: u64,
        processing: u64,
        oldest_pending_age_ms: Option<u64>,
    ) {
        self.backlog_pending.store(pending, Ordering::Relaxed);
        self.backlog_processing.store(processing, Ordering::Relaxed);
        self.backlog_oldest_pending_age_ms
            .store(oldest_pending_age_ms.unwrap_or(0), Ordering::Relaxed);
    }

    pub(crate) fn durable_queue_finished(
        &self,
        queue: DurableQueueMetricKind,
        result: DurableQueueMetricResult,
    ) {
        let counters = match queue {
            DurableQueueMetricKind::CacheRepair => &self.cache_repair_results,
            DurableQueueMetricKind::Outbox => &self.outbox_results,
        };
        counters[result as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn cache_repair_depth_updated(
        &self,
        pending: u64,
        processing: u64,
        dead_lettered: u64,
        oldest_age_ms: Option<u64>,
    ) {
        self.cache_repair_pending.store(pending, Ordering::Relaxed);
        self.cache_repair_processing
            .store(processing, Ordering::Relaxed);
        self.cache_repair_dead_lettered
            .store(dead_lettered, Ordering::Relaxed);
        self.cache_repair_oldest_age_ms
            .store(oldest_age_ms.unwrap_or(0), Ordering::Relaxed);
    }

    pub(crate) fn outbox_depth_updated(
        &self,
        pending: u64,
        processing: u64,
        dead_lettered: u64,
        oldest_age_ms: Option<u64>,
    ) {
        self.outbox_pending.store(pending, Ordering::Relaxed);
        self.outbox_processing.store(processing, Ordering::Relaxed);
        self.outbox_dead_lettered
            .store(dead_lettered, Ordering::Relaxed);
        self.outbox_oldest_age_ms
            .store(oldest_age_ms.unwrap_or(0), Ordering::Relaxed);
    }

    fn is_live(&self) -> bool {
        self.live.load(Ordering::Acquire)
    }

    fn is_ready(&self) -> bool {
        self.is_live() && self.ready.load(Ordering::Acquire)
    }

    pub(crate) fn prometheus(&self, storage_backend: &str) -> String {
        let mut output = String::with_capacity(16 * 1024);
        metric_header(
            &mut output,
            "dbproxy_live",
            "DBProxy process liveness",
            "gauge",
        );
        writeln!(output, "dbproxy_live {}", u8::from(self.is_live())).unwrap();
        metric_header(&mut output, "dbproxy_ready", "DBProxy readiness", "gauge");
        writeln!(output, "dbproxy_ready {}", u8::from(self.is_ready())).unwrap();
        metric_header(
            &mut output,
            "dbproxy_uptime_seconds",
            "DBProxy process uptime",
            "gauge",
        );
        writeln!(
            output,
            "dbproxy_uptime_seconds {:.3}",
            self.started_at.elapsed().as_secs_f64()
        )
        .unwrap();
        metric_header(
            &mut output,
            "dbproxy_build_info",
            "DBProxy build and storage information",
            "gauge",
        );
        writeln!(
            output,
            "dbproxy_build_info{{version=\"{}\",storage_backend=\"{}\"}} 1",
            env!("CARGO_PKG_VERSION"),
            storage_backend
        )
        .unwrap();
        write_atomic_metric(
            &mut output,
            "dbproxy_connections_total",
            "Accepted DBProxy TCP connections",
            "counter",
            &self.accepted_connections,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_connections_active",
            "Current DBProxy TCP connections",
            "gauge",
            &self.active_connections,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_connection_errors_total",
            "DBProxy TCP connections closed with an error",
            "counter",
            &self.connection_errors,
        );
        metric_header(
            &mut output,
            "dbproxy_handshake_rejections_total",
            "Rejected DBProxy handshakes by bounded reason",
            "counter",
        );
        for (index, reason) in ["protocol_mismatch", "unauthorized", "invalid_client"]
            .iter()
            .enumerate()
        {
            writeln!(
                output,
                "dbproxy_handshake_rejections_total{{reason=\"{reason}\"}} {}",
                self.handshake_rejections[index].load(Ordering::Relaxed)
            )
            .unwrap();
        }
        write_atomic_metric(
            &mut output,
            "dbproxy_requests_in_flight",
            "Current DBProxy RPC requests",
            "gauge",
            &self.requests_in_flight,
        );

        write_atomic_metric(
            &mut output,
            "dbproxy_cache_hits_total",
            "Redis cache results served without waiting for an authoritative lookup",
            "counter",
            &self.cache_hits,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_cache_misses_total",
            "Initial Redis snapshot cache misses",
            "counter",
            &self.cache_misses,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_cache_read_errors_total",
            "Redis snapshot cache read or decode errors",
            "counter",
            &self.cache_read_errors,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_cache_writes_total",
            "Successful Redis snapshot cache writes",
            "counter",
            &self.cache_writes,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_cache_write_errors_total",
            "Redis snapshot cache write or delete errors",
            "counter",
            &self.cache_write_errors,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_cache_negative_hits_total",
            "Redis negative cache hits",
            "counter",
            &self.cache_negative_hits,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_cache_stale_hits_total",
            "Stale Redis snapshot cache entries served while refreshing",
            "counter",
            &self.cache_stale_hits,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_cache_negative_writes_total",
            "Negative cache entries written to Redis",
            "counter",
            &self.cache_negative_writes,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_cache_refresh_started_total",
            "Background cache refreshes started",
            "counter",
            &self.cache_refresh_started,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_cache_refresh_completed_total",
            "Background cache refreshes completed successfully",
            "counter",
            &self.cache_refresh_completed,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_cache_refresh_errors_total",
            "Background cache refresh errors",
            "counter",
            &self.cache_refresh_errors,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_postgres_fallbacks_total",
            "PostgreSQL fallback attempts after a Redis cache miss",
            "counter",
            &self.postgres_fallbacks,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_postgres_fallback_errors_total",
            "PostgreSQL fallback read errors",
            "counter",
            &self.postgres_fallback_errors,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_postgres_fallback_timeouts_total",
            "PostgreSQL fallback reads that exceeded the configured timeout",
            "counter",
            &self.postgres_fallback_timeouts,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_postgres_fallback_circuit_open_total",
            "PostgreSQL fallback requests rejected while the circuit was open",
            "counter",
            &self.postgres_fallback_circuit_open,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_cache_fallback_lock_acquired_total",
            "Redis distributed locks acquired for PostgreSQL cache fallbacks",
            "counter",
            &self.cache_fallback_lock_acquired,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_cache_fallback_lock_contention_total",
            "Redis distributed lock acquisition contentions",
            "counter",
            &self.cache_fallback_lock_contention,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_cache_fallback_lock_timeouts_total",
            "Redis distributed lock waits that expired before acquisition",
            "counter",
            &self.cache_fallback_lock_timeouts,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_cache_fallback_lock_errors_total",
            "Redis distributed lock command or recheck errors",
            "counter",
            &self.cache_fallback_lock_errors,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_cache_fallback_lock_release_errors_total",
            "Redis distributed lock release errors",
            "counter",
            &self.cache_fallback_lock_release_errors,
        );

        metric_header(
            &mut output,
            "dbproxy_rpc_requests_total",
            "DBProxy RPC requests by operation",
            "counter",
        );
        metric_header(
            &mut output,
            "dbproxy_rpc_failures_total",
            "DBProxy RPC failures by operation",
            "counter",
        );
        metric_header(
            &mut output,
            "dbproxy_rpc_records_total",
            "Logical records processed by DBProxy RPC operations",
            "counter",
        );
        metric_header(
            &mut output,
            "dbproxy_rpc_payload_bytes_total",
            "Binary request payload and transaction result bytes by operation",
            "counter",
        );
        metric_header(
            &mut output,
            "dbproxy_rpc_duration_seconds",
            "DBProxy RPC duration by operation",
            "histogram",
        );
        metric_header(
            &mut output,
            "dbproxy_rpc_errors_total",
            "DBProxy RPC errors by operation and bounded error code",
            "counter",
        );
        for operation in RpcOperation::ALL {
            let name = operation.name();
            let metric = &self.operations[operation.index()];
            let requests = metric.requests.load(Ordering::Relaxed);
            writeln!(
                output,
                "dbproxy_rpc_requests_total{{operation=\"{name}\"}} {requests}"
            )
            .unwrap();
            writeln!(
                output,
                "dbproxy_rpc_failures_total{{operation=\"{name}\"}} {}",
                metric.failures.load(Ordering::Relaxed)
            )
            .unwrap();
            writeln!(
                output,
                "dbproxy_rpc_records_total{{operation=\"{name}\"}} {}",
                metric.records.load(Ordering::Relaxed)
            )
            .unwrap();
            writeln!(
                output,
                "dbproxy_rpc_payload_bytes_total{{operation=\"{name}\"}} {}",
                metric.payload_bytes.load(Ordering::Relaxed)
            )
            .unwrap();
            for (index, bound) in DURATION_BUCKETS_SECONDS.iter().enumerate() {
                writeln!(
                    output,
                    "dbproxy_rpc_duration_seconds_bucket{{operation=\"{name}\",le=\"{bound}\"}} {}",
                    metric.duration_buckets[index].load(Ordering::Relaxed)
                )
                .unwrap();
            }
            writeln!(
                output,
                "dbproxy_rpc_duration_seconds_bucket{{operation=\"{name}\",le=\"+Inf\"}} {requests}"
            )
            .unwrap();
            writeln!(
                output,
                "dbproxy_rpc_duration_seconds_sum{{operation=\"{name}\"}} {:.6}",
                metric.duration_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0
            )
            .unwrap();
            writeln!(
                output,
                "dbproxy_rpc_duration_seconds_count{{operation=\"{name}\"}} {requests}"
            )
            .unwrap();
            for (index, code) in ERROR_CODE_NAMES.iter().enumerate() {
                writeln!(
                    output,
                    "dbproxy_rpc_errors_total{{operation=\"{name}\",code=\"{code}\"}} {}",
                    metric.error_codes[index].load(Ordering::Relaxed)
                )
                .unwrap();
            }
        }
        metric_header(
            &mut output,
            "dbproxy_backlog_polls_total",
            "Snapshot backlog worker outcomes",
            "counter",
        );
        writeln!(
            output,
            "dbproxy_backlog_polls_total{{result=\"committed\"}} {}",
            self.backlog_committed.load(Ordering::Relaxed)
        )
        .unwrap();
        writeln!(
            output,
            "dbproxy_backlog_polls_total{{result=\"empty\"}} {}",
            self.backlog_empty_polls.load(Ordering::Relaxed)
        )
        .unwrap();
        writeln!(
            output,
            "dbproxy_backlog_polls_total{{result=\"failure\"}} {}",
            self.backlog_failures.load(Ordering::Relaxed)
        )
        .unwrap();
        write_atomic_metric(
            &mut output,
            "dbproxy_backlog_processing_seconds_total",
            "Total time spent polling and committing snapshot backlog work",
            "counter",
            &AtomicSeconds(&self.backlog_duration_micros),
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_backlog_pending",
            "Current Redis snapshot backlog items waiting for PostgreSQL",
            "gauge",
            &self.backlog_pending,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_backlog_processing",
            "Current Redis snapshot backlog items leased by workers",
            "gauge",
            &self.backlog_processing,
        );
        write_atomic_metric(
            &mut output,
            "dbproxy_backlog_oldest_pending_age_seconds",
            "Age of the oldest pending Redis snapshot backlog item",
            "gauge",
            &AtomicMilliseconds(&self.backlog_oldest_pending_age_ms),
        );
        write_queue_metrics(
            &mut output,
            "cache_repair",
            "PostgreSQL durable cache repair queue",
            DurableQueueMetricRefs {
                results: &self.cache_repair_results,
                pending: &self.cache_repair_pending,
                processing: &self.cache_repair_processing,
                dead_lettered: &self.cache_repair_dead_lettered,
                oldest_age_ms: &self.cache_repair_oldest_age_ms,
            },
        );
        write_queue_metrics(
            &mut output,
            "outbox",
            "PostgreSQL transactional outbox",
            DurableQueueMetricRefs {
                results: &self.outbox_results,
                pending: &self.outbox_pending,
                processing: &self.outbox_processing,
                dead_lettered: &self.outbox_dead_lettered,
                oldest_age_ms: &self.outbox_oldest_age_ms,
            },
        );
        output
    }
}

pub(crate) enum BacklogMetricResult {
    Committed,
    Empty,
    Failure,
}

#[derive(Clone, Copy)]
pub(crate) enum DurableQueueMetricKind {
    CacheRepair,
    Outbox,
}

#[derive(Clone, Copy)]
pub(crate) enum DurableQueueMetricResult {
    Committed,
    RetryScheduled,
    DeadLettered,
    LeaseLost,
    Empty,
    Failure,
}

pub(crate) enum HandshakeRejection {
    ProtocolMismatch,
    Unauthorized,
    InvalidClient,
}

const ERROR_CODE_NAMES: [&str; 11] = [
    "invalid_request",
    "unauthorized",
    "protocol_mismatch",
    "revision_conflict",
    "idempotency_conflict",
    "operation_conflict",
    "trade_conflict",
    "ledger_conflict",
    "outbox_conflict",
    "storage_unavailable",
    "internal",
];

const DURABLE_QUEUE_RESULT_NAMES: [&str; 6] = [
    "committed",
    "retry_scheduled",
    "dead_lettered",
    "lease_lost",
    "empty",
    "failure",
];

struct DurableQueueMetricRefs<'a> {
    results: &'a [AtomicU64; DURABLE_QUEUE_RESULT_NAMES.len()],
    pending: &'a AtomicU64,
    processing: &'a AtomicU64,
    dead_lettered: &'a AtomicU64,
    oldest_age_ms: &'a AtomicU64,
}

fn write_queue_metrics(
    output: &mut String,
    prefix: &str,
    help_prefix: &str,
    metrics: DurableQueueMetricRefs<'_>,
) {
    let attempts = format!("dbproxy_{prefix}_worker_polls_total");
    metric_header(
        output,
        &attempts,
        &format!("{help_prefix} worker outcomes"),
        "counter",
    );
    for (index, result) in DURABLE_QUEUE_RESULT_NAMES.iter().enumerate() {
        writeln!(
            output,
            "{attempts}{{result=\"{result}\"}} {}",
            metrics.results[index].load(Ordering::Relaxed)
        )
        .unwrap();
    }
    write_atomic_metric(
        output,
        &format!("dbproxy_{prefix}_pending"),
        &format!("{help_prefix} items ready or waiting for retry"),
        "gauge",
        metrics.pending,
    );
    write_atomic_metric(
        output,
        &format!("dbproxy_{prefix}_processing"),
        &format!("{help_prefix} items currently leased"),
        "gauge",
        metrics.processing,
    );
    write_atomic_metric(
        output,
        &format!("dbproxy_{prefix}_dead_lettered"),
        &format!("{help_prefix} dead-letter items"),
        "gauge",
        metrics.dead_lettered,
    );
    write_atomic_metric(
        output,
        &format!("dbproxy_{prefix}_oldest_age_seconds"),
        &format!("Age of the oldest active {help_prefix} item"),
        "gauge",
        &AtomicMilliseconds(metrics.oldest_age_ms),
    );
}

fn error_code_index(code: wire::ErrorCode) -> usize {
    match code {
        wire::ErrorCode::InvalidRequest => 0,
        wire::ErrorCode::Unauthorized => 1,
        wire::ErrorCode::ProtocolMismatch => 2,
        wire::ErrorCode::RevisionConflict => 3,
        wire::ErrorCode::IdempotencyConflict => 4,
        wire::ErrorCode::OperationConflict => 5,
        wire::ErrorCode::TradeConflict => 6,
        wire::ErrorCode::LedgerConflict => 7,
        wire::ErrorCode::OutboxConflict => 8,
        wire::ErrorCode::StorageUnavailable => 9,
        wire::ErrorCode::Internal | wire::ErrorCode::Unspecified => 10,
    }
}

fn metric_header(output: &mut String, name: &str, help: &str, kind: &str) {
    writeln!(output, "# HELP {name} {help}").unwrap();
    writeln!(output, "# TYPE {name} {kind}").unwrap();
}

fn write_atomic_metric(
    output: &mut String,
    name: &str,
    help: &str,
    kind: &str,
    value: &impl AtomicMetricValue,
) {
    metric_header(output, name, help, kind);
    writeln!(output, "{name} {}", value.metric_value()).unwrap();
}

trait AtomicMetricValue {
    fn metric_value(&self) -> String;
}

impl AtomicMetricValue for AtomicU64 {
    fn metric_value(&self) -> String {
        self.load(Ordering::Relaxed).to_string()
    }
}

struct AtomicSeconds<'a>(&'a AtomicU64);

impl AtomicMetricValue for AtomicSeconds<'_> {
    fn metric_value(&self) -> String {
        format!("{:.6}", self.0.load(Ordering::Relaxed) as f64 / 1_000_000.0)
    }
}

struct AtomicMilliseconds<'a>(&'a AtomicU64);

impl AtomicMetricValue for AtomicMilliseconds<'_> {
    fn metric_value(&self) -> String {
        format!("{:.3}", self.0.load(Ordering::Relaxed) as f64 / 1_000.0)
    }
}

pub struct ObservabilityServer {
    local_addr: SocketAddr,
    task: JoinHandle<()>,
}

impl ObservabilityServer {
    pub async fn start(
        listen_addr: SocketAddr,
        metrics: Arc<DbProxyMetrics>,
        storage_backend: &'static str,
        mut shutdown: watch::Receiver<bool>,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(listen_addr).await?;
        let local_addr = listener.local_addr()?;
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { break; }
                    }
                    accepted = listener.accept() => {
                        match accepted {
                            Ok((stream, _)) => {
                                let metrics = Arc::clone(&metrics);
                                tokio::spawn(async move {
                                    if let Err(error) = serve_http(stream, &metrics, storage_backend).await {
                                        tracing::debug!(%error, "DBProxy observability connection failed");
                                    }
                                });
                            }
                            Err(error) => {
                                tracing::error!(%error, "DBProxy observability listener failed");
                                break;
                            }
                        }
                    }
                }
            }
        });
        Ok(Self { local_addr, task })
    }

    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn stop(self) {
        let _ = self.task.await;
    }
}

async fn serve_http(
    mut stream: TcpStream,
    metrics: &DbProxyMetrics,
    storage_backend: &str,
) -> io::Result<()> {
    let path = read_path(&mut stream).await?;
    let (status, content_type, body) = match path.as_str() {
        "/live" if metrics.is_live() => (
            "200 OK",
            "application/json",
            "{\"status\":\"live\"}".to_string(),
        ),
        "/live" => (
            "503 Service Unavailable",
            "application/json",
            "{\"status\":\"stopped\"}".to_string(),
        ),
        "/ready" if metrics.is_ready() => (
            "200 OK",
            "application/json",
            "{\"status\":\"ready\"}".to_string(),
        ),
        "/ready" => (
            "503 Service Unavailable",
            "application/json",
            "{\"status\":\"not-ready\"}".to_string(),
        ),
        "/metrics" => (
            "200 OK",
            "text/plain; version=0.0.4",
            metrics.prometheus(storage_backend),
        ),
        _ => (
            "404 Not Found",
            "application/json",
            "{\"status\":\"not-found\"}".to_string(),
        ),
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nCache-Control: no-store\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

async fn read_path(stream: &mut TcpStream) -> io::Result<String> {
    let mut bytes = Vec::with_capacity(512);
    loop {
        if bytes.len() >= MAX_HTTP_REQUEST_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HTTP request is too large",
            ));
        }
        let mut chunk = [0_u8; 512];
        let length = timeout(Duration::from_secs(2), stream.read(&mut chunk))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "HTTP request timed out"))??;
        if length == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..length]);
        if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    let header = std::str::from_utf8(&bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "HTTP header is not UTF-8"))?;
    let mut request = header.lines().next().unwrap_or_default().split_whitespace();
    if request.next() != Some("GET") {
        return Ok(String::new());
    }
    Ok(request.next().unwrap_or_default().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_output_uses_bounded_labels_and_histograms() {
        let metrics = DbProxyMetrics::default();
        metrics.mark_ready();
        metrics.request_started();
        metrics.request_finished(
            RpcOperation::LoadMultiSnapshot,
            30,
            4,
            Duration::from_millis(4),
            Some(wire::ErrorCode::StorageUnavailable),
        );
        metrics.storage_metrics_updated(StorageMetricsSnapshot {
            cache_hits: 7,
            cache_misses: 3,
            cache_read_errors: 1,
            cache_writes: 5,
            cache_write_errors: 2,
            cache_negative_hits: 8,
            cache_stale_hits: 9,
            cache_negative_writes: 10,
            cache_refresh_started: 11,
            cache_refresh_completed: 12,
            cache_refresh_errors: 13,
            postgres_fallbacks: 3,
            postgres_fallback_errors: 1,
            postgres_fallback_timeouts: 1,
            postgres_fallback_circuit_open: 2,
            cache_fallback_lock_acquired: 3,
            cache_fallback_lock_contention: 4,
            cache_fallback_lock_timeouts: 5,
            cache_fallback_lock_errors: 6,
            cache_fallback_lock_release_errors: 7,
        });
        metrics.backlog_depth_updated(4, 2, Some(2_500));
        metrics.durable_queue_finished(
            DurableQueueMetricKind::CacheRepair,
            DurableQueueMetricResult::RetryScheduled,
        );
        metrics.cache_repair_depth_updated(5, 1, 2, Some(3_500));
        metrics.durable_queue_finished(
            DurableQueueMetricKind::Outbox,
            DurableQueueMetricResult::Committed,
        );
        metrics.outbox_depth_updated(6, 2, 1, Some(4_500));
        let output = metrics.prometheus("memory");
        assert!(output.contains("dbproxy_ready 1"));
        assert!(output.contains("dbproxy_rpc_records_total{operation=\"load_multi_snapshot\"} 30"));
        assert!(
            output.contains("dbproxy_rpc_payload_bytes_total{operation=\"load_multi_snapshot\"} 4")
        );
        assert!(output.contains("dbproxy_rpc_errors_total{operation=\"load_multi_snapshot\",code=\"storage_unavailable\"} 1"));
        assert!(output.contains(
            "dbproxy_rpc_duration_seconds_bucket{operation=\"load_multi_snapshot\",le=\"0.005\"} 1"
        ));
        assert!(output.contains("dbproxy_cache_hits_total 7"));
        assert!(output.contains("dbproxy_cache_read_errors_total 1"));
        assert!(output.contains("dbproxy_cache_negative_hits_total 8"));
        assert!(output.contains("dbproxy_cache_stale_hits_total 9"));
        assert!(output.contains("dbproxy_cache_negative_writes_total 10"));
        assert!(output.contains("dbproxy_cache_refresh_started_total 11"));
        assert!(output.contains("dbproxy_cache_refresh_completed_total 12"));
        assert!(output.contains("dbproxy_cache_refresh_errors_total 13"));
        assert!(output.contains("dbproxy_postgres_fallback_timeouts_total 1"));
        assert!(output.contains("dbproxy_postgres_fallback_circuit_open_total 2"));
        assert!(output.contains("dbproxy_cache_fallback_lock_acquired_total 3"));
        assert!(output.contains("dbproxy_cache_fallback_lock_contention_total 4"));
        assert!(output.contains("dbproxy_backlog_pending 4"));
        assert!(output.contains("dbproxy_backlog_oldest_pending_age_seconds 2.500"));
        assert!(
            output
                .contains("dbproxy_cache_repair_worker_polls_total{result=\"retry_scheduled\"} 1")
        );
        assert!(output.contains("dbproxy_cache_repair_pending 5"));
        assert!(output.contains("dbproxy_cache_repair_dead_lettered 2"));
        assert!(output.contains("dbproxy_cache_repair_oldest_age_seconds 3.500"));
        assert!(output.contains("dbproxy_outbox_worker_polls_total{result=\"committed\"} 1"));
        assert!(output.contains("dbproxy_outbox_pending 6"));
        assert!(output.contains("dbproxy_outbox_dead_lettered 1"));
        assert!(output.contains("dbproxy_outbox_oldest_age_seconds 4.500"));
        assert!(!output.contains("hot-key"));
    }

    #[tokio::test]
    async fn http_server_exposes_ready_and_prometheus_routes() {
        let metrics = Arc::new(DbProxyMetrics::default());
        metrics.mark_ready();
        let (shutdown, receiver) = watch::channel(false);
        let server = ObservabilityServer::start(
            "127.0.0.1:0".parse().unwrap(),
            Arc::clone(&metrics),
            "memory",
            receiver,
        )
        .await
        .unwrap();

        let mut stream = TcpStream::connect(server.local_addr()).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        let response = String::from_utf8(response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("dbproxy_ready 1"));

        shutdown.send(true).unwrap();
        server.stop().await;
    }
}
