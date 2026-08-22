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
    Invalid,
}

impl RpcOperation {
    const ALL: [Self; 11] = [
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
            Some(_) => 1,
            None => 0,
        }
    }
}

#[derive(Default)]
struct OperationMetrics {
    requests: AtomicU64,
    failures: AtomicU64,
    records: AtomicU64,
    duration_micros: AtomicU64,
    duration_buckets: [AtomicU64; DURATION_BUCKETS_SECONDS.len()],
    error_codes: [AtomicU64; 8],
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
        elapsed: Duration,
        error: Option<wire::ErrorCode>,
    ) {
        self.requests_in_flight.fetch_sub(1, Ordering::Relaxed);
        let metric = &self.operations[operation.index()];
        metric.requests.fetch_add(1, Ordering::Relaxed);
        metric.records.fetch_add(records, Ordering::Relaxed);
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
        output
    }
}

pub(crate) enum BacklogMetricResult {
    Committed,
    Empty,
    Failure,
}

pub(crate) enum HandshakeRejection {
    ProtocolMismatch,
    Unauthorized,
    InvalidClient,
}

const ERROR_CODE_NAMES: [&str; 8] = [
    "invalid_request",
    "unauthorized",
    "protocol_mismatch",
    "revision_conflict",
    "idempotency_conflict",
    "operation_conflict",
    "storage_unavailable",
    "internal",
];

fn error_code_index(code: wire::ErrorCode) -> usize {
    match code {
        wire::ErrorCode::InvalidRequest => 0,
        wire::ErrorCode::Unauthorized => 1,
        wire::ErrorCode::ProtocolMismatch => 2,
        wire::ErrorCode::RevisionConflict => 3,
        wire::ErrorCode::IdempotencyConflict => 4,
        wire::ErrorCode::OperationConflict => 5,
        wire::ErrorCode::StorageUnavailable => 6,
        wire::ErrorCode::Internal | wire::ErrorCode::Unspecified => 7,
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
            Duration::from_millis(4),
            Some(wire::ErrorCode::StorageUnavailable),
        );
        let output = metrics.prometheus("memory");
        assert!(output.contains("dbproxy_ready 1"));
        assert!(output.contains("dbproxy_rpc_records_total{operation=\"load_multi_snapshot\"} 30"));
        assert!(output.contains("dbproxy_rpc_errors_total{operation=\"load_multi_snapshot\",code=\"storage_unavailable\"} 1"));
        assert!(output.contains(
            "dbproxy_rpc_duration_seconds_bucket{operation=\"load_multi_snapshot\",le=\"0.005\"} 1"
        ));
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
