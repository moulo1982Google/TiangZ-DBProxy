//! DBProxy 网络服务实现。
//! DBProxy network service implementation.
//!
//! 服务端只调度通用快照和记录事务。游戏 Repository、Entity 生命周期与业务校验
//! 必须留在 TiangZ。The server only dispatches generic snapshots and record transactions;
//! game repositories, entity lifecycle, and business validation stay in TiangZ.

pub mod config;

use std::{
    fmt,
    hash::{Hash, Hasher},
    io,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use thiserror::Error;
use tiangz_dbproxy_core::{
    AsyncMultiRecordTransactionStore, AsyncSnapshotStore, AsyncTransactionalStore,
    MultiRecordTransactionalWrite, MultiRecordTransactionalWriteOutcome, RecordKey, Revision,
    SnapshotEnvelope, SnapshotWrite, SnapshotWriteOutcome, StoreError, TransactionReceipt,
    TransactionalWrite, TransactionalWriteOutcome,
};
use tiangz_dbproxy_protocol::{
    DEFAULT_MAX_FRAME_BYTES, MAX_AUTH_TOKEN_BYTES, MAX_CLIENT_NAME_BYTES, PROTOCOL_FINGERPRINT,
    PROTOCOL_VERSION, ProtocolError, read_message, wire, write_message,
};
use tiangz_dbproxy_storage::{
    RedisSnapshotBacklog, SnapshotBacklogAck, StorageError, TieredSnapshotStore,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinSet,
    time::{sleep, timeout},
};

#[derive(Debug, Error)]
pub enum BackendError {
    #[error("invalid backend configuration: {0}")]
    InvalidConfig(&'static str),
    #[error(transparent)]
    Core(#[from] StoreError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// 网络层依赖的最小后端接口。实现不能把业务对象泄漏到 DBProxy。
/// Minimal backend used by the network layer; implementations must remain business-agnostic.
#[async_trait]
pub trait DbProxyBackend: Send + Sync + 'static {
    async fn load(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, BackendError>;
    async fn save(&self, request: SnapshotWrite) -> Result<SnapshotWriteOutcome, BackendError>;
    async fn enqueue_snapshot(&self, request: SnapshotWrite) -> Result<(), BackendError>;
    async fn apply_transaction(
        &self,
        request: TransactionalWrite,
    ) -> Result<TransactionalWriteOutcome, BackendError>;
    async fn load_transaction(
        &self,
        operation_id: &str,
        record: &RecordKey,
    ) -> Result<Option<TransactionReceipt>, BackendError>;
    async fn apply_multi_transaction(
        &self,
        request: MultiRecordTransactionalWrite,
    ) -> Result<MultiRecordTransactionalWriteOutcome, BackendError> {
        let _ = request;
        Err(BackendError::InvalidConfig(
            "multi-record transactions are not supported by this backend",
        ))
    }
    async fn load_multi_transaction(
        &self,
        operation_id: &str,
        records: &[RecordKey],
    ) -> Result<Option<tiangz_dbproxy_core::MultiRecordTransactionReceipt>, BackendError> {
        let _ = (operation_id, records);
        Err(BackendError::InvalidConfig(
            "multi-record transactions are not supported by this backend",
        ))
    }
}

/// 真实 PostgreSQL/Redis 后端。每个 shard 使用独立数据库连接，并按 RecordKey 稳定路由，
/// 避免整个服务被单个 `tokio-postgres::Client` 的事务锁串行化。
/// Real PostgreSQL/Redis backend. Stable record sharding avoids one global client lock.
pub struct StorageBackend {
    shards: Vec<TieredSnapshotStore>,
    backlog: RedisSnapshotBacklog,
}

impl StorageBackend {
    /// 创建固定数量的连接分片。分片数是启动配置，运行时不能热改。
    /// Create a fixed connection-shard count; changing it requires a service restart.
    pub async fn connect(
        postgres_url: &str,
        redis_url: &str,
        shard_count: usize,
    ) -> Result<Self, BackendError> {
        if shard_count == 0 {
            return Err(BackendError::InvalidConfig("storage shard count is zero"));
        }
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(TieredSnapshotStore::connect(postgres_url, redis_url).await?);
        }
        Ok(Self {
            shards,
            backlog: RedisSnapshotBacklog::connect(redis_url).await?,
        })
    }

    fn shard(&self, record: &RecordKey) -> TieredSnapshotStore {
        let mut hasher = StableHasher::default();
        record.hash(&mut hasher);
        let index = (hasher.finish() as usize) % self.shards.len();
        self.shards[index].clone()
    }

    fn shard_for_operation(&self, operation_id: &str) -> TieredSnapshotStore {
        let mut hasher = StableHasher::default();
        operation_id.hash(&mut hasher);
        self.shards[(hasher.finish() as usize) % self.shards.len()].clone()
    }

    /// 处理一条 Redis backlog。数据库成功但 ACK 失败时不能伪装成完全成功；lease 到期后
    /// 会以原 request_id 重试并命中 PostgreSQL 幂等记录。
    /// Process one durable backlog item. A failed ACK is recovered by lease expiry and idempotency.
    pub async fn process_backlog_once(
        &self,
        lease_ms: u64,
    ) -> Result<BacklogProcessOutcome, BackendError> {
        let Some(lease) = self.backlog.claim(lease_ms).await? else {
            return Ok(BacklogProcessOutcome::Empty);
        };
        match self.save(lease.request.clone()).await {
            Ok(_) => {
                let ack = self.backlog.ack(&lease).await?;
                Ok(BacklogProcessOutcome::Committed(ack))
            }
            Err(error) => {
                if let Err(release_error) = self.backlog.release(&lease).await {
                    tracing::error!(%release_error, "failed to release snapshot backlog lease");
                }
                Err(error)
            }
        }
    }
}

#[async_trait]
impl DbProxyBackend for StorageBackend {
    async fn load(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, BackendError> {
        Ok(self.shard(record).load(record).await?)
    }

    async fn save(&self, request: SnapshotWrite) -> Result<SnapshotWriteOutcome, BackendError> {
        let mut store = self.shard(&request.record);
        Ok(store.save(request).await?)
    }

    async fn enqueue_snapshot(&self, request: SnapshotWrite) -> Result<(), BackendError> {
        Ok(self.backlog.enqueue(request).await?)
    }

    async fn apply_transaction(
        &self,
        request: TransactionalWrite,
    ) -> Result<TransactionalWriteOutcome, BackendError> {
        let mut store = self.shard(&request.record);
        Ok(store.apply(request).await?)
    }

    async fn load_transaction(
        &self,
        operation_id: &str,
        record: &RecordKey,
    ) -> Result<Option<TransactionReceipt>, BackendError> {
        Ok(self
            .shard(record)
            .load_receipt(operation_id, record)
            .await?)
    }

    async fn apply_multi_transaction(
        &self,
        request: MultiRecordTransactionalWrite,
    ) -> Result<MultiRecordTransactionalWriteOutcome, BackendError> {
        let mut store = self.shard_for_operation(&request.operation_id);
        Ok(store.apply_multi(request).await?)
    }

    async fn load_multi_transaction(
        &self,
        operation_id: &str,
        records: &[RecordKey],
    ) -> Result<Option<tiangz_dbproxy_core::MultiRecordTransactionReceipt>, BackendError> {
        Ok(self
            .shard_for_operation(operation_id)
            .load_multi_receipt(operation_id, records)
            .await?)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BacklogProcessOutcome {
    Empty,
    Committed(SnapshotBacklogAck),
}

/// 持续消费普通快照积压。停机只停止领取新项；已领取项要么完成 ACK，要么由 lease 回收。
/// Continuously consume ordinary snapshots. Shutdown stops new claims; leases recover interrupted work.
pub async fn run_backlog_worker(
    backend: Arc<StorageBackend>,
    lease_ms: u64,
    idle_delay: Duration,
    failure_delay: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        match backend.process_backlog_once(lease_ms).await {
            Ok(BacklogProcessOutcome::Committed(_)) => continue,
            Ok(BacklogProcessOutcome::Empty) => {
                tokio::select! {
                    _ = sleep(idle_delay) => {}
                    _ = shutdown.changed() => return,
                }
            }
            Err(error) => {
                tracing::error!(%error, "snapshot backlog flush failed");
                tokio::select! {
                    _ = sleep(failure_delay) => {}
                    _ = shutdown.changed() => return,
                }
            }
        }
    }
}

/// TCP 服务配置。认证令牌必须通过部署密钥注入，禁止使用仓库中的本地示例密码。
/// TCP server settings. Inject the auth token as a deployment secret, never from sample credentials.
#[derive(Clone)]
pub struct ServerConfig {
    pub listen_addr: SocketAddr,
    pub auth_token: String,
    pub max_frame_bytes: usize,
    pub handshake_timeout: Duration,
    pub shutdown_grace: Duration,
}

impl fmt::Debug for ServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerConfig")
            .field("listen_addr", &self.listen_addr)
            .field("auth_token", &"[REDACTED]")
            .field("max_frame_bytes", &self.max_frame_bytes)
            .field("handshake_timeout", &self.handshake_timeout)
            .field("shutdown_grace", &self.shutdown_grace)
            .finish()
    }
}

impl ServerConfig {
    pub fn new(listen_addr: SocketAddr, auth_token: impl Into<String>) -> Self {
        Self {
            listen_addr,
            auth_token: auth_token.into(),
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            handshake_timeout: Duration::from_secs(5),
            shutdown_grace: Duration::from_secs(5),
        }
    }

    fn validate(&self) -> Result<(), ServerError> {
        if !(16..=MAX_AUTH_TOKEN_BYTES).contains(&self.auth_token.len()) {
            return Err(ServerError::InvalidConfig(
                "auth token length is outside 16..=512 bytes",
            ));
        }
        if self.max_frame_bytes == 0 {
            return Err(ServerError::InvalidConfig("max frame bytes is zero"));
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("invalid server configuration: {0}")]
    InvalidConfig(&'static str),
    #[error(transparent)]
    Io(#[from] io::Error),
}

pub struct DbProxyServer {
    listener: TcpListener,
    config: Arc<ServerConfig>,
    backend: Arc<dyn DbProxyBackend>,
}

impl DbProxyServer {
    pub async fn bind(
        config: ServerConfig,
        backend: Arc<dyn DbProxyBackend>,
    ) -> Result<Self, ServerError> {
        config.validate()?;
        let listener = TcpListener::bind(config.listen_addr).await?;
        Ok(Self {
            listener,
            config: Arc::new(config),
            backend,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, ServerError> {
        Ok(self.listener.local_addr()?)
    }

    /// 接收连接直到收到 shutdown；随后通知连接任务并在有限窗口内等待退出。
    /// Accept until shutdown, then signal connection tasks and wait within a bounded grace period.
    pub async fn serve(self, mut shutdown: watch::Receiver<bool>) -> Result<(), ServerError> {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                accepted = self.listener.accept() => {
                    let (stream, peer) = accepted?;
                    let config = Arc::clone(&self.config);
                    let backend = Arc::clone(&self.backend);
                    let connection_shutdown = shutdown.clone();
                    connections.spawn(async move {
                        if let Err(error) = handle_connection(
                            stream,
                            config,
                            backend,
                            connection_shutdown,
                        ).await {
                            tracing::warn!(%peer, %error, "DBProxy connection closed with an error");
                        }
                    });
                }
            }
            while let Some(joined) = connections.try_join_next() {
                if let Err(error) = joined {
                    tracing::error!(%error, "DBProxy connection task panicked");
                }
            }
        }

        let grace = self.config.shutdown_grace;
        if timeout(grace, async {
            while let Some(joined) = connections.join_next().await {
                if let Err(error) = joined {
                    tracing::error!(%error, "DBProxy connection task panicked during shutdown");
                }
            }
        })
        .await
        .is_err()
        {
            connections.abort_all();
            tracing::warn!(?grace, "DBProxy connection shutdown grace expired");
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
enum ConnectionError {
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error("connection closed before the handshake")]
    ClosedBeforeHandshake,
    #[error("handshake timed out")]
    HandshakeTimeout,
    #[error("first frame was not a handshake")]
    MissingHandshake,
}

async fn handle_connection(
    mut stream: TcpStream,
    config: Arc<ServerConfig>,
    backend: Arc<dyn DbProxyBackend>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ConnectionError> {
    stream.set_nodelay(true).map_err(ProtocolError::from)?;
    let first = timeout(
        config.handshake_timeout,
        read_message::<_, wire::ClientFrame>(&mut stream, config.max_frame_bytes),
    )
    .await
    .map_err(|_| ConnectionError::HandshakeTimeout)??
    .ok_or(ConnectionError::ClosedBeforeHandshake)?;
    let wire::client_frame::Body::Hello(hello) =
        first.body.ok_or(ConnectionError::MissingHandshake)?
    else {
        return Err(ConnectionError::MissingHandshake);
    };

    if hello.protocol_version != PROTOCOL_VERSION
        || hello.protocol_fingerprint != PROTOCOL_FINGERPRINT
    {
        write_hello_rejection(
            &mut stream,
            config.max_frame_bytes,
            wire::ErrorCode::ProtocolMismatch,
            "DBProxy protocol version or fingerprint does not match",
        )
        .await?;
        return Ok(());
    }
    if hello.auth_token.len() > MAX_AUTH_TOKEN_BYTES {
        write_hello_rejection(
            &mut stream,
            config.max_frame_bytes,
            wire::ErrorCode::Unauthorized,
            "DBProxy authentication failed",
        )
        .await?;
        return Ok(());
    }
    if !constant_time_token_eq(config.auth_token.as_bytes(), hello.auth_token.as_bytes()) {
        write_hello_rejection(
            &mut stream,
            config.max_frame_bytes,
            wire::ErrorCode::Unauthorized,
            "DBProxy authentication failed",
        )
        .await?;
        return Ok(());
    }
    if hello.client_name.trim().is_empty() || hello.client_name.len() > MAX_CLIENT_NAME_BYTES {
        write_hello_rejection(
            &mut stream,
            config.max_frame_bytes,
            wire::ErrorCode::InvalidRequest,
            "DBProxy client name is empty or too long",
        )
        .await?;
        return Ok(());
    }

    let accepted = wire::ServerFrame {
        body: Some(wire::server_frame::Body::Hello(wire::ServerHello {
            protocol_version: PROTOCOL_VERSION,
            protocol_fingerprint: PROTOCOL_FINGERPRINT.to_string(),
            accepted: true,
            error: None,
        })),
    };
    write_message(&mut stream, &accepted, config.max_frame_bytes).await?;
    tracing::debug!(client_name = %hello.client_name, "DBProxy client authenticated");

    loop {
        let frame = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
                continue;
            }
            frame = read_message::<_, wire::ClientFrame>(&mut stream, config.max_frame_bytes) => frame?,
        };
        let Some(frame) = frame else {
            return Ok(());
        };
        let Some(wire::client_frame::Body::Request(request)) = frame.body else {
            return Err(ConnectionError::MissingHandshake);
        };
        let response = dispatch(request, backend.as_ref()).await;
        write_message(&mut stream, &response, config.max_frame_bytes).await?;
    }
}

async fn write_hello_rejection(
    stream: &mut TcpStream,
    maximum: usize,
    code: wire::ErrorCode,
    message: &str,
) -> Result<(), ProtocolError> {
    write_message(
        stream,
        &wire::ServerFrame {
            body: Some(wire::server_frame::Body::Hello(wire::ServerHello {
                protocol_version: PROTOCOL_VERSION,
                protocol_fingerprint: PROTOCOL_FINGERPRINT.to_string(),
                accepted: false,
                error: Some(wire_error(code, message, None)),
            })),
        },
        maximum,
    )
    .await
}

async fn dispatch(
    request: wire::RequestEnvelope,
    backend: &dyn DbProxyBackend,
) -> wire::ServerFrame {
    let rpc_id = request.rpc_id;
    let result = dispatch_body(request.body, backend).await;
    let response = match result {
        Ok(body) => wire::ResponseEnvelope {
            rpc_id,
            error: None,
            body: Some(body),
        },
        Err(failure) => wire::ResponseEnvelope {
            rpc_id,
            error: Some(wire_error(
                failure.code,
                &failure.public_message,
                failure.actual_revision,
            )),
            body: None,
        },
    };
    wire::ServerFrame {
        body: Some(wire::server_frame::Body::Response(response)),
    }
}

async fn dispatch_body(
    body: Option<wire::request_envelope::Body>,
    backend: &dyn DbProxyBackend,
) -> Result<wire::response_envelope::Body, RpcFailure> {
    match body.ok_or_else(|| RpcFailure::invalid("request body is missing"))? {
        wire::request_envelope::Body::LoadSnapshot(request) => {
            let record = request
                .record
                .ok_or_else(|| RpcFailure::invalid("load_snapshot.record is missing"))?
                .try_into()
                .map_err(RpcFailure::from_protocol)?;
            let snapshot = backend
                .load(&record)
                .await
                .map_err(RpcFailure::from_backend)?;
            Ok(wire::response_envelope::Body::LoadSnapshot(
                wire::LoadSnapshotResponse {
                    snapshot: snapshot.as_ref().map(Into::into),
                },
            ))
        }
        wire::request_envelope::Body::SaveSnapshot(request) => {
            let request = request.try_into().map_err(RpcFailure::from_protocol)?;
            let outcome = backend
                .save(request)
                .await
                .map_err(RpcFailure::from_backend)?;
            let (disposition, revision) = snapshot_outcome(outcome);
            Ok(wire::response_envelope::Body::SaveSnapshot(
                wire::SaveSnapshotResponse {
                    disposition: disposition.into(),
                    revision: revision.0,
                },
            ))
        }
        wire::request_envelope::Body::EnqueueSnapshot(request) => {
            let request = request
                .write
                .ok_or_else(|| RpcFailure::invalid("enqueue_snapshot.write is missing"))?
                .try_into()
                .map_err(RpcFailure::from_protocol)?;
            backend
                .enqueue_snapshot(request)
                .await
                .map_err(RpcFailure::from_backend)?;
            Ok(wire::response_envelope::Body::EnqueueSnapshot(
                wire::EnqueueSnapshotResponse { accepted: true },
            ))
        }
        wire::request_envelope::Body::ApplyTransaction(request) => {
            let request = request.try_into().map_err(RpcFailure::from_protocol)?;
            let outcome = backend
                .apply_transaction(request)
                .await
                .map_err(RpcFailure::from_backend)?;
            let (disposition, revision, result) = transaction_outcome(outcome);
            Ok(wire::response_envelope::Body::ApplyTransaction(
                wire::ApplyTransactionResponse {
                    disposition: disposition.into(),
                    new_revision: revision.0,
                    result,
                },
            ))
        }
        wire::request_envelope::Body::LoadTransaction(request) => {
            let record = request
                .record
                .ok_or_else(|| RpcFailure::invalid("load_transaction.record is missing"))?
                .try_into()
                .map_err(RpcFailure::from_protocol)?;
            let receipt = backend
                .load_transaction(&request.operation_id, &record)
                .await
                .map_err(RpcFailure::from_backend)?;
            Ok(wire::response_envelope::Body::LoadTransaction(
                wire::LoadTransactionResponse {
                    receipt: receipt.map(transaction_receipt),
                },
            ))
        }
        wire::request_envelope::Body::ApplyMultiTransaction(request) => {
            if request.operation_id.trim().is_empty() {
                return Err(RpcFailure::invalid(
                    "apply_multi_transaction.operation_id is empty",
                ));
            }
            if request.writes.is_empty() {
                return Err(RpcFailure::invalid(
                    "apply_multi_transaction.writes is empty",
                ));
            }
            if request.writes.len() > tiangz_dbproxy_protocol::MAX_TRANSACTION_RECORDS {
                return Err(RpcFailure::invalid(
                    "apply_multi_transaction.writes exceeds the record limit",
                ));
            }
            let writes = request
                .writes
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()
                .map_err(RpcFailure::from_protocol)?;
            let outcome = backend
                .apply_multi_transaction(MultiRecordTransactionalWrite {
                    operation_id: request.operation_id,
                    writes,
                    result: request.result,
                })
                .await
                .map_err(RpcFailure::from_backend)?;
            Ok(wire::response_envelope::Body::ApplyMultiTransaction(
                multi_transaction_outcome(outcome),
            ))
        }
        wire::request_envelope::Body::LoadMultiTransaction(request) => {
            if request.operation_id.trim().is_empty() {
                return Err(RpcFailure::invalid(
                    "load_multi_transaction.operation_id is empty",
                ));
            }
            if request.records.is_empty() {
                return Err(RpcFailure::invalid(
                    "load_multi_transaction.records is empty",
                ));
            }
            if request.records.len() > tiangz_dbproxy_protocol::MAX_TRANSACTION_RECORDS {
                return Err(RpcFailure::invalid(
                    "load_multi_transaction.records exceeds the record limit",
                ));
            }
            let records = request
                .records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()
                .map_err(RpcFailure::from_protocol)?;
            let receipt = backend
                .load_multi_transaction(&request.operation_id, &records)
                .await
                .map_err(RpcFailure::from_backend)?;
            Ok(wire::response_envelope::Body::LoadMultiTransaction(
                wire::LoadMultiTransactionResponse {
                    receipt: receipt.map(multi_transaction_receipt),
                },
            ))
        }
    }
}

fn multi_transaction_record_receipt(
    receipt: tiangz_dbproxy_core::TransactionRecordReceipt,
) -> wire::MultiTransactionRecordReceipt {
    wire::MultiTransactionRecordReceipt {
        record: Some((&receipt.record).into()),
        new_revision: receipt.new_revision.0,
    }
}

fn multi_transaction_receipt(
    receipt: tiangz_dbproxy_core::MultiRecordTransactionReceipt,
) -> wire::MultiTransactionReceipt {
    wire::MultiTransactionReceipt {
        operation_id: receipt.operation_id,
        records: receipt
            .records
            .into_iter()
            .map(multi_transaction_record_receipt)
            .collect(),
        result: receipt.result,
    }
}

fn multi_transaction_outcome(
    outcome: MultiRecordTransactionalWriteOutcome,
) -> wire::ApplyMultiTransactionResponse {
    let (disposition, records, result) = match outcome {
        MultiRecordTransactionalWriteOutcome::Applied { records, result } => {
            (wire::WriteDisposition::Applied, records, result)
        }
        MultiRecordTransactionalWriteOutcome::Duplicate { records, result } => {
            (wire::WriteDisposition::Duplicate, records, result)
        }
    };
    wire::ApplyMultiTransactionResponse {
        disposition: disposition.into(),
        records: records
            .into_iter()
            .map(multi_transaction_record_receipt)
            .collect(),
        result,
    }
}

fn transaction_receipt(receipt: TransactionReceipt) -> wire::TransactionReceipt {
    wire::TransactionReceipt {
        operation_id: receipt.operation_id,
        record: Some((&receipt.record).into()),
        new_revision: receipt.new_revision.0,
        result: receipt.result,
    }
}

fn snapshot_outcome(outcome: SnapshotWriteOutcome) -> (wire::WriteDisposition, Revision) {
    match outcome {
        SnapshotWriteOutcome::Applied { revision } => (wire::WriteDisposition::Applied, revision),
        SnapshotWriteOutcome::Duplicate { revision } => {
            (wire::WriteDisposition::Duplicate, revision)
        }
    }
}

fn transaction_outcome(
    outcome: TransactionalWriteOutcome,
) -> (wire::WriteDisposition, Revision, Vec<u8>) {
    match outcome {
        TransactionalWriteOutcome::Applied {
            new_revision,
            result,
        } => (wire::WriteDisposition::Applied, new_revision, result),
        TransactionalWriteOutcome::Duplicate {
            new_revision,
            result,
        } => (wire::WriteDisposition::Duplicate, new_revision, result),
    }
}

struct RpcFailure {
    code: wire::ErrorCode,
    public_message: String,
    actual_revision: Option<u64>,
}

impl RpcFailure {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: wire::ErrorCode::InvalidRequest,
            public_message: message.into(),
            actual_revision: None,
        }
    }

    fn from_protocol(error: ProtocolError) -> Self {
        match error {
            ProtocolError::Store(error) => Self::from_store(error),
            ProtocolError::MissingField(field) => Self::invalid(format!("missing field: {field}")),
            ProtocolError::InvalidField(field) => Self::invalid(format!("invalid field: {field}")),
            other => {
                tracing::error!(%other, "unexpected request conversion error");
                Self {
                    code: wire::ErrorCode::Internal,
                    public_message: "request conversion failed".to_string(),
                    actual_revision: None,
                }
            }
        }
    }

    fn from_backend(error: BackendError) -> Self {
        match error {
            BackendError::InvalidConfig(message) => {
                tracing::error!(%message, "invalid DBProxy backend configuration reached RPC dispatch");
                Self {
                    code: wire::ErrorCode::Internal,
                    public_message: "DBProxy backend is misconfigured".to_string(),
                    actual_revision: None,
                }
            }
            BackendError::Core(error) | BackendError::Storage(StorageError::Core(error)) => {
                Self::from_store(error)
            }
            BackendError::Storage(error) => {
                tracing::error!(%error, "DBProxy storage operation failed");
                Self {
                    code: wire::ErrorCode::StorageUnavailable,
                    public_message: "storage operation failed; retry with the same idempotency key"
                        .to_string(),
                    actual_revision: None,
                }
            }
        }
    }

    fn from_store(error: StoreError) -> Self {
        match error {
            StoreError::InvalidKey(_)
            | StoreError::EmptyRequestId
            | StoreError::EmptyOperationId
            | StoreError::EmptyTransactionRecords
            | StoreError::DuplicateTransactionRecord { .. }
            | StoreError::QueuedSnapshotRequiresUnconditionalWrite { .. } => {
                Self::invalid(error.to_string())
            }
            StoreError::IdempotencyConflict { .. } => Self {
                code: wire::ErrorCode::IdempotencyConflict,
                public_message: error.to_string(),
                actual_revision: None,
            },
            StoreError::OperationIdConflict { .. } => Self {
                code: wire::ErrorCode::OperationConflict,
                public_message: error.to_string(),
                actual_revision: None,
            },
            StoreError::RevisionConflict { actual, .. } => Self {
                code: wire::ErrorCode::RevisionConflict,
                public_message: error.to_string(),
                actual_revision: Some(actual.0),
            },
            StoreError::RevisionExhausted { .. } => {
                tracing::error!(%error, "DBProxy revision exhausted");
                Self {
                    code: wire::ErrorCode::Internal,
                    public_message: "revision exhausted".to_string(),
                    actual_revision: None,
                }
            }
        }
    }
}

fn wire_error(
    code: wire::ErrorCode,
    message: &str,
    actual_revision: Option<u64>,
) -> wire::RpcError {
    wire::RpcError {
        code: code.into(),
        message: message.to_string(),
        actual_revision,
    }
}

/// 比较固定密钥时遍历两侧最大长度，避免在第一个不同字节提前返回。
/// Compare the full maximum length so mismatches do not return at the first differing byte.
fn constant_time_token_eq(expected: &[u8], actual: &[u8]) -> bool {
    let mut difference = expected.len() ^ actual.len();
    let maximum = expected.len().max(actual.len());
    for index in 0..maximum {
        let left = expected.get(index).copied().unwrap_or_default();
        let right = actual.get(index).copied().unwrap_or_default();
        difference |= usize::from(left ^ right);
    }
    difference == 0
}

struct StableHasher(u64);

impl Default for StableHasher {
    fn default() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }
}

impl Hasher for StableHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_comparison_handles_equal_and_different_lengths() {
        assert!(constant_time_token_eq(
            b"abcdefghijklmnop",
            b"abcdefghijklmnop"
        ));
        assert!(!constant_time_token_eq(
            b"abcdefghijklmnop",
            b"abcdefghijklmnoq"
        ));
        assert!(!constant_time_token_eq(b"abcdefghijklmnop", b"abc"));
    }

    #[test]
    fn stable_hasher_is_repeatable() {
        let record = RecordKey::new("player", "1001").unwrap();
        let mut first = StableHasher::default();
        let mut second = StableHasher::default();
        record.hash(&mut first);
        record.hash(&mut second);
        assert_eq!(first.finish(), second.finish());
    }

    #[test]
    fn server_config_debug_redacts_the_auth_token() {
        let token = "secret-server-token";
        let config = ServerConfig::new("127.0.0.1:7800".parse().unwrap(), token);
        let debug = format!("{config:?}");
        assert!(!debug.contains(token));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn server_rejects_short_or_oversized_tokens() {
        let mut config = ServerConfig::new("127.0.0.1:7800".parse().unwrap(), "short");
        assert!(matches!(
            config.validate(),
            Err(ServerError::InvalidConfig(_))
        ));
        config.auth_token = "x".repeat(MAX_AUTH_TOKEN_BYTES + 1);
        assert!(matches!(
            config.validate(),
            Err(ServerError::InvalidConfig(_))
        ));
    }
}
