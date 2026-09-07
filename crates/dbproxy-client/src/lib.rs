//! DBProxy 的异步 Rust 客户端。
//! Async Rust client for DBProxy.
//!
//! 一个客户端连接内的请求按顺序执行；需要更高并发时应创建连接池，不能在 TiangZ
//! 业务线程中等待同步数据库调用。Requests are serialized per connection. Higher concurrency
//! should use a pool, and TiangZ business threads must never perform blocking database I/O.

use std::{
    collections::HashSet,
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use thiserror::Error;
use tiangz_dbproxy_core::{
    AsyncMultiRecordTransactionStore, AsyncSnapshotStore, AsyncTradeStore, AsyncTransactionalStore,
    MultiRecordTransactionReceipt, MultiRecordTransactionalWrite,
    MultiRecordTransactionalWriteOutcome, RecordKey, Revision, SnapshotEnvelope, SnapshotWrite,
    SnapshotWriteOutcome, TradeEnvelope, TradeReceipt, TradeTransaction, TradeTransactionOutcome,
    TransactionReceipt, TransactionRecordReceipt, TransactionalWrite, TransactionalWriteOutcome,
};
use tiangz_dbproxy_protocol::{
    DEFAULT_MAX_FRAME_BYTES, MAX_AUTH_TOKEN_BYTES, MAX_BATCH_LOAD_RECORDS,
    MAX_BATCH_SNAPSHOT_WRITES, MAX_CLIENT_NAME_BYTES, MAX_TRANSACTION_RECORDS,
    PROTOCOL_FINGERPRINT, PROTOCOL_VERSION, ProtocolError, read_message, wire, write_message,
};
use tokio::{net::TcpStream, sync::Mutex, time::timeout};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientConnectionOutcome {
    Connected,
    Timeout,
    Unavailable,
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientRequestOutcome {
    Success,
    Timeout,
    Unavailable,
    RemoteError,
    ProtocolError,
}

/// 单次已结束请求的互斥锁等待与持锁处理时间；不包含重连或外层重试。
/// Timing for one completed attempt, excluding reconnects and outer retries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientRequestTiming {
    pub queue_wait: Duration,
    /// 包含客户端编解码、网络及服务端处理，不是数据库执行时间；失效连接可能未发送。
    /// Includes codec, network and server work, not SQL alone; an unusable connection may not send.
    pub exchange: Duration,
}

impl ClientRequestTiming {
    pub fn total(self) -> Duration {
        self.queue_wait.saturating_add(self.exchange)
    }
}

/// 可选的低开销客户端观测器。实现只能记录有界指标，不得把RecordKey或幂等ID作为标签。
/// Optional low-overhead client observer. Implementations must not label metrics with RecordKey or idempotency IDs.
pub trait ClientObserver: Send + Sync + 'static {
    fn connection_attempt(
        &self,
        endpoint_index: usize,
        elapsed: Duration,
        outcome: ClientConnectionOutcome,
    );

    fn endpoint_failover(&self, from_endpoint_index: usize, to_endpoint_index: usize);

    fn request_attempt(
        &self,
        endpoint_index: usize,
        operation: &'static str,
        elapsed: Duration,
        outcome: ClientRequestOutcome,
    );

    /// 分阶段回调默认转发旧回调一次，已有观察者无需修改；回调不得阻塞。
    /// Defaults to exactly one legacy callback for compatibility; observers must not block.
    fn request_attempt_timed(
        &self,
        endpoint_index: usize,
        operation: &'static str,
        timing: ClientRequestTiming,
        outcome: ClientRequestOutcome,
    ) {
        self.request_attempt(endpoint_index, operation, timing.total(), outcome);
    }
}

/// 客户端连接参数；令牌只用于内部服务认证，不能写入日志或提交到生产配置。
/// Client connection settings; the internal token must not be logged or committed as production data.
#[derive(Clone)]
pub struct ClientConfig {
    pub endpoint: String,
    pub failover_endpoints: Arc<[String]>,
    pub auth_token: String,
    pub client_name: String,
    pub max_frame_bytes: usize,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub observer: Option<Arc<dyn ClientObserver>>,
}

impl fmt::Debug for ClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientConfig")
            .field("endpoint", &self.endpoint)
            .field("failover_endpoints", &self.failover_endpoints)
            .field("auth_token", &"[REDACTED]")
            .field("client_name", &self.client_name)
            .field("max_frame_bytes", &self.max_frame_bytes)
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("observer", &self.observer.as_ref().map(|_| "configured"))
            .finish()
    }
}

impl ClientConfig {
    pub fn new(
        endpoint: impl Into<String>,
        auth_token: impl Into<String>,
        client_name: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            failover_endpoints: Arc::from([]),
            auth_token: auth_token.into(),
            client_name: client_name.into(),
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            observer: None,
        }
    }

    /// 设置有序的故障切换地址；第一个地址仍然是首选 Endpoint。
    /// Set ordered failover endpoints; `endpoint` remains the preferred address.
    pub fn with_endpoints(mut self, endpoints: impl IntoIterator<Item = String>) -> Self {
        self.failover_endpoints = endpoints.into_iter().collect::<Vec<_>>().into();
        self
    }

    /// 安装运行时观测器；它不参与重试决策，也不能读取认证令牌。
    /// Install an observer that never participates in retry decisions or receives credentials.
    pub fn with_observer(mut self, observer: Arc<dyn ClientObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    fn endpoint_candidates(&self) -> Result<Vec<String>, ClientError> {
        let mut candidates = Vec::with_capacity(1 + self.failover_endpoints.len());
        for endpoint in
            std::iter::once(self.endpoint.clone()).chain(self.failover_endpoints.iter().cloned())
        {
            if endpoint.trim().is_empty() {
                return Err(ClientError::InvalidConfig("endpoint is empty"));
            }
            if !candidates.iter().any(|item| item == &endpoint) {
                candidates.push(endpoint);
            }
        }
        if candidates.is_empty() {
            return Err(ClientError::InvalidConfig("endpoint list is empty"));
        }
        Ok(candidates)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteError {
    pub code: wire::ErrorCode,
    pub message: String,
    pub actual_revision: Option<Revision>,
}

pub type BatchSnapshotWriteOutcome = Result<SnapshotWriteOutcome, RemoteError>;
pub type BatchSnapshotEnqueueOutcome = Result<(), RemoteError>;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("invalid client configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("DBProxy connect timed out")]
    ConnectTimeout,
    #[error("DBProxy request timed out; this connection can no longer be reused")]
    RequestTimeout,
    #[error("DBProxy connection can no longer be reused after an incomplete request")]
    ConnectionUnusable,
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error("DBProxy closed the connection")]
    ConnectionClosed,
    #[error("unexpected DBProxy response: {0}")]
    UnexpectedResponse(&'static str),
    #[error("DBProxy rejected the request: {0:?}")]
    Remote(RemoteError),
}

struct ClientConnection {
    stream: TcpStream,
    endpoint_index: usize,
    next_rpc_id: u64,
    max_frame_bytes: usize,
    request_timeout: Duration,
    usable: bool,
}

/// 可克隆的客户端句柄；克隆只共享同一条连接，不会自动增加并发度。
/// Cloneable client handle; clones share one connection and do not add parallelism.
#[derive(Clone)]
pub struct DbProxyClient {
    connection: Arc<Mutex<ClientConnection>>,
    config: ClientConfig,
}

/// 多连接客户端池；同一个 RecordKey 在每类连接中稳定路由，不同记录可以并行请求。
/// Multi-connection pool; one RecordKey is stable within each connection class while different
/// records run in parallel.
#[derive(Clone)]
pub struct DbProxyClientPool {
    read_clients: Arc<[DbProxyClient]>,
    write_clients: Arc<[DbProxyClient]>,
}

impl DbProxyClientPool {
    /// Create one shared connection class. This preserves the original ordering and connection
    /// count: reads and writes for the same routing key use the same TCP connection.
    pub async fn connect(config: ClientConfig, size: usize) -> Result<Self, ClientError> {
        let clients = Self::connect_clients(config, size, "client pool size is zero").await?;
        Ok(Self {
            read_clients: Arc::clone(&clients),
            write_clients: clients,
        })
    }

    /// Create independent read and write connection classes.
    ///
    /// A slow write, reconnect, or durable AOF acknowledgement can no longer hold the mutex of a
    /// read connection. Callers must still serialize game rules where required and use revision/CAS
    /// for correctness; this method only isolates transport head-of-line blocking.
    pub async fn connect_split(
        config: ClientConfig,
        read_size: usize,
        write_size: usize,
    ) -> Result<Self, ClientError> {
        if read_size == 0 {
            return Err(ClientError::InvalidConfig("read pool size is zero"));
        }
        if write_size == 0 {
            return Err(ClientError::InvalidConfig("write pool size is zero"));
        }
        let read_clients =
            Self::connect_clients(config.clone(), read_size, "read pool size is zero").await?;
        let write_clients =
            Self::connect_clients(config, write_size, "write pool size is zero").await?;
        Ok(Self {
            read_clients,
            write_clients,
        })
    }

    async fn connect_clients(
        config: ClientConfig,
        size: usize,
        zero_error: &'static str,
    ) -> Result<Arc<[DbProxyClient]>, ClientError> {
        if size == 0 {
            return Err(ClientError::InvalidConfig(zero_error));
        }
        let mut clients = Vec::with_capacity(size);
        for _ in 0..size {
            clients.push(DbProxyClient::connect(config.clone()).await?);
        }
        Ok(clients.into())
    }

    /// Total number of physical TCP connections owned by this pool.
    pub fn len(&self) -> usize {
        if Arc::ptr_eq(&self.read_clients, &self.write_clients) {
            self.read_clients.len()
        } else {
            self.read_clients.len() + self.write_clients.len()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.read_clients.is_empty() || self.write_clients.is_empty()
    }

    pub fn read_len(&self) -> usize {
        self.read_clients.len()
    }

    pub fn write_len(&self) -> usize {
        self.write_clients.len()
    }

    pub fn is_split(&self) -> bool {
        !Arc::ptr_eq(&self.read_clients, &self.write_clients)
    }

    fn client_for_record<'a>(
        clients: &'a [DbProxyClient],
        record: &RecordKey,
    ) -> &'a DbProxyClient {
        let mut hasher = StableHasher::default();
        record.hash(&mut hasher);
        &clients[(hasher.finish() as usize) % clients.len()]
    }

    fn read_client(&self, record: &RecordKey) -> &DbProxyClient {
        Self::client_for_record(&self.read_clients, record)
    }

    fn write_client(&self, record: &RecordKey) -> &DbProxyClient {
        Self::client_for_record(&self.write_clients, record)
    }

    pub async fn load(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, ClientError> {
        self.read_client(record).load(record).await
    }

    pub async fn load_multi(
        &self,
        records: &[RecordKey],
    ) -> Result<Vec<Option<SnapshotEnvelope>>, ClientError> {
        let first = records
            .first()
            .ok_or(ClientError::InvalidConfig("batch load records are empty"))?;
        self.read_client(first).load_multi(records).await
    }

    pub async fn save(&self, request: SnapshotWrite) -> Result<SnapshotWriteOutcome, ClientError> {
        self.write_client(&request.record).save(request).await
    }

    pub async fn save_multi(
        &self,
        requests: &[SnapshotWrite],
    ) -> Result<Vec<BatchSnapshotWriteOutcome>, ClientError> {
        let first = requests
            .first()
            .ok_or(ClientError::InvalidConfig("batch save writes are empty"))?;
        self.write_client(&first.record).save_multi(requests).await
    }

    pub async fn enqueue_snapshot(&self, request: SnapshotWrite) -> Result<(), ClientError> {
        self.write_client(&request.record)
            .enqueue_snapshot(request)
            .await
    }

    pub async fn enqueue_multi_snapshot(
        &self,
        requests: &[SnapshotWrite],
    ) -> Result<Vec<BatchSnapshotEnqueueOutcome>, ClientError> {
        let first = requests
            .first()
            .ok_or(ClientError::InvalidConfig("batch enqueue writes are empty"))?;
        self.write_client(&first.record)
            .enqueue_multi_snapshot(requests)
            .await
    }

    pub async fn apply_transaction(
        &self,
        request: TransactionalWrite,
    ) -> Result<TransactionalWriteOutcome, ClientError> {
        self.write_client(&request.record)
            .apply_transaction(request)
            .await
    }

    pub async fn load_transaction(
        &self,
        operation_id: &str,
        record: &RecordKey,
    ) -> Result<Option<TransactionReceipt>, ClientError> {
        self.read_client(record)
            .load_transaction(operation_id, record)
            .await
    }

    pub async fn apply_multi_transaction(
        &self,
        request: MultiRecordTransactionalWrite,
    ) -> Result<MultiRecordTransactionalWriteOutcome, ClientError> {
        self.write_client_for_operation(&request.operation_id)
            .apply_multi_transaction(request)
            .await
    }

    pub async fn load_multi_transaction(
        &self,
        operation_id: &str,
        records: &[RecordKey],
    ) -> Result<Option<MultiRecordTransactionReceipt>, ClientError> {
        self.read_client_for_operation(operation_id)
            .load_multi_transaction(operation_id, records)
            .await
    }

    pub async fn commit_records(
        &self,
        request: MultiRecordTransactionalWrite,
        effects: tiangz_dbproxy_core::CommitEffects,
    ) -> Result<MultiRecordTransactionalWriteOutcome, ClientError> {
        self.write_client_for_operation(&request.operation_id)
            .commit_records(request, effects)
            .await
    }

    pub async fn load_trade(&self, trade_id: &str) -> Result<Option<TradeEnvelope>, ClientError> {
        self.read_client_for_operation(trade_id)
            .load_trade(trade_id)
            .await
    }

    pub async fn apply_trade_transaction(
        &self,
        request: TradeTransaction,
    ) -> Result<TradeTransactionOutcome, ClientError> {
        self.write_client_for_operation(&request.operation_id)
            .apply_trade_transaction(request)
            .await
    }

    pub async fn load_trade_transaction(
        &self,
        operation_id: &str,
        trade_id: &str,
    ) -> Result<Option<TradeReceipt>, ClientError> {
        self.read_client_for_operation(operation_id)
            .load_trade_transaction(operation_id, trade_id)
            .await
    }

    fn client_for_operation<'a>(
        clients: &'a [DbProxyClient],
        operation_id: &str,
    ) -> &'a DbProxyClient {
        let mut hasher = StableHasher::default();
        operation_id.hash(&mut hasher);
        &clients[(hasher.finish() as usize) % clients.len()]
    }

    fn read_client_for_operation(&self, operation_id: &str) -> &DbProxyClient {
        Self::client_for_operation(&self.read_clients, operation_id)
    }

    fn write_client_for_operation(&self, operation_id: &str) -> &DbProxyClient {
        Self::client_for_operation(&self.write_clients, operation_id)
    }
}

impl DbProxyClient {
    /// 连接并完成版本、指纹和令牌握手。
    /// Connect and complete protocol-version, fingerprint, and token negotiation.
    pub async fn connect(config: ClientConfig) -> Result<Self, ClientError> {
        let candidates = config.endpoint_candidates()?;
        let mut last_error = None;
        for endpoint_index in 0..candidates.len() {
            match Self::connect_observed(config.clone(), endpoint_index).await {
                Ok(client) => return Ok(client),
                Err(error) if is_endpoint_unavailable(&error) => last_error = Some(error),
                Err(error) => return Err(error),
            }
        }
        Err(last_error.unwrap_or(ClientError::ConnectionClosed))
    }

    async fn connect_observed(
        config: ClientConfig,
        endpoint_index: usize,
    ) -> Result<Self, ClientError> {
        let started_at = Instant::now();
        let result = Self::connect_single(config.clone(), endpoint_index).await;
        if let Some(observer) = &config.observer {
            observer.connection_attempt(
                endpoint_index,
                started_at.elapsed(),
                connection_outcome(&result),
            );
        }
        result
    }

    async fn connect_single(
        config: ClientConfig,
        endpoint_index: usize,
    ) -> Result<Self, ClientError> {
        let candidates = config.endpoint_candidates()?;
        let endpoint = candidates
            .get(endpoint_index)
            .ok_or(ClientError::InvalidConfig("endpoint index is invalid"))?;
        if config.endpoint.trim().is_empty() {
            return Err(ClientError::InvalidConfig("endpoint is empty"));
        }
        if !(16..=MAX_AUTH_TOKEN_BYTES).contains(&config.auth_token.len()) {
            return Err(ClientError::InvalidConfig(
                "auth token length is outside 16..=512 bytes",
            ));
        }
        if config.client_name.trim().is_empty() || config.client_name.len() > MAX_CLIENT_NAME_BYTES
        {
            return Err(ClientError::InvalidConfig(
                "client name is empty or too long",
            ));
        }
        if config.max_frame_bytes == 0 {
            return Err(ClientError::InvalidConfig("max frame bytes is zero"));
        }

        let mut stream = timeout(config.connect_timeout, TcpStream::connect(endpoint))
            .await
            .map_err(|_| ClientError::ConnectTimeout)?
            .map_err(ProtocolError::from)?;
        stream.set_nodelay(true).map_err(ProtocolError::from)?;

        let hello = wire::ClientFrame {
            body: Some(wire::client_frame::Body::Hello(wire::ClientHello {
                protocol_version: PROTOCOL_VERSION,
                protocol_fingerprint: PROTOCOL_FINGERPRINT.to_string(),
                auth_token: config.auth_token.clone(),
                client_name: config.client_name.clone(),
            })),
        };
        timeout(
            config.connect_timeout,
            write_message(&mut stream, &hello, config.max_frame_bytes),
        )
        .await
        .map_err(|_| ClientError::ConnectTimeout)??;
        let frame = timeout(
            config.connect_timeout,
            read_message::<_, wire::ServerFrame>(&mut stream, config.max_frame_bytes),
        )
        .await
        .map_err(|_| ClientError::ConnectTimeout)??
        .ok_or(ClientError::ConnectionClosed)?;
        let wire::server_frame::Body::Hello(hello) = frame
            .body
            .ok_or(ClientError::UnexpectedResponse("empty handshake frame"))?
        else {
            return Err(ClientError::UnexpectedResponse(
                "server sent a response before handshake",
            ));
        };
        if !hello.accepted {
            return Err(ClientError::Remote(remote_error(hello.error)));
        }
        if hello.protocol_version != PROTOCOL_VERSION
            || hello.protocol_fingerprint != PROTOCOL_FINGERPRINT
            || !hello.supports_outbox_relay
        {
            return Err(ClientError::UnexpectedResponse(
                "server accepted a different protocol",
            ));
        }

        Ok(Self {
            connection: Arc::new(Mutex::new(ClientConnection {
                stream,
                endpoint_index,
                next_rpc_id: 1,
                max_frame_bytes: config.max_frame_bytes,
                request_timeout: config.request_timeout,
                usable: true,
            })),
            config,
        })
    }

    async fn call_once(
        &self,
        body: wire::request_envelope::Body,
    ) -> Result<wire::ResponseEnvelope, ClientError> {
        let operation = request_operation(&body);
        let started_at = Instant::now();
        let mut connection = self.connection.lock().await;
        let queue_wait = started_at.elapsed();
        let endpoint_index = connection.endpoint_index;
        let result = async {
            if !connection.usable {
                return Err(ClientError::ConnectionUnusable);
            }
            let rpc_id = connection.next_rpc_id;
            connection.next_rpc_id = connection.next_rpc_id.wrapping_add(1).max(1);
            let frame = wire::ClientFrame {
                body: Some(wire::client_frame::Body::Request(wire::RequestEnvelope {
                    rpc_id,
                    body: Some(body),
                })),
            };
            let max_frame_bytes = connection.max_frame_bytes;
            let request_timeout = connection.request_timeout;
            let exchange = async {
                write_message(&mut connection.stream, &frame, max_frame_bytes).await?;
                read_message::<_, wire::ServerFrame>(&mut connection.stream, max_frame_bytes).await
            };
            let result = match timeout(request_timeout, exchange).await {
                Ok(result) => result,
                Err(_) => {
                    connection.usable = false;
                    return Err(ClientError::RequestTimeout);
                }
            };
            let frame = match result {
                Ok(Some(frame)) => frame,
                Ok(None) => {
                    connection.usable = false;
                    return Err(ClientError::ConnectionClosed);
                }
                Err(error) => {
                    connection.usable = false;
                    return Err(error.into());
                }
            };
            let wire::server_frame::Body::Response(response) = frame
                .body
                .ok_or(ClientError::UnexpectedResponse("empty response frame"))?
            else {
                connection.usable = false;
                return Err(ClientError::UnexpectedResponse(
                    "server repeated the handshake",
                ));
            };
            if response.rpc_id != rpc_id {
                connection.usable = false;
                return Err(ClientError::UnexpectedResponse("rpc id mismatch"));
            }
            if response.error.is_some() {
                return Err(ClientError::Remote(remote_error(response.error)));
            }
            Ok(response)
        }
        .await;
        if let Some(observer) = &self.config.observer {
            observer.request_attempt_timed(
                endpoint_index,
                operation,
                ClientRequestTiming {
                    queue_wait,
                    exchange: started_at.elapsed().saturating_sub(queue_wait),
                },
                request_outcome(&result),
            );
        }
        result
    }

    async fn call(
        &self,
        body: wire::request_envelope::Body,
    ) -> Result<wire::ResponseEnvelope, ClientError> {
        match self.call_once(body.clone()).await {
            Ok(response) => Ok(response),
            Err(error) if is_reconnectable(&error) => {
                self.reconnect_next().await?;
                self.call_once(body).await
            }
            Err(error) => Err(error),
        }
    }

    async fn reconnect_next(&self) -> Result<(), ClientError> {
        let candidates = self.config.endpoint_candidates()?;
        let mut connection = self.connection.lock().await;
        // 并发失败者可能排在成功重连者后面，不要再替换已修复的连接。
        // A concurrent caller may already have repaired this shared connection.
        if connection.usable {
            return Ok(());
        }
        let current_index = connection.endpoint_index;
        let mut last_error = None;
        for offset in 1..=candidates.len() {
            let endpoint_index = (current_index + offset) % candidates.len();
            match Self::connect_observed(self.config.clone(), endpoint_index).await {
                Ok(next) => {
                    let next_connection = Arc::try_unwrap(next.connection)
                        .map_err(|_| ClientError::ConnectionUnusable)?
                        .into_inner();
                    *connection = next_connection;
                    if let Some(observer) = &self.config.observer {
                        observer.endpoint_failover(current_index, endpoint_index);
                    }
                    return Ok(());
                }
                Err(error) if is_endpoint_unavailable(&error) => last_error = Some(error),
                Err(error) => return Err(error),
            }
        }
        connection.usable = false;
        Err(last_error.unwrap_or(ClientError::ConnectionClosed))
    }

    pub async fn load(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, ClientError> {
        let response = self
            .call(wire::request_envelope::Body::LoadSnapshot(
                wire::LoadSnapshotRequest {
                    record: Some(record.into()),
                },
            ))
            .await?;
        let Some(wire::response_envelope::Body::LoadSnapshot(result)) = response.body else {
            return Err(ClientError::UnexpectedResponse(
                "load returned another response type",
            ));
        };
        result
            .snapshot
            .map(TryInto::try_into)
            .transpose()
            .map_err(Into::into)
    }

    pub async fn load_multi(
        &self,
        records: &[RecordKey],
    ) -> Result<Vec<Option<SnapshotEnvelope>>, ClientError> {
        if records.is_empty() || records.len() > MAX_BATCH_LOAD_RECORDS {
            return Err(ClientError::InvalidConfig(
                "batch load size is outside the protocol limit",
            ));
        }
        if records.iter().collect::<HashSet<_>>().len() != records.len() {
            return Err(ClientError::InvalidConfig(
                "batch load records contain duplicates",
            ));
        }
        let response = self
            .call(wire::request_envelope::Body::LoadMultiSnapshot(
                wire::LoadMultiSnapshotRequest {
                    records: records.iter().map(Into::into).collect(),
                },
            ))
            .await?;
        let Some(wire::response_envelope::Body::LoadMultiSnapshot(result)) = response.body else {
            return Err(ClientError::UnexpectedResponse(
                "batch load returned another response type",
            ));
        };
        if result.entries.len() != records.len() {
            return Err(ClientError::UnexpectedResponse(
                "batch load returned a mismatched result count",
            ));
        }
        result
            .entries
            .into_iter()
            .zip(records)
            .map(|(entry, expected)| {
                let snapshot = entry
                    .snapshot
                    .map(TryInto::try_into)
                    .transpose()
                    .map_err(ClientError::from)?;
                if snapshot
                    .as_ref()
                    .is_some_and(|snapshot: &SnapshotEnvelope| &snapshot.record != expected)
                {
                    return Err(ClientError::UnexpectedResponse(
                        "batch load snapshot identity mismatch",
                    ));
                }
                Ok(snapshot)
            })
            .collect()
    }

    pub async fn save(&self, request: SnapshotWrite) -> Result<SnapshotWriteOutcome, ClientError> {
        let response = self
            .call(wire::request_envelope::Body::SaveSnapshot(
                (&request).into(),
            ))
            .await?;
        let Some(wire::response_envelope::Body::SaveSnapshot(result)) = response.body else {
            return Err(ClientError::UnexpectedResponse(
                "save returned another response type",
            ));
        };
        snapshot_write_outcome(result)
    }

    pub async fn save_multi(
        &self,
        requests: &[SnapshotWrite],
    ) -> Result<Vec<BatchSnapshotWriteOutcome>, ClientError> {
        validate_snapshot_write_batch(requests)?;
        let response = self
            .call(wire::request_envelope::Body::SaveMultiSnapshot(
                wire::SaveMultiSnapshotRequest {
                    writes: requests.iter().map(Into::into).collect(),
                },
            ))
            .await?;
        let Some(wire::response_envelope::Body::SaveMultiSnapshot(result)) = response.body else {
            return Err(ClientError::UnexpectedResponse(
                "batch save returned another response type",
            ));
        };
        if result.entries.len() != requests.len() {
            return Err(ClientError::UnexpectedResponse(
                "batch save returned a mismatched result count",
            ));
        }
        result
            .entries
            .into_iter()
            .map(|entry| match (entry.result, entry.error) {
                (Some(result), None) => snapshot_write_outcome(result).map(Ok),
                (None, Some(error)) => Ok(Err(remote_error(Some(error)))),
                _ => Err(ClientError::UnexpectedResponse(
                    "batch save entry has an invalid result shape",
                )),
            })
            .collect()
    }

    /// 把允许回退的普通快照写入 Redis 持久积压；成功只表示 backlog 已接收，
    /// 不表示 PostgreSQL 已完成。Enqueue a rollback-tolerant snapshot. Success means the durable
    /// backlog accepted it, not that PostgreSQL has already committed it.
    pub async fn enqueue_snapshot(&self, request: SnapshotWrite) -> Result<(), ClientError> {
        let response = self
            .call(wire::request_envelope::Body::EnqueueSnapshot(
                wire::EnqueueSnapshotRequest {
                    write: Some((&request).into()),
                },
            ))
            .await?;
        let Some(wire::response_envelope::Body::EnqueueSnapshot(result)) = response.body else {
            return Err(ClientError::UnexpectedResponse(
                "enqueue returned another response type",
            ));
        };
        if !result.accepted {
            return Err(ClientError::UnexpectedResponse(
                "enqueue returned accepted=false without an error",
            ));
        }
        Ok(())
    }

    pub async fn enqueue_multi_snapshot(
        &self,
        requests: &[SnapshotWrite],
    ) -> Result<Vec<BatchSnapshotEnqueueOutcome>, ClientError> {
        validate_snapshot_write_batch(requests)?;
        let response = self
            .call(wire::request_envelope::Body::EnqueueMultiSnapshot(
                wire::EnqueueMultiSnapshotRequest {
                    writes: requests.iter().map(Into::into).collect(),
                },
            ))
            .await?;
        let Some(wire::response_envelope::Body::EnqueueMultiSnapshot(result)) = response.body
        else {
            return Err(ClientError::UnexpectedResponse(
                "batch enqueue returned another response type",
            ));
        };
        if result.entries.len() != requests.len() {
            return Err(ClientError::UnexpectedResponse(
                "batch enqueue returned a mismatched result count",
            ));
        }
        result
            .entries
            .into_iter()
            .map(|entry| match (entry.accepted, entry.error) {
                (true, None) => Ok(Ok(())),
                (false, Some(error)) => Ok(Err(remote_error(Some(error)))),
                _ => Err(ClientError::UnexpectedResponse(
                    "batch enqueue entry has an invalid result shape",
                )),
            })
            .collect()
    }

    pub async fn apply_transaction(
        &self,
        request: TransactionalWrite,
    ) -> Result<TransactionalWriteOutcome, ClientError> {
        let response = self
            .call(wire::request_envelope::Body::ApplyTransaction(
                (&request).into(),
            ))
            .await?;
        let Some(wire::response_envelope::Body::ApplyTransaction(result)) = response.body else {
            return Err(ClientError::UnexpectedResponse(
                "transaction returned another response type",
            ));
        };
        match wire::WriteDisposition::try_from(result.disposition).ok() {
            Some(wire::WriteDisposition::Applied) => Ok(TransactionalWriteOutcome::Applied {
                new_revision: Revision(result.new_revision),
                result: result.result,
            }),
            Some(wire::WriteDisposition::Duplicate) => Ok(TransactionalWriteOutcome::Duplicate {
                new_revision: Revision(result.new_revision),
                result: result.result,
            }),
            _ => Err(ClientError::UnexpectedResponse(
                "transaction returned an invalid disposition",
            )),
        }
    }

    /// 查询一次已经提交的事务结果；只用于恢复“提交成功但调用方未收到响应”的窄窗口。
    /// Load a committed transaction result to recover the narrow window where
    /// storage committed but the caller did not receive the response.
    pub async fn load_transaction(
        &self,
        operation_id: &str,
        record: &RecordKey,
    ) -> Result<Option<TransactionReceipt>, ClientError> {
        let response = self
            .call(wire::request_envelope::Body::LoadTransaction(
                wire::LoadTransactionRequest {
                    operation_id: operation_id.to_string(),
                    record: Some(record.into()),
                },
            ))
            .await?;
        let Some(wire::response_envelope::Body::LoadTransaction(result)) = response.body else {
            return Err(ClientError::UnexpectedResponse(
                "transaction lookup returned another response type",
            ));
        };
        let Some(receipt) = result.receipt else {
            return Ok(None);
        };
        let receipt_record: RecordKey = receipt
            .record
            .ok_or(ClientError::UnexpectedResponse(
                "transaction receipt is missing its record",
            ))?
            .try_into()?;
        if receipt.operation_id != operation_id || &receipt_record != record {
            return Err(ClientError::UnexpectedResponse(
                "transaction receipt identity mismatch",
            ));
        }
        Ok(Some(TransactionReceipt {
            operation_id: receipt.operation_id,
            record: receipt_record,
            new_revision: Revision(receipt.new_revision),
            result: receipt.result,
        }))
    }

    pub async fn apply_multi_transaction(
        &self,
        request: MultiRecordTransactionalWrite,
    ) -> Result<MultiRecordTransactionalWriteOutcome, ClientError> {
        self.apply_records(request, None).await
    }

    pub async fn commit_records(
        &self,
        request: MultiRecordTransactionalWrite,
        effects: tiangz_dbproxy_core::CommitEffects,
    ) -> Result<MultiRecordTransactionalWriteOutcome, ClientError> {
        self.apply_records(request, Some(effects)).await
    }

    async fn apply_records(
        &self,
        request: MultiRecordTransactionalWrite,
        effects: Option<tiangz_dbproxy_core::CommitEffects>,
    ) -> Result<MultiRecordTransactionalWriteOutcome, ClientError> {
        if request.writes.is_empty() || request.writes.len() > MAX_TRANSACTION_RECORDS {
            return Err(ClientError::InvalidConfig(
                "multi-record transaction size is outside the protocol limit",
            ));
        }
        let is_commit = effects.is_some();
        let body = if let Some(effects) = effects {
            if effects.appends.len() > MAX_TRANSACTION_RECORDS
                || effects.outbox_events.len() > tiangz_dbproxy_protocol::MAX_OUTBOX_EVENTS
            {
                return Err(ClientError::InvalidConfig("commit effects exceed limits"));
            }
            wire::request_envelope::Body::CommitRecords(wire::CommitRecordsRequest {
                operation_id: request.operation_id,
                writes: request.writes.iter().map(Into::into).collect(),
                result: request.result,
                appends: effects.appends.iter().map(Into::into).collect(),
                outbox_events: effects.outbox_events.iter().map(Into::into).collect(),
            })
        } else {
            wire::request_envelope::Body::ApplyMultiTransaction(
                wire::ApplyMultiTransactionRequest {
                    operation_id: request.operation_id,
                    writes: request.writes.iter().map(Into::into).collect(),
                    result: request.result,
                },
            )
        };
        let response = self.call(body).await?;
        let result = match response.body {
            Some(wire::response_envelope::Body::ApplyMultiTransaction(r)) if !is_commit => r,
            Some(wire::response_envelope::Body::CommitRecords(r)) if is_commit => r,
            _ => {
                return Err(ClientError::UnexpectedResponse(
                    "record commit returned another response type",
                ));
            }
        };
        let records = result
            .records
            .into_iter()
            .map(|receipt| {
                let record = receipt
                    .record
                    .ok_or(ClientError::UnexpectedResponse(
                        "multi-transaction receipt is missing its record",
                    ))?
                    .try_into()?;
                Ok(TransactionRecordReceipt {
                    record,
                    new_revision: Revision(receipt.new_revision),
                })
            })
            .collect::<Result<Vec<_>, ClientError>>()?;
        let disposition = wire::WriteDisposition::try_from(result.disposition).map_err(|_| {
            ClientError::UnexpectedResponse("multi-transaction disposition is invalid")
        })?;
        match disposition {
            wire::WriteDisposition::Applied => Ok(MultiRecordTransactionalWriteOutcome::Applied {
                records,
                result: result.result,
            }),
            wire::WriteDisposition::Duplicate => {
                Ok(MultiRecordTransactionalWriteOutcome::Duplicate {
                    records,
                    result: result.result,
                })
            }
            _ => Err(ClientError::UnexpectedResponse(
                "multi-transaction returned an invalid disposition",
            )),
        }
    }

    pub async fn load_multi_transaction(
        &self,
        operation_id: &str,
        records: &[RecordKey],
    ) -> Result<Option<MultiRecordTransactionReceipt>, ClientError> {
        if operation_id.trim().is_empty()
            || records.is_empty()
            || records.len() > MAX_TRANSACTION_RECORDS
        {
            return Err(ClientError::InvalidConfig(
                "multi-transaction lookup arguments are invalid",
            ));
        }
        let response = self
            .call(wire::request_envelope::Body::LoadMultiTransaction(
                wire::LoadMultiTransactionRequest {
                    operation_id: operation_id.to_string(),
                    records: records.iter().map(Into::into).collect(),
                },
            ))
            .await?;
        let Some(wire::response_envelope::Body::LoadMultiTransaction(result)) = response.body
        else {
            return Err(ClientError::UnexpectedResponse(
                "multi-transaction lookup returned another response type",
            ));
        };
        let Some(receipt) = result.receipt else {
            return Ok(None);
        };
        if receipt.operation_id != operation_id {
            return Err(ClientError::UnexpectedResponse(
                "multi-transaction receipt identity mismatch",
            ));
        }
        let receipts = receipt
            .records
            .into_iter()
            .map(|item| {
                Ok(TransactionRecordReceipt {
                    record: item
                        .record
                        .ok_or(ClientError::UnexpectedResponse(
                            "multi-transaction lookup record is missing",
                        ))?
                        .try_into()?,
                    new_revision: Revision(item.new_revision),
                })
            })
            .collect::<Result<Vec<_>, ClientError>>()?;
        Ok(Some(MultiRecordTransactionReceipt {
            operation_id: receipt.operation_id,
            records: receipts,
            result: receipt.result,
        }))
    }

    pub async fn load_trade(&self, trade_id: &str) -> Result<Option<TradeEnvelope>, ClientError> {
        let response = self
            .call(wire::request_envelope::Body::LoadTrade(
                wire::LoadTradeRequest {
                    trade_id: trade_id.to_string(),
                },
            ))
            .await?;
        let Some(wire::response_envelope::Body::LoadTrade(result)) = response.body else {
            return Err(ClientError::UnexpectedResponse(
                "trade lookup returned another response type",
            ));
        };
        result
            .trade
            .map(TryInto::try_into)
            .transpose()
            .map_err(Into::into)
    }

    pub async fn apply_trade_transaction(
        &self,
        request: TradeTransaction,
    ) -> Result<TradeTransactionOutcome, ClientError> {
        let operation_id = request.operation_id.clone();
        let trade_id = request.transition.trade_id.clone();
        let response = self
            .call(wire::request_envelope::Body::ApplyTradeTransaction(
                (&request).into(),
            ))
            .await?;
        let Some(wire::response_envelope::Body::ApplyTradeTransaction(result)) = response.body
        else {
            return Err(ClientError::UnexpectedResponse(
                "trade transaction returned another response type",
            ));
        };
        let receipt: TradeReceipt = result
            .receipt
            .ok_or(ClientError::UnexpectedResponse(
                "trade transaction response is missing its receipt",
            ))?
            .try_into()?;
        if receipt.operation_id != operation_id
            || receipt.trade_id != trade_id
            || !trade_receipt_matches_request(&receipt, &request)
        {
            return Err(ClientError::UnexpectedResponse(
                "trade transaction receipt does not match the request",
            ));
        }
        match wire::WriteDisposition::try_from(result.disposition).ok() {
            Some(wire::WriteDisposition::Applied) => Ok(TradeTransactionOutcome::Applied(receipt)),
            Some(wire::WriteDisposition::Duplicate) => {
                Ok(TradeTransactionOutcome::Duplicate(receipt))
            }
            _ => Err(ClientError::UnexpectedResponse(
                "trade transaction returned an invalid disposition",
            )),
        }
    }

    pub async fn load_trade_transaction(
        &self,
        operation_id: &str,
        trade_id: &str,
    ) -> Result<Option<TradeReceipt>, ClientError> {
        let response = self
            .call(wire::request_envelope::Body::LoadTradeTransaction(
                wire::LoadTradeTransactionRequest {
                    operation_id: operation_id.to_string(),
                    trade_id: trade_id.to_string(),
                },
            ))
            .await?;
        let Some(wire::response_envelope::Body::LoadTradeTransaction(result)) = response.body
        else {
            return Err(ClientError::UnexpectedResponse(
                "trade transaction lookup returned another response type",
            ));
        };
        let receipt = result
            .receipt
            .map(TryInto::try_into)
            .transpose()
            .map_err(ClientError::from)?;
        if receipt.as_ref().is_some_and(|receipt: &TradeReceipt| {
            receipt.operation_id != operation_id || receipt.trade_id != trade_id
        }) {
            return Err(ClientError::UnexpectedResponse(
                "trade transaction lookup identity mismatch",
            ));
        }
        Ok(receipt)
    }
}

fn trade_receipt_matches_request(receipt: &TradeReceipt, request: &TradeTransaction) -> bool {
    let Some(expected_trade_version) = request.transition.expected_version.0.checked_add(1) else {
        return false;
    };
    if receipt.new_trade_version != Revision(expected_trade_version)
        || receipt.state != request.transition.next_state
        || receipt.result != request.result
    {
        return false;
    }

    let mut expected_records = request
        .writes
        .iter()
        .filter_map(|write| {
            write
                .expected_revision
                .0
                .checked_add(1)
                .map(|revision| (&write.record, Revision(revision)))
        })
        .collect::<Vec<_>>();
    if expected_records.len() != request.writes.len() {
        return false;
    }
    expected_records.sort_by(|left, right| {
        left.0
            .namespace
            .cmp(&right.0.namespace)
            .then_with(|| left.0.key.cmp(&right.0.key))
    });
    let mut actual_records = receipt.records.iter().collect::<Vec<_>>();
    actual_records.sort_by(|left, right| {
        left.record
            .namespace
            .cmp(&right.record.namespace)
            .then_with(|| left.record.key.cmp(&right.record.key))
    });
    if expected_records.len() != actual_records.len()
        || expected_records.iter().zip(actual_records).any(
            |((expected_record, expected_revision), actual)| {
                *expected_record != &actual.record || *expected_revision != actual.new_revision
            },
        )
    {
        return false;
    }

    let mut expected_postings = request
        .ledger_postings
        .iter()
        .map(|posting| posting.posting_id.as_str())
        .collect::<Vec<_>>();
    expected_postings.sort_unstable();
    let mut actual_postings = receipt
        .ledger_posting_ids
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    actual_postings.sort_unstable();
    let mut expected_events = request
        .outbox_events
        .iter()
        .map(|event| event.event_id.as_str())
        .collect::<Vec<_>>();
    expected_events.sort_unstable();
    let mut actual_events = receipt
        .outbox_event_ids
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    actual_events.sort_unstable();
    expected_postings == actual_postings && expected_events == actual_events
}

#[async_trait]
impl AsyncSnapshotStore for DbProxyClient {
    type Error = ClientError;

    async fn load(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, Self::Error> {
        DbProxyClient::load(self, record).await
    }

    async fn save(&mut self, request: SnapshotWrite) -> Result<SnapshotWriteOutcome, Self::Error> {
        DbProxyClient::save(self, request).await
    }
}

#[async_trait]
impl AsyncTransactionalStore for DbProxyClient {
    type Error = ClientError;

    async fn load_receipt(
        &self,
        operation_id: &str,
        record: &RecordKey,
    ) -> Result<Option<TransactionReceipt>, Self::Error> {
        self.load_transaction(operation_id, record).await
    }

    async fn apply(
        &mut self,
        request: TransactionalWrite,
    ) -> Result<TransactionalWriteOutcome, Self::Error> {
        self.apply_transaction(request).await
    }
}

#[async_trait]
impl AsyncMultiRecordTransactionStore for DbProxyClient {
    type Error = ClientError;

    async fn load_multi_receipt(
        &self,
        operation_id: &str,
        records: &[RecordKey],
    ) -> Result<Option<MultiRecordTransactionReceipt>, Self::Error> {
        self.load_multi_transaction(operation_id, records).await
    }

    async fn apply_multi(
        &mut self,
        request: MultiRecordTransactionalWrite,
    ) -> Result<MultiRecordTransactionalWriteOutcome, Self::Error> {
        self.apply_multi_transaction(request).await
    }
}

#[async_trait]
impl AsyncTradeStore for DbProxyClient {
    type Error = ClientError;

    async fn load_trade(&self, trade_id: &str) -> Result<Option<TradeEnvelope>, Self::Error> {
        DbProxyClient::load_trade(self, trade_id).await
    }

    async fn load_trade_receipt(
        &self,
        operation_id: &str,
        trade_id: &str,
    ) -> Result<Option<TradeReceipt>, Self::Error> {
        self.load_trade_transaction(operation_id, trade_id).await
    }

    async fn apply_trade(
        &mut self,
        request: TradeTransaction,
    ) -> Result<TradeTransactionOutcome, Self::Error> {
        self.apply_trade_transaction(request).await
    }
}

#[async_trait]
impl AsyncSnapshotStore for DbProxyClientPool {
    type Error = ClientError;

    async fn load(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, Self::Error> {
        DbProxyClientPool::load(self, record).await
    }

    async fn save(&mut self, request: SnapshotWrite) -> Result<SnapshotWriteOutcome, Self::Error> {
        DbProxyClientPool::save(self, request).await
    }
}

#[async_trait]
impl AsyncTransactionalStore for DbProxyClientPool {
    type Error = ClientError;

    async fn load_receipt(
        &self,
        operation_id: &str,
        record: &RecordKey,
    ) -> Result<Option<TransactionReceipt>, Self::Error> {
        self.load_transaction(operation_id, record).await
    }

    async fn apply(
        &mut self,
        request: TransactionalWrite,
    ) -> Result<TransactionalWriteOutcome, Self::Error> {
        self.apply_transaction(request).await
    }
}

#[async_trait]
impl AsyncMultiRecordTransactionStore for DbProxyClientPool {
    type Error = ClientError;

    async fn load_multi_receipt(
        &self,
        operation_id: &str,
        records: &[RecordKey],
    ) -> Result<Option<MultiRecordTransactionReceipt>, Self::Error> {
        self.load_multi_transaction(operation_id, records).await
    }

    async fn apply_multi(
        &mut self,
        request: MultiRecordTransactionalWrite,
    ) -> Result<MultiRecordTransactionalWriteOutcome, Self::Error> {
        self.apply_multi_transaction(request).await
    }
}

#[async_trait]
impl AsyncTradeStore for DbProxyClientPool {
    type Error = ClientError;

    async fn load_trade(&self, trade_id: &str) -> Result<Option<TradeEnvelope>, Self::Error> {
        DbProxyClientPool::load_trade(self, trade_id).await
    }

    async fn load_trade_receipt(
        &self,
        operation_id: &str,
        trade_id: &str,
    ) -> Result<Option<TradeReceipt>, Self::Error> {
        self.load_trade_transaction(operation_id, trade_id).await
    }

    async fn apply_trade(
        &mut self,
        request: TradeTransaction,
    ) -> Result<TradeTransactionOutcome, Self::Error> {
        self.apply_trade_transaction(request).await
    }
}

fn remote_error(error: Option<wire::RpcError>) -> RemoteError {
    let error = error.unwrap_or_else(|| wire::RpcError {
        code: wire::ErrorCode::Internal.into(),
        message: "server rejected the request without an error payload".to_string(),
        actual_revision: None,
    });
    RemoteError {
        code: wire::ErrorCode::try_from(error.code).unwrap_or(wire::ErrorCode::Internal),
        message: error.message,
        actual_revision: error.actual_revision.map(Revision),
    }
}

fn snapshot_write_outcome(
    result: wire::SaveSnapshotResponse,
) -> Result<SnapshotWriteOutcome, ClientError> {
    match wire::WriteDisposition::try_from(result.disposition).ok() {
        Some(wire::WriteDisposition::Applied) => Ok(SnapshotWriteOutcome::Applied {
            revision: Revision(result.revision),
        }),
        Some(wire::WriteDisposition::Duplicate) => Ok(SnapshotWriteOutcome::Duplicate {
            revision: Revision(result.revision),
        }),
        _ => Err(ClientError::UnexpectedResponse(
            "save returned an invalid disposition",
        )),
    }
}

fn validate_snapshot_write_batch(requests: &[SnapshotWrite]) -> Result<(), ClientError> {
    if requests.is_empty() || requests.len() > MAX_BATCH_SNAPSHOT_WRITES {
        return Err(ClientError::InvalidConfig(
            "batch snapshot write size is outside the protocol limit",
        ));
    }
    if requests
        .iter()
        .map(|request| &request.record)
        .collect::<HashSet<_>>()
        .len()
        != requests.len()
    {
        return Err(ClientError::InvalidConfig(
            "batch snapshot writes contain duplicate records",
        ));
    }
    if requests
        .iter()
        .map(|request| request.request_id.as_str())
        .collect::<HashSet<_>>()
        .len()
        != requests.len()
    {
        return Err(ClientError::InvalidConfig(
            "batch snapshot writes contain duplicate request ids",
        ));
    }
    Ok(())
}

fn is_reconnectable(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::RequestTimeout
            | ClientError::ConnectionUnusable
            | ClientError::ConnectionClosed
            | ClientError::Protocol(ProtocolError::Io(_))
    )
}

fn is_endpoint_unavailable(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::ConnectTimeout
            | ClientError::ConnectionClosed
            | ClientError::Protocol(ProtocolError::Io(_))
    )
}

fn request_operation(body: &wire::request_envelope::Body) -> &'static str {
    match body {
        wire::request_envelope::Body::LoadSnapshot(_) => "load_snapshot",
        wire::request_envelope::Body::LoadMultiSnapshot(_) => "load_multi_snapshot",
        wire::request_envelope::Body::SaveSnapshot(_) => "save_snapshot",
        wire::request_envelope::Body::SaveMultiSnapshot(_) => "save_multi_snapshot",
        wire::request_envelope::Body::EnqueueSnapshot(_) => "enqueue_snapshot",
        wire::request_envelope::Body::EnqueueMultiSnapshot(_) => "enqueue_multi_snapshot",
        wire::request_envelope::Body::ApplyTransaction(_) => "apply_transaction",
        wire::request_envelope::Body::LoadTransaction(_) => "load_transaction",
        wire::request_envelope::Body::ApplyMultiTransaction(_) => "apply_multi_transaction",
        wire::request_envelope::Body::LoadMultiTransaction(_) => "load_multi_transaction",
        wire::request_envelope::Body::ApplyTradeTransaction(_) => "apply_trade_transaction",
        wire::request_envelope::Body::LoadTrade(_) => "load_trade",
        wire::request_envelope::Body::LoadTradeTransaction(_) => "load_trade_transaction",
        wire::request_envelope::Body::CommitRecords(_) => "commit_records",
    }
}

fn connection_outcome(result: &Result<DbProxyClient, ClientError>) -> ClientConnectionOutcome {
    match result {
        Ok(_) => ClientConnectionOutcome::Connected,
        Err(ClientError::ConnectTimeout) => ClientConnectionOutcome::Timeout,
        Err(error) if is_endpoint_unavailable(error) => ClientConnectionOutcome::Unavailable,
        Err(_) => ClientConnectionOutcome::Rejected,
    }
}

fn request_outcome(result: &Result<wire::ResponseEnvelope, ClientError>) -> ClientRequestOutcome {
    match result {
        Ok(_) => ClientRequestOutcome::Success,
        Err(ClientError::RequestTimeout) => ClientRequestOutcome::Timeout,
        Err(ClientError::ConnectionUnusable | ClientError::ConnectionClosed) => {
            ClientRequestOutcome::Unavailable
        }
        Err(ClientError::Protocol(ProtocolError::Io(_))) => ClientRequestOutcome::Unavailable,
        Err(ClientError::Remote(_)) => ClientRequestOutcome::RemoteError,
        Err(_) => ClientRequestOutcome::ProtocolError,
    }
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
mod reconnect_tests;

#[cfg(test)]
mod timing_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_config_debug_redacts_the_auth_token() {
        let token = "secret-client-token";
        let debug = format!("{:?}", ClientConfig::new("127.0.0.1:7800", token, "test"));
        assert!(!debug.contains(token));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn only_transport_failures_are_failover_candidates() {
        assert!(is_endpoint_unavailable(&ClientError::ConnectTimeout));
        assert!(is_endpoint_unavailable(&ClientError::ConnectionClosed));
        assert!(!is_endpoint_unavailable(&ClientError::Remote(
            RemoteError {
                code: wire::ErrorCode::Unauthorized,
                message: "bad token".to_string(),
                actual_revision: None,
            }
        )));
        assert!(!is_endpoint_unavailable(&ClientError::InvalidConfig(
            "bad endpoint"
        )));
    }

    #[test]
    fn endpoint_candidates_keep_primary_order_and_remove_duplicates() {
        let config =
            ClientConfig::new("127.0.0.1:7800", "secret-client-token", "test").with_endpoints(
                vec!["127.0.0.1:7800".to_string(), "127.0.0.1:7801".to_string()],
            );
        assert_eq!(
            config.endpoint_candidates().unwrap(),
            vec!["127.0.0.1:7800", "127.0.0.1:7801"]
        );
    }

    #[test]
    fn trade_receipt_must_match_the_submitted_transaction() {
        let record = RecordKey::new("wallet", "buyer").unwrap();
        let request = TradeTransaction {
            operation_id: "trade-op".to_string(),
            transition: tiangz_dbproxy_core::TradeTransition {
                trade_id: "trade-1".to_string(),
                expected_version: Revision::ZERO,
                expected_state: None,
                next_state: tiangz_dbproxy_core::TradeState::Escrowed,
                payload: Vec::new(),
                updated_at_unix_ms: 1,
            },
            writes: vec![tiangz_dbproxy_core::TransactionalRecordWrite {
                record: record.clone(),
                schema: "wallet.snapshot".to_string(),
                schema_version: 1,
                expected_revision: Revision::ZERO,
                payload: Vec::new(),
                updated_at_unix_ms: 1,
            }],
            ledger_postings: Vec::new(),
            outbox_events: Vec::new(),
            result: b"committed".to_vec(),
        };
        let receipt = TradeReceipt {
            operation_id: request.operation_id.clone(),
            trade_id: request.transition.trade_id.clone(),
            new_trade_version: Revision(1),
            state: tiangz_dbproxy_core::TradeState::Escrowed,
            records: vec![TransactionRecordReceipt {
                record,
                new_revision: Revision(1),
            }],
            ledger_posting_ids: Vec::new(),
            outbox_event_ids: Vec::new(),
            result: b"committed".to_vec(),
        };
        assert!(trade_receipt_matches_request(&receipt, &request));

        let mut tampered = receipt;
        tampered.records[0].new_revision = Revision(2);
        assert!(!trade_receipt_matches_request(&tampered, &request));
    }

    #[tokio::test]
    async fn split_pool_rejects_zero_sized_connection_classes_before_connecting() {
        let config = ClientConfig::new(
            "127.0.0.1:1",
            "secret-client-token",
            "split-pool-validation",
        );
        assert!(matches!(
            DbProxyClientPool::connect_split(config.clone(), 0, 1).await,
            Err(ClientError::InvalidConfig("read pool size is zero"))
        ));
        assert!(matches!(
            DbProxyClientPool::connect_split(config, 1, 0).await,
            Err(ClientError::InvalidConfig("write pool size is zero"))
        ));
    }
}
