//! DBProxy 的异步 Rust 客户端。
//! Async Rust client for DBProxy.
//!
//! 一个客户端连接内的请求按顺序执行；需要更高并发时应创建连接池，不能在 TiangZ
//! 业务线程中等待同步数据库调用。Requests are serialized per connection. Higher concurrency
//! should use a pool, and TiangZ business threads must never perform blocking database I/O.

use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use thiserror::Error;
use tiangz_dbproxy_core::{
    AsyncSnapshotStore, AsyncTransactionalStore, RecordKey, Revision, SnapshotEnvelope,
    SnapshotWrite, SnapshotWriteOutcome, TransactionalWrite, TransactionalWriteOutcome,
};
use tiangz_dbproxy_protocol::{
    DEFAULT_MAX_FRAME_BYTES, MAX_AUTH_TOKEN_BYTES, MAX_CLIENT_NAME_BYTES, PROTOCOL_FINGERPRINT,
    PROTOCOL_VERSION, ProtocolError, read_message, wire, write_message,
};
use tokio::{net::TcpStream, sync::Mutex, time::timeout};

/// 客户端连接参数；令牌只用于内部服务认证，不能写入日志或提交到生产配置。
/// Client connection settings; the internal token must not be logged or committed as production data.
#[derive(Clone)]
pub struct ClientConfig {
    pub endpoint: String,
    pub auth_token: String,
    pub client_name: String,
    pub max_frame_bytes: usize,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
}

impl fmt::Debug for ClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientConfig")
            .field("endpoint", &self.endpoint)
            .field("auth_token", &"[REDACTED]")
            .field("client_name", &self.client_name)
            .field("max_frame_bytes", &self.max_frame_bytes)
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
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
            auth_token: auth_token.into(),
            client_name: client_name.into(),
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteError {
    pub code: wire::ErrorCode,
    pub message: String,
    pub actual_revision: Option<Revision>,
}

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
}

/// 多连接客户端池；同一个 RecordKey 稳定落到同一连接，不同记录可以并行请求。
/// Multi-connection pool; one RecordKey stays on one connection while different records run in parallel.
#[derive(Clone)]
pub struct DbProxyClientPool {
    clients: Arc<[DbProxyClient]>,
}

impl DbProxyClientPool {
    pub async fn connect(config: ClientConfig, size: usize) -> Result<Self, ClientError> {
        if size == 0 {
            return Err(ClientError::InvalidConfig("client pool size is zero"));
        }
        let mut clients = Vec::with_capacity(size);
        for _ in 0..size {
            clients.push(DbProxyClient::connect(config.clone()).await?);
        }
        Ok(Self {
            clients: clients.into(),
        })
    }

    pub fn len(&self) -> usize {
        self.clients.len()
    }

    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    fn client(&self, record: &RecordKey) -> &DbProxyClient {
        let mut hasher = StableHasher::default();
        record.hash(&mut hasher);
        &self.clients[(hasher.finish() as usize) % self.clients.len()]
    }

    pub async fn load(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, ClientError> {
        self.client(record).load(record).await
    }

    pub async fn save(&self, request: SnapshotWrite) -> Result<SnapshotWriteOutcome, ClientError> {
        self.client(&request.record).save(request).await
    }

    pub async fn enqueue_snapshot(&self, request: SnapshotWrite) -> Result<(), ClientError> {
        self.client(&request.record).enqueue_snapshot(request).await
    }

    pub async fn apply_transaction(
        &self,
        request: TransactionalWrite,
    ) -> Result<TransactionalWriteOutcome, ClientError> {
        self.client(&request.record)
            .apply_transaction(request)
            .await
    }
}

impl DbProxyClient {
    /// 连接并完成版本、指纹和令牌握手。
    /// Connect and complete protocol-version, fingerprint, and token negotiation.
    pub async fn connect(config: ClientConfig) -> Result<Self, ClientError> {
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

        let mut stream = timeout(config.connect_timeout, TcpStream::connect(&config.endpoint))
            .await
            .map_err(|_| ClientError::ConnectTimeout)?
            .map_err(ProtocolError::from)?;
        stream.set_nodelay(true).map_err(ProtocolError::from)?;

        let hello = wire::ClientFrame {
            body: Some(wire::client_frame::Body::Hello(wire::ClientHello {
                protocol_version: PROTOCOL_VERSION,
                protocol_fingerprint: PROTOCOL_FINGERPRINT.to_string(),
                auth_token: config.auth_token,
                client_name: config.client_name,
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
        {
            return Err(ClientError::UnexpectedResponse(
                "server accepted a different protocol",
            ));
        }

        Ok(Self {
            connection: Arc::new(Mutex::new(ClientConnection {
                stream,
                next_rpc_id: 1,
                max_frame_bytes: config.max_frame_bytes,
                request_timeout: config.request_timeout,
                usable: true,
            })),
        })
    }

    async fn call(
        &self,
        body: wire::request_envelope::Body,
    ) -> Result<wire::ResponseEnvelope, ClientError> {
        let mut connection = self.connection.lock().await;
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

    async fn apply(
        &mut self,
        request: TransactionalWrite,
    ) -> Result<TransactionalWriteOutcome, Self::Error> {
        self.apply_transaction(request).await
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

    async fn apply(
        &mut self,
        request: TransactionalWrite,
    ) -> Result<TransactionalWriteOutcome, Self::Error> {
        self.apply_transaction(request).await
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
    fn client_config_debug_redacts_the_auth_token() {
        let token = "secret-client-token";
        let debug = format!("{:?}", ClientConfig::new("127.0.0.1:7800", token, "test"));
        assert!(!debug.contains(token));
        assert!(debug.contains("[REDACTED]"));
    }
}
