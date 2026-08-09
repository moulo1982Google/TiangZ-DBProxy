use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use tiangz_dbproxy_client::{ClientConfig, ClientError, DbProxyClient};
use tiangz_dbproxy_core::{
    InMemorySnapshotStore, InMemoryTransactionalStore, RecordKey, Revision, SnapshotEnvelope,
    SnapshotStore, SnapshotWrite, SnapshotWriteOutcome, TransactionReceipt, TransactionStore,
    TransactionalWrite, TransactionalWriteOutcome,
};
use tiangz_dbproxy_protocol::{
    DEFAULT_MAX_FRAME_BYTES, PROTOCOL_FINGERPRINT, read_message, wire, write_message,
};
use tiangz_dbproxy_server::{BackendError, DbProxyBackend, DbProxyServer, ServerConfig};
use tokio::{net::TcpStream, sync::Mutex, sync::watch, task::JoinHandle};

#[derive(Default)]
struct MemoryBackend {
    snapshots: Mutex<InMemorySnapshotStore>,
    transactions: Mutex<InMemoryTransactionalStore>,
    queued: Mutex<Vec<SnapshotWrite>>,
}

#[async_trait]
impl DbProxyBackend for MemoryBackend {
    async fn load(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, BackendError> {
        Ok(self.snapshots.lock().await.load(record)?)
    }

    async fn save(&self, request: SnapshotWrite) -> Result<SnapshotWriteOutcome, BackendError> {
        Ok(self.snapshots.lock().await.save(request)?)
    }

    async fn enqueue_snapshot(&self, request: SnapshotWrite) -> Result<(), BackendError> {
        if request.expected_revision.is_some() {
            return Err(
                tiangz_dbproxy_core::StoreError::QueuedSnapshotRequiresUnconditionalWrite {
                    record: request.record,
                }
                .into(),
            );
        }
        self.queued.lock().await.push(request);
        Ok(())
    }

    async fn apply_transaction(
        &self,
        request: TransactionalWrite,
    ) -> Result<TransactionalWriteOutcome, BackendError> {
        Ok(self.transactions.lock().await.apply(request)?)
    }

    async fn load_transaction(
        &self,
        operation_id: &str,
        record: &RecordKey,
    ) -> Result<Option<TransactionReceipt>, BackendError> {
        Ok(self
            .transactions
            .lock()
            .await
            .load_receipt(operation_id, record)?)
    }
}

struct TestServer {
    endpoint: String,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl TestServer {
    async fn start(token: &str) -> Self {
        let backend: Arc<dyn DbProxyBackend> = Arc::new(MemoryBackend::default());
        let config = ServerConfig::new("127.0.0.1:0".parse().unwrap(), token);
        let server = DbProxyServer::bind(config, backend).await.unwrap();
        let endpoint = server.local_addr().unwrap().to_string();
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(async move {
            server.serve(receiver).await.unwrap();
        });
        Self {
            endpoint,
            shutdown,
            task,
        }
    }

    async fn stop(self) {
        self.shutdown.send(true).unwrap();
        self.task.await.unwrap();
    }
}

fn snapshot(request_id: &str, expected_revision: Option<Revision>) -> SnapshotWrite {
    SnapshotWrite {
        request_id: request_id.to_string(),
        record: RecordKey::new("player", "1001").unwrap(),
        schema: "player.snapshot".to_string(),
        schema_version: 1,
        payload: b"hp=100".to_vec(),
        expected_revision,
        updated_at_unix_ms: 100,
    }
}

fn transaction(operation_id: &str) -> TransactionalWrite {
    TransactionalWrite {
        operation_id: operation_id.to_string(),
        record: RecordKey::new("wallet", "1001").unwrap(),
        schema: "wallet.snapshot".to_string(),
        schema_version: 1,
        expected_revision: Revision::ZERO,
        payload: b"coins=100".to_vec(),
        result: b"granted=100".to_vec(),
        updated_at_unix_ms: 100,
    }
}

#[tokio::test]
async fn client_server_round_trip_preserves_persistence_semantics() {
    const TOKEN: &str = "network-test-token-1234";
    let server = TestServer::start(TOKEN).await;
    let mut config = ClientConfig::new(&server.endpoint, TOKEN, "network-test");
    config.request_timeout = Duration::from_secs(1);
    let client = DbProxyClient::connect(config).await.unwrap();

    let write = snapshot("request-1", Some(Revision::ZERO));
    assert_eq!(
        client.save(write.clone()).await.unwrap(),
        SnapshotWriteOutcome::Applied {
            revision: Revision(1)
        }
    );
    assert_eq!(
        client.save(write).await.unwrap(),
        SnapshotWriteOutcome::Duplicate {
            revision: Revision(1)
        }
    );
    let loaded = client
        .load(&RecordKey::new("player", "1001").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.revision, Revision(1));
    assert_eq!(loaded.payload, b"hp=100");

    let error = client
        .save(snapshot("request-2", Some(Revision::ZERO)))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ClientError::Remote(ref remote)
            if remote.code == wire::ErrorCode::RevisionConflict
                && remote.actual_revision == Some(Revision(1))
    ));

    let transaction = transaction("operation-1");
    assert_eq!(
        client.apply_transaction(transaction.clone()).await.unwrap(),
        TransactionalWriteOutcome::Applied {
            new_revision: Revision(1),
            result: b"granted=100".to_vec()
        }
    );
    assert_eq!(
        client.apply_transaction(transaction).await.unwrap(),
        TransactionalWriteOutcome::Duplicate {
            new_revision: Revision(1),
            result: b"granted=100".to_vec()
        }
    );
    assert_eq!(
        client
            .load_transaction("operation-1", &RecordKey::new("wallet", "1001").unwrap(),)
            .await
            .unwrap(),
        Some(TransactionReceipt {
            operation_id: "operation-1".to_string(),
            record: RecordKey::new("wallet", "1001").unwrap(),
            new_revision: Revision(1),
            result: b"granted=100".to_vec(),
        })
    );

    client
        .enqueue_snapshot(snapshot("queued-1", None))
        .await
        .unwrap();
    server.stop().await;
}

#[tokio::test]
async fn authentication_failure_is_explicit() {
    const TOKEN: &str = "network-test-token-1234";
    let server = TestServer::start(TOKEN).await;
    let error = match DbProxyClient::connect(ClientConfig::new(
        &server.endpoint,
        "wrong-token-12345678",
        "bad-client",
    ))
    .await
    {
        Ok(_) => panic!("wrong token unexpectedly authenticated"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        ClientError::Remote(ref remote) if remote.code == wire::ErrorCode::Unauthorized
    ));
    server.stop().await;
}

#[tokio::test]
async fn protocol_fingerprint_mismatch_is_rejected_before_rpc() {
    const TOKEN: &str = "network-test-token-1234";
    let server = TestServer::start(TOKEN).await;
    let mut stream = TcpStream::connect(&server.endpoint).await.unwrap();
    write_message(
        &mut stream,
        &wire::ClientFrame {
            body: Some(wire::client_frame::Body::Hello(wire::ClientHello {
                protocol_version: 1,
                protocol_fingerprint: format!("{PROTOCOL_FINGERPRINT}-changed"),
                auth_token: TOKEN.to_string(),
                client_name: "old-client".to_string(),
            })),
        },
        DEFAULT_MAX_FRAME_BYTES,
    )
    .await
    .unwrap();
    let response = read_message::<_, wire::ServerFrame>(&mut stream, DEFAULT_MAX_FRAME_BYTES)
        .await
        .unwrap()
        .unwrap();
    let Some(wire::server_frame::Body::Hello(hello)) = response.body else {
        panic!("expected handshake response");
    };
    assert!(!hello.accepted);
    assert_eq!(
        wire::ErrorCode::try_from(hello.error.unwrap().code).unwrap(),
        wire::ErrorCode::ProtocolMismatch
    );
    server.stop().await;
}
