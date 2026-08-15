use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use tiangz_dbproxy_client::{ClientConfig, ClientError, DbProxyClient};
use tiangz_dbproxy_core::{
    AsyncMultiRecordTransactionStore, InMemoryMultiRecordTransactionStore, InMemorySnapshotStore,
    InMemoryTransactionalStore, MultiRecordTransactionReceipt, MultiRecordTransactionalWrite,
    MultiRecordTransactionalWriteOutcome, RecordKey, Revision, SnapshotEnvelope, SnapshotStore,
    SnapshotWrite, SnapshotWriteOutcome, TransactionReceipt, TransactionStore, TransactionalWrite,
    TransactionalWriteOutcome,
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
    multi_transactions: Mutex<InMemoryMultiRecordTransactionStore>,
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

    async fn apply_multi_transaction(
        &self,
        request: MultiRecordTransactionalWrite,
    ) -> Result<MultiRecordTransactionalWriteOutcome, BackendError> {
        Ok(self
            .multi_transactions
            .lock()
            .await
            .apply_multi(request)
            .await?)
    }

    async fn load_multi_transaction(
        &self,
        operation_id: &str,
        records: &[RecordKey],
    ) -> Result<Option<MultiRecordTransactionReceipt>, BackendError> {
        Ok(self
            .multi_transactions
            .lock()
            .await
            .load_multi_receipt(operation_id, records)
            .await?)
    }
}

struct TestServer {
    endpoint: String,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

fn unused_endpoint() -> String {
    // 绑定后立即释放端口，只用于模拟首个 Endpoint 尚未启动。
    // Bind and release a port immediately to simulate an unavailable primary Endpoint.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

impl TestServer {
    async fn start(token: &str) -> Self {
        Self::start_with_backend(token, Arc::new(MemoryBackend::default())).await
    }

    async fn start_with_backend(token: &str, backend: Arc<dyn DbProxyBackend>) -> Self {
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

fn multi_transaction(operation_id: &str) -> MultiRecordTransactionalWrite {
    MultiRecordTransactionalWrite {
        operation_id: operation_id.to_string(),
        writes: vec![
            tiangz_dbproxy_core::TransactionalRecordWrite {
                record: RecordKey::new("wallet", "buyer").unwrap(),
                schema: "wallet.snapshot".to_string(),
                schema_version: 1,
                expected_revision: Revision::ZERO,
                payload: b"coins=0".to_vec(),
                updated_at_unix_ms: 100,
            },
            tiangz_dbproxy_core::TransactionalRecordWrite {
                record: RecordKey::new("wallet", "seller").unwrap(),
                schema: "wallet.snapshot".to_string(),
                schema_version: 1,
                expected_revision: Revision::ZERO,
                payload: b"coins=100".to_vec(),
                updated_at_unix_ms: 100,
            },
        ],
        result: b"trade-complete".to_vec(),
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

#[tokio::test]
async fn client_fails_over_to_the_second_endpoint_and_replays_the_same_write() {
    const TOKEN: &str = "network-failover-test-token";
    let backend: Arc<dyn DbProxyBackend> = Arc::new(MemoryBackend::default());
    let primary = TestServer::start_with_backend(TOKEN, Arc::clone(&backend)).await;
    let secondary = TestServer::start_with_backend(TOKEN, Arc::clone(&backend)).await;
    let config = ClientConfig::new(&primary.endpoint, TOKEN, "failover-test")
        .with_endpoints(vec![secondary.endpoint.clone()]);
    let client = DbProxyClient::connect(config).await.unwrap();

    assert_eq!(
        client
            .save(snapshot("failover-first", Some(Revision::ZERO)))
            .await
            .unwrap(),
        SnapshotWriteOutcome::Applied {
            revision: Revision(1)
        }
    );
    primary.stop().await;

    let second = SnapshotWrite {
        request_id: "failover-second".to_string(),
        record: RecordKey::new("player", "1001").unwrap(),
        schema: "player.snapshot".to_string(),
        schema_version: 1,
        payload: b"hp=90".to_vec(),
        expected_revision: Some(Revision(1)),
        updated_at_unix_ms: 200,
    };
    assert_eq!(
        client.save(second.clone()).await.unwrap(),
        SnapshotWriteOutcome::Applied {
            revision: Revision(2)
        }
    );
    assert_eq!(
        client.save(second).await.unwrap(),
        SnapshotWriteOutcome::Duplicate {
            revision: Revision(2)
        }
    );
    secondary.stop().await;
}

#[tokio::test]
async fn client_connects_to_backup_when_primary_endpoint_is_unavailable() {
    const TOKEN: &str = "network-initial-failover-token";
    let server = TestServer::start(TOKEN).await;
    let config = ClientConfig::new(unused_endpoint(), TOKEN, "initial-failover-test")
        .with_endpoints(vec![server.endpoint.clone()]);
    let client = DbProxyClient::connect(config).await.unwrap();

    assert_eq!(
        client
            .save(snapshot("initial-failover", Some(Revision::ZERO)))
            .await
            .unwrap(),
        SnapshotWriteOutcome::Applied {
            revision: Revision(1)
        }
    );
    server.stop().await;
}

#[tokio::test]
async fn multi_record_transaction_is_atomic_idempotent_and_recoverable() {
    const TOKEN: &str = "network-multi-transaction-token";
    let server = TestServer::start(TOKEN).await;
    let client = DbProxyClient::connect(ClientConfig::new(
        &server.endpoint,
        TOKEN,
        "multi-transaction-test",
    ))
    .await
    .unwrap();
    let request = multi_transaction("trade-network-1");
    assert!(matches!(
        client.apply_multi_transaction(request.clone()).await.unwrap(),
        MultiRecordTransactionalWriteOutcome::Applied { records, result }
            if records.len() == 2 && result == b"trade-complete"
    ));
    assert!(matches!(
        client.apply_multi_transaction(request).await.unwrap(),
        MultiRecordTransactionalWriteOutcome::Duplicate { records, result }
            if records.len() == 2 && result == b"trade-complete"
    ));
    let records = vec![
        RecordKey::new("wallet", "buyer").unwrap(),
        RecordKey::new("wallet", "seller").unwrap(),
    ];
    let receipt = client
        .load_multi_transaction("trade-network-1", &records)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.operation_id, "trade-network-1");
    assert_eq!(receipt.records.len(), 2);
    assert_eq!(receipt.result, b"trade-complete");
    server.stop().await;
}
