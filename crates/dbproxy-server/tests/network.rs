use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use tiangz_dbproxy_client::{
    ClientConfig, ClientConnectionOutcome, ClientError, ClientObserver, ClientRequestOutcome,
    DbProxyClient, DbProxyClientPool,
};
use tiangz_dbproxy_core::{
    AsyncMultiRecordTransactionStore, InMemoryMultiRecordTransactionStore, InMemorySnapshotStore,
    InMemoryTransactionalStore, LedgerPosting, MultiRecordTransactionReceipt,
    MultiRecordTransactionalWrite, MultiRecordTransactionalWriteOutcome, OutboxEvent, RecordKey,
    Revision, SnapshotEnvelope, SnapshotStore, SnapshotWrite, SnapshotWriteOutcome, TradeState,
    TradeTransaction, TradeTransactionOutcome, TradeTransition, TransactionReceipt,
    TransactionStore, TransactionalRecordWrite, TransactionalWrite, TransactionalWriteOutcome,
};
use tiangz_dbproxy_protocol::{
    DEFAULT_MAX_FRAME_BYTES, LEGACY_PROTOCOL_FINGERPRINT_V2, PROTOCOL_FINGERPRINT,
    PROTOCOL_VERSION, read_message, wire, write_message,
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

#[derive(Default)]
struct RecordingObserver {
    connected: AtomicU64,
    unavailable: AtomicU64,
    failovers: AtomicU64,
    request_successes: AtomicU64,
    request_failures: AtomicU64,
}

impl ClientObserver for RecordingObserver {
    fn connection_attempt(
        &self,
        _endpoint_index: usize,
        _elapsed: Duration,
        outcome: ClientConnectionOutcome,
    ) {
        match outcome {
            ClientConnectionOutcome::Connected => &self.connected,
            ClientConnectionOutcome::Timeout | ClientConnectionOutcome::Unavailable => {
                &self.unavailable
            }
            ClientConnectionOutcome::Rejected => return,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    fn endpoint_failover(&self, _from_endpoint_index: usize, _to_endpoint_index: usize) {
        self.failovers.fetch_add(1, Ordering::Relaxed);
    }

    fn request_attempt(
        &self,
        _endpoint_index: usize,
        _operation: &'static str,
        _elapsed: Duration,
        outcome: ClientRequestOutcome,
    ) {
        match outcome {
            ClientRequestOutcome::Success => &self.request_successes,
            _ => &self.request_failures,
        }
        .fetch_add(1, Ordering::Relaxed);
    }
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

fn domain_snapshot(
    request_id: &str,
    domain: &str,
    expected_revision: Option<Revision>,
) -> SnapshotWrite {
    SnapshotWrite {
        request_id: request_id.to_string(),
        record: RecordKey::new(domain, "1001").unwrap(),
        schema: format!("{domain}.snapshot"),
        schema_version: 1,
        payload: format!("{domain}=value").into_bytes(),
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
    let batch = client
        .load_multi(&[
            RecordKey::new("player", "1001").unwrap(),
            RecordKey::new("player", "missing").unwrap(),
        ])
        .await
        .unwrap();
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[0].as_ref().unwrap().revision, Revision(1));
    assert!(batch[1].is_none());

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

    let first_batch = vec![
        domain_snapshot("batch-wallet-1", "wallet", Some(Revision::ZERO)),
        domain_snapshot("batch-items-1", "items", Some(Revision::ZERO)),
    ];
    let outcomes = client.save_multi(&first_batch).await.unwrap();
    assert_eq!(
        outcomes,
        vec![
            Ok(SnapshotWriteOutcome::Applied {
                revision: Revision(1)
            }),
            Ok(SnapshotWriteOutcome::Applied {
                revision: Revision(1)
            }),
        ]
    );
    let partial_batch = vec![
        domain_snapshot("batch-wallet-2", "wallet", Some(Revision::ZERO)),
        domain_snapshot("batch-items-2", "items", Some(Revision(1))),
    ];
    let outcomes = client.save_multi(&partial_batch).await.unwrap();
    assert!(matches!(
        &outcomes[0],
        Err(remote)
            if remote.code == wire::ErrorCode::RevisionConflict
                && remote.actual_revision == Some(Revision(1))
    ));
    assert_eq!(
        outcomes[1],
        Ok(SnapshotWriteOutcome::Applied {
            revision: Revision(2)
        })
    );

    client
        .enqueue_multi_snapshot(&[
            domain_snapshot("queued-wallet", "queued-wallet", None),
            domain_snapshot("queued-items", "queued-items", None),
        ])
        .await
        .unwrap()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

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
async fn legacy_line_ending_fingerprint_is_accepted_and_echoed() {
    const TOKEN: &str = "network-test-token-1234";
    let server = TestServer::start(TOKEN).await;
    for fingerprint in [
        LEGACY_PROTOCOL_FINGERPRINT_V2,
        tiangz_dbproxy_protocol::PRE_COMMIT_PROTOCOL_FINGERPRINT_V2,
        tiangz_dbproxy_protocol::PRE_RELAY_PROTOCOL_FINGERPRINT_V2,
    ] {
        let mut stream = TcpStream::connect(&server.endpoint).await.unwrap();
        write_message(
            &mut stream,
            &wire::ClientFrame {
                body: Some(wire::client_frame::Body::Hello(wire::ClientHello {
                    protocol_version: PROTOCOL_VERSION,
                    protocol_fingerprint: fingerprint.to_string(),
                    auth_token: TOKEN.to_string(),
                    client_name: "legacy-line-ending-client".to_string(),
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
        assert!(hello.accepted);
        assert_eq!(hello.protocol_fingerprint, fingerprint);
        // Exercise an unchanged RPC after the handshake, as pinned SDKs do.
        write_message(
            &mut stream,
            &wire::ClientFrame {
                body: Some(wire::client_frame::Body::Request(wire::RequestEnvelope {
                    rpc_id: 1,
                    body: Some(wire::request_envelope::Body::LoadSnapshot(
                        wire::LoadSnapshotRequest {
                            record: Some(wire::RecordKey {
                                namespace: "document".into(),
                                key: "legacy".into(),
                            }),
                        },
                    )),
                })),
            },
            DEFAULT_MAX_FRAME_BYTES,
        )
        .await
        .unwrap();
        let frame = read_message::<_, wire::ServerFrame>(&mut stream, DEFAULT_MAX_FRAME_BYTES)
            .await
            .unwrap()
            .unwrap();
        let Some(wire::server_frame::Body::Response(response)) = frame.body else {
            panic!("expected RPC response")
        };
        assert!(response.error.is_none());
        assert!(matches!(
            response.body,
            Some(wire::response_envelope::Body::LoadSnapshot(_))
        ));
    }
    server.stop().await;
}

#[tokio::test]
async fn generic_commit_is_atomic_idempotent_and_preserves_newer_snapshots() {
    use tiangz_dbproxy_core::{AppendRecord, CommitEffects};
    const TOKEN: &str = "generic-commit-test-token";
    let server = TestServer::start_with_backend(
        TOKEN,
        Arc::new(tiangz_dbproxy_server::MemoryBackend::new(4).unwrap()),
    )
    .await;
    let client =
        DbProxyClient::connect(ClientConfig::new(&server.endpoint, TOKEN, "generic-commit"))
            .await
            .unwrap();
    let writes = ["left", "right"]
        .into_iter()
        .map(|key| TransactionalRecordWrite {
            record: RecordKey::new("document", key).unwrap(),
            schema: "document.v1".into(),
            schema_version: 1,
            expected_revision: Revision::ZERO,
            payload: b"new".to_vec(),
            updated_at_unix_ms: 1,
        })
        .collect::<Vec<_>>();
    let request = MultiRecordTransactionalWrite {
        operation_id: "commit-1".into(),
        writes,
        result: b"receipt".to_vec(),
    };
    let effects = CommitEffects {
        appends: vec![AppendRecord {
            record: RecordKey::new("audit", "fact-1").unwrap(),
            schema: "opaque".into(),
            schema_version: 1,
            payload: b"fact".to_vec(),
            occurred_at_unix_ms: 1,
        }],
        outbox_events: vec![OutboxEvent {
            event_id: "event-1".into(),
            topic: "document.changed".into(),
            partition_key: "left".into(),
            payload: b"event".to_vec(),
            occurred_at_unix_ms: 1,
        }],
    };
    let mut stale = request.clone();
    stale.writes[1].expected_revision = Revision(9);
    assert!(client.commit_records(stale, effects.clone()).await.is_err());
    assert!(
        client
            .load(&request.writes[0].record)
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        client
            .commit_records(request.clone(), effects.clone())
            .await
            .unwrap(),
        MultiRecordTransactionalWriteOutcome::Applied { .. }
    ));
    let mut tampered = effects.clone();
    tampered.appends[0].payload.push(9);
    assert!(
        client
            .commit_records(request.clone(), tampered)
            .await
            .is_err()
    );
    assert!(
        client
            .apply_multi_transaction(request.clone())
            .await
            .is_err()
    );
    let mut colliding = request.clone();
    colliding.operation_id = "commit-2".into();
    for write in &mut colliding.writes {
        write.expected_revision = Revision(1);
    }
    assert!(
        client
            .commit_records(colliding.clone(), effects.clone())
            .await
            .is_err()
    );
    let event_only = CommitEffects {
        appends: vec![],
        outbox_events: effects.outbox_events.clone(),
    };
    assert!(client.commit_records(colliding, event_only).await.is_err());
    assert_eq!(
        client
            .load(&request.writes[0].record)
            .await
            .unwrap()
            .unwrap()
            .revision,
        Revision(1)
    );
    client
        .save(SnapshotWrite {
            request_id: "later".into(),
            record: request.writes[0].record.clone(),
            schema: "document.v1".into(),
            schema_version: 1,
            expected_revision: Some(Revision(1)),
            payload: b"later".to_vec(),
            updated_at_unix_ms: 2,
        })
        .await
        .unwrap();
    assert!(matches!(
        client
            .commit_records(request.clone(), effects)
            .await
            .unwrap(),
        MultiRecordTransactionalWriteOutcome::Duplicate { .. }
    ));
    assert_eq!(
        client
            .load(&request.writes[0].record)
            .await
            .unwrap()
            .unwrap()
            .payload,
        b"later"
    );
    assert_eq!(
        client
            .load_multi_transaction(
                &request.operation_id,
                &request
                    .writes
                    .iter()
                    .map(|w| w.record.clone())
                    .collect::<Vec<_>>()
            )
            .await
            .unwrap()
            .unwrap()
            .result,
        b"receipt"
    );
    server.stop().await;
}

#[tokio::test]
async fn generic_envelope_crosses_the_real_wire_without_changing_snapshot_semantics() {
    use tiangz_dbproxy_core::{CommitEffects, EventEnvelope};
    let server = TestServer::start_with_backend(
        "relay-test-token",
        Arc::new(tiangz_dbproxy_server::MemoryBackend::new(2).unwrap()),
    )
    .await;
    let client = DbProxyClient::connect(ClientConfig::new(
        &server.endpoint,
        "relay-test-token",
        "relay-test",
    ))
    .await
    .unwrap();
    let event = EventEnvelope {
        event_id: "document-event".into(),
        producer: "game".into(),
        event_type: "DocumentChanged".into(),
        aggregate_type: "document".into(),
        aggregate_id: "document-1".into(),
        partition_key: "document-1".into(),
        schema_version: 1,
        content_type: "application/octet-stream".into(),
        payload: vec![0, 255],
        occurred_at_unix_ms: 1,
        route_version: 1,
    }
    .into_outbox()
    .unwrap();
    let request = MultiRecordTransactionalWrite {
        operation_id: "relay-operation".into(),
        writes: vec![TransactionalRecordWrite {
            record: RecordKey::new("document", "1").unwrap(),
            schema: "document".into(),
            schema_version: 1,
            expected_revision: Revision::ZERO,
            payload: vec![9],
            updated_at_unix_ms: 1,
        }],
        result: vec![8],
    };
    let effects = CommitEffects {
        appends: vec![],
        outbox_events: vec![event],
    };
    assert!(matches!(
        client
            .commit_records(request.clone(), effects.clone())
            .await
            .unwrap(),
        MultiRecordTransactionalWriteOutcome::Applied { .. }
    ));
    assert!(matches!(
        client
            .commit_records(request.clone(), effects.clone())
            .await
            .unwrap(),
        MultiRecordTransactionalWriteOutcome::Duplicate { .. }
    ));
    let mut bad = effects;
    bad.outbox_events[0].payload = vec![0];
    assert!(client.commit_records(request.clone(), bad).await.is_err());
    assert_eq!(
        client
            .load(&request.writes[0].record)
            .await
            .unwrap()
            .unwrap()
            .revision,
        Revision(1)
    );
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn client_fails_over_to_the_second_endpoint_and_replays_the_same_write() {
    const TOKEN: &str = "network-failover-test-token";
    let backend: Arc<dyn DbProxyBackend> = Arc::new(MemoryBackend::default());
    let primary = TestServer::start_with_backend(TOKEN, Arc::clone(&backend)).await;
    let secondary = TestServer::start_with_backend(TOKEN, Arc::clone(&backend)).await;
    let observer = Arc::new(RecordingObserver::default());
    let config = ClientConfig::new(&primary.endpoint, TOKEN, "failover-test")
        .with_endpoints(vec![secondary.endpoint.clone()])
        .with_observer(observer.clone());
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
    assert_eq!(observer.connected.load(Ordering::Relaxed), 2);
    assert_eq!(observer.failovers.load(Ordering::Relaxed), 1);
    assert!(observer.request_successes.load(Ordering::Relaxed) >= 3);
    assert!(observer.request_failures.load(Ordering::Relaxed) >= 1);
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

#[tokio::test]
async fn split_client_pool_routes_reads_and_writes_over_independent_connections() {
    const TOKEN: &str = "network-split-pool-token";
    let server = TestServer::start(TOKEN).await;
    let mut config = ClientConfig::new(&server.endpoint, TOKEN, "network-split-pool-test");
    config.request_timeout = Duration::from_secs(1);
    let pool = DbProxyClientPool::connect_split(config, 2, 3)
        .await
        .unwrap();

    assert!(pool.is_split());
    assert_eq!(pool.read_len(), 2);
    assert_eq!(pool.write_len(), 3);
    assert_eq!(pool.len(), 5);
    assert!(!pool.is_empty());

    let record = RecordKey::new("split-pool", "1001").unwrap();
    let mut write = snapshot("split-pool-request", Some(Revision::ZERO));
    write.record = record.clone();
    assert_eq!(
        pool.save(write).await.unwrap(),
        SnapshotWriteOutcome::Applied {
            revision: Revision(1)
        }
    );
    assert_eq!(
        pool.load(&record).await.unwrap().unwrap().revision,
        Revision(1)
    );

    server.stop().await;
}

#[tokio::test]
async fn trade_transaction_round_trip_preserves_state_ledger_and_receipt() {
    const TOKEN: &str = "network-trade-transaction-token";
    let backend: Arc<dyn DbProxyBackend> =
        Arc::new(tiangz_dbproxy_server::MemoryBackend::new(4).unwrap());
    let server = TestServer::start_with_backend(TOKEN, backend).await;
    let client = DbProxyClient::connect(ClientConfig::new(
        &server.endpoint,
        TOKEN,
        "trade-transaction-test",
    ))
    .await
    .unwrap();
    let trade_id = "network-trade-1".to_string();
    let request = TradeTransaction {
        operation_id: "network-trade-op-1".to_string(),
        transition: TradeTransition {
            trade_id: trade_id.clone(),
            expected_version: Revision::ZERO,
            expected_state: None,
            next_state: TradeState::Escrowed,
            payload: b"escrow".to_vec(),
            updated_at_unix_ms: 100,
        },
        writes: vec![
            TransactionalRecordWrite {
                record: RecordKey::new("trade-wallet", "buyer").unwrap(),
                schema: "wallet.snapshot".to_string(),
                schema_version: 1,
                expected_revision: Revision::ZERO,
                payload: b"gold=0".to_vec(),
                updated_at_unix_ms: 100,
            },
            TransactionalRecordWrite {
                record: RecordKey::new("trade-inventory", "seller").unwrap(),
                schema: "inventory.snapshot".to_string(),
                schema_version: 1,
                expected_revision: Revision::ZERO,
                payload: b"item=escrow".to_vec(),
                updated_at_unix_ms: 100,
            },
        ],
        ledger_postings: vec![
            LedgerPosting {
                posting_id: "network-buyer-debit".to_string(),
                account_id: "buyer".to_string(),
                asset: "gold".to_string(),
                amount: -100,
                metadata: Vec::new(),
            },
            LedgerPosting {
                posting_id: "network-escrow-credit".to_string(),
                account_id: "trade-escrow".to_string(),
                asset: "gold".to_string(),
                amount: 100,
                metadata: Vec::new(),
            },
        ],
        outbox_events: vec![OutboxEvent {
            event_id: "network-trade-event".to_string(),
            topic: "trade.escrowed".to_string(),
            partition_key: trade_id.clone(),
            payload: b"event".to_vec(),
            occurred_at_unix_ms: 100,
        }],
        result: b"escrowed".to_vec(),
    };
    let applied = client
        .apply_trade_transaction(request.clone())
        .await
        .unwrap();
    assert!(matches!(applied, TradeTransactionOutcome::Applied(_)));
    assert!(matches!(
        client.apply_trade_transaction(request).await.unwrap(),
        TradeTransactionOutcome::Duplicate(_)
    ));
    let trade = client.load_trade(&trade_id).await.unwrap().unwrap();
    assert_eq!(trade.version, Revision(1));
    assert_eq!(trade.state, TradeState::Escrowed);
    let receipt = client
        .load_trade_transaction("network-trade-op-1", &trade_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.new_trade_version, Revision(1));
    assert_eq!(receipt.records.len(), 2);
    assert_eq!(receipt.ledger_posting_ids.len(), 2);
    assert_eq!(receipt.outbox_event_ids, ["network-trade-event"]);
    server.stop().await;
}
