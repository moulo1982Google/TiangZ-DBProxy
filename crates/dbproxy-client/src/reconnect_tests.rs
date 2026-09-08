use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::{net::TcpListener, task::JoinSet};

#[derive(Default)]
struct CandidateObserver(std::sync::Mutex<Vec<(usize, ClientConnectionOutcome)>>);

impl ClientObserver for CandidateObserver {
    fn connection_attempt(&self, index: usize, _: Duration, outcome: ClientConnectionOutcome) {
        self.0.lock().unwrap().push((index, outcome));
    }
    fn endpoint_failover(&self, _: usize, _: usize) {}
    fn request_attempt(&self, _: usize, _: &'static str, _: Duration, _: ClientRequestOutcome) {}
}

#[derive(Clone, Copy)]
enum CandidateBehavior {
    Healthy,
    Denied(wire::ErrorCode),
    WrongFingerprint,
    MissingCapability,
    BusinessDenied,
}

struct CandidateServer {
    endpoint: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for CandidateServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl CandidateServer {
    async fn new(behavior: CandidateBehavior) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let frame = read_message::<_, wire::ClientFrame>(&mut stream, DEFAULT_MAX_FRAME_BYTES)
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(
                frame.body,
                Some(wire::client_frame::Body::Hello(_))
            ));
            let rejection = match behavior {
                CandidateBehavior::Denied(code) => Some(code),
                _ => None,
            };
            write_message(
                &mut stream,
                &wire::ServerFrame {
                    body: Some(wire::server_frame::Body::Hello(wire::ServerHello {
                        protocol_version: PROTOCOL_VERSION,
                        protocol_fingerprint: if matches!(
                            behavior,
                            CandidateBehavior::WrongFingerprint
                        ) {
                            "wrong".into()
                        } else {
                            PROTOCOL_FINGERPRINT.into()
                        },
                        supports_outbox_relay: !matches!(
                            behavior,
                            CandidateBehavior::MissingCapability
                        ),
                        accepted: rejection.is_none(),
                        error: rejection.map(|code| wire::RpcError {
                            code: code as i32,
                            message: "candidate refused handshake".into(),
                            ..Default::default()
                        }),
                    })),
                },
                DEFAULT_MAX_FRAME_BYTES,
            )
            .await
            .unwrap();
            if !matches!(
                behavior,
                CandidateBehavior::Healthy | CandidateBehavior::BusinessDenied
            ) {
                return;
            }
            while let Ok(Some(frame)) =
                read_message::<_, wire::ClientFrame>(&mut stream, DEFAULT_MAX_FRAME_BYTES).await
            {
                let Some(wire::client_frame::Body::Request(request)) = frame.body else {
                    panic!("expected request")
                };
                let error =
                    matches!(behavior, CandidateBehavior::BusinessDenied).then(|| wire::RpcError {
                        code: wire::ErrorCode::Unauthorized as i32,
                        message: "business rejected".into(),
                        ..Default::default()
                    });
                if write_message(
                    &mut stream,
                    &wire::ServerFrame {
                        body: Some(wire::server_frame::Body::Response(wire::ResponseEnvelope {
                            rpc_id: request.rpc_id,
                            error,
                            body: Some(wire::response_envelope::Body::LoadSnapshot(
                                wire::LoadSnapshotResponse { snapshot: None },
                            )),
                        })),
                    },
                    DEFAULT_MAX_FRAME_BYTES,
                )
                .await
                .is_err()
                {
                    return;
                }
            }
        });
        Self { endpoint, task }
    }
}

fn rejected_behaviors() -> [CandidateBehavior; 4] {
    [
        CandidateBehavior::Denied(wire::ErrorCode::Unauthorized),
        CandidateBehavior::Denied(wire::ErrorCode::ProtocolMismatch),
        CandidateBehavior::WrongFingerprint,
        CandidateBehavior::MissingCapability,
    ]
}

// A bound, non-listening socket reserves the port without accepting connections.
fn unavailable() -> (tokio::net::TcpSocket, String) {
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let endpoint = socket.local_addr().unwrap().to_string();
    (socket, endpoint)
}

fn candidate_config(endpoints: Vec<String>, observer: Arc<CandidateObserver>) -> ClientConfig {
    let mut config = ClientConfig::new(&endpoints[0], "candidate-test-token", "candidate-test")
        .with_endpoints(endpoints)
        .with_observer(observer);
    config.connect_timeout = Duration::from_secs(1);
    config.request_timeout = Duration::from_secs(1);
    config
}

#[tokio::test]
async fn initial_connect_skips_handshake_rejections_and_preserves_observations() {
    for behavior in rejected_behaviors() {
        let (_reserved, a) = unavailable();
        let b = CandidateServer::new(behavior).await;
        let c = CandidateServer::new(CandidateBehavior::Healthy).await;
        let observer = Arc::new(CandidateObserver::default());
        let client = DbProxyClient::connect(candidate_config(
            vec![a, b.endpoint.clone(), c.endpoint.clone()],
            observer.clone(),
        ))
        .await
        .unwrap();
        assert!(
            client
                .load(&RecordKey::new("test", "initial").unwrap())
                .await
                .unwrap()
                .is_none()
        );
        let attempts = observer.0.lock().unwrap();
        assert_eq!(attempts.len(), 3);
        assert!(matches!(
            attempts[0].1,
            ClientConnectionOutcome::Unavailable | ClientConnectionOutcome::Timeout
        ));
        assert_eq!(attempts[1], (1, ClientConnectionOutcome::Rejected));
        assert_eq!(attempts[2], (2, ClientConnectionOutcome::Connected));
    }
}

#[tokio::test]
async fn reconnect_skips_handshake_rejections_after_original_endpoint_closes() {
    for behavior in rejected_behaviors() {
        let a = CandidateServer::new(CandidateBehavior::Healthy).await;
        let b = CandidateServer::new(behavior).await;
        let c = CandidateServer::new(CandidateBehavior::Healthy).await;
        let observer = Arc::new(CandidateObserver::default());
        let client = DbProxyClient::connect(candidate_config(
            vec![a.endpoint.clone(), b.endpoint.clone(), c.endpoint.clone()],
            observer.clone(),
        ))
        .await
        .unwrap();
        a.task.abort();
        // Await cancellation so the original connection is definitively closed.
        while !a.task.is_finished() {
            tokio::task::yield_now().await;
        }
        assert!(
            client
                .load(&RecordKey::new("test", "reconnect").unwrap())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            *observer.0.lock().unwrap(),
            vec![
                (0, ClientConnectionOutcome::Connected),
                (1, ClientConnectionOutcome::Rejected),
                (2, ClientConnectionOutcome::Connected)
            ]
        );
    }
}

#[tokio::test]
async fn candidate_loops_fail_immediately_on_local_configuration_errors() {
    let (_reserved, endpoint) = unavailable();
    for invalid in 0..3 {
        let observer = Arc::new(CandidateObserver::default());
        let mut config = candidate_config(
            vec![endpoint.clone(), "127.0.0.1:1".into()],
            observer.clone(),
        );
        match invalid {
            0 => config.auth_token.clear(),
            1 => config.client_name.clear(),
            _ => config.max_frame_bytes = 0,
        }
        assert!(matches!(
            DbProxyClient::connect(config).await,
            Err(ClientError::InvalidConfig(_))
        ));
        assert_eq!(observer.0.lock().unwrap().len(), 1);

        let server = CandidateServer::new(CandidateBehavior::Healthy).await;
        let mut client = DbProxyClient::connect(candidate_config(
            vec![server.endpoint.clone(), endpoint.clone()],
            observer.clone(),
        ))
        .await
        .unwrap();
        observer.0.lock().unwrap().clear();
        match invalid {
            0 => client.config.auth_token.clear(),
            1 => client.config.client_name.clear(),
            _ => client.config.max_frame_bytes = 0,
        }
        client.connection.lock().await.usable = false;
        assert!(matches!(
            client.reconnect_next().await,
            Err(ClientError::InvalidConfig(_))
        ));
        assert_eq!(observer.0.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn exhausted_candidates_preserve_the_handshake_rejection_in_both_loops() {
    for reconnect in [false, true] {
        let (_reserved_a, a) = unavailable();
        let b =
            CandidateServer::new(CandidateBehavior::Denied(wire::ErrorCode::Unauthorized)).await;
        let (_reserved_c, c) = unavailable();
        let observer = Arc::new(CandidateObserver::default());
        let config = candidate_config(vec![a, b.endpoint.clone(), c], observer.clone());
        let error = if reconnect {
            let server = CandidateServer::new(CandidateBehavior::Healthy).await;
            let mut client = DbProxyClient::connect(ClientConfig::new(
                &server.endpoint,
                "candidate-test-token",
                "test",
            ))
            .await
            .unwrap();
            client.config = config;
            {
                let mut connection = client.connection.lock().await;
                connection.endpoint_index = 2;
                connection.usable = false;
            }
            client.reconnect_next().await.err().unwrap()
        } else {
            DbProxyClient::connect(config).await.err().unwrap()
        };
        assert!(
            matches!(error, ClientError::Remote(RemoteError { code: wire::ErrorCode::Unauthorized, ref message, .. }) if message == "candidate refused handshake")
        );
        assert_eq!(observer.0.lock().unwrap().len(), 3);
    }
}

#[tokio::test]
async fn business_remote_errors_do_not_switch_endpoints() {
    let a = CandidateServer::new(CandidateBehavior::BusinessDenied).await;
    let b = CandidateServer::new(CandidateBehavior::Healthy).await;
    let observer = Arc::new(CandidateObserver::default());
    let client = DbProxyClient::connect(candidate_config(
        vec![a.endpoint.clone(), b.endpoint.clone()],
        observer.clone(),
    ))
    .await
    .unwrap();
    for _ in 0..2 {
        assert!(matches!(
            client
                .load(&RecordKey::new("test", "business").unwrap())
                .await,
            Err(ClientError::Remote(RemoteError {
                code: wire::ErrorCode::Unauthorized,
                ..
            }))
        ));
    }
    assert_eq!(
        *observer.0.lock().unwrap(),
        vec![(0, ClientConnectionOutcome::Connected)]
    );
}

#[tokio::test]
async fn concurrent_recovery_reuses_the_repaired_connection_and_allows_later_failures() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let connections = Arc::new(AtomicUsize::new(0));
    let accepted = connections.clone();
    let server = tokio::spawn(async move {
        let mut peers = JoinSet::new();
        loop {
            tokio::select! {
                connection = listener.accept() => {
                    let (mut stream, _) = connection.unwrap();
                    accepted.fetch_add(1, Ordering::SeqCst);
                    peers.spawn(async move {
                        let hello = read_message::<_, wire::ClientFrame>(&mut stream, DEFAULT_MAX_FRAME_BYTES)
                            .await.unwrap().unwrap();
                        assert!(matches!(hello.body, Some(wire::client_frame::Body::Hello(_))));
                        write_message(&mut stream, &wire::ServerFrame {
                            body: Some(wire::server_frame::Body::Hello(wire::ServerHello {
                                protocol_version: PROTOCOL_VERSION,
                                protocol_fingerprint: PROTOCOL_FINGERPRINT.to_string(),
                                supports_outbox_relay: true, accepted: true, error: None,
                            })),
                        }, DEFAULT_MAX_FRAME_BYTES).await.unwrap();
                        while let Some(frame) = read_message::<_, wire::ClientFrame>(&mut stream, DEFAULT_MAX_FRAME_BYTES)
                            .await.unwrap() {
                            let Some(wire::client_frame::Body::Request(request)) = frame.body else {
                                panic!("expected a request after handshake");
                            };
                            assert!(matches!(request.body, Some(wire::request_envelope::Body::LoadSnapshot(_))));
                            write_message(&mut stream, &wire::ServerFrame {
                                body: Some(wire::server_frame::Body::Response(wire::ResponseEnvelope {
                                    rpc_id: request.rpc_id, error: None,
                                    body: Some(wire::response_envelope::Body::LoadSnapshot(wire::LoadSnapshotResponse {
                                        snapshot: None,
                                    })),
                                })),
                            }, DEFAULT_MAX_FRAME_BYTES).await.unwrap();
                        }
                    });
                }
                Some(result) = peers.join_next(), if !peers.is_empty() => { result.unwrap(); }
            }
        }
    });
    let outcome = timeout(Duration::from_secs(10), async {
        let client = DbProxyClient::connect(ClientConfig::new(
            endpoint,
            "test-reconnect-token",
            "reconnect-test",
        ))
        .await
        .unwrap();
        let record = RecordKey::new("test", "reconnect").unwrap();
        assert!(client.load(&record).await.unwrap().is_none());
        for generation in 1..=2 {
            // All callers have observed the same unusable connection. Later callers must
            // not replace the successful repair made by the first one.
            client.connection.lock().await.usable = false;
            let mut callers = JoinSet::new();
            for _ in 0..8 {
                let client = client.clone();
                callers.spawn(async move { client.reconnect_next().await });
            }
            while let Some(result) = callers.join_next().await {
                result.unwrap().unwrap();
            }
            assert_eq!(
                connections.load(Ordering::SeqCst),
                generation + 1,
                "one reconnect per failed generation, not per waiting caller"
            );
            assert!(client.load(&record).await.unwrap().is_none());
        }
    })
    .await;
    server.abort();
    outcome.expect("reconnect regression must finish within its deadline");
}
