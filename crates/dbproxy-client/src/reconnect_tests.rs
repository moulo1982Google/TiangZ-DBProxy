use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::{net::TcpListener, task::JoinSet};

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
