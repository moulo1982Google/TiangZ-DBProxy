use super::*;
use tokio::{net::TcpListener, sync::mpsc};

/// 握手后把收到的请求交给测试，由测试决定按什么顺序、回不回复。
/// After the handshake, hands every request to the test, which decides whether and in which order to answer.
async fn scripted_server() -> (
    String,
    mpsc::UnboundedReceiver<wire::RequestEnvelope>,
    mpsc::UnboundedSender<u64>,
    JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let (requests, received) = mpsc::unbounded_channel();
    let (answer, mut answers) = mpsc::unbounded_channel::<u64>();
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (mut reader, mut writer) = stream.into_split();
        let hello = read_message::<_, wire::ClientFrame>(&mut reader, DEFAULT_MAX_FRAME_BYTES)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            hello.body,
            Some(wire::client_frame::Body::Hello(_))
        ));
        write_message(
            &mut writer,
            &wire::ServerFrame {
                body: Some(wire::server_frame::Body::Hello(wire::ServerHello {
                    protocol_version: PROTOCOL_VERSION,
                    protocol_fingerprint: PROTOCOL_FINGERPRINT.to_owned(),
                    supports_outbox_relay: true,
                    accepted: true,
                    error: None,
                })),
            },
            DEFAULT_MAX_FRAME_BYTES,
        )
        .await
        .unwrap();
        let reading = tokio::spawn(async move {
            while let Ok(Some(frame)) =
                read_message::<_, wire::ClientFrame>(&mut reader, DEFAULT_MAX_FRAME_BYTES).await
            {
                let Some(wire::client_frame::Body::Request(request)) = frame.body else {
                    panic!("expected a request");
                };
                if requests.send(request).is_err() {
                    return;
                }
            }
        });
        while let Some(rpc_id) = answers.recv().await {
            let response = wire::ServerFrame {
                body: Some(wire::server_frame::Body::Response(wire::ResponseEnvelope {
                    rpc_id,
                    error: None,
                    body: Some(wire::response_envelope::Body::LoadSnapshot(
                        wire::LoadSnapshotResponse { snapshot: None },
                    )),
                })),
            };
            if write_message(&mut writer, &response, DEFAULT_MAX_FRAME_BYTES)
                .await
                .is_err()
            {
                break;
            }
        }
        reading.abort();
    });
    (endpoint, received, answer, task)
}

fn load(record: &str) -> wire::request_envelope::Body {
    wire::request_envelope::Body::LoadSnapshot(wire::LoadSnapshotRequest {
        allow_stale: false,
        min_revision: None,
        record: Some((&RecordKey::new("multiplex", record).unwrap()).into()),
    })
}

fn record_of(request: &wire::RequestEnvelope) -> String {
    let Some(wire::request_envelope::Body::LoadSnapshot(load)) = &request.body else {
        panic!("expected a load");
    };
    load.record.as_ref().unwrap().key.clone()
}

async fn client(endpoint: String, request_timeout: Duration) -> DbProxyClient {
    let mut config = ClientConfig::new(endpoint, "multiplex-test-token", "multiplex-test");
    config.request_timeout = request_timeout;
    DbProxyClient::connect(config).await.unwrap()
}

#[tokio::test]
async fn one_connection_carries_several_requests_and_matches_out_of_order_responses() {
    timeout(Duration::from_secs(5), async {
        let (endpoint, mut received, answer, server) = scripted_server().await;
        let client = client(endpoint, Duration::from_secs(3)).await;
        let first = tokio::spawn({
            let client = client.clone();
            async move { client.call_once(load("first")).await }
        });
        let first_request = received.recv().await.unwrap();
        let second = tokio::spawn({
            let client = client.clone();
            async move { client.call_once(load("second")).await }
        });
        // 第一个请求尚未回复，第二个请求已经发到服务端。 / The second request is sent while the first is unanswered.
        let second_request = received.recv().await.unwrap();
        assert_eq!(record_of(&first_request), "first");
        assert_eq!(record_of(&second_request), "second");
        assert_ne!(first_request.rpc_id, second_request.rpc_id);

        answer.send(second_request.rpc_id).unwrap();
        let second = second.await.unwrap().unwrap();
        assert_eq!(second.rpc_id, second_request.rpc_id);
        assert!(!first.is_finished());
        answer.send(first_request.rpc_id).unwrap();
        assert_eq!(first.await.unwrap().unwrap().rpc_id, first_request.rpc_id);
        drop(answer);
        server.await.unwrap();
    })
    .await
    .expect("multiplexing fixture must terminate");
}

#[tokio::test]
async fn a_slow_request_times_out_alone_while_the_connection_keeps_answering() {
    timeout(Duration::from_secs(5), async {
        let (endpoint, mut received, answer, server) = scripted_server().await;
        let client = client(endpoint, Duration::from_millis(300)).await;
        let slow = tokio::spawn({
            let client = client.clone();
            async move { client.call_once(load("slow")).await }
        });
        let slow_request = received.recv().await.unwrap();
        let fast = client.call_once(load("fast"));
        let answering = async {
            let fast_request = received.recv().await.unwrap();
            answer.send(fast_request.rpc_id).unwrap();
        };
        let (fast, ()) = tokio::join!(fast, answering);
        fast.unwrap();
        assert!(matches!(
            slow.await.unwrap(),
            Err(ClientError::RequestTimeout)
        ));
        // 连接在慢请求发出后仍有响应，所以不判定失效；迟到的响应被丢弃。
        // The connection answered after the slow send, so it stays usable; the late response is discarded.
        assert!(client.current().shared.usable());
        answer.send(slow_request.rpc_id).unwrap();
        let next = client.call_once(load("next"));
        let answering = async {
            let next_request = received.recv().await.unwrap();
            answer.send(next_request.rpc_id).unwrap();
            next_request.rpc_id
        };
        let (next, rpc_id) = tokio::join!(next, answering);
        assert_eq!(next.unwrap().rpc_id, rpc_id);
        drop(answer);
        server.await.unwrap();
    })
    .await
    .expect("timeout fixture must terminate");
}

#[tokio::test]
async fn closing_the_connection_fails_every_waiting_request() {
    timeout(Duration::from_secs(5), async {
        let (endpoint, mut received, answer, server) = scripted_server().await;
        let client = client(endpoint, Duration::from_secs(3)).await;
        let mut waiting = Vec::new();
        for record in ["a", "b", "c"] {
            let client = client.clone();
            waiting.push(tokio::spawn(
                async move { client.call_once(load(record)).await },
            ));
            received.recv().await.unwrap();
        }
        drop(answer);
        server.await.unwrap();
        for call in waiting {
            assert!(matches!(
                call.await.unwrap(),
                Err(ClientError::ConnectionClosed)
            ));
        }
        assert!(!client.current().shared.usable());
    })
    .await
    .expect("close fixture must terminate");
}

#[tokio::test]
async fn in_flight_limit_is_validated_before_connecting() {
    let mut config = ClientConfig::new("127.0.0.1:1", "multiplex-test-token", "multiplex-test");
    config.max_in_flight = 0;
    assert!(matches!(
        DbProxyClient::connect(config).await,
        Err(ClientError::InvalidConfig(_))
    ));
}
