use super::multiplex_tests::{load, scripted_server};
use super::*;
use std::{future::Future, task::Poll};
use tokio::{net::TcpListener, sync::mpsc};

#[tokio::test]
async fn invalid_request_budgets_fail_before_connecting() {
    for duration in [Duration::ZERO, Duration::MAX] {
        let mut config = ClientConfig::new("127.0.0.1:1", "budget-test-token", "budget-test");
        config.request_timeout = duration;
        assert!(matches!(
            DbProxyClient::connect(config).await,
            Err(ClientError::InvalidConfig(_))
        ));
    }
}

#[tokio::test]
async fn expired_budget_does_not_send_even_when_the_writer_is_ready() {
    timeout(Duration::from_secs(3), async {
        let (endpoint, mut received, answer, server) = scripted_server().await;
        let client = DbProxyClient::connect(ClientConfig::new(
            endpoint,
            "budget-test-token",
            "budget-test",
        ))
        .await
        .unwrap();
        let mut budget = RequestBudget::new(Duration::from_millis(1)).unwrap();
        budget.deadline = tokio::time::Instant::now();
        let result = client.call(budget, load("expired")).await;
        assert!(matches!(result, Err(ClientError::RequestNotSentTimeout)));
        assert!(received.try_recv().is_err());
        assert!(client.current().shared.pending().waiters.is_empty());
        drop(answer);
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn retry_queue_timeout_cannot_erase_an_earlier_possible_send() {
    timeout(Duration::from_secs(3), async {
        let (endpoint, mut received, answer, server) = scripted_server().await;
        let client = DbProxyClient::connect(ClientConfig::new(
            endpoint,
            "budget-test-token",
            "budget-test",
        ))
        .await
        .unwrap();
        let connection = client.current();
        let writer = connection.writer.lock().await;
        let mut budget = RequestBudget::new(Duration::from_millis(30)).unwrap();
        budget.start_write();
        let result = client.call_attempt(load("retry"), &mut budget).await;
        assert!(matches!(result, Err(ClientError::RequestTimeout)));
        assert!(received.try_recv().is_err());
        assert!(connection.shared.usable());
        drop(writer);
        drop(answer);
        server.await.unwrap();
    })
    .await
    .unwrap();
}

/// 把请求明确推进到受控等待点，避免用 sleep 猜测是否已经入队。 / Polls a request into its controlled wait before timing the scenario.
async fn poll_pending<F: Future>(future: std::pin::Pin<&mut F>) {
    let mut future = future;
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

/// 等待准入和等待写锁均须消耗预算；失败后验证没有发包且连接可恢复。 / Admission and writer waits consume the budget without sending or poisoning the connection.
async fn queued_timeout(hold_slot: bool) {
    let (endpoint, mut received, answer, server) = scripted_server().await;
    let mut config = ClientConfig::new(endpoint, "budget-test-token", "budget-test");
    config.max_in_flight = 1;
    config.request_timeout = Duration::from_millis(80);
    let client = DbProxyClient::connect(config).await.unwrap();
    let connection = client.current();
    let slot = if hold_slot {
        Some(connection.in_flight.acquire().await.unwrap())
    } else {
        None
    };
    let writer = if hold_slot {
        None
    } else {
        Some(connection.writer.lock().await)
    };
    let record = RecordKey::new("budget", "queued").unwrap();
    let result = timeout(Duration::from_millis(500), client.load(&record)).await;
    let pending = connection.shared.pending().waiters.len();
    let unsent = received.try_recv().is_err();
    drop(slot);
    drop(writer);
    let recovery = client.call_once(load("recovered"));
    let answer_next = async {
        let request = received.recv().await.unwrap();
        answer.send(request.rpc_id).unwrap();
    };
    let (recovery, ()) = tokio::join!(recovery, answer_next);
    drop(answer);
    server.await.unwrap();
    assert!(result.is_ok(), "queued request exceeded its total budget");
    let error = result.unwrap().unwrap_err();
    assert!(error.to_string().contains("before sending"), "{error}");
    assert_eq!(pending, 0);
    assert!(unsent);
    assert_eq!(connection.in_flight.available_permits(), 1);
    recovery.unwrap();
}

#[tokio::test]
async fn admission_wait_expires_without_sending_and_releases_resources() {
    timeout(Duration::from_secs(3), queued_timeout(true))
        .await
        .unwrap();
}

#[tokio::test]
async fn writer_wait_expires_without_sending_and_releases_resources() {
    timeout(Duration::from_secs(3), queued_timeout(false))
        .await
        .unwrap();
}

#[tokio::test]
async fn response_wait_uses_only_the_budget_remaining_after_queueing() {
    timeout(Duration::from_secs(3), async {
        let (endpoint, mut received, answer, server) = scripted_server().await;
        let mut config = ClientConfig::new(endpoint, "budget-test-token", "budget-test");
        config.request_timeout = Duration::from_millis(300);
        let client = DbProxyClient::connect(config).await.unwrap();
        let connection = client.current();
        let writer = connection.writer.lock().await;
        let mut request = Box::pin(client.call_once(load("queued-then-sent")));
        poll_pending(request.as_mut()).await;
        tokio::time::sleep(Duration::from_millis(180)).await;
        drop(writer);
        let respond = async {
            let request = received.recv().await.unwrap();
            tokio::time::sleep(Duration::from_millis(180)).await;
            answer.send(request.rpc_id).unwrap();
        };
        let (result, ()) = tokio::join!(request, respond);
        drop(answer);
        server.await.unwrap();
        assert!(
            matches!(result, Err(ClientError::RequestTimeout)),
            "{result:?}"
        );
        assert!(connection.shared.pending().waiters.is_empty());
        assert_eq!(
            connection.in_flight.available_permits(),
            DEFAULT_MAX_IN_FLIGHT
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn reconnect_lock_wait_is_bounded_and_does_not_claim_a_request_was_sent() {
    timeout(Duration::from_secs(3), async {
        let (endpoint, received, answer, server) = scripted_server().await;
        let mut config = ClientConfig::new(endpoint, "budget-test-token", "budget-test");
        config.request_timeout = Duration::from_millis(80);
        let client = DbProxyClient::connect(config).await.unwrap();
        let connection = client.current();
        connection.shared.mark_unusable();
        let reconnecting = client.reconnecting.lock().await;
        let record = RecordKey::new("budget", "reconnect").unwrap();
        let result = timeout(Duration::from_millis(500), client.load(&record)).await;
        drop(reconnecting);
        drop(received);
        drop(answer);
        server.await.unwrap();
        assert!(
            result.is_ok(),
            "reconnect lock exceeded the logical request budget"
        );
        let error = result.unwrap().unwrap_err();
        assert!(error.to_string().contains("before sending"), "{error}");
        assert!(connection.shared.pending().waiters.is_empty());
    })
    .await
    .unwrap();
}

/// 接受真实握手，确保阶段耗时来自可控服务端。 / Accepts a real handshake for controlled phase timing.
async fn accept_hello(listener: TcpListener, delay: Duration) -> TcpStream {
    let (mut stream, _) = listener.accept().await.unwrap();
    let hello = read_message::<_, wire::ClientFrame>(&mut stream, DEFAULT_MAX_FRAME_BYTES)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        hello.body,
        Some(wire::client_frame::Body::Hello(_))
    ));
    tokio::time::sleep(delay).await;
    let _ = write_message(
        &mut stream,
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
    .await;
    stream
}

#[tokio::test]
async fn failover_handshake_consumes_the_original_budget_and_preserves_unknown_outcome() {
    timeout(Duration::from_secs(3), async {
        let primary = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let secondary = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = ClientConfig::new(
            primary.local_addr().unwrap().to_string(),
            "budget-test-token",
            "budget-test",
        )
        .with_endpoints([secondary.local_addr().unwrap().to_string()]);
        config.request_timeout = Duration::from_millis(300);
        config.connect_timeout = Duration::from_secs(1);
        let first = tokio::spawn(async move {
            let mut stream = accept_hello(primary, Duration::ZERO).await;
            let frame = read_message::<_, wire::ClientFrame>(&mut stream, DEFAULT_MAX_FRAME_BYTES)
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(
                frame.body,
                Some(wire::client_frame::Body::Request(_))
            ));
            tokio::time::sleep(Duration::from_millis(180)).await;
        });
        let (sent, mut received) = mpsc::channel(1);
        let second = tokio::spawn(async move {
            let mut stream = accept_hello(secondary, Duration::from_millis(180)).await;
            if let Ok(Some(frame)) =
                read_message::<_, wire::ClientFrame>(&mut stream, DEFAULT_MAX_FRAME_BYTES).await
            {
                let Some(wire::client_frame::Body::Request(request)) = frame.body else {
                    panic!("expected request")
                };
                sent.send(request.rpc_id).await.unwrap();
                let _ = write_message(
                    &mut stream,
                    &wire::ServerFrame {
                        body: Some(wire::server_frame::Body::Response(wire::ResponseEnvelope {
                            rpc_id: request.rpc_id,
                            error: None,
                            body: Some(wire::response_envelope::Body::LoadSnapshot(
                                wire::LoadSnapshotResponse { snapshot: None },
                            )),
                        })),
                    },
                    DEFAULT_MAX_FRAME_BYTES,
                )
                .await;
            }
        });
        let client = DbProxyClient::connect(config).await.unwrap();
        let result = client
            .load(&RecordKey::new("budget", "failover").unwrap())
            .await;
        first.await.unwrap();
        second.await.unwrap();
        assert!(
            matches!(result, Err(ClientError::RequestTimeout)),
            "{result:?}"
        );
        assert!(
            received.try_recv().is_err(),
            "expired operation must not send to the next endpoint"
        );
        assert!(client.current().shared.pending().waiters.is_empty());
    })
    .await
    .unwrap();
}
