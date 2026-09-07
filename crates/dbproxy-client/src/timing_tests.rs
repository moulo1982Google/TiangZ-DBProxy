use super::*;
use std::{future::Future, sync::Mutex as StdMutex, task::Poll};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

#[derive(Default)]
struct Observer(StdMutex<Vec<(ClientRequestTiming, ClientRequestOutcome)>>);

impl ClientObserver for Observer {
    fn connection_attempt(&self, _: usize, _: Duration, _: ClientConnectionOutcome) {}
    fn endpoint_failover(&self, _: usize, _: usize) {}
    fn request_attempt(&self, _: usize, _: &'static str, _: Duration, _: ClientRequestOutcome) {
        panic!("the SDK must invoke the timed callback only once");
    }
    fn request_attempt_timed(
        &self,
        _: usize,
        _: &'static str,
        timing: ClientRequestTiming,
        outcome: ClientRequestOutcome,
    ) {
        self.0.lock().unwrap().push((timing, outcome));
    }
}

// A real wire exchange with an explicit server release, not a sleep-based guess about arrival.
async fn fixture(
    request_timeout: Duration,
) -> (
    DbProxyClient,
    Arc<Observer>,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
    JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let (arrived, arrival) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let hello = read_message::<_, wire::ClientFrame>(&mut stream, DEFAULT_MAX_FRAME_BYTES)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            hello.body,
            Some(wire::client_frame::Body::Hello(_))
        ));
        write_message(
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
        .await
        .unwrap();
        let frame = read_message::<_, wire::ClientFrame>(&mut stream, DEFAULT_MAX_FRAME_BYTES)
            .await
            .unwrap()
            .unwrap();
        let Some(wire::client_frame::Body::Request(request)) = frame.body else {
            panic!("expected request");
        };
        arrived.send(()).unwrap();
        if released.await.is_ok() {
            write_message(
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
            .await
            .unwrap();
        }
    });
    let observer = Arc::new(Observer::default());
    let mut config = ClientConfig::new(endpoint, "timing-test-token", "timing-test")
        .with_observer(observer.clone());
    config.request_timeout = request_timeout;
    let client = DbProxyClient::connect(config).await.unwrap();
    (client, observer, arrival, release, server)
}

fn request() -> wire::request_envelope::Body {
    wire::request_envelope::Body::LoadSnapshot(wire::LoadSnapshotRequest {
        record: Some((&RecordKey::new("timing", "one").unwrap()).into()),
    })
}

#[tokio::test]
async fn queue_and_server_wait_are_attributed_to_separate_stages() {
    timeout(Duration::from_secs(5), async {
        let (client, observer, arrival, release, server) = fixture(Duration::from_secs(2)).await;
        let lock = client.connection.lock().await;
        let mut call = Box::pin(client.call_once(request()));
        let before_attempt = Instant::now();
        // Poll while owning the lock: the attempt is provably queued before starting the interval.
        std::future::poll_fn(|cx| {
            assert!(call.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let queued_at = Instant::now();
        tokio::time::sleep(Duration::from_millis(30)).await;
        let queued_for = queued_at.elapsed();
        let before_unlock = Instant::now();
        drop(lock);
        let server_control = async {
            arrival.await.unwrap();
            let arrived_at = Instant::now();
            let queue_upper_bound = before_attempt.elapsed();
            tokio::time::sleep(Duration::from_millis(30)).await;
            let server_wait = arrived_at.elapsed();
            release.send(()).unwrap();
            (server_wait, queue_upper_bound)
        };
        let (result, (server_wait, queue_upper_bound)) = tokio::join!(call, server_control);
        let exchange_upper_bound = before_unlock.elapsed();
        result.unwrap();
        server.await.unwrap();
        let samples = observer.0.lock().unwrap();
        assert_eq!(samples.len(), 1);
        let (timing, outcome) = samples[0];
        assert_eq!(outcome, ClientRequestOutcome::Success);
        assert!(timing.queue_wait >= queued_for);
        assert!(timing.queue_wait <= queue_upper_bound);
        assert!(timing.exchange >= server_wait);
        assert!(timing.exchange <= exchange_upper_bound);
        assert_eq!(timing.total(), timing.queue_wait + timing.exchange);
    })
    .await
    .expect("timing fixture must terminate");
}

#[tokio::test]
async fn timeout_and_unsent_unusable_attempts_are_both_observed() {
    timeout(Duration::from_secs(5), async {
        let deadline = Duration::from_millis(30);
        let (client, observer, arrival, release, server) = fixture(deadline).await;
        assert!(matches!(
            client.call_once(request()).await,
            Err(ClientError::RequestTimeout)
        ));
        arrival.await.unwrap();
        assert!(matches!(
            client.call_once(request()).await,
            Err(ClientError::ConnectionUnusable)
        ));
        drop(release);
        server.await.unwrap();
        let samples = observer.0.lock().unwrap();
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].1, ClientRequestOutcome::Timeout);
        assert!(samples[0].0.exchange >= deadline);
        assert_eq!(samples[1].1, ClientRequestOutcome::Unavailable);
    })
    .await
    .expect("timeout fixture must terminate");
}

#[test]
fn legacy_observer_receives_exactly_one_total_duration() {
    #[derive(Default)]
    struct Legacy(StdMutex<Vec<Duration>>);
    impl ClientObserver for Legacy {
        fn connection_attempt(&self, _: usize, _: Duration, _: ClientConnectionOutcome) {}
        fn endpoint_failover(&self, _: usize, _: usize) {}
        fn request_attempt(
            &self,
            _: usize,
            _: &'static str,
            elapsed: Duration,
            _: ClientRequestOutcome,
        ) {
            self.0.lock().unwrap().push(elapsed);
        }
    }
    let observer = Legacy::default();
    observer.request_attempt_timed(
        0,
        "load",
        ClientRequestTiming {
            queue_wait: Duration::from_millis(40),
            exchange: Duration::from_millis(60),
        },
        ClientRequestOutcome::Success,
    );
    assert_eq!(*observer.0.lock().unwrap(), [Duration::from_millis(100)]);
}
