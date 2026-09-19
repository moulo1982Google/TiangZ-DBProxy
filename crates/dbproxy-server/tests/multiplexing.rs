//! 同一连接多请求在途：不同记录并发、同一记录按到达顺序、停机时收尾已接收请求。
//! Several requests in flight on one connection: different records run concurrently, one record keeps
//! arrival order, and shutdown finishes every accepted request.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use async_trait::async_trait;
use tiangz_dbproxy_core::{
    RecordKey, SnapshotEnvelope, SnapshotWrite, SnapshotWriteOutcome, TransactionReceipt,
    TransactionalWrite, TransactionalWriteOutcome,
};
use tiangz_dbproxy_protocol::{
    DEFAULT_MAX_FRAME_BYTES, PROTOCOL_FINGERPRINT, PROTOCOL_VERSION, read_message, wire,
    write_message,
};
use tiangz_dbproxy_server::{BackendError, DbProxyBackend, DbProxyServer, ServerConfig};
use tokio::{
    net::TcpStream,
    sync::{Semaphore, watch},
    task::JoinHandle,
    time::timeout,
};

const TOKEN: &str = "multiplexing-test-token";

/// 载荷以 "hold" 开头的入队会等待测试放行；日志记录开始与结束顺序。
/// Enqueues whose payload starts with "hold" wait for the test; the log records start/finish order.
#[derive(Default)]
struct GatedBackend {
    gates: StdMutex<HashMap<Vec<u8>, Arc<Semaphore>>>,
    log: StdMutex<Vec<String>>,
}

impl GatedBackend {
    fn gate(&self, payload: &[u8]) -> Arc<Semaphore> {
        Arc::clone(
            self.gates
                .lock()
                .unwrap()
                .entry(payload.to_vec())
                .or_insert_with(|| Arc::new(Semaphore::new(0))),
        )
    }

    fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }
}

#[async_trait]
impl DbProxyBackend for GatedBackend {
    async fn load(&self, _: &RecordKey) -> Result<Option<SnapshotEnvelope>, BackendError> {
        Ok(None)
    }

    async fn save(&self, _: SnapshotWrite) -> Result<SnapshotWriteOutcome, BackendError> {
        Err(BackendError::InvalidConfig("not used"))
    }

    async fn enqueue_snapshot(&self, request: SnapshotWrite) -> Result<(), BackendError> {
        let name = String::from_utf8(request.payload.clone()).unwrap();
        self.log.lock().unwrap().push(format!("start {name}"));
        if request.payload.starts_with(b"hold") {
            self.gate(&request.payload)
                .acquire()
                .await
                .unwrap()
                .forget();
        }
        self.log.lock().unwrap().push(format!("end {name}"));
        Ok(())
    }

    async fn apply_transaction(
        &self,
        _: TransactionalWrite,
    ) -> Result<TransactionalWriteOutcome, BackendError> {
        Err(BackendError::InvalidConfig("not used"))
    }

    async fn load_transaction(
        &self,
        _: &str,
        _: &RecordKey,
    ) -> Result<Option<TransactionReceipt>, BackendError> {
        Ok(None)
    }
}

struct TestServer {
    endpoint: String,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

async fn start(backend: Arc<GatedBackend>, max_in_flight: usize) -> TestServer {
    let mut config = ServerConfig::new("127.0.0.1:0".parse().unwrap(), TOKEN);
    config.max_in_flight_per_connection = max_in_flight;
    let server = DbProxyServer::bind(config, backend).await.unwrap();
    let endpoint = server.local_addr().unwrap().to_string();
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(async move { server.serve(receiver).await.unwrap() });
    TestServer {
        endpoint,
        shutdown,
        task,
    }
}

async fn connect(endpoint: &str) -> TcpStream {
    let mut stream = TcpStream::connect(endpoint).await.unwrap();
    let hello = wire::ClientFrame {
        body: Some(wire::client_frame::Body::Hello(wire::ClientHello {
            protocol_version: PROTOCOL_VERSION,
            protocol_fingerprint: PROTOCOL_FINGERPRINT.to_string(),
            auth_token: TOKEN.to_string(),
            client_name: "multiplexing-test".to_string(),
        })),
    };
    write_message(&mut stream, &hello, DEFAULT_MAX_FRAME_BYTES)
        .await
        .unwrap();
    let reply = read_message::<_, wire::ServerFrame>(&mut stream, DEFAULT_MAX_FRAME_BYTES)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        reply.body,
        Some(wire::server_frame::Body::Hello(wire::ServerHello {
            accepted: true,
            ..
        }))
    ));
    stream
}

async fn send_enqueue(stream: &mut TcpStream, rpc_id: u64, record: &str, payload: &str) {
    let write = SnapshotWrite {
        request_id: format!("request-{rpc_id}"),
        record: RecordKey::new("player", record).unwrap(),
        schema: "player.snapshot".to_string(),
        schema_version: 1,
        payload: payload.as_bytes().to_vec(),
        expected_revision: None,
        updated_at_unix_ms: 1,
    };
    let frame = wire::ClientFrame {
        body: Some(wire::client_frame::Body::Request(wire::RequestEnvelope {
            rpc_id,
            body: Some(wire::request_envelope::Body::EnqueueSnapshot(
                wire::EnqueueSnapshotRequest {
                    write: Some((&write).into()),
                },
            )),
        })),
    };
    write_message(stream, &frame, DEFAULT_MAX_FRAME_BYTES)
        .await
        .unwrap();
}

async fn next_response(stream: &mut TcpStream) -> Option<u64> {
    let frame = timeout(
        Duration::from_secs(5),
        read_message::<_, wire::ServerFrame>(stream, DEFAULT_MAX_FRAME_BYTES),
    )
    .await
    .expect("the server must answer or close within the deadline")
    .unwrap()?;
    let Some(wire::server_frame::Body::Response(response)) = frame.body else {
        panic!("expected a response frame");
    };
    assert!(response.error.is_none(), "{:?}", response.error);
    Some(response.rpc_id)
}

async fn assert_no_response(stream: &mut TcpStream) {
    let waited = timeout(
        Duration::from_millis(200),
        read_message::<_, wire::ServerFrame>(stream, DEFAULT_MAX_FRAME_BYTES),
    )
    .await;
    assert!(waited.is_err(), "no response may be ready yet");
}

async fn wait_for_log(backend: &GatedBackend, entry: &str) {
    timeout(Duration::from_secs(5), async {
        while !backend.log().iter().any(|item| item == entry) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("backend never logged {entry}: {:?}", backend.log()));
}

#[tokio::test]
async fn other_records_overtake_a_slow_request_but_one_record_keeps_arrival_order() {
    let backend = Arc::new(GatedBackend::default());
    let server = start(Arc::clone(&backend), 64).await;
    let mut stream = connect(&server.endpoint).await;

    send_enqueue(&mut stream, 1, "a", "hold-a1").await;
    send_enqueue(&mut stream, 2, "a", "a2").await;
    send_enqueue(&mut stream, 3, "b", "b1").await;

    // b 不等 a 的慢请求；响应不按发送顺序返回。 / b does not wait for a; responses leave send order.
    assert_eq!(next_response(&mut stream).await, Some(3));
    assert_no_response(&mut stream).await;
    assert!(
        !backend.log().iter().any(|item| item == "start a2"),
        "a2 must not start before a1 finishes: {:?}",
        backend.log()
    );

    backend.gate(b"hold-a1").add_permits(1);
    assert_eq!(next_response(&mut stream).await, Some(1));
    assert_eq!(next_response(&mut stream).await, Some(2));
    let log = backend.log();
    let position = |entry: &str| log.iter().position(|item| item == entry).unwrap();
    assert!(position("end hold-a1") < position("start a2"), "{log:?}");

    drop(stream);
    server.shutdown.send(true).unwrap();
    server.task.await.unwrap();
}

#[tokio::test]
async fn the_in_flight_limit_applies_backpressure_per_connection() {
    let backend = Arc::new(GatedBackend::default());
    let server = start(Arc::clone(&backend), 1).await;
    let mut stream = connect(&server.endpoint).await;

    send_enqueue(&mut stream, 1, "a", "hold-a1").await;
    wait_for_log(&backend, "start hold-a1").await;
    send_enqueue(&mut stream, 2, "b", "b1").await;
    assert_no_response(&mut stream).await;
    assert!(!backend.log().iter().any(|item| item == "start b1"));

    backend.gate(b"hold-a1").add_permits(1);
    assert_eq!(next_response(&mut stream).await, Some(1));
    assert_eq!(next_response(&mut stream).await, Some(2));

    drop(stream);
    server.shutdown.send(true).unwrap();
    server.task.await.unwrap();
}

#[tokio::test]
async fn shutdown_finishes_accepted_requests_and_writes_their_responses() {
    let backend = Arc::new(GatedBackend::default());
    let server = start(Arc::clone(&backend), 64).await;
    let mut stream = connect(&server.endpoint).await;

    send_enqueue(&mut stream, 7, "a", "hold-a1").await;
    wait_for_log(&backend, "start hold-a1").await;
    server.shutdown.send(true).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    backend.gate(b"hold-a1").add_permits(1);

    assert_eq!(next_response(&mut stream).await, Some(7));
    assert_eq!(
        next_response(&mut stream).await,
        None,
        "then the server closes"
    );
    server.task.await.unwrap();
}

#[tokio::test]
async fn invalid_in_flight_limits_are_rejected_before_bind() {
    for limit in [0, tiangz_dbproxy_server::MAX_IN_FLIGHT_PER_CONNECTION + 1] {
        let mut config = ServerConfig::new("127.0.0.1:0".parse().unwrap(), TOKEN);
        config.max_in_flight_per_connection = limit;
        assert!(
            DbProxyServer::bind(config, Arc::new(GatedBackend::default()))
                .await
                .is_err()
        );
    }
}
