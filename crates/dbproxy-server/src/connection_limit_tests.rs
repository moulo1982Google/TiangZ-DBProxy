use super::*;
use std::sync::atomic::{AtomicU8, Ordering};
use tiangz_dbproxy_client::{ClientConfig, DbProxyClient};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    task::JoinHandle,
};

const TOKEN: &str = "connection-limit-test-token";

struct Backend {
    inner: MemoryBackend,
    mode: AtomicU8,
}

#[async_trait]
impl DbProxyBackend for Backend {
    async fn load(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, BackendError> {
        match self.mode.load(Ordering::SeqCst) {
            1 => panic!("injected backend panic"),
            2 => std::future::pending().await,
            _ => self.inner.load(record).await,
        }
    }
    async fn save(&self, request: SnapshotWrite) -> Result<SnapshotWriteOutcome, BackendError> {
        self.inner.save(request).await
    }
    async fn enqueue_snapshot(&self, request: SnapshotWrite) -> Result<(), BackendError> {
        self.inner.enqueue_snapshot(request).await
    }
    async fn apply_transaction(
        &self,
        request: TransactionalWrite,
    ) -> Result<TransactionalWriteOutcome, BackendError> {
        self.inner.apply_transaction(request).await
    }
    async fn load_transaction(
        &self,
        id: &str,
        record: &RecordKey,
    ) -> Result<Option<TransactionReceipt>, BackendError> {
        self.inner.load_transaction(id, record).await
    }
}

struct Fixture {
    endpoint: SocketAddr,
    metrics: Arc<DbProxyMetrics>,
    backend: Arc<Backend>,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<(), ServerError>>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Fixture {
    async fn new(limit: usize) -> Self {
        let mut config = ServerConfig::new("127.0.0.1:0".parse().unwrap(), TOKEN);
        config.max_connections = limit;
        config.handshake_timeout = Duration::from_millis(500);
        config.shutdown_grace = Duration::from_millis(20);
        let metrics = config.metrics.clone();
        let backend = Arc::new(Backend {
            inner: MemoryBackend::new(1).unwrap(),
            mode: AtomicU8::new(0),
        });
        let server = DbProxyServer::bind(config, backend.clone()).await.unwrap();
        let endpoint = server.local_addr().unwrap();
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(server.serve(receiver));
        Self {
            endpoint,
            metrics,
            backend,
            shutdown,
            task,
        }
    }
    async fn client(&self) -> DbProxyClient {
        let mut config = ClientConfig::new(self.endpoint.to_string(), TOKEN, "limit-test");
        config.connect_timeout = Duration::from_secs(1);
        config.request_timeout = Duration::from_secs(1);
        DbProxyClient::connect(config).await.unwrap()
    }
    async fn wait_metric(&self, name: &str, value: u64) {
        timeout(Duration::from_secs(3), async {
            loop {
                if self
                    .metrics
                    .prometheus("memory")
                    .lines()
                    .any(|line| line == format!("{name} {value}"))
                {
                    break;
                }
                sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("metric {name} did not reach {value}"));
    }
}

#[tokio::test]
async fn unauthenticated_connections_consume_slots_and_excess_connections_are_closed() {
    let f = Fixture::new(2).await;
    let first = TcpStream::connect(f.endpoint).await.unwrap();
    let second = TcpStream::connect(f.endpoint).await.unwrap();
    f.wait_metric("dbproxy_connections_active", 2).await;
    for rejected in 1..=4 {
        let mut excess = TcpStream::connect(f.endpoint).await.unwrap();
        let mut byte = [0];
        let result = timeout(Duration::from_secs(1), excess.read(&mut byte))
            .await
            .unwrap();
        assert!(
            matches!(result, Ok(0) | Err(_)),
            "over-capacity socket received protocol bytes"
        );
        f.wait_metric("dbproxy_connections_rejected_total", rejected)
            .await;
        f.wait_metric("dbproxy_connections_active", 2).await;
    }
    f.wait_metric("dbproxy_connections_total", 2).await;
    f.wait_metric("dbproxy_connections_limit", 2).await;
    drop(first);
    f.wait_metric("dbproxy_connections_active", 1).await;
    let client = f.client().await;
    assert!(
        client
            .load(&RecordKey::new("test", "slot").unwrap())
            .await
            .unwrap()
            .is_none()
    );
    drop(second);
    drop(client);
    f.wait_metric("dbproxy_connections_active", 0).await;
}

#[tokio::test]
async fn timeout_auth_rejection_and_malformed_frames_release_slots() {
    let f = Fixture::new(1).await;
    let idle = TcpStream::connect(f.endpoint).await.unwrap();
    f.wait_metric("dbproxy_connections_active", 1).await;
    f.wait_metric("dbproxy_connections_active", 0).await; // Handshake deadline, without client close.
    drop(idle);
    assert!(
        DbProxyClient::connect(ClientConfig::new(
            f.endpoint.to_string(),
            "wrong-but-long-token",
            "bad-auth"
        ))
        .await
        .is_err()
    );
    f.wait_metric("dbproxy_connections_active", 0).await;
    let mut malformed = TcpStream::connect(f.endpoint).await.unwrap();
    malformed.write_all(&0_u32.to_be_bytes()).await.unwrap();
    let _ = timeout(Duration::from_secs(1), malformed.read(&mut [0]))
        .await
        .unwrap();
    f.wait_metric("dbproxy_connections_active", 0).await;
    let client = f.client().await;
    assert!(
        client
            .load(&RecordKey::new("test", "usable").unwrap())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn panic_releases_slot_and_forced_shutdown_drains_active_gauge() {
    let mut f = Fixture::new(1).await;
    f.backend.mode.store(1, Ordering::SeqCst);
    let client = f.client().await;
    // A backend panic closes the socket; automatic client retry may panic a second task too.
    assert!(
        client
            .load(&RecordKey::new("test", "panic").unwrap())
            .await
            .is_err()
    );
    f.wait_metric("dbproxy_connections_active", 0).await;
    f.backend.mode.store(0, Ordering::SeqCst);
    let recovered = f.client().await;
    assert!(
        recovered
            .load(&RecordKey::new("test", "recovered").unwrap())
            .await
            .unwrap()
            .is_none()
    );
    f.backend.mode.store(2, Ordering::SeqCst);
    let request = tokio::spawn(async move {
        recovered
            .load(&RecordKey::new("test", "pending").unwrap())
            .await
    });
    // Observe the in-flight RPC before asking shutdown to cancel it.
    f.wait_metric("dbproxy_requests_in_flight", 1).await;
    f.shutdown.send(true).unwrap();
    timeout(Duration::from_secs(2), &mut f.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    f.wait_metric("dbproxy_connections_active", 0).await;
    request.abort();
    f.wait_metric("dbproxy_requests_in_flight", 0).await;
}

#[tokio::test]
async fn direct_server_api_rejects_invalid_connection_limits_before_bind() {
    for limit in [0, usize::MAX] {
        let mut config = ServerConfig::new("127.0.0.1:0".parse().unwrap(), TOKEN);
        config.max_connections = limit;
        assert!(matches!(
            DbProxyServer::bind(config, Arc::new(MemoryBackend::new(1).unwrap())).await,
            Err(ServerError::InvalidConfig(_))
        ));
    }
}
