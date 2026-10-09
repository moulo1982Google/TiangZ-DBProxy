//! Real executable acceptance against two isolated PostgreSQL databases.
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};
use tiangz_dbproxy_client::{ClientConfig, DbProxyClient};
use tiangz_dbproxy_core::{RecordKey, Revision, SnapshotWrite, SnapshotWriteOutcome};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn free_port() -> String {
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    socket.local_addr().unwrap().to_string()
}
fn spawn(config: &Path, dir: &Path, round: usize, pg_a: &str, pg_b: &str) -> Server {
    let binary = std::env::var("DBPROXY_ACCEPTANCE_BINARY")
        .unwrap_or_else(|_| env!("CARGO_BIN_EXE_tiangz-dbproxy-server").into());
    let child = Command::new(binary)
        .arg("--tenants")
        .arg(config)
        .env("ACCEPT_PG_A", pg_a)
        .env("ACCEPT_PG_B", pg_b)
        .env("ACCEPT_AUTH_A", "acceptance-tenant-a-token")
        .env("ACCEPT_AUTH_B", "acceptance-tenant-b-token")
        .env("ACCEPT_REDIS_A", std::env::var("ACCEPT_REDIS_A").unwrap())
        .env("ACCEPT_REDIS_B", std::env::var("ACCEPT_REDIS_B").unwrap())
        .env("ACCEPT_CACHE_A", std::env::var("ACCEPT_CACHE_A").unwrap())
        .env("ACCEPT_CACHE_B", std::env::var("ACCEPT_CACHE_B").unwrap())
        .stdin(Stdio::null())
        .stdout(std::fs::File::create(dir.join(format!("server-{round}.stdout"))).unwrap())
        .stderr(std::fs::File::create(dir.join(format!("server-{round}.stderr"))).unwrap())
        .spawn()
        .unwrap();
    Server(child)
}
async fn client(endpoint: &str, token: &str, server: &mut Server) -> DbProxyClient {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            assert!(
                server.0.try_wait().unwrap().is_none(),
                "server exited during startup"
            );
            if let Ok(client) =
                DbProxyClient::connect(ClientConfig::new(endpoint, token, "process-acceptance"))
                    .await
            {
                return client;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
}
async fn sql(url: &str) -> tokio_postgres::Client {
    let (sql, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    sql
}
async fn metrics(endpoint: &str) -> String {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut stream = tokio::net::TcpStream::connect(endpoint).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    })
    .await
    .unwrap()
}
fn request(payload: u8) -> SnapshotWrite {
    SnapshotWrite {
        request_id: "identical-request".into(),
        record: RecordKey::new("process-acceptance", "same-key").unwrap(),
        schema: "test".into(),
        schema_version: 1,
        payload: vec![payload],
        expected_revision: Some(Revision::ZERO),
        updated_at_unix_ms: 1,
    }
}

#[tokio::test]
#[ignore = "fresh isolated PG; two release server processes, 32 retries and concurrent cleanup"]
async fn two_processes_share_receipts_without_duplicate_application() {
    use tiangz_dbproxy_core::AsyncSnapshotStore;
    assert_eq!(
        std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(),
        Ok("1")
    );
    let pg = std::env::var("ACCEPT_PG_A").unwrap();
    let root =
        PathBuf::from(std::env::var("DBPROXY_ACCEPTANCE_ARTIFACTS").unwrap()).join("two-processes");
    std::fs::create_dir_all(&root).unwrap();
    let mut store = tiangz_dbproxy_storage::PostgresSnapshotStore::connect(&pg)
        .await
        .unwrap();
    let mut write = request(42);
    write.request_id = "two-process-same-request".into();
    write.record = RecordKey::new("two-process-acceptance", "same-key").unwrap();
    write.expected_revision = None;
    store.save(write.clone()).await.unwrap();
    let sql = sql(&pg).await;
    sql.batch_execute("UPDATE dbproxy_idempotency SET recorded_at=clock_timestamp()-interval '169 hours' WHERE request_id='two-process-same-request';
        INSERT INTO dbproxy_idempotency(request_id,namespace,record_key,schema_name,schema_version,payload,revision,recorded_at)
        SELECT 'two-process-old-'||n,'two-process-old',n::text,'test',1,'',1,clock_timestamp()-interval '169 hours' FROM generate_series(1,1101) n").await.unwrap();
    drop(store);
    let mut servers = Vec::new();
    let mut clients = Vec::new();
    for n in 0..2 {
        let dir = root.join(n.to_string());
        std::fs::create_dir_all(&dir).unwrap();
        let endpoint = free_port();
        let config = serde_json::json!({"configVersion":1,"server":{"listenAddr":endpoint,"authTokenEnv":"ACCEPT_AUTH_A","maxConnections":32},"runtime":{"workerThreads":4},"storage":{"backend":"postgresRedis","postgresUrlEnv":"ACCEPT_PG_A","redisUrlEnv":"ACCEPT_REDIS_A","cacheRedisUrlEnv":"ACCEPT_CACHE_A","shards":2}});
        std::fs::write(
            dir.join("A.json"),
            serde_json::to_vec_pretty(&config).unwrap(),
        )
        .unwrap();
        let deployment = dir.join("tenants.json");
        std::fs::write(&deployment,serde_json::to_vec_pretty(&serde_json::json!({"configVersion":1,"listenAddr":endpoint,"maxConnections":32,"tenants":[{"id":"a","config":"A.json"}]})).unwrap()).unwrap();
        let mut server = spawn(&deployment, &dir, 1, &pg, &pg);
        clients.push(std::sync::Arc::new(
            client(&endpoint, "acceptance-tenant-a-token", &mut server).await,
        ));
        servers.push(server);
    }
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(33));
    let mut tasks = tokio::task::JoinSet::new();
    for n in 0..32 {
        let client = clients[n % 2].clone();
        let write = write.clone();
        let barrier = barrier.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            client.save(write).await.unwrap()
        });
    }
    barrier.wait().await;
    let mut applied = 0;
    while let Some(result) = tasks.join_next().await {
        match result.unwrap() {
            SnapshotWriteOutcome::Applied { revision } => {
                applied += 1;
                assert_eq!(revision, Revision(2));
            }
            SnapshotWriteOutcome::Duplicate { revision } => {
                assert!([Revision(1), Revision(2)].contains(&revision))
            }
        }
    }
    assert!(applied <= 1);
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let remaining: i64 = sql
                .query_one(
                    "SELECT count(*) FROM dbproxy_idempotency WHERE namespace='two-process-old'",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            if remaining == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    for client in clients {
        let snapshot = client.load(&write.record).await.unwrap().unwrap();
        assert_eq!(snapshot.revision, Revision(1 + applied));
        assert_eq!(snapshot.payload, write.payload);
    }
    std::fs::write(root.join("result.json"),serde_json::to_vec_pretty(&serde_json::json!({"processes":2,"callers":32,"new_applications":applied,"old_fixture_receipts_remaining":0,"snapshot_revision":1+applied})).unwrap()).unwrap();
    drop(servers);
}
#[tokio::test]
#[ignore = "authorized isolated PG/Redis, real DP subprocesses; no old-schema upgrade"]
async fn fresh_process_tenants_cleanup_crash_restart_and_index_guard() {
    assert_eq!(
        std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(),
        Ok("1")
    );
    let pg_a = std::env::var("ACCEPT_PG_A").unwrap();
    let pg_b = std::env::var("ACCEPT_PG_B").unwrap();
    let dir = PathBuf::from(std::env::var("DBPROXY_ACCEPTANCE_ARTIFACTS").unwrap()).join("process");
    std::fs::create_dir_all(&dir).unwrap();
    let endpoint = free_port();
    let obs_a = free_port();
    let obs_b = free_port();
    for (id, obs) in [("A", &obs_a), ("B", &obs_b)] {
        let config = serde_json::json!({"configVersion":1,"server":{"listenAddr":endpoint,"authTokenEnv":format!("ACCEPT_AUTH_{id}"),"maxConnections":16},"runtime":{"workerThreads":4},"storage":{"backend":"postgresRedis","postgresUrlEnv":format!("ACCEPT_PG_{id}"),"redisUrlEnv":format!("ACCEPT_REDIS_{id}"),"cacheRedisUrlEnv":format!("ACCEPT_CACHE_{id}"),"shards":4},"observability":{"listenAddr":obs}});
        std::fs::write(
            dir.join(format!("{id}.json")),
            serde_json::to_vec_pretty(&config).unwrap(),
        )
        .unwrap();
    }
    let deployment = dir.join("tenants.json");
    std::fs::write(&deployment,serde_json::to_vec_pretty(&serde_json::json!({"configVersion":1,"listenAddr":endpoint,"maxConnections":32,"tenants":[{"id":"a","config":"A.json"},{"id":"b","config":"B.json"}]})).unwrap()).unwrap();
    let mut server = spawn(&deployment, &dir, 1, &pg_a, &pg_b);
    let a = client(&endpoint, "acceptance-tenant-a-token", &mut server).await;
    let b = client(&endpoint, "acceptance-tenant-b-token", &mut server).await;
    for (client, byte) in [(&a, 7), (&b, 9)] {
        assert_eq!(
            client.save(request(byte)).await.unwrap(),
            SnapshotWriteOutcome::Applied {
                revision: Revision(1)
            }
        );
        assert_eq!(
            client.save(request(byte)).await.unwrap(),
            SnapshotWriteOutcome::Duplicate {
                revision: Revision(1)
            }
        );
        assert!(client.save(request(byte + 1)).await.is_err());
        assert_eq!(
            client
                .load(&request(byte).record)
                .await
                .unwrap()
                .unwrap()
                .payload,
            vec![byte]
        );
    }
    let sql_a = sql(&pg_a).await;
    let sql_b = sql(&pg_b).await;
    server.0.kill().unwrap();
    server.0.wait().unwrap();
    drop(a);
    drop(b);
    sql_a.batch_execute("UPDATE dbproxy_idempotency SET recorded_at=clock_timestamp()-interval '169 hours' WHERE request_id='identical-request'").await.unwrap();
    let original_time:String=sql_b.query_one("SELECT recorded_at::TEXT FROM dbproxy_idempotency WHERE request_id='identical-request'",&[]).await.unwrap().get(0);
    let mut server = spawn(&deployment, &dir, 2, &pg_a, &pg_b);
    let a = client(&endpoint, "acceptance-tenant-a-token", &mut server).await;
    let b = client(&endpoint, "acceptance-tenant-b-token", &mut server).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let count: i64 = sql_a
                .query_one(
                    "SELECT count(*) FROM dbproxy_idempotency WHERE request_id='identical-request'",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            if count == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        a.load(&request(7).record).await.unwrap().unwrap().payload,
        vec![7]
    );
    assert!(
        a.save(request(7)).await.is_err(),
        "expired ordinary receipt must not bypass CAS"
    );
    assert_eq!(
        b.save(request(9)).await.unwrap(),
        SnapshotWriteOutcome::Duplicate {
            revision: Revision(1)
        }
    );
    let retained_time:String=sql_b.query_one("SELECT recorded_at::TEXT FROM dbproxy_idempotency WHERE request_id='identical-request'",&[]).await.unwrap().get(0);
    assert_eq!(
        original_time, retained_time,
        "restart and retry must not renew receipt age"
    );
    let text = metrics(&obs_a).await;
    assert!(text.contains("dbproxy_receipt_cleanup_deleted_total 1"));
    std::fs::write(dir.join("tenant-a-metrics.txt"), text).unwrap();
    std::fs::write(dir.join("tenant-b-metrics.txt"), metrics(&obs_b).await).unwrap();
    server.0.kill().unwrap();
    server.0.wait().unwrap();
    drop(a);
    drop(b);
    sql_a
        .batch_execute("DROP INDEX dbproxy_idempotency_retention")
        .await
        .unwrap();
    let mut invalid = spawn(&deployment, &dir, 3, &pg_a, &pg_b);
    let exit = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = invalid.0.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(!exit.success());
    assert!(
        std::fs::read_to_string(dir.join("server-3.stderr"))
            .unwrap()
            .contains("dbproxy_idempotency_retention")
    );
    sql_a.batch_execute("CREATE INDEX dbproxy_idempotency_retention ON dbproxy_idempotency(recorded_at,request_id)").await.unwrap();
    let mut repaired = spawn(&deployment, &dir, 4, &pg_a, &pg_b);
    let a = client(&endpoint, "acceptance-tenant-a-token", &mut repaired).await;
    assert_eq!(
        a.load(&request(7).record).await.unwrap().unwrap().revision,
        Revision(1)
    );
    std::fs::write(dir.join("result.json"),serde_json::to_vec_pretty(&serde_json::json!({"passed":["fresh-init","cross-tenant-same-id","retry-content-conflict","crash-restart","automatic-tenant-cleanup","unchanged-receipt-age","metrics","startup-index-rejection","repair-restart"],"full_plan_complete":false})).unwrap()).unwrap();
}
