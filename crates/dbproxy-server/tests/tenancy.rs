use std::{sync::Arc, time::Duration};
use tiangz_dbproxy_client::{ClientConfig, DbProxyClient};
use tiangz_dbproxy_core::{
    AppendRecord, CommitEffects, MultiRecordTransactionalWrite, OutboxEvent, RecordKey, Revision,
    SnapshotWrite, TransactionalRecordWrite,
};
use tiangz_dbproxy_server::{
    DbProxyMetrics, DbProxyServer, MemoryBackend, ServerConfig, TenantBackend,
};
use tokio::sync::watch;

const A: &str = "tenant-a-test-secret-123";
const B: &str = "tenant-b-test-secret-456";

struct OwnedProcess(std::process::Child);
impl Drop for OwnedProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn executable_loads_static_tenant_deployment_without_database_services() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "dbproxy-tenant-cli-{}-{unique}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    drop(listener);
    for (id, token_env) in [("a", "TEST_TENANT_A_TOKEN"), ("b", "TEST_TENANT_B_TOKEN")] {
        let value = serde_json::json!({"configVersion":1,"server":{"listenAddr":endpoint,"authTokenEnv":token_env,"maxConnections":4},
            "runtime":{"workerThreads":2},"storage":{"backend":"memory","shards":2}});
        std::fs::write(
            root.join(format!("{id}.json")),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();
    }
    let deployment = root.join("tenants.json");
    std::fs::write(
        &deployment,
        serde_json::to_vec(
            &serde_json::json!({"configVersion":1,"listenAddr":endpoint,"maxConnections":16,
        "tenants":[{"id":"a","config":"a.json"},{"id":"b","config":"b.json"}]}),
        )
        .unwrap(),
    )
    .unwrap();
    let child = std::process::Command::new(env!("CARGO_BIN_EXE_tiangz-dbproxy-server"))
        .arg("--tenants")
        .arg(&deployment)
        .env("TEST_TENANT_A_TOKEN", A)
        .env("TEST_TENANT_B_TOKEN", B)
        .env("RUST_LOG", "error")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let mut owned = OwnedProcess(child);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let a = loop {
        assert!(
            owned.0.try_wait().unwrap().is_none(),
            "tenant server exited during startup"
        );
        if let Ok(client) =
            DbProxyClient::connect(ClientConfig::new(&endpoint, A, "cli-probe")).await
        {
            break client;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "tenant server startup timeout"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let b = connect(&endpoint, B).await;
    a.save(write(11)).await.unwrap();
    b.save(write(22)).await.unwrap();
    assert_eq!(
        a.load(&write(1).record).await.unwrap().unwrap().payload,
        [11]
    );
    assert_eq!(
        b.load(&write(1).record).await.unwrap().unwrap().payload,
        [22]
    );
}
fn route(id: &str, token: &str, limit: usize) -> TenantBackend {
    TenantBackend::new(
        id,
        token,
        Arc::new(MemoryBackend::new(2).unwrap()),
        limit,
        Arc::new(DbProxyMetrics::default()),
    )
    .unwrap()
}
fn write(payload: u8) -> SnapshotWrite {
    SnapshotWrite {
        request_id: "same-request".into(),
        record: RecordKey::new("player", "1001").unwrap(),
        schema: "player".into(),
        schema_version: 1,
        payload: vec![payload],
        expected_revision: Some(Revision::ZERO),
        updated_at_unix_ms: 1,
    }
}
async fn connect(endpoint: &str, token: &str) -> DbProxyClient {
    DbProxyClient::connect(ClientConfig::new(
        endpoint,
        token,
        "deliberately-not-a-tenant-selector",
    ))
    .await
    .unwrap()
}

#[tokio::test]
async fn tcp_credentials_bind_all_records_receipts_and_events_to_one_tenant() {
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap(), "legacy-token-must-not-work");
    let server =
        DbProxyServer::bind_tenants(config, vec![route("game-a", A, 8), route("game-b", B, 8)])
            .await
            .unwrap();
    let endpoint = server.local_addr().unwrap().to_string();
    let (stop, rx) = watch::channel(false);
    let task = tokio::spawn(server.serve(rx));
    for token in [
        "invalid-test-token",
        "legacy-token-must-not-work",
        "unused-multi-tenant-placeholder",
    ] {
        assert!(
            DbProxyClient::connect(ClientConfig::new(&endpoint, token, "game-a"))
                .await
                .is_err()
        );
    }
    let a = connect(&endpoint, A).await;
    let b = connect(&endpoint, B).await;
    a.save(write(1)).await.unwrap();
    assert!(b.load(&write(1).record).await.unwrap().is_none());
    b.save(write(2)).await.unwrap();
    assert_eq!(
        a.load(&write(1).record).await.unwrap().unwrap().payload,
        [1]
    );
    assert_eq!(
        b.load(&write(1).record).await.unwrap().unwrap().payload,
        [2]
    );
    for (client, value) in [(&a, 3), (&b, 4)] {
        let request = MultiRecordTransactionalWrite {
            operation_id: "same-operation".into(),
            result: vec![value],
            writes: ["left", "right"]
                .into_iter()
                .map(|key| TransactionalRecordWrite {
                    record: RecordKey::new("wallet", key).unwrap(),
                    schema: "wallet".into(),
                    schema_version: 1,
                    expected_revision: Revision::ZERO,
                    payload: vec![value],
                    updated_at_unix_ms: 1,
                })
                .collect(),
        };
        let effects = CommitEffects {
            appends: vec![AppendRecord {
                record: RecordKey::new("audit", "same-fact").unwrap(),
                schema: "audit".into(),
                schema_version: 1,
                payload: vec![value],
                occurred_at_unix_ms: 1,
            }],
            outbox_events: vec![OutboxEvent {
                event_id: "same-event".into(),
                topic: "wallet.changed".into(),
                partition_key: "same".into(),
                payload: vec![value],
                occurred_at_unix_ms: 1,
            }],
        };
        client
            .commit_records(request.clone(), effects.clone())
            .await
            .unwrap();
        client.commit_records(request, effects).await.unwrap();
        assert_eq!(
            client
                .load(&RecordKey::new("wallet", "left").unwrap())
                .await
                .unwrap()
                .unwrap()
                .payload,
            [value]
        );
    }
    drop(a);
    let a = connect(&endpoint, A).await;
    assert_eq!(
        a.load(&write(1).record).await.unwrap().unwrap().payload,
        [1]
    );
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn tenant_connection_budget_does_not_block_other_tenant_and_is_released() {
    let server = DbProxyServer::bind_tenants(
        ServerConfig::new("127.0.0.1:0".parse().unwrap(), "unused-token-0000"),
        vec![route("game-a", A, 1), route("game-b", B, 1)],
    )
    .await
    .unwrap();
    let endpoint = server.local_addr().unwrap().to_string();
    let (stop, rx) = watch::channel(false);
    let task = tokio::spawn(server.serve(rx));
    let a = connect(&endpoint, A).await;
    assert!(
        DbProxyClient::connect(ClientConfig::new(&endpoint, A, "overflow"))
            .await
            .is_err()
    );
    let b = connect(&endpoint, B).await;
    b.save(write(9)).await.unwrap();
    drop(a);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if DbProxyClient::connect(ClientConfig::new(&endpoint, A, "replacement"))
            .await
            .is_ok()
        {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn ambiguous_routes_fail_before_binding_and_secrets_are_redacted() {
    let a = route("a", A, 1);
    assert!(!format!("{a:?}").contains(A));
    let config = || ServerConfig::new("127.0.0.1:0".parse().unwrap(), "unused-token-0000");
    assert!(DbProxyServer::bind_tenants(config(), vec![]).await.is_err());
    assert!(
        DbProxyServer::bind_tenants(config(), vec![a.clone(), a])
            .await
            .is_err()
    );
    assert!(
        DbProxyServer::bind_tenants(config(), vec![route("a", A, 1), route("b", A, 1)])
            .await
            .is_err()
    );
}
