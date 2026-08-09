use std::{
    env,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tiangz_dbproxy_client::{ClientConfig, DbProxyClient};
use tiangz_dbproxy_core::{
    RecordKey, Revision, SnapshotWrite, SnapshotWriteOutcome, TransactionalWrite,
    TransactionalWriteOutcome,
};
use tiangz_dbproxy_server::{
    DbProxyBackend, DbProxyServer, ServerConfig, StorageBackend, run_backlog_worker,
};
use tokio::{sync::watch, time::sleep};

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{}-{nanos}", std::process::id())
}

fn snapshot(
    request_id: String,
    record: RecordKey,
    payload: &[u8],
    expected_revision: Option<Revision>,
) -> SnapshotWrite {
    SnapshotWrite {
        request_id,
        record,
        schema: "network.snapshot".to_string(),
        schema_version: 1,
        payload: payload.to_vec(),
        expected_revision,
        updated_at_unix_ms: 100,
    }
}

#[tokio::test]
#[ignore = "requires the local PostgreSQL and Redis containers"]
async fn network_service_reaches_real_storage_and_durable_backlog() {
    let postgres_url = env::var("DBPROXY_POSTGRES_URL")
        .unwrap_or_else(|_| "postgres://tiangz:tiangz_dev@127.0.0.1:5432/tiangz".to_string());
    let redis_url = env::var("DBPROXY_REDIS_URL")
        .unwrap_or_else(|_| "redis://:tiangz_dev@127.0.0.1:6379/0".to_string());
    let suffix = unique_suffix();
    let token = "real-storage-network-test-token";

    let backend = Arc::new(
        StorageBackend::connect(&postgres_url, &redis_url, 2)
            .await
            .unwrap(),
    );
    let backend_trait: Arc<dyn DbProxyBackend> = backend.clone();
    let server = DbProxyServer::bind(
        ServerConfig::new("127.0.0.1:0".parse().unwrap(), token),
        backend_trait,
    )
    .await
    .unwrap();
    let endpoint = server.local_addr().unwrap().to_string();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server_task = tokio::spawn(server.serve(shutdown_rx.clone()));
    let worker_task = tokio::spawn(run_backlog_worker(
        backend,
        5_000,
        Duration::from_millis(10),
        Duration::from_millis(50),
        shutdown_rx,
    ));
    let client = DbProxyClient::connect(ClientConfig::new(endpoint, token, "storage-smoke"))
        .await
        .unwrap();

    let direct_record = RecordKey::new("network-direct", &suffix).unwrap();
    let direct_write = snapshot(
        format!("direct-{suffix}"),
        direct_record.clone(),
        b"direct",
        Some(Revision::ZERO),
    );
    assert_eq!(
        client.save(direct_write.clone()).await.unwrap(),
        SnapshotWriteOutcome::Applied {
            revision: Revision(1)
        }
    );
    assert_eq!(
        client.save(direct_write).await.unwrap(),
        SnapshotWriteOutcome::Duplicate {
            revision: Revision(1)
        }
    );
    assert_eq!(
        client.load(&direct_record).await.unwrap().unwrap().payload,
        b"direct"
    );

    let transaction_record = RecordKey::new("network-transaction", &suffix).unwrap();
    let transaction = TransactionalWrite {
        operation_id: format!("operation-{suffix}"),
        record: transaction_record,
        schema: "network.transaction".to_string(),
        schema_version: 1,
        expected_revision: Revision::ZERO,
        payload: b"balance=10".to_vec(),
        result: b"granted=10".to_vec(),
        updated_at_unix_ms: 100,
    };
    assert!(matches!(
        client.apply_transaction(transaction.clone()).await.unwrap(),
        TransactionalWriteOutcome::Applied {
            new_revision: Revision(1),
            ..
        }
    ));
    assert!(matches!(
        client.apply_transaction(transaction).await.unwrap(),
        TransactionalWriteOutcome::Duplicate {
            new_revision: Revision(1),
            ..
        }
    ));

    let queued_record = RecordKey::new("network-backlog", &suffix).unwrap();
    client
        .enqueue_snapshot(snapshot(
            format!("queued-{suffix}"),
            queued_record.clone(),
            b"queued",
            None,
        ))
        .await
        .unwrap();
    let mut persisted = false;
    for _ in 0..100 {
        if client
            .load(&queued_record)
            .await
            .unwrap()
            .is_some_and(|snapshot| snapshot.payload == b"queued")
        {
            persisted = true;
            break;
        }
        sleep(Duration::from_millis(20)).await;
    }
    assert!(persisted, "durable backlog was not flushed to PostgreSQL");

    shutdown_tx.send(true).unwrap();
    server_task.await.unwrap().unwrap();
    worker_task.await.unwrap();
}
