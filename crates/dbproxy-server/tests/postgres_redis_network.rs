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

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL/Redis; batch commit count and partial outcomes"]
async fn snapshot_batch_uses_one_commit_and_preserves_independent_results() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let pg = env::var("DBPROXY_POSTGRES_URL").unwrap();
        let redis = env::var("DBPROXY_REDIS_URL").unwrap();
        let backend = StorageBackend::connect(&pg, &redis, 2).await.unwrap();
        let suffix = unique_suffix();
        let requests: Vec<_> = (0..32)
            .map(|i| {
                snapshot(
                    format!("batch-{suffix}-{i}"),
                    RecordKey::new("batch-commit", format!("{suffix}-{i}")).unwrap(),
                    b"one",
                    Some(Revision::ZERO),
                )
            })
            .collect();
        let operations = || {
            backend
                .metrics()
                .latency_snapshot()
                .into_iter()
                .find(|s| s.stage == "postgres_operation")
                .unwrap()
                .buckets
                .iter()
                .sum::<u64>()
        };
        let before = operations();
        let results = backend.save_multi(requests.clone()).await.unwrap();
        assert!(results.iter().all(|r| matches!(
            r,
            Ok(SnapshotWriteOutcome::Applied {
                revision: Revision(1)
            })
        )));
        assert_eq!(
            operations() - before,
            1,
            "one same-database batch must not fan out into several commits"
        );
        assert!(
            backend
                .save_multi(requests.clone())
                .await
                .unwrap()
                .iter()
                .all(|r| matches!(
                    r,
                    Ok(SnapshotWriteOutcome::Duplicate {
                        revision: Revision(1)
                    })
                ))
        );
        let mut next = requests.clone();
        for request in &mut next {
            request.request_id.push_str("-next");
            request.expected_revision = Some(Revision(1));
            request.payload = b"two".to_vec();
        }
        next[7].expected_revision = Some(Revision::ZERO);
        next[19].request_id = requests[19].request_id.clone();
        let results = backend.save_multi(next.clone()).await.unwrap();
        for (index, outcome) in results.iter().enumerate() {
            if [7, 19].contains(&index) {
                assert!(outcome.is_err());
            } else {
                assert!(matches!(
                    outcome,
                    Ok(SnapshotWriteOutcome::Applied {
                        revision: Revision(2)
                    })
                ));
            }
        }
        let retry = backend.save_multi(next).await.unwrap();
        for (index, outcome) in retry.iter().enumerate() {
            if [7, 19].contains(&index) {
                assert!(outcome.is_err());
            } else {
                assert!(matches!(
                    outcome,
                    Ok(SnapshotWriteOutcome::Duplicate {
                        revision: Revision(2)
                    })
                ));
            }
            assert_eq!(
                backend
                    .load(&requests[index].record)
                    .await
                    .unwrap()
                    .unwrap()
                    .revision,
                Revision(if [7, 19].contains(&index) { 1 } else { 2 })
            );
        }
        // 相反输入顺序的重叠批次仍各自按记录排序，并保持每条 CAS 只成功一次。
        // Opposite input orders must preserve record lock order and exactly one CAS winner.
        let racing = |suffix: &str| {
            requests
                .iter()
                .enumerate()
                .map(|(index, request)| {
                    let mut request = request.clone();
                    request.request_id.push_str(suffix);
                    request.expected_revision =
                        Some(Revision(if [7, 19].contains(&index) { 1 } else { 2 }));
                    request
                })
                .collect::<Vec<_>>()
        };
        let left = racing("-left");
        let mut right = racing("-right");
        right.reverse();
        let (left, right) = tokio::join!(backend.save_multi(left), backend.save_multi(right));
        let left = left.unwrap();
        let right = right.unwrap();
        for (left, right) in left.iter().zip(right.iter().rev()) {
            assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        }
    })
    .await
    .expect("batch commit regression deadline");
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
