//! Production server components with separate test-owned shutdown controls.
use super::*;
use tiangz_dbproxy_server::{
    DbProxyMetrics, DbProxyServer, ObservabilityServer, ServerConfig, StorageBackend,
    StorageBackendConfig, run_receipt_cleanup_worker,
};

#[tokio::test]
#[ignore = "F15: stop and restore the real observability listener while RPC and cleanup continue"]
async fn f15_listener_stop_does_not_stop_business_or_cleanup() {
    struct Tasks(Vec<tokio::task::JoinHandle<()>>);
    impl Drop for Tasks {
        fn drop(&mut self) {
            for task in &self.0 {
                task.abort();
            }
        }
    }
    let env = env();
    let db = format!("{}_f15listener", env.run_id);
    let owner: String = sql(&format!("{}/postgres", env.admin_base))
        .await
        .query_one("SELECT current_user::text", &[])
        .await
        .unwrap()
        .get(0);
    create_database(&env.admin_base, &db, &owner).await;
    let url = format!("{}/{db}", env.admin_base);
    let dir = env.artifacts.join("f15-listener");
    std::fs::create_dir_all(&dir).unwrap();
    let backend = Arc::new(
        StorageBackend::connect_with_redis_urls(
            &url,
            &env.redis[0],
            &env.cache[0],
            StorageBackendConfig {
                shard_count: 2,
                read_connection_count: 2,
                tiered: Default::default(),
                enqueue: Default::default(),
            },
        )
        .await
        .unwrap(),
    );
    let admin = sql(&url).await;
    let request_metrics = Arc::new(DbProxyMetrics::default());
    let mut config = ServerConfig::new("127.0.0.1:0".parse().unwrap(), TOKEN_A);
    config.metrics = request_metrics.clone();
    let server = DbProxyServer::bind(config, backend.clone()).await.unwrap();
    let endpoint = server.local_addr().unwrap().to_string();
    let (business_stop, business_shutdown) = tokio::sync::watch::channel(false);
    let server_task = tokio::spawn(async move {
        server.serve(business_shutdown).await.unwrap();
    });
    let mut tasks = Tasks(vec![server_task]);
    let (monitor_stop, monitor_shutdown) = tokio::sync::watch::channel(false);
    let monitor = ObservabilityServer::start(
        "127.0.0.1:0".parse().unwrap(),
        request_metrics.clone(),
        "postgresRedis",
        monitor_shutdown,
    )
    .await
    .unwrap();
    let monitor_address = monitor.local_addr();
    let obs = monitor_address.to_string();
    let client = DbProxyClient::connect(ClientConfig::new(&endpoint, TOKEN_A, "listener-fault"))
        .await
        .unwrap();
    assert!(metrics(&obs).await.starts_with("HTTP/1.1 200 OK"));
    for n in 0..20u8 {
        client
            .save(write("f15-listener", &format!("before-{n}"), n))
            .await
            .unwrap();
    }
    monitor_stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), monitor.stop())
        .await
        .expect("monitor listener failed to stop");
    insert_expired(&admin, "f15-listener-expired", 1501).await;
    let (cleanup_stop, cleanup_shutdown) = tokio::sync::watch::channel(false);
    tasks.0.push(tokio::spawn(run_receipt_cleanup_worker(
        backend,
        "listener-test".into(),
        tiangz_dbproxy_storage::DEFAULT_RECEIPT_RETENTION,
        request_metrics.clone(),
        cleanup_shutdown,
    )));
    let mut max_write_seconds = 0.0f64;
    for n in 0..40u8 {
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            tokio::net::TcpStream::connect(monitor_address),
        )
        .await
        .unwrap()
        .expect_err("monitor listener still accepts connections");
        assert_eq!(error.kind(), std::io::ErrorKind::ConnectionRefused);
        let request = write("f15-listener", &format!("down-{n}"), n);
        let started = Instant::now();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), client.save(request.clone()))
                .await
                .unwrap()
                .unwrap(),
            SnapshotWriteOutcome::Applied {
                revision: Revision(1)
            }
        );
        max_write_seconds = max_write_seconds.max(started.elapsed().as_secs_f64());
        assert_eq!(
            client.load(&request.record).await.unwrap().unwrap().payload,
            vec![n]
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        count(
            &admin,
            "SELECT count(*) FROM dbproxy_idempotency WHERE namespace='f15-listener-expired'"
        )
        .await,
        0
    );
    let (restored_stop, restored_shutdown) = tokio::sync::watch::channel(false);
    let restored = ObservabilityServer::start(
        monitor_address,
        request_metrics,
        "postgresRedis",
        restored_shutdown,
    )
    .await
    .unwrap();
    let restored_metrics = metrics(&obs).await;
    assert!(restored_metrics.starts_with("HTTP/1.1 200 OK"));
    assert_eq!(
        metric_sum(&restored_metrics, "dbproxy_receipt_cleanup_deleted_total"),
        1501.0
    );
    std::fs::write(dir.join("metrics-restored.txt"), restored_metrics).unwrap();
    for n in 0..20u8 {
        client
            .save(write("f15-listener", &format!("after-{n}"), n))
            .await
            .unwrap();
    }
    assert_eq!(
        count(
            &admin,
            "SELECT count(*) FROM dbproxy_snapshots WHERE namespace='f15-listener'"
        )
        .await,
        80
    );
    for (phase, rows) in [("before", 20), ("down", 40), ("after", 20)] {
        for n in 0..rows {
            let key = format!("{phase}-{n}");
            let row = admin.query_one("SELECT payload,revision FROM dbproxy_snapshots WHERE namespace='f15-listener' AND record_key=$1", &[&key]).await.unwrap();
            assert_eq!(row.get::<_, Vec<u8>>(0), vec![n as u8]);
            assert_eq!(row.get::<_, i64>(1), 1);
            assert_eq!(
                client
                    .save(write("f15-listener", &key, n as u8))
                    .await
                    .unwrap(),
                SnapshotWriteOutcome::Duplicate {
                    revision: Revision(1)
                }
            );
        }
    }
    restored_stop.send(true).unwrap();
    restored.stop().await;
    cleanup_stop.send(true).unwrap();
    business_stop.send(true).unwrap();
    for task in &mut tasks.0 {
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
    }
    let result = serde_json::json!({"run":env.run_id,"scope":"production components in test process, independent monitor shutdown channel","connection_refusals":40,"writes_verified_pg":80,"duplicates_verified":80,"deleted_while_monitor_stopped":1501,"max_outage_write_seconds":max_write_seconds,"business_restarted":false});
    std::fs::write(
        dir.join("result.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    println!("F15_LISTENER_RESULT {result}");
}
