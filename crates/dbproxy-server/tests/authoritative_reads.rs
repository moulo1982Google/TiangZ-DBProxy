use std::{
    env,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tiangz_dbproxy_client::{ClientConfig, DbProxyClient};
use tiangz_dbproxy_core::{AsyncSnapshotStore, RecordKey, Revision, SnapshotWrite};
use tiangz_dbproxy_server::{
    DbProxyBackend, DbProxyServer, ServerConfig, StorageBackend, StorageBackendConfig,
};
use tiangz_dbproxy_storage::{
    CacheFallbackConfig, PostgresSnapshotStore, TieredSnapshotStoreConfig,
};
use tokio::sync::watch;

fn write(record: &RecordKey, revision: u64, payload: &[u8]) -> SnapshotWrite {
    SnapshotWrite {
        request_id: format!("{}:{}:{revision}", record.namespace, record.key),
        record: record.clone(),
        schema: "test".into(),
        schema_version: 1,
        payload: payload.to_vec(),
        expected_revision: Some(Revision(revision)),
        updated_at_unix_ms: revision + 1,
    }
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and Redis; intentionally blocks PG reads"]
async fn default_reads_over_tcp_never_serve_stale_or_negative_cache() {
    tokio::time::timeout(Duration::from_secs(45), async {
        let pg = env::var("DBPROXY_POSTGRES_URL").unwrap();
        let redis = env::var("DBPROXY_REDIS_URL").unwrap();
        let namespace = format!(
            "authority-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let record = RecordKey::new(&namespace, "player").unwrap();
        let missing = RecordKey::new(&namespace, "missing").unwrap();
        let negative = RecordKey::new(&namespace, "negative").unwrap();
        let relaxed = RecordKey::new(format!("cache-{namespace}"), "other").unwrap();
        let cached = StorageBackend::connect(&pg, &redis, 1).await.unwrap();
        cached.save(write(&record, 0, b"old")).await.unwrap();
        cached.save(write(&relaxed, 0, b"cached")).await.unwrap();
        assert!(cached.load_cached(&negative).await.unwrap().is_none());
        let mut writer = PostgresSnapshotStore::connect(&pg).await.unwrap();
        writer.save(write(&record, 1, b"new")).await.unwrap();
        writer.save(write(&negative, 0, b"exists")).await.unwrap();
        writer.save(write(&relaxed, 1, b"committed")).await.unwrap();
        assert_eq!(cached.load_cached(&record).await.unwrap().unwrap().payload, b"old");
        assert!(cached.load_cached(&negative).await.unwrap().is_none());
        let backend = Arc::new(
            StorageBackend::connect_with_config(
                &pg,
                &redis,
                StorageBackendConfig {
                    shard_count: 2,
                    tiered: TieredSnapshotStoreConfig {
                        fallback: CacheFallbackConfig {
                            max_concurrent: 2,
                            timeout: Duration::from_millis(500),
                        },
                        ..Default::default()
                    },
                },
            )
            .await
            .unwrap(),
        );
        let server = DbProxyServer::bind(
            ServerConfig::new("127.0.0.1:0".parse().unwrap(), "authoritative-test-token"),
            backend.clone(),
        )
        .await
        .unwrap();
        let endpoint = server.local_addr().unwrap().to_string();
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(server.serve(receiver));
        let client = DbProxyClient::connect(ClientConfig::new(
            endpoint,
            "authoritative-test-token",
            "authority-test",
        ))
        .await
        .unwrap();
        assert_eq!(
            client.load(&record).await.unwrap().unwrap().revision,
            Revision(2)
        );
        let values = client
            .load_multi(&[negative.clone(), missing.clone(), record.clone()])
            .await
            .unwrap();
        assert_eq!(values[0].as_ref().unwrap().payload, b"exists");
        assert!(values[1].is_none());
        assert_eq!(values[2].as_ref().unwrap().payload, b"new");
        assert_eq!(backend.metrics().snapshot().cache_hits, 0);
        // 所有命名空间默认查PG，缓存读取必须显式选择。
        // Every namespace defaults to PG; cached reads require explicit opt-in.
        assert_eq!(
            client.load(&relaxed).await.unwrap().unwrap().payload,
            b"committed"
        );
        assert_eq!(client.load_cached(&record, None).await.unwrap().unwrap().revision, Revision(1));
        assert_eq!(client.load_cached(&record, Some(Revision(2))).await.unwrap().unwrap().revision, Revision(2));
        assert!(client.load_cached(&negative, None).await.unwrap().is_none());
        assert_eq!(client.load_cached(&negative, Some(Revision(1))).await.unwrap().unwrap().payload, b"exists");
        assert!(matches!(client.load_cached(&record, Some(Revision(3))).await,
            Err(tiangz_dbproxy_client::ClientError::Remote(error)) if error.code == tiangz_dbproxy_protocol::wire::ErrorCode::StorageUnavailable));
        assert!(client.load_cached(&missing, Some(Revision(1))).await.is_err());
        let fenced = client.load_cached_multi(&[relaxed.clone(), record.clone()], &[Revision(2), Revision(2)]).await.unwrap();
        assert_eq!(fenced[0].as_ref().unwrap().payload, b"committed");
        assert_eq!(fenced[1].as_ref().unwrap().revision, Revision(2));
        assert!(client.load_cached_multi(&[record.clone(), missing.clone()], &[Revision(2), Revision(1)]).await.is_err());

        // 缓存写暂停后PG提交仍成功；独立读节点与新建节点都必须读到新值。
        // A paused cache writer cannot invalidate a PG commit or reads from another/new peer.
        let mut admin = redis::Client::open(redis.as_str()).unwrap().get_multiplexed_async_connection().await.unwrap();
        let _: () = redis::cmd("CLIENT").arg("PAUSE").arg(3000).arg("WRITE").query_async(&mut admin).await.unwrap();
        let save = cached.save(write(&record, 2, b"after-timeout")).await;
        let read = client.load(&record).await;
        let batch = client.load_multi(&[record.clone(), relaxed.clone()]).await;
        let _: () = redis::cmd("CLIENT").arg("UNPAUSE").query_async(&mut admin).await.unwrap();
        save.unwrap();
        assert_eq!(read.unwrap().unwrap().revision, Revision(3));
        assert_eq!(batch.unwrap()[0].as_ref().unwrap().revision, Revision(3));
        let restarted = StorageBackend::connect(&pg, &redis, 2).await.unwrap();
        assert_eq!(restarted.load(&record).await.unwrap().unwrap().revision, Revision(3));

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
        let mixed = client
            .load_multi(&[relaxed.clone(), record.clone()])
            .await
            .unwrap();
        assert_eq!(mixed[0].as_ref().unwrap().payload, b"committed");
        assert_eq!(
            operations() - before,
            1,
            "authoritative batch uses one PG statement"
        );

        // 负缓存不能伪装成不存在；PG阻塞时也不能返回现成的旧缓存。
        // A blocked authority must fail, never return the known stale cached value.
        let (mut sql, connection) = tokio_postgres::connect(&pg, tokio_postgres::NoTls)
            .await
            .unwrap();
        let connection_task = tokio::spawn(connection);
        let tx = sql.transaction().await.unwrap();
        tx.batch_execute("LOCK TABLE dbproxy_snapshots IN ACCESS EXCLUSIVE MODE")
            .await
            .unwrap();
        let blocked = client.load(&record).await;
        tx.rollback().await.unwrap();
        assert!(
            blocked.is_err(),
            "PG timeout must not return cached success"
        );
        assert_eq!(client.load(&record).await.unwrap().unwrap().payload, b"after-timeout");
        // 持续原子更新两条记录，批量读取不得拼出不同提交的版本。
        // Concurrent atomic updates must never produce mixed revisions in batch reads.
        let pair = [RecordKey::new(&namespace, "pair-a").unwrap(), RecordKey::new(&namespace, "pair-b").unwrap()];
        for key in &pair { cached.save(write(key, 0, b"pair")).await.unwrap(); }
        let pair_namespace = namespace.clone();
        let update = tokio::spawn(async move {
            for _ in 0..100 {
                sql.execute("UPDATE dbproxy_snapshots SET revision=revision+1 WHERE namespace=$1 AND record_key IN ('pair-a','pair-b')", &[&pair_namespace]).await.unwrap();
                tokio::task::yield_now().await;
            }
        });
        for _ in 0..100 {
            let pair_read = client.load_multi(&pair).await.unwrap();
            assert_eq!(pair_read[0].as_ref().unwrap().revision, pair_read[1].as_ref().unwrap().revision);
        }
        update.await.unwrap();
        connection_task.await.unwrap().unwrap();
        shutdown.send(true).unwrap();
        drop(client);
        task.await.unwrap().unwrap();
    })
    .await
    .expect("bounded authority regression");
}
