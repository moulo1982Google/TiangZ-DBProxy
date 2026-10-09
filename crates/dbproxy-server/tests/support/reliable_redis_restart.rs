//! Dedicated AOF Redis, with a reply gate to make the unconfirmed enqueue interval observable.
use super::*;
use tiangz_dbproxy_server::{
    DbProxyBackend, DbProxyMetrics, StorageBackend, StorageBackendConfig, run_backlog_worker,
    run_receipt_cleanup_worker,
};
use tiangz_dbproxy_storage::RedisSnapshotBacklog;

struct Task(tokio::task::JoinHandle<()>);
impl Drop for Task {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct ReplyGate {
    url: String,
    hold: Arc<AtomicBool>,
    held: Arc<AtomicBool>,
    reset: tokio::sync::watch::Sender<u64>,
    _task: Task,
}

impl ReplyGate {
    async fn start(url: &str) -> Self {
        let client = redis::Client::open(url).unwrap();
        let target = match client.get_connection_info().addr() {
            redis::ConnectionAddr::Tcp(host, port) => format!("{host}:{port}"),
            _ => panic!("test requires plain TCP Redis"),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let relay_url = url.replace(&target, &address.to_string());
        let hold = Arc::new(AtomicBool::new(false));
        let held = Arc::new(AtomicBool::new(false));
        let (reset, mut changed) = tokio::sync::watch::channel(0u64);
        let gate = hold.clone();
        let observed = held.clone();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        let (incoming, _) = result.unwrap();
                        let target = target.clone();
                        let gate = gate.clone();
                        let observed = observed.clone();
                        connections.spawn(async move {
                            let upstream = tokio::net::TcpStream::connect(target).await?;
                            incoming.set_nodelay(true)?;
                            upstream.set_nodelay(true)?;
                            let (mut cr, mut cw) = incoming.into_split();
                            let (mut sr, mut sw) = upstream.into_split();
                            tokio::select! {
                                result = tokio::io::copy(&mut cr, &mut sw) => { result?; }
                                result = async {
                                    let mut buffer = [0u8;8192];
                                    loop {
                                        let len = sr.read(&mut buffer).await?;
                                        if len == 0 { break; }
                                        if gate.load(Ordering::SeqCst) {
                                            observed.store(true, Ordering::SeqCst);
                                            std::future::pending::<()>().await;
                                        }
                                        cw.write_all(&buffer[..len]).await?;
                                    }
                                    Ok::<(), std::io::Error>(())
                                } => { result?; }
                            }
                            Ok::<(), std::io::Error>(())
                        });
                    }
                    result = changed.changed() => {
                        if result.is_err() { break; }
                        connections.abort_all();
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Self {
            url: relay_url,
            hold,
            held,
            reset,
            _task: Task(task),
        }
    }

    fn disconnect(&self) {
        self.hold.store(false, Ordering::SeqCst);
        self.reset.send_modify(|generation| *generation += 1);
    }
}

#[tokio::test]
#[ignore = "F11: dedicated AOF Redis stop/kill at an unconfirmed enqueue, cleanup continues"]
async fn f11_redis_restart_preserves_confirmed_and_retries_unknown() {
    assert_eq!(std::env::var("FAULT_DEDICATED_REDIS").as_deref(), Ok("1"));
    let env = env();
    let owner: String = sql(&format!("{}/postgres", env.admin_base))
        .await
        .query_one("SELECT current_user::text", &[])
        .await
        .unwrap()
        .get(0);
    for mode in ["stop", "kill"] {
        let db = format!("{}_f11_{mode}", env.run_id);
        create_database(&env.admin_base, &db, &owner).await;
        let pg = format!("{}/{db}", env.admin_base);
        let dir = env.artifacts.join("f11").join(mode);
        std::fs::create_dir_all(&dir).unwrap();
        let relay = ReplyGate::start(&env.redis[0]).await;
        let backend = Arc::new(
            StorageBackend::connect_with_redis_urls(
                &pg,
                &relay.url,
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
        let admin = sql(&pg).await;
        let queue = RedisSnapshotBacklog::connect(&env.redis[0]).await.unwrap();
        assert_eq!(queue.stats().await.unwrap().pending, 0);
        let ns = format!("f11-{}-{mode}", env.run_id);
        let requests: Vec<_> = (0..21u8)
            .map(|n| {
                let mut request = write(&ns, &format!("record-{n}"), n);
                request.expected_revision = None;
                request
            })
            .collect();
        for outcome in backend
            .enqueue_multi_snapshot(requests[..20].to_vec())
            .await
            .unwrap()
        {
            outcome.unwrap();
        }
        assert_eq!(queue.stats().await.unwrap().pending, 20);
        assert_eq!(
            count(&admin, "SELECT count(*) FROM dbproxy_snapshots").await,
            0
        );
        insert_expired(&admin, "f11-expired", 20000).await;
        let (stop, shutdown) = tokio::sync::watch::channel(false);
        let mut cleanup = Task(tokio::spawn(run_receipt_cleanup_worker(
            backend.clone(),
            "f11".into(),
            tiangz_dbproxy_storage::DEFAULT_RECEIPT_RETENTION,
            Arc::new(DbProxyMetrics::default()),
            shutdown,
        )));
        wait_until(Duration::from_secs(5), "cleanup started", || async {
            count(
                &admin,
                "SELECT count(*) FROM dbproxy_idempotency WHERE namespace='f11-expired'",
            )
            .await
                < 20000
        })
        .await;
        relay.hold.store(true, Ordering::SeqCst);
        let pending_backend = backend.clone();
        let pending_request = requests[20].clone();
        let mut pending =
            tokio::spawn(async move { pending_backend.enqueue_snapshot(pending_request).await });
        wait_until(
            Duration::from_secs(5),
            "Redis applied enqueue but reply is held",
            || async {
                relay.held.load(Ordering::SeqCst) && queue.stats().await.unwrap().pending == 21
            },
        )
        .await;
        assert!(
            !pending.is_finished(),
            "unconfirmed enqueue must not have returned success"
        );
        std::fs::write(
            dir.join("fault-go"),
            "21 queued; 20 confirmed; reply 21 withheld",
        )
        .unwrap();
        wait_until(
            Duration::from_secs(25),
            "dedicated Redis stopped",
            || async { dir.join("fault-offline").exists() },
        )
        .await;
        relay.disconnect();
        let unknown_error = tokio::time::timeout(Duration::from_secs(15), &mut pending)
            .await
            .expect("enqueue did not report lost connection")
            .unwrap()
            .expect_err("unconfirmed enqueue returned success");
        let before = count(
            &admin,
            "SELECT count(*) FROM dbproxy_idempotency WHERE namespace='f11-expired'",
        )
        .await;
        tokio::time::sleep(Duration::from_millis(2200)).await;
        let after = count(
            &admin,
            "SELECT count(*) FROM dbproxy_idempotency WHERE namespace='f11-expired'",
        )
        .await;
        assert!(
            after < before && before > 0,
            "cleanup did not progress during Redis outage: {before}->{after}"
        );
        std::fs::write(
            dir.join("offline-checked"),
            format!("{unknown_error}; cleanup {before}->{after}"),
        )
        .unwrap();
        wait_until(
            Duration::from_secs(45),
            "Redis restarted with original volume",
            || async { dir.join("fault-release").exists() },
        )
        .await;
        let recovered_queue = RedisSnapshotBacklog::connect(&env.redis[0]).await.unwrap();
        let recovered = recovered_queue.stats().await.unwrap().pending;
        assert!(
            (20..=21).contains(&recovered),
            "confirmed entries lost: {recovered}"
        );
        // The 21st request is genuinely unknown: replay its ORIGINAL ID before draining.
        // It may or may not have reached AOF, and neither possibility permits a new business ID.
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if backend.enqueue_snapshot(requests[20].clone()).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(recovered_queue.stats().await.unwrap().pending, 21);
        let (drain_stop, drain_shutdown) = tokio::sync::watch::channel(false);
        let mut drain = Task(tokio::spawn(run_backlog_worker(
            backend.clone(),
            30000,
            Duration::from_millis(20),
            Duration::from_secs(1),
            drain_shutdown,
        )));
        wait_until(
            Duration::from_secs(35),
            "production backlog worker recovered and drained",
            || async {
                let stats = recovered_queue.stats().await.unwrap();
                stats.pending == 0
                    && stats.processing == 0
                    && count(&admin, "SELECT count(*) FROM dbproxy_snapshots").await == 21
            },
        )
        .await;
        drain_stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), &mut drain.0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            count(&admin, "SELECT count(*) FROM dbproxy_snapshots").await,
            21
        );
        for request in &requests {
            let row = admin.query_one("SELECT revision,payload FROM dbproxy_snapshots WHERE namespace=$1 AND record_key=$2", &[&request.record.namespace, &request.record.key]).await.unwrap();
            assert_eq!(row.get::<_, i64>(0), 1);
            assert_eq!(row.get::<_, Vec<u8>>(1), request.payload);
            assert_eq!(
                backend.save(request.clone()).await.unwrap(),
                SnapshotWriteOutcome::Duplicate {
                    revision: Revision(1)
                }
            );
        }
        wait_until(
            Duration::from_secs(50),
            "remaining cleanup drained",
            || async {
                count(
                    &admin,
                    "SELECT count(*) FROM dbproxy_idempotency WHERE namespace='f11-expired'",
                )
                .await
                    == 0
            },
        )
        .await;
        stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), &mut cleanup.0)
            .await
            .unwrap()
            .unwrap();
        let result = serde_json::json!({"mode":mode,"confirmed":20,"unconfirmed":1,"recovered_pending":recovered,"replayed_original_id":true,"verified_pg":21,"revision":1,"outage_cleanup_before":before,"outage_cleanup_after":after,"cleanup_deleted":20000,"unknown_error":unknown_error.to_string(),"backend_restarted":false,"scope":"production backend, cleanup and backlog worker components; backlog worker started after original-ID replay"});
        std::fs::write(
            dir.join("result.json"),
            serde_json::to_vec_pretty(&result).unwrap(),
        )
        .unwrap();
        println!("F11_REDIS_RESULT {result}");
    }
}
