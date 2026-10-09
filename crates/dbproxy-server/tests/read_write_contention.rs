//! Demonstrates connection-level head-of-line blocking independently of disk timing.
use std::{sync::Arc, time::Duration};
use tiangz_dbproxy_core::{AsyncSnapshotStore, RecordKey, Revision, SnapshotWrite};
use tiangz_dbproxy_server::{DbProxyBackend, StorageBackend, StorageBackendConfig};

async fn backend(url: &str, shards: usize, reads: usize) -> StorageBackend {
    StorageBackend::connect_with_config(
        url,
        &std::env::var("DBPROXY_REDIS_URL").unwrap(),
        StorageBackendConfig {
            shard_count: shards,
            read_connection_count: reads,
            tiered: Default::default(),
            enqueue: Default::default(),
        },
    )
    .await
    .unwrap()
}
use tiangz_dbproxy_storage::PostgresSnapshotStore;

fn write(namespace: &str) -> SnapshotWrite {
    SnapshotWrite {
        request_id: namespace.into(),
        record: RecordKey::new(namespace, "one").unwrap(),
        schema: "test".into(),
        schema_version: 1,
        payload: vec![7],
        expected_revision: Some(Revision::ZERO),
        updated_at_unix_ms: 1,
    }
}

#[tokio::test]
#[ignore = "fresh isolated PG/Redis; precise write barrier with an unrelated authority read"]
async fn pending_write_blocks_shared_connection_but_not_independent_reader() {
    tokio::time::timeout(Duration::from_secs(30),async {
        assert_eq!(std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(),Ok("1"));
        let url=std::env::var("DBPROXY_TEST_POSTGRES_URL").unwrap();
        let backend=Arc::new(backend(&url, 1, 0).await);
        let existing=write("contention-read");
        backend.save(existing.clone()).await.unwrap();
        let reader=PostgresSnapshotStore::connect_existing(&url).await.unwrap();
        let (sql,connection)=tokio_postgres::connect(&url,tokio_postgres::NoTls).await.unwrap();
        tokio::spawn(async move {let _=connection.await;});
        sql.batch_execute("CREATE FUNCTION contention_barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
            IF NEW.namespace='contention-write' THEN PERFORM pg_advisory_xact_lock(827613); END IF;
            RETURN NEW; END $$;
            CREATE TRIGGER contention_barrier BEFORE INSERT ON dbproxy_idempotency FOR EACH ROW EXECUTE FUNCTION contention_barrier();
            SELECT pg_advisory_lock(827613)").await.unwrap();
        let writer=backend.clone();
        let writing=tokio::spawn(async move {writer.save(write("contention-write")).await.unwrap()});
        loop {
            let waiting:bool=sql.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory')",&[]).await.unwrap().get(0);
            if waiting {break;}
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let shared=backend.clone();let key=existing.record.clone();
        let mut reading=tokio::spawn(async move {shared.load(&key).await.unwrap()});
        // Neither read touches the row held by the blocked writer.
        assert!(tokio::time::timeout(Duration::from_millis(150),&mut reading).await.is_err());
        let snapshot=tokio::time::timeout(Duration::from_secs(2),reader.load(&existing.record)).await.unwrap().unwrap().unwrap();
        assert_eq!(snapshot.revision,Revision(1));assert_eq!(snapshot.payload,existing.payload);
        assert!(!reading.is_finished());assert!(!writing.is_finished());
        sql.batch_execute("SELECT pg_advisory_unlock(827613)").await.unwrap();
        writing.await.unwrap();
        let snapshot=reading.await.unwrap().unwrap();
        assert_eq!(snapshot.revision,Revision(1));assert_eq!(snapshot.payload,existing.payload);
        sql.batch_execute("DROP TRIGGER contention_barrier ON dbproxy_idempotency; DROP FUNCTION contention_barrier()").await.unwrap();
        println!("CONNECTION_CONTENTION shared_reader=blocked independent_reader=correct writer_released=correct");
    }).await.unwrap();
}

// Match the existing SDK/server FNV routing to deliberately choose distinct keys:
// writer on TCP slot 0 / PG shard 0; blocked reader on TCP slot 4 / PG shard 0;
// unaffected reader on TCP slot 1 / PG shard 1. Shared8 therefore already gives
// the writer and both readers separate TCP connections.
fn routed_write(prefix: &str, slot: u64) -> SnapshotWrite {
    use std::hash::{Hash, Hasher};
    struct RouteHash(u64);
    impl Hasher for RouteHash {
        fn finish(&self) -> u64 {
            self.0
        }
        fn write(&mut self, bytes: &[u8]) {
            for byte in bytes {
                self.0 ^= u64::from(*byte);
                self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
            }
        }
    }
    for n in 0..1000 {
        let mut request = write(prefix);
        request.record = RecordKey::new(prefix, format!("key-{n}")).unwrap();
        let mut hash = RouteHash(0xcbf2_9ce4_8422_2325);
        request.record.hash(&mut hash);
        if hash.finish() % 8 == slot {
            return request;
        }
    }
    panic!("failed to find routing fixture")
}

#[tokio::test]
#[ignore = "fresh isolated PG/Redis; real SDK/TCP with a controlled write barrier"]
async fn eight_tcp_connections_do_not_isolate_reads_on_the_same_pg_shard() {
    check_sdk_barrier(0).await;
}

#[tokio::test]
#[ignore = "fresh isolated PG/Redis; production read pool must bypass blocked writes"]
async fn production_read_pool_bypasses_blocked_writes_over_tcp() {
    check_sdk_barrier(2).await;
}

#[tokio::test]
#[ignore = "fresh isolated PG/Redis; terminates only backends of the dedicated test database"]
async fn interrupted_pg_requests_recover_over_tcp_without_duplicate_writes() {
    use tiangz_dbproxy_client::{ClientConfig, DbProxyClientPool};
    use tiangz_dbproxy_core::SnapshotWriteOutcome;
    use tiangz_dbproxy_server::{DbProxyServer, ServerConfig};
    tokio::time::timeout(Duration::from_secs(45), async {
        assert_eq!(std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(), Ok("1"));
        let url = std::env::var("DBPROXY_TEST_POSTGRES_URL").unwrap();
        let backend = Arc::new(backend(&url, 4, 2).await);
        let (sql, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await.unwrap();
        tokio::spawn(async move { let _ = connection.await; });
        // An explicit second guard is required before this test can kill database sessions.
        let database: String = sql.query_one("SELECT current_database()", &[]).await.unwrap().get(0);
        assert_eq!(std::env::var("DBPROXY_FAULT_DATABASE").as_deref(), Ok(database.as_str()));
        assert!(database.starts_with("tcp_fault_"));
        sql.batch_execute("CREATE FUNCTION tcp_fault_barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
            IF NEW.namespace LIKE 'tcp-fault-write-%' THEN PERFORM pg_advisory_xact_lock(827615); END IF;
            RETURN NEW; END $$;
            CREATE TRIGGER tcp_fault_barrier BEFORE INSERT ON dbproxy_idempotency FOR EACH ROW EXECUTE FUNCTION tcp_fault_barrier()")
            .await.unwrap();
        let token = "tcp-fault-private-token";
        let server = DbProxyServer::bind(ServerConfig::new("127.0.0.1:0".parse().unwrap(), token), backend).await.unwrap();
        let endpoint = server.local_addr().unwrap().to_string();
        let (stop, rx) = tokio::sync::watch::channel(false);
        let serving = tokio::spawn(server.serve(rx));
        let pool = DbProxyClientPool::connect_split(ClientConfig::new(endpoint, token, "tcp-fault-client"), 4, 4).await.unwrap();
        for round in 0..3 {
            let seed = write(&format!("tcp-fault-seed-{round}"));
            assert_eq!(pool.save(seed.clone()).await.unwrap(), SnapshotWriteOutcome::Applied { revision: Revision(1) });
            let request = write(&format!("tcp-fault-write-{round}"));
            sql.batch_execute("SELECT pg_advisory_lock(827615)").await.unwrap();
            let client = pool.clone();
            let pending = request.clone();
            let writing = tokio::spawn(async move { client.save(pending).await });
            let writer_pid = loop {
                let rows = sql.query("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event='advisory'", &[]).await.unwrap();
                if let Some(row) = rows.first() { assert_eq!(rows.len(), 1); break row.get::<_, i32>(0); }
                tokio::time::sleep(Duration::from_millis(5)).await;
            };
            for _ in 0..20 {
                let snapshot = tokio::time::timeout(Duration::from_secs(1), pool.load(&seed.record)).await.unwrap().unwrap().unwrap();
                assert_eq!(snapshot.payload, seed.payload);
                assert_eq!(snapshot.revision, Revision(1));
            }
            assert!(!writing.is_finished());
            assert!(sql.query_one("SELECT pg_terminate_backend($1)", &[&writer_pid]).await.unwrap().get::<_, bool>(0));
            assert!(writing.await.unwrap().is_err(), "interrupted transaction must not report success");
            sql.batch_execute("SELECT pg_advisory_unlock(827615)").await.unwrap();
            assert!(pool.load(&request.record).await.unwrap().is_none());
            let count: i64 = sql.query_one("SELECT count(*) FROM dbproxy_idempotency WHERE request_id=$1", &[&request.request_id]).await.unwrap().get(0);
            assert_eq!(count, 0, "aborted transaction must not leave a receipt");
            assert_eq!(pool.save(request.clone()).await.unwrap(), SnapshotWriteOutcome::Applied { revision: Revision(1) });
            assert_eq!(pool.save(request.clone()).await.unwrap(), SnapshotWriteOutcome::Duplicate { revision: Revision(1) });

            // Block an actual SELECT, prove it reached PG, then disconnect its read backend.
            sql.batch_execute("BEGIN; LOCK TABLE dbproxy_snapshots IN ACCESS EXCLUSIVE MODE").await.unwrap();
            let client = pool.clone();
            let key = seed.record.clone();
            let reading = tokio::spawn(async move { client.load(&key).await });
            let reader_pid = loop {
                let rows = sql.query("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE '%dbproxy_snapshots%'", &[]).await.unwrap();
                if let Some(row) = rows.first() { assert_eq!(rows.len(), 1); break row.get::<_, i32>(0); }
                tokio::time::sleep(Duration::from_millis(5)).await;
            };
            assert!(sql.query_one("SELECT pg_terminate_backend($1)", &[&reader_pid]).await.unwrap().get::<_, bool>(0));
            assert!(reading.await.unwrap().is_err(), "interrupted authority read must not return cached success");
            sql.batch_execute("ROLLBACK").await.unwrap();
            for _ in 0..4 {
                assert_eq!(pool.load(&seed.record).await.unwrap().unwrap().payload, seed.payload);
            }
            // Kill every other client backend in this owned database; keep the admin session.
            let killed = sql.query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND backend_type='client backend'", &[]).await.unwrap();
            if round == 0 {
                assert_eq!(killed.len(), 8, "four writes, two reads, two maintenance connections");
            } else {
                // Unused shards and maintenance reconnect lazily after the previous fault.
                assert!((3..=8).contains(&killed.len()));
            }
            assert!(killed.iter().all(|r| r.get::<_, bool>(0)));
            // Allow socket closure notification; no explicit reconnect or new SDK pool is used.
            tokio::time::sleep(Duration::from_millis(100)).await;
            for _ in 0..4 {
                let snapshot = pool.load(&request.record).await.unwrap().unwrap();
                assert_eq!(snapshot.revision, Revision(1));
                assert_eq!(snapshot.payload, request.payload);
            }
            assert_eq!(pool.save(request.clone()).await.unwrap(), SnapshotWriteOutcome::Duplicate { revision: Revision(1) });
            let count: i64 = sql.query_one("SELECT count(*) FROM dbproxy_idempotency WHERE request_id=$1", &[&request.request_id]).await.unwrap().get(0);
            assert_eq!(count, 1);
            println!("TCP_FAULT round={round} unrelated_reads=20 interrupted_write=error rollback=clean retry_revision=1 replay=duplicate interrupted_read=error all_connections_killed={} recovery=correct", killed.len());
        }
        sql.batch_execute("DROP TRIGGER tcp_fault_barrier ON dbproxy_idempotency; DROP FUNCTION tcp_fault_barrier()").await.unwrap();
        stop.send(true).unwrap();
        serving.await.unwrap().unwrap();
    }).await.unwrap();
}

async fn check_sdk_barrier(reads: usize) {
    use tiangz_dbproxy_client::{ClientConfig, DbProxyClientPool};
    use tiangz_dbproxy_core::SnapshotWriteOutcome;
    use tiangz_dbproxy_server::{DbProxyServer, ServerConfig};
    tokio::time::timeout(Duration::from_secs(30), async {
        assert_eq!(std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(), Ok("1"));
        let url = std::env::var("DBPROXY_TEST_POSTGRES_URL").unwrap();
        let backend = Arc::new(backend(&url, 4, reads).await);
        let mut direct = PostgresSnapshotStore::connect_existing(&url).await.unwrap();
        let (sql, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await.unwrap();
        tokio::spawn(async move { let _ = connection.await; });
        sql.batch_execute("CREATE FUNCTION sdk_contention_barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
            IF NEW.namespace LIKE 'sdk-blocked-%' THEN PERFORM pg_advisory_xact_lock(827614); END IF;
            RETURN NEW; END $$;
            CREATE TRIGGER sdk_contention_barrier BEFORE INSERT ON dbproxy_idempotency FOR EACH ROW EXECUTE FUNCTION sdk_contention_barrier()")
            .await.unwrap();
        let token = "sdk-controlled-contention-token";
        let server = DbProxyServer::bind(ServerConfig::new("127.0.0.1:0".parse().unwrap(), token), backend).await.unwrap();
        let endpoint = server.local_addr().unwrap().to_string();
        let (stop, rx) = tokio::sync::watch::channel(false);
        let serving = tokio::spawn(server.serve(rx));
        let suffix = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        for (round, split) in [false, true, true, false, false, true].into_iter().enumerate() {
            let config = ClientConfig::new(&endpoint, token, "controlled-sdk-probe");
            let pool = if split { DbProxyClientPool::connect_split(config, 4, 4).await.unwrap() }
                else { DbProxyClientPool::connect(config, 8).await.unwrap() };
            assert_eq!(pool.is_split(), split);
            assert_eq!(pool.read_len(), if split { 4 } else { 8 });
            assert_eq!(pool.write_len(), if split { 4 } else { 8 });
            let same = routed_write(&format!("sdk-same-{reads}-{round}"), 4);
            let other = routed_write(&format!("sdk-other-{reads}-{round}"), 1);
            let blocked = routed_write(&format!("sdk-blocked-{reads}-{round}"), 0);
            pool.save(same.clone()).await.unwrap();
            pool.save(other.clone()).await.unwrap();
            let cold = routed_write(&format!("sdk-cold-{suffix}-{round}"), 4);
            let cold_batch = routed_write(&format!("sdk-cold-batch-{suffix}-{round}"), 4);
            if reads > 0 {
                direct.save(cold.clone()).await.unwrap();
                direct.save(cold_batch.clone()).await.unwrap();
            }
            sql.batch_execute("SELECT pg_advisory_lock(827614)").await.unwrap();
            let writer = pool.clone();
            let writing = tokio::spawn(async move { writer.save(blocked).await.unwrap() });
            loop {
                let waiting: bool = sql.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory')", &[]).await.unwrap().get(0);
                if waiting { break; }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let client = pool.clone();
            let key = same.record.clone();
            let mut reading = tokio::spawn(async move { client.load(&key).await.unwrap() });
            let early = if reads == 0 {
                assert!(tokio::time::timeout(Duration::from_millis(150), &mut reading).await.is_err());
                None
            } else {
                Some(tokio::time::timeout(Duration::from_secs(1), &mut reading).await.unwrap().unwrap().unwrap())
            };
            if reads > 0 {
                tokio::time::timeout(Duration::from_secs(2), async {
                    assert_eq!(pool.load_multi(std::slice::from_ref(&same.record)).await.unwrap()[0].as_ref().unwrap().payload, same.payload);
                    assert_eq!(pool.load_cached(&cold.record, None).await.unwrap().unwrap().payload, cold.payload);
                    assert_eq!(pool.load_cached_multi(std::slice::from_ref(&cold_batch.record), &[Revision::ZERO]).await.unwrap()[0].as_ref().unwrap().payload, cold_batch.payload);
                    assert!(pool.load_transaction("missing-operation", &same.record).await.unwrap().is_none());
                    assert!(pool.load_multi_transaction("missing-multi", std::slice::from_ref(&same.record)).await.unwrap().is_none());
                    assert!(pool.load_trade("missing-trade").await.unwrap().is_none());
                    assert!(pool.load_trade_transaction("missing-trade-operation", "missing-trade").await.unwrap().is_none());
                }).await.unwrap();
            }
            let unaffected = tokio::time::timeout(Duration::from_secs(1), pool.load(&other.record)).await.unwrap().unwrap().unwrap();
            assert_eq!(unaffected.revision, Revision(1));
            assert_eq!(unaffected.payload, other.payload);
            let independent = tokio::time::timeout(Duration::from_secs(1), direct.load(&same.record)).await.unwrap().unwrap().unwrap();
            assert_eq!(independent.revision, Revision(1));
            assert_eq!(independent.payload, same.payload);
            assert_eq!(reading.is_finished(), reads > 0);
            assert!(!writing.is_finished());
            sql.batch_execute("SELECT pg_advisory_unlock(827614)").await.unwrap();
            assert_eq!(writing.await.unwrap(), SnapshotWriteOutcome::Applied { revision: Revision(1) });
            let snapshot = match early { Some(snapshot) => snapshot, None => reading.await.unwrap().unwrap() };
            assert_eq!(snapshot.revision, Revision(1));
            assert_eq!(snapshot.payload, same.payload);
            println!("SDK_BARRIER read_pool={reads} round={round} split={split} tcp_connections=8 same_pg_checked=true other_pg=correct direct_pg=correct after_release=correct");
        }
        sql.batch_execute("DROP TRIGGER sdk_contention_barrier ON dbproxy_idempotency; DROP FUNCTION sdk_contention_barrier()").await.unwrap();
        stop.send(true).unwrap();
        serving.await.unwrap().unwrap();
    }).await.unwrap();
}

/// Test-only TCP forwarder between the SDK and the server. Every pending drop swallows one
/// server-to-client read (normally one response frame) and then closes that client socket, so
/// the client sees a lost response after PostgreSQL has already committed.
struct LostResponseForwarder {
    endpoint: String,
    pending_drops: Arc<std::sync::atomic::AtomicU64>,
    swallowed_frames: Arc<std::sync::atomic::AtomicU64>,
}

async fn start_lost_response_forwarder(target: std::net::SocketAddr) -> LostResponseForwarder {
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let pending_drops = Arc::new(AtomicU64::new(0));
    let swallowed_frames = Arc::new(AtomicU64::new(0));
    let (drops, swallowed) = (pending_drops.clone(), swallowed_frames.clone());
    tokio::spawn(async move {
        loop {
            let Ok((client, _)) = listener.accept().await else {
                return;
            };
            let Ok(server) = tokio::net::TcpStream::connect(target).await else {
                return;
            };
            let (mut client_read, mut client_write) = client.into_split();
            let (mut server_read, mut server_write) = server.into_split();
            tokio::spawn(async move {
                let _ = tokio::io::copy(&mut client_read, &mut server_write).await;
            });
            let (drops, swallowed) = (drops.clone(), swallowed.clone());
            tokio::spawn(async move {
                let mut buffer = vec![0_u8; 64 * 1024];
                loop {
                    let Ok(read) = server_read.read(&mut buffer).await else {
                        return;
                    };
                    if read == 0 {
                        return;
                    }
                    let dropping = drops
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                        .is_ok();
                    if dropping {
                        // Count whole frames in the swallowed bytes: 4-byte big-endian length prefix.
                        let (mut offset, mut frames) = (0_usize, 0_u64);
                        while offset + 4 <= read {
                            let length =
                                u32::from_be_bytes(buffer[offset..offset + 4].try_into().unwrap())
                                    as usize;
                            if offset + 4 + length > read {
                                break;
                            }
                            offset += 4 + length;
                            frames += 1;
                        }
                        swallowed.fetch_add(frames, Ordering::SeqCst);
                        let _ = client_write.shutdown().await;
                        return;
                    }
                    if client_write.write_all(&buffer[..read]).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    LostResponseForwarder {
        endpoint,
        pending_drops,
        swallowed_frames,
    }
}

#[tokio::test]
#[ignore = "fresh isolated PG/Redis; PostgreSQL commits, then the response to the SDK is lost"]
async fn committed_write_with_lost_response_replays_as_duplicate_over_tcp() {
    use std::sync::atomic::Ordering;
    use tiangz_dbproxy_client::{ClientConfig, DbProxyClientPool};
    use tiangz_dbproxy_core::SnapshotWriteOutcome;
    use tiangz_dbproxy_server::{DbProxyServer, ServerConfig};
    tokio::time::timeout(Duration::from_secs(60), async {
        assert_eq!(std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(), Ok("1"));
        let url = std::env::var("DBPROXY_TEST_POSTGRES_URL").unwrap();
        let backend = Arc::new(backend(&url, 4, 2).await);
        let (sql, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await.unwrap();
        tokio::spawn(async move { let _ = connection.await; });
        sql.batch_execute("CREATE FUNCTION lost_response_barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
            IF NEW.namespace LIKE 'lost-response-write-%' THEN PERFORM pg_advisory_xact_lock(827617); END IF;
            RETURN NEW; END $$;
            CREATE TRIGGER lost_response_barrier BEFORE INSERT ON dbproxy_idempotency FOR EACH ROW EXECUTE FUNCTION lost_response_barrier()")
            .await.unwrap();
        let token = "lost-response-private-token";
        let server = DbProxyServer::bind(ServerConfig::new("127.0.0.1:0".parse().unwrap(), token), backend).await.unwrap();
        let target = server.local_addr().unwrap();
        let (stop, rx) = tokio::sync::watch::channel(false);
        let serving = tokio::spawn(server.serve(rx));
        let forwarder = start_lost_response_forwarder(target).await;
        // The SDK only ever talks to the forwarder; the server sees ordinary TCP clients.
        let pool = DbProxyClientPool::connect_split(ClientConfig::new(forwarder.endpoint.clone(), token, "lost-response-client"), 4, 4).await.unwrap();
        // Rounds 0/2: drop one response, the SDK's own same-request-id retry must observe Duplicate.
        // Rounds 1/3: drop the retry too, the caller gets an unknown result and retries explicitly.
        for round in 0..4_u64 {
            let drops = 1 + round % 2;
            let request = write(&format!("lost-response-write-{round}"));
            sql.batch_execute("SELECT pg_advisory_lock(827617)").await.unwrap();
            let client = pool.clone();
            let pending = request.clone();
            let writing = tokio::spawn(async move { client.save(pending).await });
            loop {
                let waiting: bool = sql.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event='advisory')", &[]).await.unwrap().get(0);
                if waiting { break; }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            // Nothing else is in flight, so the next server-to-client frame is this write's response.
            let swallowed_before = forwarder.swallowed_frames.load(Ordering::SeqCst);
            forwarder.pending_drops.store(drops, Ordering::SeqCst);
            sql.batch_execute("SELECT pg_advisory_unlock(827617)").await.unwrap();
            let outcome = writing.await.unwrap();
            // PostgreSQL committed regardless of what the client saw.
            let receipts: i64 = sql.query_one("SELECT count(*) FROM dbproxy_idempotency WHERE request_id=$1", &[&request.request_id]).await.unwrap().get(0);
            assert_eq!(receipts, 1, "the write must be committed exactly once");
            let committed = pool.load(&request.record).await.unwrap().unwrap();
            assert_eq!(committed.revision, Revision(1));
            assert_eq!(committed.payload, request.payload);
            let swallowed = forwarder.swallowed_frames.load(Ordering::SeqCst) - swallowed_before;
            if drops == 1 {
                assert_eq!(swallowed, 1, "exactly the first response frame was lost");
                assert_eq!(outcome.unwrap(), SnapshotWriteOutcome::Duplicate { revision: Revision(1) }, "the SDK retry with the same request id must see the committed receipt");
            } else {
                assert_eq!(swallowed, 2, "the response and the SDK retry response were both lost");
                let error = outcome.expect_err("two lost responses leave the caller with an unknown result");
                assert!(!error.to_string().contains("rejected"), "an unknown result must not look like a definite rejection: {error}");
            }
            assert_eq!(forwarder.pending_drops.load(Ordering::SeqCst), 0);
            // Explicit caller retries with the original request id return the original revision.
            assert_eq!(pool.save(request.clone()).await.unwrap(), SnapshotWriteOutcome::Duplicate { revision: Revision(1) });
            assert_eq!(pool.save(request.clone()).await.unwrap(), SnapshotWriteOutcome::Duplicate { revision: Revision(1) });
            let receipts: i64 = sql.query_one("SELECT count(*) FROM dbproxy_idempotency WHERE request_id=$1", &[&request.request_id]).await.unwrap().get(0);
            assert_eq!(receipts, 1);
            let revision: i64 = sql.query_one("SELECT revision FROM dbproxy_snapshots WHERE namespace=$1 AND record_key=$2", &[&request.record.namespace, &request.record.key]).await.unwrap().get(0);
            assert_eq!(revision, 1, "retries after a lost response must not advance the revision");
            // Different content under the same request id is still a conflict, not a new write.
            let mut altered = request.clone();
            altered.payload = vec![8];
            assert!(pool.save(altered).await.is_err());
            println!("LOST_RESPONSE round={round} dropped_frames={swallowed} committed_receipts=1 revision=1 client_outcome={} explicit_retry=duplicate", if drops == 1 { "sdk_retry_duplicate" } else { "unknown_result" });
        }
        sql.batch_execute("DROP TRIGGER lost_response_barrier ON dbproxy_idempotency; DROP FUNCTION lost_response_barrier()").await.unwrap();
        stop.send(true).unwrap();
        serving.await.unwrap().unwrap();
    }).await.unwrap();
}
