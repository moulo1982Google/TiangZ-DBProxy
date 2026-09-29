use super::*;

#[tokio::test]
#[ignore = "F13: SIGKILL after real Redis publication before PG ack, lease fencing and consumer dedup"]
async fn f13_published_message_survives_process_crash_before_ack() {
    #[cfg(unix)]
    use std::os::unix::process::ExitStatusExt;
    use tiangz_dbproxy_core::{
        CommitEffects, MultiRecordTransactionalWrite, OutboxEvent, TransactionalRecordWrite,
    };
    assert_eq!(
        std::env::consts::FAMILY,
        "unix",
        "this real SIGKILL acceptance must run on Unix"
    );
    let env = env();
    let db = format!("{}_f13", env.run_id);
    let dir = env.artifacts.join("f13");
    std::fs::create_dir_all(&dir).unwrap();
    let owner: String = sql(&format!("{}/postgres", env.admin_base))
        .await
        .query_one("SELECT current_user::text", &[])
        .await
        .unwrap()
        .get(0);
    create_database(&env.admin_base, &db, &owner).await;
    let url = format!("{}/{db}", env.admin_base);
    let mut admin = sql(&url).await;
    let endpoint = free_port();
    let obs = free_port();
    tenant_config(&dir, "A", &endpoint, &obs);
    let deployment = deployment(&dir, &endpoint, &["A"]);
    let mut seed = spawn(&deployment, &dir, "seed", &env, &url, &url);
    let seed_client = client(&endpoint, TOKEN_A, &mut seed).await;
    drop(seed_client);
    drop(seed);
    let mut store = tiangz_dbproxy_storage::PostgresSnapshotStore::connect_existing(&url)
        .await
        .unwrap();
    let queue = store.outbox_queue();
    let topic = format!("f13-{}", env.run_id);
    let stream = format!(
        "{}{topic}",
        tiangz_dbproxy_storage::DEFAULT_OUTBOX_STREAM_PREFIX
    );
    let ids = [
        format!("{}-head", env.run_id),
        format!("{}-tail", env.run_id),
    ];
    let mut requests = Vec::new();
    for (n, id) in ids.iter().enumerate() {
        let request = MultiRecordTransactionalWrite {
            operation_id: id.clone(),
            writes: vec![TransactionalRecordWrite {
                record: RecordKey::new("f13-business", id).unwrap(),
                schema: "test".into(),
                schema_version: 1,
                expected_revision: Revision::ZERO,
                payload: vec![n as u8],
                updated_at_unix_ms: 1,
            }],
            result: vec![n as u8],
        };
        let effects = CommitEffects {
            appends: vec![],
            outbox_events: vec![OutboxEvent {
                event_id: id.clone(),
                topic: topic.clone(),
                partition_key: "ordered".into(),
                payload: vec![n as u8],
                occurred_at_unix_ms: 1,
            }],
        };
        store
            .commit_records(request.clone(), effects.clone())
            .await
            .unwrap();
        requests.push((request, effects));
    }
    struct Relay(tokio::task::JoinHandle<()>);
    impl Drop for Relay {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (prefix, rest) = url.rsplit_once('@').unwrap();
    let upstream = rest.split('/').next().unwrap().to_owned();
    let relayed_url = format!("{prefix}@{address}/{db}?sslmode=disable");
    let held = Arc::new(AtomicBool::new(false));
    let signal = held.clone();
    let relay = Relay(tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (downstream, _) = accepted.unwrap();
                    let upstream = upstream.clone();
                    let signal = signal.clone();
                    connections.spawn(async move {
                        downstream.set_nodelay(true).unwrap();
                        let upstream = tokio::net::TcpStream::connect(upstream).await.unwrap();
                        upstream.set_nodelay(true).unwrap();
                        let (mut down_read, mut down_write) = downstream.into_split();
                        let (mut up_read, mut up_write) = upstream.into_split();
                        let requests = async {
                            // Startup has no tag; TLS is explicitly disabled only for this test.
                            let startup_length = down_read.read_u32().await?;
                            if !(8..=1024*1024).contains(&startup_length) { return Err::<(), _>(std::io::Error::other("invalid startup frame")); }
                            let mut startup = vec![0; (startup_length-4) as usize];
                            down_read.read_exact(&mut startup).await?;
                            up_write.write_u32(startup_length).await?;
                            up_write.write_all(&startup).await?;
                            loop {
                                let tag = down_read.read_u8().await?;
                                let length = down_read.read_u32().await?;
                                if !(4..=16*1024*1024).contains(&length) { return Err(std::io::Error::other("invalid PG frame")); }
                                let mut body = vec![0; (length-4) as usize];
                                down_read.read_exact(&mut body).await?;
                                let query = match tag {
                                    b'P' => body.split(|b| *b == 0).nth(1),
                                    b'Q' => body.split(|b| *b == 0).next(),
                                    _ => None,
                                };
                                if query.and_then(|q| std::str::from_utf8(q).ok()).is_some_and(|q| q.contains("UPDATE dbproxy_outbox") && q.contains("SET published_at = clock_timestamp()")) {
                                    signal.store(true, Ordering::SeqCst);
                                    std::future::pending::<()>().await;
                                }
                                up_write.write_u8(tag).await?;
                                up_write.write_u32(length).await?;
                                up_write.write_all(&body).await?;
                            }
                        };
                        tokio::select! { _ = requests => {}, _ = tokio::io::copy(&mut up_read, &mut down_write) => {} }
                    });
                },
                _ = connections.join_next(), if !connections.is_empty() => {},
            }
        }
    }));
    let mut redis = redis::Client::open(env.redis[0].as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    assert_eq!(
        redis::cmd("XLEN")
            .arg(&stream)
            .query_async::<i64>(&mut redis)
            .await
            .unwrap(),
        0
    );
    let mut server = spawn(&deployment, &dir, "kill", &env, &relayed_url, &relayed_url);
    wait_until(
        Duration::from_secs(15),
        "published-message PG acknowledgement barrier",
        || async { held.load(Ordering::SeqCst) },
    )
    .await;
    let first_messages: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
        .arg(&stream)
        .arg("-")
        .arg("+")
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(first_messages.ids.len(), 1);
    assert_eq!(
        first_messages.ids[0].get::<String>("event_id").unwrap(),
        ids[0]
    );
    assert_eq!(
        count(
            &admin,
            "SELECT count(*) FROM dbproxy_outbox WHERE published_at IS NOT NULL"
        )
        .await,
        0
    );
    let lease_row = admin
        .query_one(
            "SELECT lease_owner,lease_token FROM dbproxy_outbox WHERE event_id=$1",
            &[&ids[0]],
        )
        .await
        .unwrap();
    let original_owner: String = lease_row.get(0);
    let original_token: i64 = lease_row.get(1);
    server.0.kill().unwrap();
    let killed = server.0.wait().unwrap();
    assert!(!killed.success());
    #[cfg(unix)]
    assert_eq!(killed.signal(), Some(9));
    drop(server);
    drop(relay);
    assert!(
        queue.claim("other-worker", 30000).await.unwrap().is_none(),
        "valid head lease or ordered follower was claimed"
    );
    admin.execute("UPDATE dbproxy_outbox SET lease_until=clock_timestamp()-interval '1 second' WHERE event_id=$1", &[&ids[0]]).await.unwrap();
    let renewed = queue.claim(&original_owner, 30000).await.unwrap().unwrap();
    assert_eq!(renewed.event.event_id, ids[0]);
    assert!(renewed.lease_token > original_token);
    // The same worker identity reacquires. Changing only the public fencing token recreates
    // the old acknowledgement credentials and exercises the actual queue API.
    let mut stale = renewed.clone();
    stale.lease_token = original_token;
    assert!(!queue.acknowledge(&stale).await.unwrap());
    assert!(
        queue
            .claim("follower-worker", 30000)
            .await
            .unwrap()
            .is_none()
    );
    admin.execute("UPDATE dbproxy_outbox SET lease_until=clock_timestamp()-interval '1 second' WHERE event_id=$1", &[&ids[0]]).await.unwrap();
    let mut restarted = spawn(&deployment, &dir, "restart", &env, &url, &url);
    let client_a = client(&endpoint, TOKEN_A, &mut restarted).await;
    wait_until(
        Duration::from_secs(15),
        "republication and ordered follower acknowledgement",
        || async {
            count(
                &admin,
                "SELECT count(*) FROM dbproxy_outbox WHERE published_at IS NOT NULL",
            )
            .await
                == 2
        },
    )
    .await;
    assert!(!queue.acknowledge(&renewed).await.unwrap());
    let messages: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
        .arg(&stream)
        .arg("-")
        .arg("+")
        .query_async(&mut redis)
        .await
        .unwrap();
    let published_ids: Vec<String> = messages
        .ids
        .iter()
        .map(|m| m.get("event_id").unwrap())
        .collect();
    assert_eq!(
        published_ids,
        vec![ids[0].clone(), ids[0].clone(), ids[1].clone()]
    );
    for (n, message) in messages.ids.iter().enumerate() {
        assert_eq!(
            message.get::<Vec<u8>>("payload").unwrap(),
            vec![if n < 2 { 0 } else { 1 }]
        );
    }
    // Consume through a real Redis group. Inbox and side-effect counter commit atomically
    // in the test PG before XACK, so duplicate delivery cannot repeat the projection update.
    admin.batch_execute("CREATE TABLE f13_inbox(event_id TEXT PRIMARY KEY); CREATE TABLE f13_projection(id INTEGER PRIMARY KEY, updates INTEGER NOT NULL); INSERT INTO f13_projection VALUES(1,0)").await.unwrap();
    let group = format!("consumer-{}", env.run_id);
    redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&stream)
        .arg(&group)
        .arg("0")
        .query_async::<()>(&mut redis)
        .await
        .unwrap();
    let delivered: redis::streams::StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg(&group)
        .arg("worker")
        .arg("COUNT")
        .arg(10)
        .arg("STREAMS")
        .arg(&stream)
        .arg(">")
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(delivered.keys.len(), 1);
    assert_eq!(delivered.keys[0].ids.len(), 3);
    for message in &delivered.keys[0].ids {
        let id: String = message.get("event_id").unwrap();
        let tx = admin.transaction().await.unwrap();
        let inserted = tx
            .execute(
                "INSERT INTO f13_inbox VALUES($1) ON CONFLICT DO NOTHING",
                &[&id],
            )
            .await
            .unwrap();
        if inserted == 1 {
            tx.execute(
                "UPDATE f13_projection SET updates=updates+1 WHERE id=1",
                &[],
            )
            .await
            .unwrap();
        }
        tx.commit().await.unwrap();
        assert_eq!(
            redis::cmd("XACK")
                .arg(&stream)
                .arg(&group)
                .arg(&message.id)
                .query_async::<i64>(&mut redis)
                .await
                .unwrap(),
            1
        );
    }
    assert_eq!(count(&admin, "SELECT count(*) FROM f13_inbox").await, 2);
    assert_eq!(
        admin
            .query_one("SELECT updates FROM f13_projection WHERE id=1", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        2
    );
    for (n, (request, effects)) in requests.into_iter().enumerate() {
        assert!(matches!(
            client_a
                .commit_records(request.clone(), effects)
                .await
                .unwrap(),
            tiangz_dbproxy_core::MultiRecordTransactionalWriteOutcome::Duplicate { .. }
        ));
        let row = admin.query_one("SELECT payload,revision FROM dbproxy_snapshots WHERE namespace='f13-business' AND record_key=$1", &[&ids[n]]).await.unwrap();
        assert_eq!(row.get::<_, Vec<u8>>(0), vec![n as u8]);
        assert_eq!(row.get::<_, i64>(1), 1);
    }
    let result = serde_json::json!({"run":env.run_id,"stream":stream,"published_ids":published_ids,"messages":3,"consumer_effects":2,"business_records":2,"valid_lease_not_reclaimed":true,"stale_ack_rejected":true,"original_token":original_token,"renewed_token":renewed.lease_token,"lease_expiry":"forced only in isolated test rows","signal":9});
    std::fs::write(
        dir.join("result.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    println!("F13_RESULT {result}");
}
