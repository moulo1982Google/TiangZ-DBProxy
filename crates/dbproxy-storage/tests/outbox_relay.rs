//! 独立 PostgreSQL + Redis >=7.2/AOF 验收；不使用正在演练的数据库。
//! Acceptance against isolated PostgreSQL and Redis >=7.2 with AOF enabled.
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tiangz_dbproxy_core::{
    AsyncSnapshotStore, CommitEffects, EventEnvelope, MultiRecordTransactionalWrite,
    MultiRecordTransactionalWriteOutcome, RecordKey, Revision, TransactionalRecordWrite,
};
use tiangz_dbproxy_storage::{
    OutboxRoute, PostgresSnapshotStore, PublishMessage, Publisher, RedisStreamPublisher,
    redis_endpoint_fingerprint,
};

#[tokio::test]
#[ignore = "requires isolated PostgreSQL + Redis >=7.2 with AOF; never run against the seven-day soak"]
async fn relay_routes_leases_audit_and_two_consumer_groups() {
    let postgres = std::env::var("DBPROXY_POSTGRES_URL").unwrap();
    let redis_url = std::env::var("DBPROXY_REDIS_URL").unwrap();
    let mut store = PostgresSnapshotStore::connect(&postgres).await.unwrap();
    let queue = store.outbox_queue();
    let (mut sql, connection) = tokio_postgres::connect(&postgres, tokio_postgres::NoTls)
        .await
        .unwrap();
    let db_task = tokio::spawn(async move { connection.await.unwrap() });
    let suffix = format!(
        "relay_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let destination = format!("test:{suffix}:events");
    queue
        .register_publisher(&suffix, &redis_endpoint_fingerprint(&redis_url).unwrap())
        .await
        .unwrap();
    let route = OutboxRoute {
        producer: suffix.clone(),
        version: 1,
        publisher: suffix.clone(),
        destination: destination.clone(),
    };
    queue.register_route(&route).await.unwrap();
    assert!(
        queue
            .register_route(&OutboxRoute {
                destination: "wrong".into(),
                ..route.clone()
            })
            .await
            .is_err()
    );
    assert!(
        queue
            .register_publisher(&suffix, "wrong-endpoint")
            .await
            .is_err()
    );
    let envelope = EventEnvelope {
        event_id: suffix.clone(),
        producer: suffix.clone(),
        event_type: "DocumentChanged".into(),
        aggregate_type: "document".into(),
        aggregate_id: suffix.clone(),
        partition_key: suffix.clone(),
        schema_version: 1,
        content_type: "application/json".into(),
        payload: br#"{"value":1}"#.to_vec(),
        occurred_at_unix_ms: 1,
        route_version: 1,
    };
    let request = MultiRecordTransactionalWrite {
        operation_id: suffix.clone(),
        writes: vec![TransactionalRecordWrite {
            record: RecordKey::new("relay_document", &suffix).unwrap(),
            schema: "document".into(),
            schema_version: 1,
            expected_revision: Revision::ZERO,
            payload: vec![1],
            updated_at_unix_ms: 1,
        }],
        result: vec![1],
    };
    let effects = CommitEffects {
        appends: vec![],
        outbox_events: vec![envelope.clone().into_outbox().unwrap()],
    };
    let unknown = EventEnvelope {
        route_version: 99,
        ..envelope.clone()
    }
    .into_outbox()
    .unwrap();
    assert!(
        store
            .commit_records(
                request.clone(),
                CommitEffects {
                    appends: vec![],
                    outbox_events: vec![unknown]
                }
            )
            .await
            .is_err()
    );
    assert!(
        store
            .load(&request.writes[0].record)
            .await
            .unwrap()
            .is_none()
    );
    store
        .commit_records(request.clone(), effects.clone())
        .await
        .unwrap();
    queue
        .register_route(&OutboxRoute {
            version: 2,
            destination: format!("{destination}:v2"),
            ..route.clone()
        })
        .await
        .unwrap();
    assert!(matches!(
        store
            .commit_records(request.clone(), effects)
            .await
            .unwrap(),
        MultiRecordTransactionalWriteOutcome::Duplicate { .. }
    ));
    assert!(
        store
            .commit_records(request.clone(), CommitEffects::default())
            .await
            .is_err()
    );
    assert_eq!(
        queue.inspect(&suffix).await.unwrap().unwrap().destination,
        destination
    );
    assert!(
        sql.execute(
            "UPDATE dbproxy_outbox SET destination='wrong' WHERE event_id=$1",
            &[&suffix]
        )
        .await
        .is_err()
    );

    let old = queue
        .claim_for_publisher("same-worker", 1, Some(&suffix))
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let lease = queue
        .claim_for_publisher("same-worker", 30_000, Some(&suffix))
        .await
        .unwrap()
        .unwrap();
    assert!(lease.lease_token > old.lease_token);
    assert!(!queue.acknowledge(&old).await.unwrap());
    assert!(!queue.fail(&old, "stale completion", 1, 1).await.unwrap());
    let publisher = RedisStreamPublisher::connect(&redis_url, "unused-legacy-prefix:")
        .await
        .unwrap();
    Publisher::publish(
        &publisher,
        PublishMessage {
            event: &lease.event,
            destination: &lease.destination,
            operation_id: &lease.operation_id,
            trade_id: &lease.trade_id,
        },
    )
    .await
    .unwrap();
    // Simulate a process crash after MQ confirmation and before the PostgreSQL ACK.
    sql.execute("UPDATE dbproxy_outbox SET lease_until=clock_timestamp()-interval '1 second' WHERE event_id=$1",&[&suffix]).await.unwrap();
    let repeated = queue
        .claim_for_publisher("recovered-worker", 30_000, Some(&suffix))
        .await
        .unwrap()
        .unwrap();
    Publisher::publish(
        &publisher,
        PublishMessage {
            event: &repeated.event,
            destination: &repeated.destination,
            operation_id: &repeated.operation_id,
            trade_id: &repeated.trade_id,
        },
    )
    .await
    .unwrap();
    assert!(queue.acknowledge(&repeated).await.unwrap());
    assert!(
        !queue
            .retry_dead_letter(&suffix, "tester", "published events cannot be requeued")
            .await
            .unwrap()
    );

    // The same non-trade event reaches both groups. Each group commits its own inbox and
    // projection atomically before XACK, so the duplicate MQ deliveries apply only once.
    let mut redis = redis::Client::open(redis_url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    sql.batch_execute("CREATE TEMP TABLE relay_demo_inbox(consumer_group TEXT,event_id TEXT,PRIMARY KEY(consumer_group,event_id)); CREATE TEMP TABLE relay_demo_projection(consumer_group TEXT PRIMARY KEY,updates BIGINT NOT NULL)").await.unwrap();
    for group in ["achievement-demo", "quest-demo"] {
        redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&destination)
            .arg(group)
            .arg("0")
            .query_async::<()>(&mut redis)
            .await
            .unwrap();
        sql.execute("INSERT INTO relay_demo_projection VALUES($1,0)", &[&group])
            .await
            .unwrap();
        let reply: redis::streams::StreamReadReply = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group)
            .arg("worker")
            .arg("COUNT")
            .arg(10)
            .arg("STREAMS")
            .arg(&destination)
            .arg(">")
            .query_async(&mut redis)
            .await
            .unwrap();
        assert_eq!(reply.keys.len(), 1);
        assert_eq!(reply.keys[0].ids.len(), 2);
        for message in &reply.keys[0].ids {
            let bytes: Vec<u8> = message.get("event").unwrap();
            let received: EventEnvelope = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(received, envelope);
            let tx = sql.transaction().await.unwrap();
            let inserted = tx
                .execute(
                    "INSERT INTO relay_demo_inbox VALUES($1,$2) ON CONFLICT DO NOTHING",
                    &[&group, &received.event_id],
                )
                .await
                .unwrap();
            if inserted == 1 {
                tx.execute(
                    "UPDATE relay_demo_projection SET updates=updates+1 WHERE consumer_group=$1",
                    &[&group],
                )
                .await
                .unwrap();
            }
            tx.commit().await.unwrap();
            redis::cmd("XACK")
                .arg(&destination)
                .arg(group)
                .arg(&message.id)
                .query_async::<i64>(&mut redis)
                .await
                .unwrap();
        }
        assert_eq!(
            sql.query_one(
                "SELECT updates FROM relay_demo_projection WHERE consumer_group=$1",
                &[&group]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
            1
        );
    }
    let dead_id = format!("{suffix}_dead");
    let mut second = request.clone();
    second.operation_id = dead_id.clone();
    second.writes[0].expected_revision = Revision(1);
    let dead = EventEnvelope {
        event_id: dead_id.clone(),
        ..envelope.clone()
    }
    .into_outbox()
    .unwrap();
    store
        .commit_records(
            second,
            CommitEffects {
                appends: vec![],
                outbox_events: vec![dead],
            },
        )
        .await
        .unwrap();
    let poison = queue
        .claim_for_publisher("worker", 30_000, Some(&suffix))
        .await
        .unwrap()
        .unwrap();
    assert!(queue.fail(&poison, "injected failure", 1, 1).await.unwrap());
    let mut third = request;
    third.operation_id = format!("{suffix}_following");
    third.writes[0].expected_revision = Revision(2);
    let following = EventEnvelope {
        event_id: third.operation_id.clone(),
        ..envelope
    }
    .into_outbox()
    .unwrap();
    store
        .commit_records(
            third,
            CommitEffects {
                appends: vec![],
                outbox_events: vec![following],
            },
        )
        .await
        .unwrap();
    assert!(
        queue
            .claim_for_publisher("worker", 30_000, Some(&suffix))
            .await
            .unwrap()
            .is_none(),
        "dead letter must block its partition"
    );
    assert!(
        queue
            .retry_dead_letter(&dead_id, "test-operator", "fixed injected failure")
            .await
            .unwrap()
    );
    assert_eq!(
        sql.query_one(
            "SELECT COUNT(*) FROM dbproxy_outbox_admin_audit WHERE event_id=$1",
            &[&dead_id]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    assert!(
        !queue
            .retry_dead_letter(&dead_id, "test-operator", "duplicate admin request")
            .await
            .unwrap()
    );
    let repaired = queue
        .claim_for_publisher("worker", 30_000, Some(&suffix))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(repaired.event.event_id, dead_id);
    assert_eq!(repaired.destination, destination);
    // Leave delivery evidence in this disposable test database; no blanket table/key cleanup.
    assert!(
        queue
            .fail(
                &repaired,
                "test finished; intentionally retained evidence",
                1,
                1
            )
            .await
            .unwrap()
    );
    assert!(
        queue
            .source_stats()
            .await
            .unwrap()
            .iter()
            .any(|s| s.publisher == suffix && s.dead == 1)
    );
    drop(store);
    drop(queue);
    drop(sql);
    db_task.abort();
}
