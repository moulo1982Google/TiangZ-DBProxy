//! Outbox 发布任务的真实链路：可靠 Redis 上成功发布并确认，无 AOF 的 Redis 上失败后重试再进死信，空队列不做事。
//! Real outbox publication: publish-and-acknowledge on the AOF Redis, retry then dead-letter on a
//! Redis without AOF (its WAITAOF can never confirm), and nothing to do on an empty queue.
//! Needs isolated PostgreSQL, a Redis with AOF and a second Redis without AOF; never shared ones.
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tiangz_dbproxy_core::{
    CommitEffects, EventEnvelope, MultiRecordTransactionalWrite, OutboxEvent, RecordKey, Revision,
    TransactionalRecordWrite,
};
use tiangz_dbproxy_server::{
    DbProxyBackend, DurableQueueProcessOutcome, RetryWorkerPolicy, StorageBackend,
    StorageBackendConfig,
    relay_config::{ResolvedOutboxRelay, ResolvedPublisher},
};
use tiangz_dbproxy_storage::OutboxRoute;

fn var(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"))
}

fn commit(
    suffix: &str,
    n: u32,
    events: Vec<OutboxEvent>,
) -> (MultiRecordTransactionalWrite, CommitEffects) {
    (
        MultiRecordTransactionalWrite {
            operation_id: format!("{suffix}-op-{n}"),
            writes: vec![TransactionalRecordWrite {
                record: RecordKey::new("outbox-worker", format!("{suffix}-{n}")).unwrap(),
                schema: "test".into(),
                schema_version: 1,
                expected_revision: Revision::ZERO,
                payload: vec![n as u8],
                updated_at_unix_ms: 1,
            }],
            result: vec![],
        },
        CommitEffects {
            appends: vec![],
            outbox_events: events,
        },
    )
}

fn legacy(suffix: &str, n: u32) -> OutboxEvent {
    OutboxEvent {
        event_id: format!("{suffix}-legacy-{n}"),
        topic: format!("outbox-worker-{suffix}"),
        // Distinct ordering groups, so one batch may carry all of them.
        partition_key: format!("partition-{n}"),
        payload: vec![n as u8],
        occurred_at_unix_ms: 1,
    }
}

async fn stream_len(redis_url: &str, stream: &str) -> usize {
    let client = redis::Client::open(redis_url).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("XLEN")
        .arg(stream)
        .query_async(&mut connection)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "isolated PostgreSQL, a Redis with AOF and a Redis without AOF"]
async fn outbox_worker_publishes_batches_retries_and_dead_letters() {
    assert_eq!(
        std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(),
        Ok("1")
    );
    let postgres = var("DBPROXY_TEST_POSTGRES_URL");
    let redis_url = var("DBPROXY_REDIS_URL");
    let no_aof = var("DBPROXY_TEST_NO_AOF_REDIS_URL");
    let suffix = format!(
        "w{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    let relay = ResolvedOutboxRelay {
        publishers: vec![ResolvedPublisher {
            id: format!("{suffix}-no-aof"),
            url: no_aof,
        }],
        routes: vec![OutboxRoute {
            producer: suffix.clone(),
            version: 1,
            publisher: format!("{suffix}-no-aof"),
            destination: format!("test:{suffix}:relay"),
        }],
        ..ResolvedOutboxRelay::default()
    };
    let backend = StorageBackend::connect_with_outbox(
        &postgres,
        &redis_url,
        &redis_url,
        StorageBackendConfig {
            shard_count: 2,
            read_connection_count: 0,
            tiered: Default::default(),
            enqueue: Default::default(),
        },
        &relay,
    )
    .await
    .unwrap();
    let policy = RetryWorkerPolicy {
        lease_ms: 30_000,
        base_retry_delay_ms: 1,
        max_retry_delay_ms: 1,
        max_attempts: 2,
    };

    // Nothing queued: both entry points report no work.
    assert!(matches!(
        backend
            .process_outbox_once("worker-a", policy)
            .await
            .unwrap(),
        DurableQueueProcessOutcome::Empty
    ));
    assert!(
        backend
            .process_outbox_batch("worker-a", policy)
            .await
            .unwrap()
            .is_empty()
    );

    // One legacy event through the single-event path.
    let (request, effects) = commit(&suffix, 0, vec![legacy(&suffix, 0)]);
    backend.commit_records(request, effects).await.unwrap();
    assert!(matches!(
        backend
            .process_outbox_once("worker-a", policy)
            .await
            .unwrap(),
        DurableQueueProcessOutcome::Committed
    ));
    let legacy_stream = format!("dbproxy:outbox:outbox-worker-{suffix}");
    assert_eq!(stream_len(&redis_url, &legacy_stream).await, 1);

    // Three independent ordering groups published together in one batch.
    for n in 1..=3 {
        let (request, effects) = commit(&suffix, n, vec![legacy(&suffix, n)]);
        backend.commit_records(request, effects).await.unwrap();
    }
    let outcomes = backend
        .process_outbox_batch("worker-b", policy)
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 3);
    assert!(
        outcomes
            .iter()
            .all(|o| matches!(o, Ok(DurableQueueProcessOutcome::Committed)))
    );
    assert_eq!(stream_len(&redis_url, &legacy_stream).await, 4);

    // A relay route on a Redis without AOF can never be confirmed: retry, then dead letter.
    let envelope = EventEnvelope {
        event_id: format!("{suffix}-relay"),
        producer: suffix.clone(),
        event_type: "Changed".into(),
        aggregate_type: "document".into(),
        aggregate_id: suffix.clone(),
        partition_key: suffix.clone(),
        schema_version: 1,
        content_type: "application/json".into(),
        payload: br#"{"v":1}"#.to_vec(),
        occurred_at_unix_ms: 1,
        route_version: 1,
    };
    let (request, effects) = commit(&suffix, 9, vec![envelope.into_outbox().unwrap()]);
    backend.commit_records(request, effects).await.unwrap();
    assert!(matches!(
        backend
            .process_outbox_once("worker-c", policy)
            .await
            .unwrap(),
        DurableQueueProcessOutcome::RetryScheduled
    ));
    // The retry delay is one millisecond; wait until the event is claimable again.
    let mut second = DurableQueueProcessOutcome::Empty;
    for _ in 0..50 {
        let outcomes = backend
            .process_outbox_batch("worker-c", policy)
            .await
            .unwrap();
        if let Some(outcome) = outcomes.into_iter().next() {
            second = outcome.unwrap();
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        matches!(second, DurableQueueProcessOutcome::DeadLettered),
        "second failure must dead-letter after max_attempts=2"
    );
    // Dead letters are not claimed again; the queue is drained.
    assert!(matches!(
        backend
            .process_outbox_once("worker-d", policy)
            .await
            .unwrap(),
        DurableQueueProcessOutcome::Empty
    ));

    // An invalid policy is rejected before touching the queue.
    let invalid = RetryWorkerPolicy {
        lease_ms: 0,
        ..policy
    };
    assert!(
        backend
            .process_outbox_once("worker-e", invalid)
            .await
            .is_err()
    );
    assert!(
        backend
            .process_outbox_batch("worker-e", invalid)
            .await
            .is_err()
    );
}
