//! 必须指向专用 AOF Redis；短对照只能证明配置及确认契约，不能代替长稳或容量结论。
//! Requires dedicated AOF Redis; short comparisons validate contracts, not soak or capacity.
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tiangz_dbproxy_core::{OutboxEvent, RecordKey, SnapshotWrite};
use tiangz_dbproxy_storage::{
    EnqueueBatchConfig, PublishMessage, Publisher, RedisDurabilityConfig, RedisOutboxPublisher,
    RedisSnapshotBacklog, SnapshotBacklogAck, StorageMetrics,
};

#[tokio::test]
#[ignore = "requires a dedicated empty AOF Redis and DBPROXY_RUN_REDIS_BUDGET_TESTS=1; writes/claims snapshots and publishes streams"]
async fn two_three_five_second_profiles_preserve_ack_and_payloads() {
    assert_eq!(
        std::env::var("DBPROXY_RUN_REDIS_BUDGET_TESTS").as_deref(),
        Ok("1")
    );
    let url = std::env::var("DBPROXY_REDIS_URL").expect("dedicated Redis URL");
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut connection = redis::Client::open(url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let config: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("appendonly")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(config, ["appendonly", "yes"]);
    let mut reports = Vec::new();
    for (aof_ms, total_ms) in [(2000, 4500), (3000, 6000), (5000, 8500)] {
        let durability = RedisDurabilityConfig {
            aof_ack_timeout: Duration::from_millis(aof_ms),
            response_timeout: Duration::from_millis(aof_ms + 1000),
        };
        let metrics = Arc::new(StorageMetrics::default());
        let backlog = RedisSnapshotBacklog::connect_with_metrics(
            &url,
            EnqueueBatchConfig {
                total_timeout: Duration::from_millis(total_ms),
                durability,
                ..Default::default()
            },
            metrics.clone(),
        )
        .await
        .unwrap();
        let publisher = RedisOutboxPublisher::connect_with_config(
            &url,
            "probe:",
            durability,
            Duration::from_millis(total_ms.max(5000)),
            metrics.clone(),
        )
        .await
        .unwrap();
        assert_eq!(
            {
                let stats = backlog.stats().await.unwrap();
                stats.pending + stats.processing
            },
            0,
            "dedicated backlog must be empty"
        );
        let destination = format!("aof-profile:{suffix}:{aof_ms}");
        let mut elapsed_ms = Vec::new();
        let mut latest = Vec::new();
        for iteration in 0..30 {
            let writes = (0..40)
                .map(|key| SnapshotWrite {
                    record: RecordKey::new(
                        format!("aof-profile-{suffix}-{aof_ms}"),
                        key.to_string(),
                    )
                    .unwrap(),
                    request_id: format!("profile-{suffix}-{aof_ms}-{iteration}-{key}"),
                    expected_revision: None,
                    schema: "budget-probe".into(),
                    updated_at_unix_ms: iteration as u64 + 1,
                    schema_version: 1,
                    payload: vec![iteration as u8; 128],
                })
                .collect::<Vec<_>>();
            let event = OutboxEvent {
                event_id: format!("event-{suffix}-{aof_ms}-{iteration}"),
                topic: "probe".into(),
                partition_key: "one".into(),
                payload: vec![iteration as u8; 128],
                occurred_at_unix_ms: 1,
            };
            let started = Instant::now();
            let (enqueue, publication) = tokio::join!(
                backlog.enqueue_multi(&writes),
                Publisher::publish(
                    &publisher,
                    PublishMessage {
                        event: &event,
                        destination: &destination,
                        operation_id: &event.event_id,
                        trade_id: "",
                    }
                )
            );
            enqueue.unwrap();
            publication.unwrap();
            elapsed_ms.push(started.elapsed().as_secs_f64() * 1000.0);
            latest = writes;
        }
        let leases = backlog.claim_multi(30_000, 40).await.unwrap();
        assert_eq!(leases.len(), 40);
        for lease in &leases {
            let expected = latest
                .iter()
                .find(|write| write.record == lease.request.record)
                .unwrap();
            assert_eq!(&lease.request, expected);
            assert!(lease.fence_sequence.is_some());
        }
        assert!(
            backlog
                .ack_multi(&leases)
                .await
                .unwrap()
                .iter()
                .all(|ack| *ack == SnapshotBacklogAck::Removed)
        );
        assert_eq!(
            {
                let stats = backlog.stats().await.unwrap();
                stats.pending + stats.processing
            },
            0
        );
        let events: i64 = redis::cmd("XLEN")
            .arg(&destination)
            .query_async(&mut connection)
            .await
            .unwrap();
        assert_eq!(events, 30);
        let snapshots = metrics.latency_snapshot();
        assert!(
            snapshots
                .iter()
                .all(|stage| stage.timeouts == 0 && stage.in_flight == 0)
        );
        for name in ["enqueue_aof", "outbox_aof", "enqueue_total", "outbox_total"] {
            assert_eq!(
                snapshots
                    .iter()
                    .find(|stage| stage.stage == name)
                    .unwrap()
                    .buckets
                    .iter()
                    .sum::<u64>(),
                30
            );
        }
        let mut sorted = elapsed_ms.clone();
        sorted.sort_by(f64::total_cmp);
        reports.push(serde_json::json!({"aofMs":aof_ms,"enqueueTotalMs":total_ms,"redisIoMs":aof_ms+1000,
            "samplesMs":elapsed_ms,"p50Ms":sorted[14],"p95Ms":sorted[28],"maxMs":sorted[29],
            "enqueuedRecords":1200,"events":events,"verifiedFinalSnapshots":40,
            "scope":"30 sequential group enqueues plus concurrent Outbox publication; not sustained overload, fault or 24h qualification"}));
    }
    println!(
        "AOF_PROFILE_COMPARISON {}",
        serde_json::to_string(&reports).unwrap()
    );
}
