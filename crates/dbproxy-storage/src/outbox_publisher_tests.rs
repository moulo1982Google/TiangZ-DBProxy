use super::*;
use crate::{
    PublishMessage, Publisher,
    redis_durability_fixture::{Failure, Fixture, Plan},
};
use std::time::Duration;

async fn publication_case(stall: bool) {
    let fixture = Fixture::start(Plan {
        write: b"XADD",
        max_ack_ms: 2000,
        failure: if stall {
            Failure::Stall
        } else {
            Failure::Unconfirmed
        },
        first_write_delay: Duration::ZERO,
        second_ack_delay: Duration::from_millis(750),
    })
    .await;
    let publisher = RedisStreamPublisher::connect(&fixture.url, "legacy-prefix:")
        .await
        .unwrap();
    let event = OutboxEvent {
        event_id: "fixture-event".into(),
        topic: "legacy.topic".into(),
        partition_key: "one".into(),
        payload: vec![1],
        occurred_at_unix_ms: 1,
    };
    let message = || PublishMessage {
        event: &event,
        destination: "fixture.destination",
        operation_id: "operation",
        trade_id: "old-trade",
    };
    let first = tokio::time::timeout(
        Duration::from_millis(100),
        Publisher::publish(&publisher, message()),
    )
    .await;
    if stall {
        assert!(first.is_err())
    } else {
        assert!(first.unwrap().is_err())
    }
    let receipt = tokio::time::timeout(
        Duration::from_secs(2),
        Publisher::publish(&publisher, message()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(receipt.message_id, "1-0");
    assert_eq!(fixture.observed.lock().unwrap().writes, [0, 1]);
    drop(publisher);
    fixture.finish().await;
}

#[tokio::test]
async fn aof_failure_requires_a_new_connection_and_republication() {
    publication_case(false).await;
}
#[tokio::test]
async fn publication_timeout_discards_the_in_flight_connection() {
    publication_case(true).await;
}

#[tokio::test]
async fn custom_aof_and_io_budgets_survive_reconnect() {
    let fixture = Fixture::start(Plan {
        write: b"XADD",
        max_ack_ms: 5000,
        failure: Failure::Disconnect,
        first_write_delay: Duration::ZERO,
        second_ack_delay: Duration::from_millis(3250),
    })
    .await;
    let metrics = Arc::new(StorageMetrics::default());
    let publisher = RedisOutboxPublisher::connect_with_config(
        &fixture.url,
        "prefix:",
        RedisDurabilityConfig {
            aof_ack_timeout: Duration::from_secs(5),
            response_timeout: Duration::from_secs(6),
        },
        Duration::from_millis(8500),
        metrics.clone(),
    )
    .await
    .unwrap();
    let event = OutboxEvent {
        event_id: "same-event".into(),
        topic: "topic".into(),
        partition_key: "key".into(),
        payload: vec![1],
        occurred_at_unix_ms: 1,
    };
    assert!(
        publisher
            .publish_to(&event, "fixture.destination", "same-operation", "")
            .await
            .is_err()
    );
    publisher
        .publish_to(&event, "fixture.destination", "same-operation", "")
        .await
        .unwrap();
    assert_eq!(
        fixture.observed.lock().unwrap().ack_ms,
        [(0, 5000), (1, 5000)]
    );
    let stage = metrics
        .latency_snapshot()
        .into_iter()
        .find(|s| s.stage == "outbox_aof")
        .unwrap();
    assert_eq!(stage.buckets.iter().sum::<u64>(), 2);
    assert_eq!(stage.timeouts, 0, "connection failure is not a timeout");
    drop(publisher);
    fixture.finish().await;
}

#[tokio::test]
async fn outbox_write_consumes_ack_budget_and_total_timeout_discards_connection() {
    let fixture = Fixture::start(Plan {
        write: b"XADD",
        max_ack_ms: 350,
        failure: Failure::Stall,
        first_write_delay: Duration::from_millis(350),
        second_ack_delay: Duration::ZERO,
    })
    .await;
    let metrics = Arc::new(StorageMetrics::default());
    let publisher = RedisOutboxPublisher::connect_with_config(
        &fixture.url,
        "prefix:",
        RedisDurabilityConfig {
            aof_ack_timeout: Duration::from_millis(350),
            response_timeout: Duration::from_millis(400),
        },
        Duration::from_millis(600),
        metrics.clone(),
    )
    .await
    .unwrap();
    let event = OutboxEvent {
        event_id: "same-event".into(),
        topic: "topic".into(),
        partition_key: "key".into(),
        payload: vec![1],
        occurred_at_unix_ms: 1,
    };
    assert!(matches!(
        publisher
            .publish_to(&event, "fixture.destination", "same-operation", "")
            .await,
        Err(StorageError::RedisDurabilityDeadlineExceeded)
    ));
    publisher
        .publish_to(&event, "fixture.destination", "same-operation", "")
        .await
        .unwrap();
    assert!(fixture.observed.lock().unwrap().ack_ms[0].1 <= 250);
    assert_eq!(fixture.observed.lock().unwrap().writes, [0, 1]);
    assert_eq!(
        metrics
            .latency_snapshot()
            .into_iter()
            .find(|s| s.stage == "outbox_total")
            .unwrap()
            .timeouts,
        1
    );
    drop(publisher);
    fixture.finish().await;
}

#[test]
fn endpoint_identity_excludes_credentials_but_includes_database() {
    use crate::redis_endpoint_fingerprint as fingerprint;
    assert_eq!(
        fingerprint("redis://alice:old@127.0.0.1:6379/1").unwrap(),
        fingerprint("redis://bob:new@127.0.0.1:6379/1").unwrap()
    );
    assert_ne!(
        fingerprint("redis://127.0.0.1:6379/1").unwrap(),
        fingerprint("redis://127.0.0.1:6379/2").unwrap()
    );
}
