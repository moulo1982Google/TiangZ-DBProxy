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

async fn batch_failure_case(failure: Failure, cancel: bool) {
    let fixture = Fixture::start(Plan {
        write: b"XADD",
        max_ack_ms: 2000,
        failure,
        first_write_delay: Duration::ZERO,
        second_ack_delay: Duration::from_millis(30),
    })
    .await;
    let publisher = RedisStreamPublisher::connect(&fixture.url, "legacy-prefix:")
        .await
        .unwrap();
    let events = (0..16)
        .map(|index| OutboxEvent {
            event_id: format!("batch-event-{index}"),
            topic: "legacy.topic".into(),
            partition_key: format!("group-{index}"),
            payload: vec![index as u8; 8],
            occurred_at_unix_ms: index,
        })
        .collect::<Vec<_>>();
    let messages = events
        .iter()
        .map(|event| PublishMessage {
            event,
            destination: "fixture.destination",
            operation_id: "same-operation",
            trade_id: "same-trade",
        })
        .collect::<Vec<_>>();
    assert!(Publisher::publish_batch(&publisher, &[]).await.is_empty());
    let oversized = vec![messages[0]; 65];
    assert!(
        Publisher::publish_batch(&publisher, &oversized)
            .await
            .iter()
            .all(Result::is_err)
    );
    assert!(fixture.observed.lock().unwrap().writes.is_empty());
    let first = tokio::time::timeout(
        Duration::from_millis(150),
        Publisher::publish_batch(&publisher, &messages),
    )
    .await;
    if cancel {
        assert!(first.is_err());
    } else {
        assert!(first.unwrap().iter().all(Result::is_err));
    }
    let receipts = tokio::time::timeout(
        Duration::from_secs(2),
        Publisher::publish_batch(&publisher, &messages),
    )
    .await
    .unwrap();
    assert_eq!(receipts.len(), 16);
    assert!(receipts.iter().all(Result::is_ok));
    {
        let observed = fixture.observed.lock().unwrap();
        assert_eq!(
            observed.writes.iter().filter(|index| **index == 1).count(),
            16
        );
        assert_eq!(
            observed
                .ack_ms
                .iter()
                .filter(|(index, _)| *index == 1)
                .count(),
            1
        );
        if matches!(failure, Failure::WriteError) {
            assert!(observed.ack_ms.iter().all(|(index, _)| *index != 0));
        } else {
            assert_eq!(
                observed.writes.iter().filter(|index| **index == 0).count(),
                16
            );
            assert_eq!(
                observed
                    .ack_ms
                    .iter()
                    .filter(|(index, _)| *index == 0)
                    .count(),
                1
            );
        }
        for (index, (_, command)) in observed
            .commands
            .iter()
            .filter(|(connection, _)| *connection == 1)
            .enumerate()
        {
            assert_eq!(command[1], b"fixture.destination");
            assert_eq!(command[4], events[index].event_id.as_bytes());
            assert_eq!(command[6], b"same-operation");
            assert_eq!(command[8], b"same-trade");
            assert_eq!(command[10], events[index].partition_key.as_bytes());
            assert_eq!(command[14], events[index].payload);
        }
    }
    drop(publisher);
    fixture.finish().await;
}

#[tokio::test]
async fn batch_aof_failure_republishes_all_events_on_one_new_connection() {
    batch_failure_case(Failure::Unconfirmed, false).await;
}

#[tokio::test]
async fn batch_timeout_discards_connection_without_successful_receipts() {
    batch_failure_case(Failure::Stall, true).await;
}

#[tokio::test]
async fn partial_batch_write_cannot_ack_any_event_before_republication() {
    batch_failure_case(Failure::WriteError, false).await;
}

#[tokio::test]
async fn invalid_envelope_does_not_dead_letter_valid_batch_members() {
    let fixture = Fixture::start(Plan {
        write: b"XADD",
        max_ack_ms: 2000,
        failure: Failure::Unconfirmed,
        first_write_delay: Duration::ZERO,
        second_ack_delay: Duration::ZERO,
    })
    .await;
    let publisher = RedisStreamPublisher::connect(&fixture.url, "legacy-prefix:")
        .await
        .unwrap();
    let valid = OutboxEvent {
        event_id: "valid-event".into(),
        topic: "legacy.topic".into(),
        partition_key: "one".into(),
        payload: vec![1],
        occurred_at_unix_ms: 1,
    };
    let mut invalid = valid.clone();
    invalid.event_id = "invalid-event".into();
    invalid.topic = format!("{}game.1", tiangz_dbproxy_core::RELAY_TOPIC_PREFIX);
    invalid.payload = vec![0];
    let messages = [&valid, &invalid, &valid].map(|event| PublishMessage {
        event,
        destination: "fixture.destination",
        operation_id: "operation",
        trade_id: "",
    });
    let first = Publisher::publish_batch(&publisher, &messages).await;
    assert!(matches!(first[0], Err(crate::PublishError::Transient(_))));
    assert!(matches!(first[1], Err(crate::PublishError::Permanent(_))));
    assert!(matches!(first[2], Err(crate::PublishError::Transient(_))));
    let second = Publisher::publish_batch(&publisher, &messages).await;
    assert!(second[0].is_ok() && second[2].is_ok());
    assert!(matches!(second[1], Err(crate::PublishError::Permanent(_))));
    assert_eq!(fixture.observed.lock().unwrap().writes, [0, 0, 1, 1]);
    drop(publisher);
    fixture.finish().await;
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
