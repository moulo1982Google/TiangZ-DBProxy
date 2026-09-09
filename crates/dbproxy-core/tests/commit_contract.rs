//! Storage-independent regression tests for generic commit identities and envelopes.
use tiangz_dbproxy_core::{AppendRecord, CommitEffects, EventEnvelope, RecordKey, StoreError};

fn envelope() -> EventEnvelope {
    serde_json::from_str(include_str!(
        "../../../sdk/typescript/test/fixtures/relay-event.json"
    ))
    .unwrap()
}

fn effects() -> CommitEffects {
    CommitEffects {
        appends: ["b", "a"]
            .into_iter()
            .map(|key| AppendRecord {
                record: RecordKey::new("facts", key).unwrap(),
                schema: "opaque".into(),
                schema_version: 1,
                payload: vec![0, 255],
                occurred_at_unix_ms: 1,
            })
            .collect(),
        outbox_events: ["b", "a"]
            .into_iter()
            .map(|id| {
                EventEnvelope {
                    event_id: id.into(),
                    ..envelope()
                }
                .into_outbox()
                .unwrap()
            })
            .collect(),
    }
}

#[test]
fn normalization_is_order_independent_and_idempotent() {
    let input = effects();
    let mut reversed = input.clone();
    reversed.appends.reverse();
    reversed.outbox_events.reverse();
    let normalized = input.normalize().unwrap();
    assert_eq!(normalized, reversed.normalize().unwrap());
    assert_eq!(normalized, normalized.clone().normalize().unwrap());
    assert_eq!(normalized.appends[0].record.key, "a");
    assert_eq!(normalized.outbox_events[0].event_id, "a");
}

#[test]
fn duplicate_append_identity_is_rejected_even_with_different_payload() {
    let mut input = effects();
    let mut duplicate = input.appends[0].clone();
    duplicate.payload.push(42);
    input.appends.push(duplicate);
    assert!(matches!(
        input.normalize(),
        Err(StoreError::DuplicateTransactionRecord { .. })
    ));
}

#[test]
fn duplicate_event_identity_is_rejected_across_routes() {
    let mut input = effects();
    input.outbox_events.push(
        EventEnvelope {
            event_id: "a".into(),
            producer: "another".into(),
            ..envelope()
        }
        .into_outbox()
        .unwrap(),
    );
    assert!(matches!(
        input.normalize(),
        Err(StoreError::DuplicateOutboxEvent { .. })
    ));
}

#[test]
fn append_namespaces_are_distinct_identities() {
    let mut input = effects();
    let mut separate = input.appends[0].clone();
    separate.record.namespace = "another".into();
    input.appends.push(separate);
    assert_eq!(input.normalize().unwrap().appends.len(), 3);
}

#[test]
fn every_outer_envelope_identity_field_is_checked() {
    for field in ["event_id", "topic", "partition_key", "occurred_at"] {
        let mut event = envelope().into_outbox().unwrap();
        match field {
            "event_id" => event.event_id.push('x'),
            "topic" => event.topic.push('x'),
            "partition_key" => event.partition_key.push('x'),
            "occurred_at" => event.occurred_at_unix_ms = 0,
            _ => unreachable!(),
        }
        assert!(EventEnvelope::from_outbox(&event).is_err(), "{field}");
    }
}

#[test]
fn malformed_reserved_payloads_never_fall_back_to_legacy() {
    for bytes in [vec![], vec![255], b"null".to_vec(), b"{}".to_vec()] {
        let mut event = envelope().into_outbox().unwrap();
        event.payload = bytes;
        assert!(EventEnvelope::from_outbox(&event).is_err());
        assert!(
            CommitEffects {
                appends: vec![],
                outbox_events: vec![event]
            }
            .normalize()
            .is_err()
        );
    }
}

#[test]
fn unknown_fields_and_invalid_timestamps_are_rejected() {
    let mut event = envelope().into_outbox().unwrap();
    let original: serde_json::Value = serde_json::from_slice(&event.payload).unwrap();
    let mut unknown = original.clone();
    unknown["destination"] = "attacker.events".into();
    event.payload = serde_json::to_vec(&unknown).unwrap();
    assert!(EventEnvelope::from_outbox(&event).is_err());
    for invalid in [
        serde_json::json!(1),
        serde_json::json!(null),
        serde_json::json!("-1"),
        serde_json::json!("18446744073709551616"),
        serde_json::json!("1.5"),
        serde_json::json!(""),
    ] {
        let mut value = original.clone();
        value["occurred_at_unix_ms"] = invalid;
        event.payload = serde_json::to_vec(&value).unwrap();
        assert!(EventEnvelope::from_outbox(&event).is_err());
    }
}

#[test]
fn all_text_fields_reject_blank_control_and_overlong_values() {
    for field in [
        "event_id",
        "producer",
        "event_type",
        "aggregate_type",
        "aggregate_id",
        "partition_key",
        "content_type",
    ] {
        for invalid in [" ".to_owned(), "line\nbreak".into(), "x".repeat(257)] {
            let mut value = serde_json::to_value(envelope()).unwrap();
            value[field] = invalid.into();
            let candidate: EventEnvelope = serde_json::from_value(value).unwrap();
            assert!(candidate.into_outbox().is_err(), "{field}");
        }
    }
}

#[test]
fn invalid_versions_and_producer_characters_are_rejected() {
    for field in ["schema_version", "route_version"] {
        let mut value = serde_json::to_value(envelope()).unwrap();
        value[field] = 0.into();
        assert!(
            serde_json::from_value::<EventEnvelope>(value)
                .unwrap()
                .into_outbox()
                .is_err()
        );
    }
    for producer in ["game.other", "game/other", "游戏"] {
        assert!(
            EventEnvelope {
                producer: producer.into(),
                ..envelope()
            }
            .into_outbox()
            .is_err()
        );
    }
}

#[test]
fn byte_payloads_and_timestamp_boundaries_round_trip() {
    for timestamp in [0, 1, i64::MAX as u64, u64::MAX] {
        for len in [0, 1, 255, 256, 1024] {
            let original = EventEnvelope {
                occurred_at_unix_ms: timestamp,
                payload: (0..len).map(|i| (i % 256) as u8).collect(),
                ..envelope()
            };
            let event = original.clone().into_outbox().unwrap();
            assert_eq!(EventEnvelope::from_outbox(&event).unwrap(), Some(original));
        }
    }
}
