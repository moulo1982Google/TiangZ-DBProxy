//! 通用事件信封；通过原有 Outbox 字节契约传输，不改变旧交易消息。
//! Generic envelope carried by the existing opaque Outbox contract.
use crate::{OutboxEvent, StoreError};
use serde::{Deserialize, Serialize};

pub const RELAY_TOPIC_PREFIX: &str = "dbproxy.relay.v1.";

/// payload 保持字节，不要求游戏改用 JSON；信封 JSON 中表现为字节数组。
/// Payload remains opaque bytes, represented as a byte array in envelope JSON.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventEnvelope {
    pub event_id: String,
    pub producer: String,
    pub event_type: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub partition_key: String,
    pub schema_version: u32,
    pub content_type: String,
    pub payload: Vec<u8>,
    #[serde(with = "timestamp")]
    pub occurred_at_unix_ms: u64,
    pub route_version: u32,
}

impl EventEnvelope {
    /// 构造路由键与稳定信封；重试不得重新生成 ID 或事件时间。
    /// Builds a stable routing key and envelope; retries preserve IDs and time.
    pub fn into_outbox(self) -> Result<OutboxEvent, StoreError> {
        self.validate()?;
        let payload = serde_json::to_vec(&self)
            .map_err(|_| StoreError::InvalidOutboxEvent("cannot encode envelope"))?;
        Ok(OutboxEvent {
            event_id: self.event_id,
            topic: format!(
                "{RELAY_TOPIC_PREFIX}{}.{}",
                self.producer, self.route_version
            ),
            partition_key: self.partition_key,
            payload,
            occurred_at_unix_ms: self.occurred_at_unix_ms,
        })
    }

    /// 保留前缀强制走新版校验；格式错误不得降级为旧 Stream 投递。
    /// Reserved topics fail closed instead of falling back to legacy publication.
    pub fn from_outbox(event: &OutboxEvent) -> Result<Option<Self>, StoreError> {
        if !event.topic.starts_with(RELAY_TOPIC_PREFIX) {
            return Ok(None);
        }
        let envelope: Self = serde_json::from_slice(&event.payload)
            .map_err(|_| StoreError::InvalidOutboxEvent("invalid relay envelope"))?;
        envelope.validate()?;
        if event.topic
            != format!(
                "{RELAY_TOPIC_PREFIX}{}.{}",
                envelope.producer, envelope.route_version
            )
            || event.event_id != envelope.event_id
            || event.partition_key != envelope.partition_key
            || event.occurred_at_unix_ms != envelope.occurred_at_unix_ms
        {
            return Err(StoreError::InvalidOutboxEvent("envelope identity mismatch"));
        }
        Ok(Some(envelope))
    }

    fn validate(&self) -> Result<(), StoreError> {
        for text in [
            &self.event_id,
            &self.producer,
            &self.event_type,
            &self.aggregate_type,
            &self.aggregate_id,
            &self.partition_key,
            &self.content_type,
        ] {
            if text.trim().is_empty() || text.len() > 256 || text.chars().any(char::is_control) {
                return Err(StoreError::InvalidOutboxEvent("invalid envelope text"));
            }
        }
        if !self
            .producer
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
            || self.schema_version == 0
            || self.route_version == 0
        {
            return Err(StoreError::InvalidOutboxEvent(
                "invalid producer or envelope version",
            ));
        }
        Ok(())
    }
}

mod timestamp {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};
    pub fn serialize<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decodes_shared_typescript_fixture() {
        let envelope: EventEnvelope = serde_json::from_str(include_str!(
            "../../../sdk/typescript/test/fixtures/relay-event.json"
        ))
        .unwrap();
        assert_eq!(envelope.payload, vec![0, 255, 128]);
        assert_eq!(envelope.occurred_at_unix_ms, u64::MAX);
        assert_eq!(
            EventEnvelope::from_outbox(&envelope.clone().into_outbox().unwrap()).unwrap(),
            Some(envelope)
        );
    }
    #[test]
    fn envelope_round_trip_preserves_bytes_and_full_width_timestamp() {
        let envelope = EventEnvelope {
            event_id: "evt1".into(),
            producer: "game".into(),
            event_type: "Changed".into(),
            aggregate_type: "document".into(),
            aggregate_id: "1".into(),
            partition_key: "1".into(),
            schema_version: 1,
            content_type: "application/octet-stream".into(),
            payload: vec![0, 255],
            occurred_at_unix_ms: u64::MAX,
            route_version: 1,
        };
        let mut event = envelope.clone().into_outbox().unwrap();
        assert_eq!(EventEnvelope::from_outbox(&event).unwrap(), Some(envelope));
        event.event_id = "changed".into();
        assert!(EventEnvelope::from_outbox(&event).is_err());
        event.topic = "legacy.trade".into();
        assert!(EventEnvelope::from_outbox(&event).unwrap().is_none());
    }
}
