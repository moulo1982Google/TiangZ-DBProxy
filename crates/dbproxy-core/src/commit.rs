//! 通用事务效果；只解释存储身份，不解释领域状态或记账规则。
//! Generic transaction effects with opaque, domain-owned content.
use std::collections::HashSet;

use crate::{RecordKey, StoreError};

/// 与权威记录原子提交、至少一次投递的不透明事件。
/// Opaque event committed with authoritative records and delivered at least once.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OutboxEvent {
    pub event_id: String,
    pub topic: String,
    pub partition_key: String,
    pub payload: Vec<u8>,
    pub occurred_at_unix_ms: u64,
}
use serde::{Deserialize, Serialize};

/// 不可变事实；namespace/key 在追加记录集合内唯一，与快照集合独立。
/// Immutable fact, uniquely addressed within the append collection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppendRecord {
    pub record: RecordKey,
    pub schema: String,
    pub schema_version: u32,
    pub payload: Vec<u8>,
    pub occurred_at_unix_ms: u64,
}

/// 与多记录 CAS 同生共死的效果；重复操作必须携带完全相同的规范化效果。
/// Effects committed atomically with record CAS and included in idempotency identity.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommitEffects {
    pub appends: Vec<AppendRecord>,
    pub outbox_events: Vec<OutboxEvent>,
}

impl CommitEffects {
    pub fn normalize(mut self) -> Result<Self, StoreError> {
        self.appends.sort_by(|a, b| {
            (&a.record.namespace, &a.record.key).cmp(&(&b.record.namespace, &b.record.key))
        });
        for append in &self.appends {
            RecordKey::new(&append.record.namespace, &append.record.key)?;
            if append.schema.trim().is_empty() {
                return Err(StoreError::InvalidKey("append schema is empty"));
            }
        }
        for pair in self.appends.windows(2) {
            if pair[0].record == pair[1].record {
                return Err(StoreError::DuplicateTransactionRecord {
                    record: pair[0].record.clone(),
                });
            }
        }
        self.outbox_events
            .sort_by(|a, b| a.event_id.cmp(&b.event_id));
        let mut ids = HashSet::new();
        for event in &self.outbox_events {
            crate::EventEnvelope::from_outbox(event)?;
            if event.event_id.trim().is_empty()
                || event.topic.trim().is_empty()
                || event.partition_key.trim().is_empty()
            {
                return Err(StoreError::InvalidOutboxEvent(
                    "event identity or route is empty",
                ));
            }
            if !ids.insert(&event.event_id) {
                return Err(StoreError::DuplicateOutboxEvent {
                    event_id: event.event_id.clone(),
                });
            }
        }
        Ok(self)
    }

    pub fn is_empty(&self) -> bool {
        self.appends.is_empty() && self.outbox_events.is_empty()
    }
}
