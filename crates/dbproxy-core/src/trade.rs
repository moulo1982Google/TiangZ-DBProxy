//! 交易持久化原语。 / Durable trade primitives.
//!
//! 本模块只保证状态迁移、快照CAS、零和账本和Outbox的原子契约；道具所有权、价格、
//! 玩家资格等规则仍由业务层验证。The module enforces durable state transitions, snapshot
//! CAS, balanced ledger postings, and outbox intent. Gameplay rules remain in the domain service.

use std::collections::{BTreeMap, HashSet};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::{
    OutboxEvent, Revision, StoreError, TransactionRecordReceipt, TransactionalRecordWrite,
};

/// DBProxy认可的最小托管状态机；Settled和Cancelled是终态。
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum TradeState {
    Proposed,
    Escrowed,
    Settled,
    Cancelled,
}

/// 一次交易状态迁移。版本独立于玩家快照Revision。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TradeTransition {
    pub trade_id: String,
    pub expected_version: Revision,
    /// 新建交易必须为None；更新交易必须与当前状态相同。
    pub expected_state: Option<TradeState>,
    pub next_state: TradeState,
    /// 由业务定义的订单/托管详情；DBProxy只做有界持久化。
    pub payload: Vec<u8>,
    pub updated_at_unix_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TradeEnvelope {
    pub trade_id: String,
    pub version: Revision,
    pub state: TradeState,
    pub payload: Vec<u8>,
    pub updated_at_unix_ms: u64,
}

/// 不可变双重记账中的一条Posting。同一asset下amount总和必须为零。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LedgerPosting {
    pub posting_id: String,
    pub account_id: String,
    pub asset: String,
    pub amount: i64,
    pub metadata: Vec<u8>,
}

/// 一次原子交易：状态、玩家/领域快照、账本和Outbox要么一起提交，要么全部回滚。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TradeTransaction {
    pub operation_id: String,
    pub transition: TradeTransition,
    pub writes: Vec<TransactionalRecordWrite>,
    pub ledger_postings: Vec<LedgerPosting>,
    pub outbox_events: Vec<OutboxEvent>,
    pub result: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TradeReceipt {
    pub operation_id: String,
    pub trade_id: String,
    pub new_trade_version: Revision,
    pub state: TradeState,
    pub records: Vec<TransactionRecordReceipt>,
    pub ledger_posting_ids: Vec<String>,
    pub outbox_event_ids: Vec<String>,
    pub result: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TradeTransactionOutcome {
    Applied(TradeReceipt),
    Duplicate(TradeReceipt),
}

impl TradeTransactionOutcome {
    pub fn receipt(&self) -> &TradeReceipt {
        match self {
            Self::Applied(receipt) | Self::Duplicate(receipt) => receipt,
        }
    }
}

#[async_trait]
pub trait AsyncTradeStore {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn load_trade(&self, trade_id: &str) -> Result<Option<TradeEnvelope>, Self::Error>;

    async fn load_trade_receipt(
        &self,
        operation_id: &str,
        trade_id: &str,
    ) -> Result<Option<TradeReceipt>, Self::Error>;

    async fn apply_trade(
        &mut self,
        request: TradeTransaction,
    ) -> Result<TradeTransactionOutcome, Self::Error>;
}

fn is_valid_transition(from: Option<TradeState>, to: TradeState) -> bool {
    matches!(
        (from, to),
        (None, TradeState::Proposed | TradeState::Escrowed)
            | (
                Some(TradeState::Proposed),
                TradeState::Escrowed | TradeState::Cancelled
            )
            | (
                Some(TradeState::Escrowed),
                TradeState::Settled | TradeState::Cancelled
            )
    )
}

/// Validate and canonicalize an incoming trade transaction before persistence or fingerprinting.
pub fn normalize_trade_transaction(
    mut request: TradeTransaction,
) -> Result<TradeTransaction, StoreError> {
    if request.operation_id.trim().is_empty() {
        return Err(StoreError::EmptyOperationId);
    }
    if request.transition.trade_id.trim().is_empty() {
        return Err(StoreError::EmptyTradeId);
    }
    if request.writes.is_empty() {
        return Err(StoreError::EmptyTradeRecords);
    }
    if (request.transition.expected_version == Revision::ZERO)
        != request.transition.expected_state.is_none()
    {
        return Err(StoreError::InvalidTradeStateTransition {
            from: request.transition.expected_state,
            to: request.transition.next_state,
        });
    }
    if !is_valid_transition(
        request.transition.expected_state,
        request.transition.next_state,
    ) {
        return Err(StoreError::InvalidTradeStateTransition {
            from: request.transition.expected_state,
            to: request.transition.next_state,
        });
    }

    request.writes.sort_by(|left, right| {
        left.record
            .namespace
            .cmp(&right.record.namespace)
            .then_with(|| left.record.key.cmp(&right.record.key))
    });
    for write in &request.writes {
        if write.record.namespace.trim().is_empty() {
            return Err(StoreError::InvalidKey("namespace is empty"));
        }
        if write.record.key.trim().is_empty() {
            return Err(StoreError::InvalidKey("key is empty"));
        }
    }
    for pair in request.writes.windows(2) {
        if pair[0].record == pair[1].record {
            return Err(StoreError::DuplicateTransactionRecord {
                record: pair[0].record.clone(),
            });
        }
    }

    request
        .ledger_postings
        .sort_by(|left, right| left.posting_id.cmp(&right.posting_id));
    let mut balances = BTreeMap::<String, i128>::new();
    let mut posting_ids = HashSet::with_capacity(request.ledger_postings.len());
    for posting in &request.ledger_postings {
        if posting.posting_id.trim().is_empty() {
            return Err(StoreError::InvalidLedgerPosting("posting id is empty"));
        }
        if !posting_ids.insert(posting.posting_id.clone()) {
            return Err(StoreError::DuplicateLedgerPosting {
                posting_id: posting.posting_id.clone(),
            });
        }
        if posting.account_id.trim().is_empty() {
            return Err(StoreError::InvalidLedgerPosting("account id is empty"));
        }
        if posting.asset.trim().is_empty() {
            return Err(StoreError::InvalidLedgerPosting("asset is empty"));
        }
        if posting.amount == 0 {
            return Err(StoreError::InvalidLedgerPosting("amount is zero"));
        }
        *balances.entry(posting.asset.clone()).or_default() += i128::from(posting.amount);
    }
    if let Some((asset, _)) = balances.into_iter().find(|(_, balance)| *balance != 0) {
        return Err(StoreError::UnbalancedLedger { asset });
    }

    request
        .outbox_events
        .sort_by(|left, right| left.event_id.cmp(&right.event_id));
    let mut event_ids = HashSet::with_capacity(request.outbox_events.len());
    for event in &request.outbox_events {
        if event.topic.starts_with(crate::RELAY_TOPIC_PREFIX) {
            return Err(StoreError::InvalidOutboxEvent(
                "relay envelopes require CommitRecords",
            ));
        }
        if event.event_id.trim().is_empty() {
            return Err(StoreError::InvalidOutboxEvent("event id is empty"));
        }
        if !event_ids.insert(event.event_id.clone()) {
            return Err(StoreError::DuplicateOutboxEvent {
                event_id: event.event_id.clone(),
            });
        }
        if event.topic.trim().is_empty() {
            return Err(StoreError::InvalidOutboxEvent("topic is empty"));
        }
        if event.partition_key.trim().is_empty() {
            return Err(StoreError::InvalidOutboxEvent("partition key is empty"));
        }
    }
    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RecordKey;

    fn write(key: &str) -> TransactionalRecordWrite {
        TransactionalRecordWrite {
            record: RecordKey::new("player", key).unwrap(),
            schema: "player.snapshot".to_string(),
            schema_version: 1,
            expected_revision: Revision::ZERO,
            payload: key.as_bytes().to_vec(),
            updated_at_unix_ms: 1,
        }
    }

    fn transaction() -> TradeTransaction {
        TradeTransaction {
            operation_id: "trade-op-1".to_string(),
            transition: TradeTransition {
                trade_id: "trade-1".to_string(),
                expected_version: Revision::ZERO,
                expected_state: None,
                next_state: TradeState::Escrowed,
                payload: b"escrow".to_vec(),
                updated_at_unix_ms: 1,
            },
            writes: vec![write("seller"), write("buyer")],
            ledger_postings: vec![
                LedgerPosting {
                    posting_id: "debit".to_string(),
                    account_id: "buyer".to_string(),
                    asset: "gold".to_string(),
                    amount: -100,
                    metadata: Vec::new(),
                },
                LedgerPosting {
                    posting_id: "credit".to_string(),
                    account_id: "trade-escrow".to_string(),
                    asset: "gold".to_string(),
                    amount: 100,
                    metadata: Vec::new(),
                },
            ],
            outbox_events: vec![OutboxEvent {
                event_id: "event-1".to_string(),
                topic: "trade.escrowed".to_string(),
                partition_key: "trade-1".to_string(),
                payload: b"event".to_vec(),
                occurred_at_unix_ms: 1,
            }],
            result: b"ok".to_vec(),
        }
    }

    #[test]
    fn canonicalizes_records_postings_and_events() {
        let normalized = normalize_trade_transaction(transaction()).unwrap();
        assert_eq!(normalized.writes[0].record.key, "buyer");
        assert_eq!(normalized.ledger_postings[0].posting_id, "credit");
        assert_eq!(normalized.outbox_events[0].event_id, "event-1");
    }

    #[test]
    fn rejects_unbalanced_postings_and_terminal_transitions() {
        let mut unbalanced = transaction();
        unbalanced.ledger_postings[1].amount = 99;
        assert!(matches!(
            normalize_trade_transaction(unbalanced),
            Err(StoreError::UnbalancedLedger { .. })
        ));

        let mut terminal = transaction();
        terminal.transition.expected_version = Revision(2);
        terminal.transition.expected_state = Some(TradeState::Settled);
        terminal.transition.next_state = TradeState::Cancelled;
        assert!(matches!(
            normalize_trade_transaction(terminal),
            Err(StoreError::InvalidTradeStateTransition { .. })
        ));
    }
}
