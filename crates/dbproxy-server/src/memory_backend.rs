use std::{
    collections::{HashMap, HashSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    sync::Arc,
};

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tiangz_dbproxy_core::{AppendRecord, CommitEffects};
use tiangz_dbproxy_core::{
    MultiRecordTransactionReceipt, MultiRecordTransactionalWrite,
    MultiRecordTransactionalWriteOutcome, RecordKey, Revision, SnapshotEnvelope, SnapshotWrite,
    SnapshotWriteOutcome, StoreError, TradeEnvelope, TradeReceipt, TradeTransaction,
    TradeTransactionOutcome, TransactionReceipt, TransactionRecordReceipt,
    TransactionalRecordWrite, TransactionalWrite, TransactionalWriteOutcome,
    normalize_trade_transaction,
};
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::{BackendError, DbProxyBackend};

#[derive(Default)]
struct MemoryShard {
    snapshots: HashMap<RecordKey, SnapshotEnvelope>,
}

#[derive(Default)]
struct ReceiptShard {
    operation_kinds: HashMap<String, &'static str>,
    snapshots: HashMap<String, SnapshotReceipt>,
    transactions: HashMap<String, StoredTransactionReceipt>,
    multi_transactions: HashMap<String, StoredMultiTransactionReceipt>,
    trade_transactions: HashMap<String, StoredTradeTransactionReceipt>,
}

#[derive(Default)]
struct TradeShard {
    trades: HashMap<String, TradeEnvelope>,
}

#[derive(Default)]
struct TradeIdentifiers {
    appends: HashMap<RecordKey, AppendRecord>,
    ledger_posting_ids: HashSet<String>,
    outbox_event_ids: HashSet<String>,
}

#[derive(Clone)]
struct SnapshotReceipt {
    record: RecordKey,
    fingerprint: SnapshotFingerprint,
    revision: Revision,
}

#[derive(Clone, Eq, PartialEq)]
struct SnapshotFingerprint {
    schema: String,
    schema_version: u32,
    payload_digest: [u8; 32],
    expected_revision: Option<Revision>,
    updated_at_unix_ms: u64,
}

impl SnapshotFingerprint {
    fn from_request(request: &SnapshotWrite) -> Self {
        Self {
            schema: request.schema.clone(),
            schema_version: request.schema_version,
            payload_digest: digest(&request.payload),
            expected_revision: request.expected_revision,
            updated_at_unix_ms: request.updated_at_unix_ms,
        }
    }
}

#[derive(Clone)]
struct StoredTransactionReceipt {
    fingerprint: TransactionFingerprint,
    new_revision: Revision,
    result: Vec<u8>,
}

#[derive(Clone, Eq, PartialEq)]
struct TransactionFingerprint {
    record: RecordKey,
    schema: String,
    schema_version: u32,
    expected_revision: Revision,
    payload_digest: [u8; 32],
    result_digest: [u8; 32],
    updated_at_unix_ms: u64,
}

impl TransactionFingerprint {
    fn from_request(request: &TransactionalWrite) -> Self {
        Self {
            record: request.record.clone(),
            schema: request.schema.clone(),
            schema_version: request.schema_version,
            expected_revision: request.expected_revision,
            payload_digest: digest(&request.payload),
            result_digest: digest(&request.result),
            updated_at_unix_ms: request.updated_at_unix_ms,
        }
    }
}

#[derive(Clone)]
struct StoredMultiTransactionReceipt {
    effects: CommitEffects,
    fingerprint: MultiTransactionFingerprint,
    records: Vec<TransactionRecordReceipt>,
    result: Vec<u8>,
}

#[derive(Clone)]
struct StoredTradeTransactionReceipt {
    request: TradeTransaction,
    receipt: TradeReceipt,
}

#[derive(Clone, Eq, PartialEq)]
struct MultiTransactionFingerprint {
    writes: Vec<TransactionalRecordFingerprint>,
    result_digest: [u8; 32],
}

#[derive(Clone, Eq, PartialEq)]
struct TransactionalRecordFingerprint {
    record: RecordKey,
    schema: String,
    schema_version: u32,
    expected_revision: Revision,
    payload_digest: [u8; 32],
    updated_at_unix_ms: u64,
}

impl MultiTransactionFingerprint {
    fn from_request(request: &MultiRecordTransactionalWrite) -> Self {
        Self {
            writes: request
                .writes
                .iter()
                .map(|write| TransactionalRecordFingerprint {
                    record: write.record.clone(),
                    schema: write.schema.clone(),
                    schema_version: write.schema_version,
                    expected_revision: write.expected_revision,
                    payload_digest: digest(&write.payload),
                    updated_at_unix_ms: write.updated_at_unix_ms,
                })
                .collect(),
            result_digest: digest(&request.result),
        }
    }
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// Volatile backend used to isolate DBProxy network and scheduling overhead from database I/O.
/// It preserves revision, CAS, idempotency, and multi-record atomicity but loses all data on exit.
pub struct MemoryBackend {
    data_shards: Vec<Arc<Mutex<MemoryShard>>>,
    receipt_shards: Vec<Arc<Mutex<ReceiptShard>>>,
    trade_shards: Vec<Arc<Mutex<TradeShard>>>,
    trade_identifiers: Arc<Mutex<TradeIdentifiers>>,
}

impl MemoryBackend {
    pub fn new(shards: usize) -> Result<Self, BackendError> {
        if shards == 0 {
            return Err(BackendError::InvalidConfig(
                "memory backend requires at least one shard",
            ));
        }
        Ok(Self {
            data_shards: (0..shards)
                .map(|_| Arc::new(Mutex::new(MemoryShard::default())))
                .collect(),
            receipt_shards: (0..shards)
                .map(|_| Arc::new(Mutex::new(ReceiptShard::default())))
                .collect(),
            trade_shards: (0..shards)
                .map(|_| Arc::new(Mutex::new(TradeShard::default())))
                .collect(),
            trade_identifiers: Arc::new(Mutex::new(TradeIdentifiers::default())),
        })
    }

    fn hash_index<T: Hash + ?Sized>(value: &T, count: usize) -> usize {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        (hasher.finish() as usize) % count
    }

    fn data_index(&self, record: &RecordKey) -> usize {
        Self::hash_index(record, self.data_shards.len())
    }

    fn receipt_index(&self, operation_id: &str) -> usize {
        Self::hash_index(operation_id, self.receipt_shards.len())
    }

    fn trade_index(&self, trade_id: &str) -> usize {
        Self::hash_index(trade_id, self.trade_shards.len())
    }

    async fn lock_data_shards(
        &self,
        writes: &[TransactionalRecordWrite],
    ) -> Vec<(usize, OwnedMutexGuard<MemoryShard>)> {
        let mut indices = writes
            .iter()
            .map(|write| self.data_index(&write.record))
            .collect::<Vec<_>>();
        indices.sort_unstable();
        indices.dedup();
        let mut guards = Vec::with_capacity(indices.len());
        for index in indices {
            guards.push((
                index,
                Arc::clone(&self.data_shards[index]).lock_owned().await,
            ));
        }
        guards
    }

    fn normalize_multi(
        mut request: MultiRecordTransactionalWrite,
    ) -> Result<MultiRecordTransactionalWrite, StoreError> {
        if request.operation_id.trim().is_empty() {
            return Err(StoreError::EmptyOperationId);
        }
        if request.writes.is_empty() {
            return Err(StoreError::EmptyTransactionRecords);
        }
        request.writes.sort_by(|left, right| {
            left.record
                .namespace
                .cmp(&right.record.namespace)
                .then_with(|| left.record.key.cmp(&right.record.key))
        });
        for pair in request.writes.windows(2) {
            if pair[0].record == pair[1].record {
                return Err(StoreError::DuplicateTransactionRecord {
                    record: pair[0].record.clone(),
                });
            }
        }
        Ok(request)
    }

    fn claim_operation(
        receipts: &mut ReceiptShard,
        operation_id: &str,
        kind: &'static str,
    ) -> Result<(), StoreError> {
        match receipts.operation_kinds.get(operation_id) {
            Some(existing) if *existing != kind => Err(StoreError::OperationIdConflict {
                operation_id: operation_id.to_string(),
            }),
            Some(_) => Ok(()),
            None => Ok(()),
        }
    }
}

#[async_trait]
impl DbProxyBackend for MemoryBackend {
    async fn load(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, BackendError> {
        let shard = self.data_shards[self.data_index(record)].lock().await;
        Ok(shard.snapshots.get(record).cloned())
    }

    async fn load_multi(
        &self,
        records: &[RecordKey],
    ) -> Result<Vec<Option<SnapshotEnvelope>>, BackendError> {
        let mut indices = records
            .iter()
            .map(|record| self.data_index(record))
            .collect::<Vec<_>>();
        indices.sort_unstable();
        indices.dedup();
        let mut shards = Vec::with_capacity(indices.len());
        for index in indices {
            shards.push((
                index,
                Arc::clone(&self.data_shards[index]).lock_owned().await,
            ));
        }
        Ok(records
            .iter()
            .map(|record| {
                let index = self.data_index(record);
                shards
                    .iter()
                    .find(|(candidate, _)| *candidate == index)
                    .expect("locked data shard missing")
                    .1
                    .snapshots
                    .get(record)
                    .cloned()
            })
            .collect())
    }

    async fn save(&self, request: SnapshotWrite) -> Result<SnapshotWriteOutcome, BackendError> {
        if request.request_id.trim().is_empty() {
            return Err(StoreError::EmptyRequestId.into());
        }
        let receipt_index = self.receipt_index(&request.request_id);
        let mut receipts = self.receipt_shards[receipt_index].lock().await;
        let fingerprint = SnapshotFingerprint::from_request(&request);
        if let Some(receipt) = receipts.snapshots.get(&request.request_id) {
            if receipt.record != request.record || receipt.fingerprint != fingerprint {
                return Err(StoreError::IdempotencyConflict {
                    request_id: request.request_id,
                }
                .into());
            }
            return Ok(SnapshotWriteOutcome::Duplicate {
                revision: receipt.revision,
            });
        }

        let mut data = self.data_shards[self.data_index(&request.record)]
            .lock()
            .await;
        let actual = data
            .snapshots
            .get(&request.record)
            .map(|snapshot| snapshot.revision)
            .unwrap_or(Revision::ZERO);
        if request
            .expected_revision
            .is_some_and(|expected| expected != actual)
        {
            return Err(StoreError::RevisionConflict {
                record: request.record,
                expected: request.expected_revision,
                actual,
            }
            .into());
        }
        let revision =
            Revision(
                actual
                    .0
                    .checked_add(1)
                    .ok_or_else(|| StoreError::RevisionExhausted {
                        record: request.record.clone(),
                    })?,
            );
        data.snapshots.insert(
            request.record.clone(),
            SnapshotEnvelope {
                record: request.record.clone(),
                schema: request.schema,
                schema_version: request.schema_version,
                revision,
                payload: request.payload,
                updated_at_unix_ms: request.updated_at_unix_ms,
            },
        );
        receipts.snapshots.insert(
            request.request_id,
            SnapshotReceipt {
                record: request.record,
                fingerprint,
                revision,
            },
        );
        Ok(SnapshotWriteOutcome::Applied { revision })
    }

    async fn enqueue_snapshot(&self, request: SnapshotWrite) -> Result<(), BackendError> {
        if request.expected_revision.is_some() {
            return Err(StoreError::QueuedSnapshotRequiresUnconditionalWrite {
                record: request.record,
            }
            .into());
        }
        self.save(request).await?;
        Ok(())
    }

    async fn apply_transaction(
        &self,
        request: TransactionalWrite,
    ) -> Result<TransactionalWriteOutcome, BackendError> {
        if request.operation_id.trim().is_empty() {
            return Err(StoreError::EmptyOperationId.into());
        }
        let receipt_index = self.receipt_index(&request.operation_id);
        let mut receipts = self.receipt_shards[receipt_index].lock().await;
        Self::claim_operation(&mut receipts, &request.operation_id, "single")?;
        let fingerprint = TransactionFingerprint::from_request(&request);
        if let Some(receipt) = receipts.transactions.get(&request.operation_id) {
            if receipt.fingerprint != fingerprint {
                return Err(StoreError::OperationIdConflict {
                    operation_id: request.operation_id,
                }
                .into());
            }
            return Ok(TransactionalWriteOutcome::Duplicate {
                new_revision: receipt.new_revision,
                result: receipt.result.clone(),
            });
        }

        let mut data = self.data_shards[self.data_index(&request.record)]
            .lock()
            .await;
        let actual = data
            .snapshots
            .get(&request.record)
            .map(|snapshot| snapshot.revision)
            .unwrap_or(Revision::ZERO);
        if actual != request.expected_revision {
            return Err(StoreError::RevisionConflict {
                record: request.record,
                expected: Some(request.expected_revision),
                actual,
            }
            .into());
        }
        let new_revision =
            Revision(
                actual
                    .0
                    .checked_add(1)
                    .ok_or_else(|| StoreError::RevisionExhausted {
                        record: request.record.clone(),
                    })?,
            );
        let result = request.result.clone();
        data.snapshots.insert(
            request.record.clone(),
            SnapshotEnvelope {
                record: request.record.clone(),
                schema: request.schema,
                schema_version: request.schema_version,
                revision: new_revision,
                payload: request.payload,
                updated_at_unix_ms: request.updated_at_unix_ms,
            },
        );
        receipts
            .operation_kinds
            .insert(request.operation_id.clone(), "single");
        receipts.transactions.insert(
            request.operation_id,
            StoredTransactionReceipt {
                fingerprint,
                new_revision,
                result: result.clone(),
            },
        );
        Ok(TransactionalWriteOutcome::Applied {
            new_revision,
            result,
        })
    }

    async fn load_transaction(
        &self,
        operation_id: &str,
        record: &RecordKey,
    ) -> Result<Option<TransactionReceipt>, BackendError> {
        if operation_id.trim().is_empty() {
            return Err(StoreError::EmptyOperationId.into());
        }
        let receipts = self.receipt_shards[self.receipt_index(operation_id)]
            .lock()
            .await;
        let Some(receipt) = receipts.transactions.get(operation_id) else {
            return Ok(None);
        };
        if &receipt.fingerprint.record != record {
            return Err(StoreError::OperationIdConflict {
                operation_id: operation_id.to_string(),
            }
            .into());
        }
        Ok(Some(TransactionReceipt {
            operation_id: operation_id.to_string(),
            record: record.clone(),
            new_revision: receipt.new_revision,
            result: receipt.result.clone(),
        }))
    }

    async fn apply_multi_transaction(
        &self,
        request: MultiRecordTransactionalWrite,
    ) -> Result<MultiRecordTransactionalWriteOutcome, BackendError> {
        self.commit_records(request, CommitEffects::default()).await
    }

    async fn commit_records(
        &self,
        request: MultiRecordTransactionalWrite,
        effects: CommitEffects,
    ) -> Result<MultiRecordTransactionalWriteOutcome, BackendError> {
        let effects = effects.normalize()?;
        let request = Self::normalize_multi(request)?;
        let receipt_index = self.receipt_index(&request.operation_id);
        let mut receipts = self.receipt_shards[receipt_index].lock().await;
        Self::claim_operation(&mut receipts, &request.operation_id, "multi")?;
        let fingerprint = MultiTransactionFingerprint::from_request(&request);
        if let Some(receipt) = receipts.multi_transactions.get(&request.operation_id) {
            if receipt.fingerprint != fingerprint || receipt.effects != effects {
                return Err(StoreError::OperationIdConflict {
                    operation_id: request.operation_id,
                }
                .into());
            }
            return Ok(MultiRecordTransactionalWriteOutcome::Duplicate {
                records: receipt.records.clone(),
                result: receipt.result.clone(),
            });
        }

        let mut identifiers = if effects.is_empty() {
            None
        } else {
            Some(self.trade_identifiers.lock().await)
        };
        if let Some(ids) = &identifiers {
            for append in &effects.appends {
                if ids.appends.contains_key(&append.record) {
                    return Err(StoreError::AppendRecordConflict {
                        record: append.record.clone(),
                    }
                    .into());
                }
            }
            for event in &effects.outbox_events {
                if ids.outbox_event_ids.contains(&event.event_id) {
                    return Err(StoreError::OutboxEventConflict {
                        event_id: event.event_id.clone(),
                    }
                    .into());
                }
            }
        }
        let mut data = self.lock_data_shards(&request.writes).await;
        let mut committed = Vec::with_capacity(request.writes.len());
        for write in &request.writes {
            let index = self.data_index(&write.record);
            let shard = &data
                .iter()
                .find(|(candidate, _)| *candidate == index)
                .expect("locked data shard missing")
                .1;
            let actual = shard
                .snapshots
                .get(&write.record)
                .map(|snapshot| snapshot.revision)
                .unwrap_or(Revision::ZERO);
            if actual != write.expected_revision {
                return Err(StoreError::RevisionConflict {
                    record: write.record.clone(),
                    expected: Some(write.expected_revision),
                    actual,
                }
                .into());
            }
            let new_revision =
                Revision(
                    actual
                        .0
                        .checked_add(1)
                        .ok_or_else(|| StoreError::RevisionExhausted {
                            record: write.record.clone(),
                        })?,
                );
            committed.push(TransactionRecordReceipt {
                record: write.record.clone(),
                new_revision,
            });
        }
        for (write, receipt) in request.writes.iter().zip(&committed) {
            let index = self.data_index(&write.record);
            let shard = &mut data
                .iter_mut()
                .find(|(candidate, _)| *candidate == index)
                .expect("locked data shard missing")
                .1;
            shard.snapshots.insert(
                write.record.clone(),
                SnapshotEnvelope {
                    record: write.record.clone(),
                    schema: write.schema.clone(),
                    schema_version: write.schema_version,
                    revision: receipt.new_revision,
                    payload: write.payload.clone(),
                    updated_at_unix_ms: write.updated_at_unix_ms,
                },
            );
        }
        receipts
            .operation_kinds
            .insert(request.operation_id.clone(), "multi");
        if let Some(ids) = &mut identifiers {
            for append in &effects.appends {
                ids.appends.insert(append.record.clone(), append.clone());
            }
            for event in &effects.outbox_events {
                ids.outbox_event_ids.insert(event.event_id.clone());
            }
        }
        receipts.multi_transactions.insert(
            request.operation_id,
            StoredMultiTransactionReceipt {
                effects,
                fingerprint,
                records: committed.clone(),
                result: request.result.clone(),
            },
        );
        Ok(MultiRecordTransactionalWriteOutcome::Applied {
            records: committed,
            result: request.result,
        })
    }

    async fn load_multi_transaction(
        &self,
        operation_id: &str,
        records: &[RecordKey],
    ) -> Result<Option<MultiRecordTransactionReceipt>, BackendError> {
        if operation_id.trim().is_empty() {
            return Err(StoreError::EmptyOperationId.into());
        }
        let mut expected = records.to_vec();
        expected.sort_by(|left, right| {
            left.namespace
                .cmp(&right.namespace)
                .then_with(|| left.key.cmp(&right.key))
        });
        let receipts = self.receipt_shards[self.receipt_index(operation_id)]
            .lock()
            .await;
        let Some(receipt) = receipts.multi_transactions.get(operation_id) else {
            return Ok(None);
        };
        let actual = receipt
            .records
            .iter()
            .map(|record| record.record.clone())
            .collect::<Vec<_>>();
        if actual != expected {
            return Err(StoreError::OperationIdConflict {
                operation_id: operation_id.to_string(),
            }
            .into());
        }
        Ok(Some(MultiRecordTransactionReceipt {
            operation_id: operation_id.to_string(),
            records: receipt.records.clone(),
            result: receipt.result.clone(),
        }))
    }

    async fn load_trade(&self, trade_id: &str) -> Result<Option<TradeEnvelope>, BackendError> {
        if trade_id.trim().is_empty() {
            return Err(StoreError::EmptyTradeId.into());
        }
        let shard = self.trade_shards[self.trade_index(trade_id)].lock().await;
        Ok(shard.trades.get(trade_id).cloned())
    }

    async fn apply_trade_transaction(
        &self,
        request: TradeTransaction,
    ) -> Result<TradeTransactionOutcome, BackendError> {
        let request = normalize_trade_transaction(request)?;
        let receipt_index = self.receipt_index(&request.operation_id);
        let mut receipts = self.receipt_shards[receipt_index].lock().await;
        Self::claim_operation(&mut receipts, &request.operation_id, "trade")?;
        if let Some(stored) = receipts.trade_transactions.get(&request.operation_id) {
            if stored.request != request {
                return Err(StoreError::OperationIdConflict {
                    operation_id: request.operation_id,
                }
                .into());
            }
            return Ok(TradeTransactionOutcome::Duplicate(stored.receipt.clone()));
        }

        // PostgreSQL gives posting_id and event_id global primary keys. Keep the volatile backend
        // behavior identical even when two trades hash to different trade shards.
        let mut identifiers = self.trade_identifiers.lock().await;
        let trade_index = self.trade_index(&request.transition.trade_id);
        let mut trades = self.trade_shards[trade_index].lock().await;
        let current = trades.trades.get(&request.transition.trade_id);
        let actual_version = current.map_or(Revision::ZERO, |trade| trade.version);
        let actual_state = current.map(|trade| trade.state);
        if actual_version != request.transition.expected_version {
            return Err(StoreError::TradeVersionConflict {
                trade_id: request.transition.trade_id,
                expected: request.transition.expected_version,
                actual: actual_version,
            }
            .into());
        }
        if actual_state != request.transition.expected_state {
            return Err(StoreError::TradeStateConflict {
                trade_id: request.transition.trade_id,
                expected: request.transition.expected_state,
                actual: actual_state,
            }
            .into());
        }
        for posting in &request.ledger_postings {
            if identifiers.ledger_posting_ids.contains(&posting.posting_id) {
                return Err(StoreError::LedgerPostingConflict {
                    posting_id: posting.posting_id.clone(),
                }
                .into());
            }
        }
        for event in &request.outbox_events {
            if identifiers.outbox_event_ids.contains(&event.event_id) {
                return Err(StoreError::OutboxEventConflict {
                    event_id: event.event_id.clone(),
                }
                .into());
            }
        }

        let mut data = self.lock_data_shards(&request.writes).await;
        let mut committed = Vec::with_capacity(request.writes.len());
        for write in &request.writes {
            let index = self.data_index(&write.record);
            let shard = &data
                .iter()
                .find(|(candidate, _)| *candidate == index)
                .expect("locked data shard missing")
                .1;
            let actual = shard
                .snapshots
                .get(&write.record)
                .map_or(Revision::ZERO, |snapshot| snapshot.revision);
            if actual != write.expected_revision {
                return Err(StoreError::RevisionConflict {
                    record: write.record.clone(),
                    expected: Some(write.expected_revision),
                    actual,
                }
                .into());
            }
            committed.push(TransactionRecordReceipt {
                record: write.record.clone(),
                new_revision: Revision(actual.0.checked_add(1).ok_or_else(|| {
                    StoreError::RevisionExhausted {
                        record: write.record.clone(),
                    }
                })?),
            });
        }
        let new_trade_version = Revision(actual_version.0.checked_add(1).ok_or_else(|| {
            StoreError::TradeVersionExhausted {
                trade_id: request.transition.trade_id.clone(),
            }
        })?);

        for (write, receipt) in request.writes.iter().zip(&committed) {
            let index = self.data_index(&write.record);
            let shard = &mut data
                .iter_mut()
                .find(|(candidate, _)| *candidate == index)
                .expect("locked data shard missing")
                .1;
            shard.snapshots.insert(
                write.record.clone(),
                SnapshotEnvelope {
                    record: write.record.clone(),
                    schema: write.schema.clone(),
                    schema_version: write.schema_version,
                    revision: receipt.new_revision,
                    payload: write.payload.clone(),
                    updated_at_unix_ms: write.updated_at_unix_ms,
                },
            );
        }
        trades.trades.insert(
            request.transition.trade_id.clone(),
            TradeEnvelope {
                trade_id: request.transition.trade_id.clone(),
                version: new_trade_version,
                state: request.transition.next_state,
                payload: request.transition.payload.clone(),
                updated_at_unix_ms: request.transition.updated_at_unix_ms,
            },
        );
        identifiers.ledger_posting_ids.extend(
            request
                .ledger_postings
                .iter()
                .map(|posting| posting.posting_id.clone()),
        );
        identifiers.outbox_event_ids.extend(
            request
                .outbox_events
                .iter()
                .map(|event| event.event_id.clone()),
        );
        let receipt = TradeReceipt {
            operation_id: request.operation_id.clone(),
            trade_id: request.transition.trade_id.clone(),
            new_trade_version,
            state: request.transition.next_state,
            records: committed,
            ledger_posting_ids: request
                .ledger_postings
                .iter()
                .map(|posting| posting.posting_id.clone())
                .collect(),
            outbox_event_ids: request
                .outbox_events
                .iter()
                .map(|event| event.event_id.clone())
                .collect(),
            result: request.result.clone(),
        };
        receipts
            .operation_kinds
            .insert(request.operation_id.clone(), "trade");
        receipts.trade_transactions.insert(
            request.operation_id.clone(),
            StoredTradeTransactionReceipt {
                request,
                receipt: receipt.clone(),
            },
        );
        Ok(TradeTransactionOutcome::Applied(receipt))
    }

    async fn load_trade_transaction(
        &self,
        operation_id: &str,
        trade_id: &str,
    ) -> Result<Option<TradeReceipt>, BackendError> {
        if operation_id.trim().is_empty() {
            return Err(StoreError::EmptyOperationId.into());
        }
        if trade_id.trim().is_empty() {
            return Err(StoreError::EmptyTradeId.into());
        }
        let receipts = self.receipt_shards[self.receipt_index(operation_id)]
            .lock()
            .await;
        let Some(stored) = receipts.trade_transactions.get(operation_id) else {
            return Ok(None);
        };
        if stored.receipt.trade_id != trade_id {
            return Err(StoreError::OperationIdConflict {
                operation_id: operation_id.to_string(),
            }
            .into());
        }
        Ok(Some(stored.receipt.clone()))
    }
}
