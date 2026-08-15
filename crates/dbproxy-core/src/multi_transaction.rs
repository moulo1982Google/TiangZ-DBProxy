//! 多记录原子事务契约。 / Multi-record atomic transaction contract.
//!
//! DBProxy只负责按RecordKey原子提交一组完整快照，不理解玩家、货币、道具或交易规则。
//! DBProxy atomically commits complete snapshots addressed by RecordKey; it does not understand
//! players, currencies, items, or trading rules.

use std::collections::HashMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::{RecordKey, Revision, SnapshotEnvelope, StoreError};

/// 多记录事务中的一个完整快照写入。 / One complete snapshot write in a multi-record transaction.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransactionalRecordWrite {
    pub record: RecordKey,
    pub schema: String,
    pub schema_version: u32,
    pub expected_revision: Revision,
    pub payload: Vec<u8>,
    pub updated_at_unix_ms: u64,
}

/// 一次跨记录原子事务。 / One atomic transaction spanning multiple records.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MultiRecordTransactionalWrite {
    /// 重试和Endpoint切换时必须保持不变。 / Must remain unchanged across retries and endpoint failover.
    pub operation_id: String,
    pub writes: Vec<TransactionalRecordWrite>,
    /// 提交后返回给业务的原始结果。 / Opaque result returned to the business after commit.
    pub result: Vec<u8>,
}

/// 一个已提交记录的新版本。 / New revision of one committed record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransactionRecordReceipt {
    pub record: RecordKey,
    pub new_revision: Revision,
}

/// 多记录事务的可恢复回执。 / Recoverable receipt for a multi-record transaction.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MultiRecordTransactionReceipt {
    pub operation_id: String,
    pub records: Vec<TransactionRecordReceipt>,
    pub result: Vec<u8>,
}

/// 多记录事务提交结果。 / Multi-record transaction outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MultiRecordTransactionalWriteOutcome {
    Applied {
        records: Vec<TransactionRecordReceipt>,
        result: Vec<u8>,
    },
    Duplicate {
        records: Vec<TransactionRecordReceipt>,
        result: Vec<u8>,
    },
}

/// 真实存储适配器必须实现的多记录事务接口。 / Multi-record transaction interface for durable adapters.
#[async_trait]
pub trait AsyncMultiRecordTransactionStore {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn load_multi_receipt(
        &self,
        operation_id: &str,
        records: &[RecordKey],
    ) -> Result<Option<MultiRecordTransactionReceipt>, Self::Error>;

    async fn apply_multi(
        &mut self,
        request: MultiRecordTransactionalWrite,
    ) -> Result<MultiRecordTransactionalWriteOutcome, Self::Error>;
}

#[derive(Clone, Debug)]
struct StoredReceipt {
    writes: Vec<TransactionalRecordWrite>,
    records: Vec<TransactionRecordReceipt>,
    result: Vec<u8>,
}

/// 仅用于契约测试的内存实现；不提供进程重启恢复能力。 / In-memory contract implementation;
/// it does not provide process-restart durability.
#[derive(Default)]
pub struct InMemoryMultiRecordTransactionStore {
    snapshots: HashMap<RecordKey, SnapshotEnvelope>,
    receipts: HashMap<String, StoredReceipt>,
}

impl InMemoryMultiRecordTransactionStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn normalize(
        request: MultiRecordTransactionalWrite,
    ) -> Result<MultiRecordTransactionalWrite, StoreError> {
        if request.operation_id.trim().is_empty() {
            return Err(StoreError::EmptyOperationId);
        }
        if request.writes.is_empty() {
            return Err(StoreError::EmptyTransactionRecords);
        }
        let mut request = request;
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

    pub fn load(&self, record: &RecordKey) -> Option<SnapshotEnvelope> {
        self.snapshots.get(record).cloned()
    }
}

#[async_trait]
impl AsyncMultiRecordTransactionStore for InMemoryMultiRecordTransactionStore {
    type Error = StoreError;

    async fn load_multi_receipt(
        &self,
        operation_id: &str,
        records: &[RecordKey],
    ) -> Result<Option<MultiRecordTransactionReceipt>, Self::Error> {
        if operation_id.trim().is_empty() {
            return Err(StoreError::EmptyOperationId);
        }
        let mut expected = records.to_vec();
        expected.sort_by(|left, right| {
            left.namespace
                .cmp(&right.namespace)
                .then_with(|| left.key.cmp(&right.key))
        });
        let Some(receipt) = self.receipts.get(operation_id) else {
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
            });
        }
        Ok(Some(MultiRecordTransactionReceipt {
            operation_id: operation_id.to_string(),
            records: receipt.records.clone(),
            result: receipt.result.clone(),
        }))
    }

    async fn apply_multi(
        &mut self,
        request: MultiRecordTransactionalWrite,
    ) -> Result<MultiRecordTransactionalWriteOutcome, Self::Error> {
        let request = Self::normalize(request)?;
        if let Some(receipt) = self.receipts.get(&request.operation_id) {
            let same_request = receipt.writes == request.writes && receipt.result == request.result;
            if !same_request {
                return Err(StoreError::OperationIdConflict {
                    operation_id: request.operation_id,
                });
            }
            return Ok(MultiRecordTransactionalWriteOutcome::Duplicate {
                records: receipt.records.clone(),
                result: receipt.result.clone(),
            });
        }

        let mut committed = Vec::with_capacity(request.writes.len());
        for write in &request.writes {
            let actual = self
                .snapshots
                .get(&write.record)
                .map(|snapshot| snapshot.revision)
                .unwrap_or(Revision::ZERO);
            if actual != write.expected_revision {
                return Err(StoreError::RevisionConflict {
                    record: write.record.clone(),
                    expected: Some(write.expected_revision),
                    actual,
                });
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
            self.snapshots.insert(
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
        self.receipts.insert(
            request.operation_id,
            StoredReceipt {
                writes: request.writes,
                records: committed.clone(),
                result: request.result.clone(),
            },
        );
        Ok(MultiRecordTransactionalWriteOutcome::Applied {
            records: committed,
            result: request.result,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(
        namespace: &str,
        key: &str,
        expected_revision: u64,
        payload: &[u8],
    ) -> TransactionalRecordWrite {
        TransactionalRecordWrite {
            record: RecordKey::new(namespace, key).unwrap(),
            schema: format!("{}.snapshot", namespace),
            schema_version: 1,
            expected_revision: Revision(expected_revision),
            payload: payload.to_vec(),
            updated_at_unix_ms: 1,
        }
    }

    #[tokio::test]
    async fn commits_two_records_atomically_and_retries_by_operation_id() {
        let mut store = InMemoryMultiRecordTransactionStore::new();
        let request = MultiRecordTransactionalWrite {
            operation_id: "trade-1".to_string(),
            writes: vec![
                write("player", "buyer", 0, b"item=100"),
                write("player", "seller", 0, b"coins=100"),
            ],
            result: b"trade-complete".to_vec(),
        };
        let applied = store.apply_multi(request.clone()).await.unwrap();
        assert!(matches!(
            applied,
            MultiRecordTransactionalWriteOutcome::Applied { records, .. }
                if records.len() == 2
        ));
        let duplicate = store.apply_multi(request).await.unwrap();
        assert!(matches!(
            duplicate,
            MultiRecordTransactionalWriteOutcome::Duplicate { records, result }
                if records.len() == 2 && result == b"trade-complete"
        ));
    }

    #[tokio::test]
    async fn stale_second_record_does_not_modify_the_first_record() {
        let mut store = InMemoryMultiRecordTransactionStore::new();
        store
            .apply_multi(MultiRecordTransactionalWrite {
                operation_id: "seed".to_string(),
                writes: vec![write("player", "seller", 0, b"v1")],
                result: Vec::new(),
            })
            .await
            .unwrap();
        let error = store
            .apply_multi(MultiRecordTransactionalWrite {
                operation_id: "trade-2".to_string(),
                writes: vec![
                    write("player", "buyer", 0, b"v1"),
                    write("player", "seller", 0, b"stale"),
                ],
                result: Vec::new(),
            })
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::RevisionConflict { .. }));
        assert!(
            store
                .load(&RecordKey::new("player", "buyer").unwrap())
                .is_none()
        );
    }
}
