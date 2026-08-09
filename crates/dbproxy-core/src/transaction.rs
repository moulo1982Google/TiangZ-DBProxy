//! 关键业务事务的通用契约。
//! Generic contract for critical business transactions.
//!
//! 事务写入和普通快照写入是两种不同语义：事务必须带有期望版本，
//! 并把可重试的业务结果一起保存；它不理解货币、道具或任务，只保存二进制数据。
//! Transactional writes differ from ordinary snapshots: they require an expected revision
//! and persist the retryable business result together. This module remains business-agnostic.

use std::collections::HashMap;

use async_trait::async_trait;

use super::{RecordKey, Revision, SnapshotEnvelope, StoreError};

/// 一次关键业务操作及其提交后的权威快照。
/// One critical operation and the authoritative snapshot produced by it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionalWrite {
    /// 调用方为同一个业务操作生成的稳定幂等号；重试必须复用。
    /// Stable idempotency key generated for one business operation; retries must reuse it.
    pub operation_id: String,
    pub record: RecordKey,
    pub schema: String,
    pub schema_version: u32,
    /// 操作执行前读到的版本；`Revision::ZERO`表示创建首个记录。
    /// Version read before executing the operation; `Revision::ZERO` creates the first record.
    pub expected_revision: Revision,
    /// 操作完成后的完整领域快照，而不是局部字段补丁。
    /// Complete domain snapshot after the operation, not a partial field patch.
    pub payload: Vec<u8>,
    /// 提交后返回给调用方的业务结果，例如实际发放数量或新余额。
    /// Durable business result returned to the caller, such as granted count or new balance.
    pub result: Vec<u8>,
    pub updated_at_unix_ms: u64,
}

/// 已提交事务的可恢复回执；只暴露调用方重试所需的版本和业务结果。
/// Recoverable receipt for a committed transaction; only the revision and
/// business result required by a retry are exposed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionReceipt {
    pub operation_id: String,
    pub record: RecordKey,
    pub new_revision: Revision,
    pub result: Vec<u8>,
}

/// 事务提交结果；Duplicate 返回第一次提交保存的原始结果。
/// Transaction outcome; Duplicate returns the original committed result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransactionalWriteOutcome {
    Applied {
        new_revision: Revision,
        result: Vec<u8>,
    },
    Duplicate {
        new_revision: Revision,
        result: Vec<u8>,
    },
}

/// 同步事务适配器，主要用于纯内存契约测试。
/// Synchronous transaction adapter, primarily for in-memory contract tests.
pub trait TransactionStore {
    fn load_receipt(
        &self,
        operation_id: &str,
        record: &RecordKey,
    ) -> Result<Option<TransactionReceipt>, StoreError>;

    fn apply(
        &mut self,
        request: TransactionalWrite,
    ) -> Result<TransactionalWriteOutcome, StoreError>;
}

/// 真实数据库适配器使用的异步事务接口。
/// Async transaction contract used by real database adapters.
#[async_trait]
pub trait AsyncTransactionalStore {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn load_receipt(
        &self,
        operation_id: &str,
        record: &RecordKey,
    ) -> Result<Option<TransactionReceipt>, Self::Error>;

    async fn apply(
        &mut self,
        request: TransactionalWrite,
    ) -> Result<TransactionalWriteOutcome, Self::Error>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TransactionFingerprint {
    record: RecordKey,
    schema: String,
    schema_version: u32,
    expected_revision: Revision,
    payload: Vec<u8>,
    result: Vec<u8>,
    updated_at_unix_ms: u64,
}

impl TransactionFingerprint {
    fn from_request(request: &TransactionalWrite) -> Self {
        Self {
            record: request.record.clone(),
            schema: request.schema.clone(),
            schema_version: request.schema_version,
            expected_revision: request.expected_revision,
            payload: request.payload.clone(),
            result: request.result.clone(),
            updated_at_unix_ms: request.updated_at_unix_ms,
        }
    }
}

#[derive(Clone, Debug)]
struct StoredTransactionReceipt {
    fingerprint: TransactionFingerprint,
    new_revision: Revision,
    result: Vec<u8>,
}

/// 仅用于契约测试和本地开发的内存实现，不提供重启恢复能力。
/// In-memory implementation for contract tests and local development; it is not restart durable.
#[derive(Default)]
pub struct InMemoryTransactionalStore {
    snapshots: HashMap<RecordKey, SnapshotEnvelope>,
    receipts: HashMap<String, StoredTransactionReceipt>,
}

impl InMemoryTransactionalStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load(&self, record: &RecordKey) -> Option<SnapshotEnvelope> {
        self.snapshots.get(record).cloned()
    }
}

impl TransactionStore for InMemoryTransactionalStore {
    fn load_receipt(
        &self,
        operation_id: &str,
        record: &RecordKey,
    ) -> Result<Option<TransactionReceipt>, StoreError> {
        if operation_id.trim().is_empty() {
            return Err(StoreError::EmptyOperationId);
        }
        let Some(receipt) = self.receipts.get(operation_id) else {
            return Ok(None);
        };
        if &receipt.fingerprint.record != record {
            return Err(StoreError::OperationIdConflict {
                operation_id: operation_id.to_string(),
            });
        }
        Ok(Some(TransactionReceipt {
            operation_id: operation_id.to_string(),
            record: record.clone(),
            new_revision: receipt.new_revision,
            result: receipt.result.clone(),
        }))
    }

    fn apply(
        &mut self,
        request: TransactionalWrite,
    ) -> Result<TransactionalWriteOutcome, StoreError> {
        if request.operation_id.trim().is_empty() {
            return Err(StoreError::EmptyOperationId);
        }

        let fingerprint = TransactionFingerprint::from_request(&request);
        if let Some(receipt) = self.receipts.get(&request.operation_id) {
            if receipt.fingerprint != fingerprint {
                return Err(StoreError::OperationIdConflict {
                    operation_id: request.operation_id,
                });
            }
            return Ok(TransactionalWriteOutcome::Duplicate {
                new_revision: receipt.new_revision,
                result: receipt.result.clone(),
            });
        }

        let actual_revision = self
            .snapshots
            .get(&request.record)
            .map(|snapshot| snapshot.revision)
            .unwrap_or(Revision::ZERO);
        if actual_revision != request.expected_revision {
            return Err(StoreError::RevisionConflict {
                record: request.record,
                expected: Some(request.expected_revision),
                actual: actual_revision,
            });
        }

        let new_revision = Revision(actual_revision.0.checked_add(1).ok_or_else(|| {
            StoreError::RevisionExhausted {
                record: request.record.clone(),
            }
        })?);
        let result = request.result.clone();
        self.snapshots.insert(
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
        self.receipts.insert(
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> RecordKey {
        RecordKey::new("player", "1001").unwrap()
    }

    fn write(
        operation_id: &str,
        expected_revision: Revision,
        payload: &[u8],
    ) -> TransactionalWrite {
        TransactionalWrite {
            operation_id: operation_id.to_string(),
            record: key(),
            schema: "player.transactional".to_string(),
            schema_version: 1,
            expected_revision,
            payload: payload.to_vec(),
            result: b"coins=100".to_vec(),
            updated_at_unix_ms: 1,
        }
    }

    #[test]
    fn first_transaction_commits_snapshot_and_result() {
        let mut store = InMemoryTransactionalStore::new();
        let result = store.apply(write("op-1", Revision::ZERO, b"v1")).unwrap();
        assert_eq!(
            result,
            TransactionalWriteOutcome::Applied {
                new_revision: Revision(1),
                result: b"coins=100".to_vec(),
            }
        );
        assert_eq!(store.load(&key()).unwrap().payload, b"v1");
    }

    #[test]
    fn retry_returns_the_original_result_without_reapplying() {
        let mut store = InMemoryTransactionalStore::new();
        let request = write("op-1", Revision::ZERO, b"v1");
        assert!(matches!(
            store.apply(request.clone()).unwrap(),
            TransactionalWriteOutcome::Applied {
                new_revision: Revision(1),
                ..
            }
        ));
        assert_eq!(
            store.apply(request).unwrap(),
            TransactionalWriteOutcome::Duplicate {
                new_revision: Revision(1),
                result: b"coins=100".to_vec(),
            }
        );
        assert_eq!(store.load(&key()).unwrap().revision, Revision(1));
    }

    #[test]
    fn failed_compare_and_swap_does_not_poison_operation_id() {
        let mut store = InMemoryTransactionalStore::new();
        store.apply(write("op-1", Revision::ZERO, b"v1")).unwrap();

        let stale = write("op-2", Revision::ZERO, b"stale");
        assert!(matches!(
            store.apply(stale.clone()),
            Err(StoreError::RevisionConflict {
                actual: Revision(1),
                ..
            })
        ));

        let mut corrected = stale;
        corrected.expected_revision = Revision(1);
        assert!(matches!(
            store.apply(corrected).unwrap(),
            TransactionalWriteOutcome::Applied {
                new_revision: Revision(2),
                ..
            }
        ));
    }

    #[test]
    fn operation_id_cannot_change_the_committed_request() {
        let mut store = InMemoryTransactionalStore::new();
        store.apply(write("op-1", Revision::ZERO, b"v1")).unwrap();
        let mut changed = write("op-1", Revision::ZERO, b"v2");
        changed.result = b"coins=999".to_vec();
        assert!(matches!(
            store.apply(changed),
            Err(StoreError::OperationIdConflict { .. })
        ));
    }

    #[test]
    fn committed_receipt_can_be_loaded_after_the_caller_loses_its_response() {
        let mut store = InMemoryTransactionalStore::new();
        store
            .apply(write("op-recover", Revision::ZERO, b"v1"))
            .unwrap();

        assert_eq!(
            store.load_receipt("op-recover", &key()).unwrap(),
            Some(TransactionReceipt {
                operation_id: "op-recover".to_string(),
                record: key(),
                new_revision: Revision(1),
                result: b"coins=100".to_vec(),
            })
        );
    }

    #[test]
    fn receipt_lookup_rejects_an_operation_owned_by_another_record() {
        let mut store = InMemoryTransactionalStore::new();
        store.apply(write("op-1", Revision::ZERO, b"v1")).unwrap();
        let other = RecordKey::new("player", "1002").unwrap();

        assert!(matches!(
            store.load_receipt("op-1", &other),
            Err(StoreError::OperationIdConflict { .. })
        ));
    }
}
