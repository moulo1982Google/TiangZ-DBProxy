//! DBProxy 的稳定核心契约。
//!
//! 本 crate 只定义与游戏无关的快照、Revision、CAS 和幂等语义，
//! 不依赖 TiangZ Runtime、TypeScript、Redis 或具体数据库。
//! Adapters and the network service are intentionally kept outside this crate.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

mod flush;
mod transaction;

pub use flush::{SnapshotFlushError, SnapshotFlushQueue, SnapshotFlushReport};
pub use transaction::{
    AsyncTransactionalStore, InMemoryTransactionalStore, TransactionReceipt, TransactionStore,
    TransactionalWrite, TransactionalWriteOutcome,
};

/// 持久化记录的逻辑地址；同一个 namespace 下的 key 必须唯一。
/// Logical address of one persisted record; keys are unique within a namespace.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct RecordKey {
    pub namespace: String,
    pub key: String,
}

impl RecordKey {
    /// 创建并校验一个记录地址。
    /// Creates and validates a record address.
    pub fn new(namespace: impl Into<String>, key: impl Into<String>) -> Result<Self, StoreError> {
        let address = Self {
            namespace: namespace.into(),
            key: key.into(),
        };
        if address.namespace.trim().is_empty() {
            return Err(StoreError::InvalidKey("namespace is empty"));
        }
        if address.key.trim().is_empty() {
            return Err(StoreError::InvalidKey("key is empty"));
        }
        Ok(address)
    }
}

/// 单调递增的持久化版本号；Revision 只由 DBProxy 生成。
/// Monotonic persistence version; DBProxy is the only component that allocates it.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Revision(pub u64);

impl Revision {
    pub const ZERO: Self = Self(0);
}

/// DBProxy 返回给业务侧的完整快照。
/// Complete snapshot returned by DBProxy to a business service.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotEnvelope {
    pub record: RecordKey,
    pub schema: String,
    pub schema_version: u32,
    pub revision: Revision,
    pub payload: Vec<u8>,
    pub updated_at_unix_ms: u64,
}

/// 一次快照写入请求。
/// One snapshot write request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotWrite {
    /// 调用方生成并在重试时保持不变的幂等请求号。
    /// Caller-generated idempotency key; retries must reuse the same value.
    pub request_id: String,
    pub record: RecordKey,
    pub schema: String,
    pub schema_version: u32,
    pub payload: Vec<u8>,
    /// `None` 表示不做版本条件；`Some(0)` 表示只允许创建，`Some(n)` 表示 CAS 更新。
    /// `None` skips version matching; `Some(0)` means create-only and `Some(n)` is a CAS update.
    pub expected_revision: Option<Revision>,
    pub updated_at_unix_ms: u64,
}

/// 写入结果；Duplicate 不会再次修改记录版本。
/// Write result; Duplicate never applies the record a second time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotWriteOutcome {
    Applied { revision: Revision },
    Duplicate { revision: Revision },
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum StoreError {
    #[error("invalid record key: {0}")]
    InvalidKey(&'static str),
    #[error("idempotency request is empty")]
    EmptyRequestId,
    #[error("transaction operation id is empty")]
    EmptyOperationId,
    #[error("idempotency request {request_id} was already used for another record")]
    IdempotencyConflict { request_id: String },
    #[error("transaction operation {operation_id} was already used for another request")]
    OperationIdConflict { operation_id: String },
    #[error("queued snapshot {record:?} must not carry an expected revision")]
    QueuedSnapshotRequiresUnconditionalWrite { record: RecordKey },
    #[error("revision conflict for {record:?}: expected {expected:?}, actual {actual:?}")]
    RevisionConflict {
        record: RecordKey,
        expected: Option<Revision>,
        actual: Revision,
    },
    #[error("revision exhausted for {record:?}")]
    RevisionExhausted { record: RecordKey },
}

/// DBProxy 适配器必须实现的最小快照接口。
/// Minimal snapshot interface implemented by DBProxy adapters.
pub trait SnapshotStore {
    fn load(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, StoreError>;

    fn save(&mut self, request: SnapshotWrite) -> Result<SnapshotWriteOutcome, StoreError>;
}

/// 真实存储适配器使用的异步快照接口。
/// Async snapshot contract used by real storage adapters.
///
/// `SnapshotStore` 保留给同步测试和纯内存实现；网络或数据库适配器必须使用本接口，
/// 避免把阻塞 I/O 带入 TiangZ 的业务线程。
/// `SnapshotStore` remains for synchronous tests and memory-only implementations;
/// network and database adapters use this contract so blocking I/O stays out of the game thread.
#[async_trait::async_trait]
pub trait AsyncSnapshotStore {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn load(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, Self::Error>;

    async fn save(&mut self, request: SnapshotWrite) -> Result<SnapshotWriteOutcome, Self::Error>;
}

#[derive(Clone, Debug)]
struct IdempotencyReceipt {
    record: RecordKey,
    fingerprint: RequestFingerprint,
    revision: Revision,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RequestFingerprint {
    schema: String,
    schema_version: u32,
    payload: Vec<u8>,
    expected_revision: Option<Revision>,
}

impl RequestFingerprint {
    fn from_request(request: &SnapshotWrite) -> Self {
        Self {
            schema: request.schema.clone(),
            schema_version: request.schema_version,
            payload: request.payload.clone(),
            expected_revision: request.expected_revision,
        }
    }
}

/// 仅用于契约测试和本地开发的内存实现，不提供进程重启后的持久性。
/// In-memory implementation for contract tests and local development; it is not restart durable.
#[derive(Default)]
pub struct InMemorySnapshotStore {
    snapshots: HashMap<RecordKey, SnapshotEnvelope>,
    receipts: HashMap<String, IdempotencyReceipt>,
}

impl InMemorySnapshotStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl SnapshotStore for InMemorySnapshotStore {
    fn load(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, StoreError> {
        Ok(self.snapshots.get(record).cloned())
    }

    fn save(&mut self, request: SnapshotWrite) -> Result<SnapshotWriteOutcome, StoreError> {
        if request.request_id.trim().is_empty() {
            return Err(StoreError::EmptyRequestId);
        }

        if let Some(receipt) = self.receipts.get(&request.request_id) {
            if receipt.record != request.record
                || receipt.fingerprint != RequestFingerprint::from_request(&request)
            {
                return Err(StoreError::IdempotencyConflict {
                    request_id: request.request_id,
                });
            }
            return Ok(SnapshotWriteOutcome::Duplicate {
                revision: receipt.revision,
            });
        }

        let actual_revision = self
            .snapshots
            .get(&request.record)
            .map(|snapshot| snapshot.revision)
            .unwrap_or(Revision::ZERO);
        if request
            .expected_revision
            .is_some_and(|expected| expected != actual_revision)
        {
            return Err(StoreError::RevisionConflict {
                record: request.record,
                expected: request.expected_revision,
                actual: actual_revision,
            });
        }

        let revision = Revision(actual_revision.0.checked_add(1).ok_or_else(|| {
            StoreError::RevisionExhausted {
                record: request.record.clone(),
            }
        })?);
        let fingerprint = RequestFingerprint::from_request(&request);
        let envelope = SnapshotEnvelope {
            record: request.record.clone(),
            schema: request.schema,
            schema_version: request.schema_version,
            revision,
            payload: request.payload,
            updated_at_unix_ms: request.updated_at_unix_ms,
        };
        self.snapshots.insert(request.record.clone(), envelope);
        self.receipts.insert(
            request.request_id,
            IdempotencyReceipt {
                record: request.record,
                fingerprint,
                revision,
            },
        );
        Ok(SnapshotWriteOutcome::Applied { revision })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> RecordKey {
        RecordKey::new("player", "1001").unwrap()
    }

    fn write(
        request_id: &str,
        expected_revision: Option<Revision>,
        payload: &[u8],
    ) -> SnapshotWrite {
        SnapshotWrite {
            request_id: request_id.to_string(),
            record: key(),
            schema: "player.snapshot".to_string(),
            schema_version: 1,
            payload: payload.to_vec(),
            expected_revision,
            updated_at_unix_ms: 1,
        }
    }

    #[test]
    fn first_write_allocates_revision_one() {
        let mut store = InMemorySnapshotStore::new();
        let result = store
            .save(write("req-1", Some(Revision::ZERO), b"v1"))
            .unwrap();
        assert_eq!(
            result,
            SnapshotWriteOutcome::Applied {
                revision: Revision(1)
            }
        );
        assert_eq!(store.load(&key()).unwrap().unwrap().payload, b"v1");
    }

    #[test]
    fn compare_and_swap_rejects_stale_revision() {
        let mut store = InMemorySnapshotStore::new();
        store
            .save(write("req-1", Some(Revision::ZERO), b"v1"))
            .unwrap();
        let error = store
            .save(write("req-2", Some(Revision::ZERO), b"stale"))
            .unwrap_err();
        assert!(matches!(
            error,
            StoreError::RevisionConflict {
                actual: Revision(1),
                ..
            }
        ));
        assert_eq!(store.load(&key()).unwrap().unwrap().payload, b"v1");
    }

    #[test]
    fn retry_is_idempotent_and_does_not_increment_revision() {
        let mut store = InMemorySnapshotStore::new();
        let request = write("req-1", Some(Revision::ZERO), b"v1");
        assert_eq!(
            store.save(request.clone()).unwrap(),
            SnapshotWriteOutcome::Applied {
                revision: Revision(1)
            }
        );
        assert_eq!(
            store.save(request).unwrap(),
            SnapshotWriteOutcome::Duplicate {
                revision: Revision(1)
            }
        );
        assert_eq!(store.load(&key()).unwrap().unwrap().revision, Revision(1));
    }

    #[test]
    fn request_id_cannot_be_reused_for_another_record() {
        let mut store = InMemorySnapshotStore::new();
        store
            .save(write("req-1", Some(Revision::ZERO), b"v1"))
            .unwrap();
        let mut request = write("req-1", Some(Revision(1)), b"v2");
        request.record = RecordKey::new("player", "1002").unwrap();
        assert!(matches!(
            store.save(request),
            Err(StoreError::IdempotencyConflict { .. })
        ));
    }

    #[test]
    fn request_id_cannot_be_reused_with_different_payload() {
        let mut store = InMemorySnapshotStore::new();
        store
            .save(write("req-1", Some(Revision::ZERO), b"v1"))
            .unwrap();
        assert!(matches!(
            store.save(write("req-1", Some(Revision::ZERO), b"v2")),
            Err(StoreError::IdempotencyConflict { .. })
        ));
        assert_eq!(store.load(&key()).unwrap().unwrap().payload, b"v1");
    }
}
