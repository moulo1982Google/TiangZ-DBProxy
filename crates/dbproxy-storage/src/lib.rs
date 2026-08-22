//! DBProxy 的真实存储适配器。
//!
//! PostgreSQL 是唯一权威写入端；Redis 只保存已经提交的快照缓存。
//! PostgreSQL is the only authoritative write target; Redis caches committed snapshots only.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use thiserror::Error;
use tiangz_dbproxy_core::{
    AsyncMultiRecordTransactionStore, AsyncSnapshotStore, AsyncTransactionalStore,
    MultiRecordTransactionReceipt, MultiRecordTransactionalWrite,
    MultiRecordTransactionalWriteOutcome, RecordKey, Revision, SnapshotEnvelope, SnapshotWrite,
    SnapshotWriteOutcome, StoreError, TransactionReceipt, TransactionRecordReceipt,
    TransactionalRecordWrite, TransactionalWrite, TransactionalWriteOutcome,
};
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls, Row};

mod backlog;

pub use backlog::{RedisSnapshotBacklog, SnapshotBacklogAck, SnapshotBacklogLease};

const SNAPSHOT_MIGRATION: &str = include_str!("../migrations/001_snapshot.sql");
const TRANSACTION_MIGRATION: &str = include_str!("../migrations/002_transactional.sql");
const MULTI_TRANSACTION_MIGRATION: &str = include_str!("../migrations/003_multi_transactional.sql");
const MIGRATION_LOCK_ID: i64 = 8_390_417_203;

/// 存储适配器错误；PostgreSQL 错误不会被包装成“保存成功”。
/// Adapter error; PostgreSQL failures are never reported as successful writes.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error(transparent)]
    Core(#[from] StoreError),
    #[error("postgres error: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("snapshot cache codec error: {0}")]
    Codec(String),
    #[error("persisted revision is invalid for {record:?}: {value}")]
    InvalidRevision { record: RecordKey, value: i64 },
    #[error("persisted timestamp is invalid for {record:?}: {value}")]
    InvalidTimestamp { record: RecordKey, value: i64 },
    #[error("schema version is too large for {record:?}")]
    SchemaVersionTooLarge { record: RecordKey },
    #[error("revision is too large for {record:?}")]
    RevisionTooLarge { record: RecordKey },
    #[error("timestamp is too large for {record:?}")]
    TimestampTooLarge { record: RecordKey },
    #[error("snapshot disappeared after a successful write: {record:?}")]
    MissingAfterWrite { record: RecordKey },
    #[error("redis cache update failed after PostgreSQL commit: {0}")]
    CacheSync(String),
    #[error("backlog clock error: {0}")]
    BacklogClock(String),
    #[error("backlog timestamp is too large")]
    BacklogTimestampTooLarge,
    #[error("backlog lease must be greater than zero")]
    InvalidBacklogLease,
    #[error("backlog lease duration is too large: {lease_ms}ms")]
    BacklogLeaseTooLarge { lease_ms: u64 },
    #[error("backlog protocol error: {0}")]
    BacklogProtocol(String),
}

fn validate_request(request: &SnapshotWrite) -> Result<(), StorageError> {
    if request.request_id.trim().is_empty() {
        return Err(StoreError::EmptyRequestId.into());
    }
    if request.record.namespace.trim().is_empty() {
        return Err(StoreError::InvalidKey("namespace is empty").into());
    }
    if request.record.key.trim().is_empty() {
        return Err(StoreError::InvalidKey("key is empty").into());
    }
    Ok(())
}

fn validate_transaction_request(request: &TransactionalWrite) -> Result<(), StorageError> {
    if request.operation_id.trim().is_empty() {
        return Err(StoreError::EmptyOperationId.into());
    }
    if request.record.namespace.trim().is_empty() {
        return Err(StoreError::InvalidKey("namespace is empty").into());
    }
    if request.record.key.trim().is_empty() {
        return Err(StoreError::InvalidKey("key is empty").into());
    }
    Ok(())
}

fn validate_receipt_lookup(operation_id: &str, record: &RecordKey) -> Result<(), StorageError> {
    if operation_id.trim().is_empty() {
        return Err(StoreError::EmptyOperationId.into());
    }
    if record.namespace.trim().is_empty() {
        return Err(StoreError::InvalidKey("namespace is empty").into());
    }
    if record.key.trim().is_empty() {
        return Err(StoreError::InvalidKey("key is empty").into());
    }
    Ok(())
}

fn validate_multi_transaction_request(
    request: &MultiRecordTransactionalWrite,
) -> Result<(), StorageError> {
    if request.operation_id.trim().is_empty() {
        return Err(StoreError::EmptyOperationId.into());
    }
    if request.writes.is_empty() {
        return Err(StoreError::EmptyTransactionRecords.into());
    }
    let mut records = request
        .writes
        .iter()
        .map(|write| write.record.clone())
        .collect::<Vec<_>>();
    records.sort_by(|left, right| {
        left.namespace
            .cmp(&right.namespace)
            .then_with(|| left.key.cmp(&right.key))
    });
    for pair in records.windows(2) {
        if pair[0] == pair[1] {
            return Err(StoreError::DuplicateTransactionRecord {
                record: pair[0].clone(),
            }
            .into());
        }
    }
    for write in &request.writes {
        if write.record.namespace.trim().is_empty() {
            return Err(StoreError::InvalidKey("namespace is empty").into());
        }
        if write.record.key.trim().is_empty() {
            return Err(StoreError::InvalidKey("key is empty").into());
        }
        if write.schema.trim().is_empty() {
            return Err(StoreError::InvalidKey("schema is empty").into());
        }
    }
    Ok(())
}

fn revision_to_i64(
    record: &RecordKey,
    revision: Option<Revision>,
) -> Result<Option<i64>, StorageError> {
    revision
        .map(|value| {
            i64::try_from(value.0).map_err(|_| StorageError::RevisionTooLarge {
                record: record.clone(),
            })
        })
        .transpose()
}

fn required_revision_to_i64(record: &RecordKey, revision: Revision) -> Result<i64, StorageError> {
    i64::try_from(revision.0).map_err(|_| StorageError::RevisionTooLarge {
        record: record.clone(),
    })
}

fn schema_version_to_i64(version: u32) -> i64 {
    i64::from(version)
}

fn timestamp_to_i64(record: &RecordKey, timestamp: u64) -> Result<i64, StorageError> {
    i64::try_from(timestamp).map_err(|_| StorageError::TimestampTooLarge {
        record: record.clone(),
    })
}

fn revision_from_i64(record: &RecordKey, value: i64) -> Result<Revision, StorageError> {
    if value < 0 {
        return Err(StorageError::InvalidRevision {
            record: record.clone(),
            value,
        });
    }
    Ok(Revision(value as u64))
}

fn timestamp_from_i64(record: &RecordKey, value: i64) -> Result<u64, StorageError> {
    if value < 0 {
        return Err(StorageError::InvalidTimestamp {
            record: record.clone(),
            value,
        });
    }
    Ok(value as u64)
}

fn snapshot_from_row(row: &Row) -> Result<SnapshotEnvelope, StorageError> {
    let record = RecordKey {
        namespace: row.get(0),
        key: row.get(1),
    };
    Ok(SnapshotEnvelope {
        record: record.clone(),
        schema: row.get(2),
        schema_version: row.get::<_, i64>(3).try_into().map_err(|_| {
            StorageError::SchemaVersionTooLarge {
                record: record.clone(),
            }
        })?,
        revision: revision_from_i64(&record, row.get(4))?,
        payload: row.get(5),
        updated_at_unix_ms: timestamp_from_i64(&record, row.get(6))?,
    })
}

fn idempotency_matches(
    row: &Row,
    request: &SnapshotWrite,
    schema_version: i64,
    expected: Option<i64>,
) -> bool {
    row.get::<_, String>(0) == request.record.namespace
        && row.get::<_, String>(1) == request.record.key
        && row.get::<_, String>(2) == request.schema
        && row.get::<_, i64>(3) == schema_version
        && row.get::<_, Vec<u8>>(4) == request.payload
        && row.get::<_, Option<i64>>(5) == expected
}

fn transaction_matches(
    row: &Row,
    request: &TransactionalWrite,
    schema_version: i64,
    expected_revision: i64,
    updated_at_unix_ms: i64,
) -> bool {
    row.get::<_, String>(0) == request.record.namespace
        && row.get::<_, String>(1) == request.record.key
        && row.get::<_, String>(2) == request.schema
        && row.get::<_, i64>(3) == schema_version
        && row.get::<_, i64>(4) == expected_revision
        && row.get::<_, Vec<u8>>(5) == request.payload
        && row.get::<_, Vec<u8>>(6) == request.result
        && row.get::<_, i64>(8) == updated_at_unix_ms
}

/// PostgreSQL 快照存储。
/// PostgreSQL snapshot store.
///
/// 幂等记录与快照写入在同一个 PostgreSQL 事务中提交。
/// The idempotency receipt and snapshot mutation commit in one PostgreSQL transaction.
#[derive(Clone)]
pub struct PostgresSnapshotStore {
    client: Arc<Mutex<Client>>,
}

impl PostgresSnapshotStore {
    /// 连接数据库并执行幂等表与快照表迁移。
    /// Connect and apply the snapshot/idempotency schema.
    pub async fn connect(url: &str) -> Result<Self, StorageError> {
        let (client, connection) = tokio_postgres::connect(url, NoTls).await?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!(%error, "postgres connection stopped");
            }
        });
        let store = Self {
            client: Arc::new(Mutex::new(client)),
        };
        store.migrate().await?;
        Ok(store)
    }

    /// 执行幂等的建表脚本；重复执行不会破坏已有数据。
    /// Apply an idempotent schema migration without modifying existing data.
    pub async fn migrate(&self) -> Result<(), StorageError> {
        let mut client = self.client.lock().await;
        let transaction = client.transaction().await?;
        // 多个DBProxy进程可能同时启动；事务级 advisory lock 防止DDL在 PostgreSQL 系统目录上竞争。
        // Multiple DBProxy processes may start together; a transaction advisory lock serializes DDL.
        transaction
            .execute("SELECT pg_advisory_xact_lock($1)", &[&MIGRATION_LOCK_ID])
            .await?;
        transaction.batch_execute(SNAPSHOT_MIGRATION).await?;
        transaction.batch_execute(TRANSACTION_MIGRATION).await?;
        transaction
            .batch_execute(MULTI_TRANSACTION_MIGRATION)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// 在一次数据库查询中读取多条快照，并保持调用方的记录顺序与缺失位置。
    /// Load multiple snapshots in one query while preserving input order and missing positions.
    pub async fn load_multi(
        &self,
        records: &[RecordKey],
    ) -> Result<Vec<Option<SnapshotEnvelope>>, StorageError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let namespaces = records
            .iter()
            .map(|record| record.namespace.clone())
            .collect::<Vec<_>>();
        let keys = records
            .iter()
            .map(|record| record.key.clone())
            .collect::<Vec<_>>();
        let client = self.client.lock().await;
        let rows = client
            .query(
                "SELECT namespace, record_key, schema_name, schema_version, revision, payload, updated_at_unix_ms FROM dbproxy_snapshots WHERE (namespace, record_key) IN (SELECT * FROM unnest($1::TEXT[], $2::TEXT[]))",
                &[&namespaces, &keys],
            )
            .await?;
        let mut snapshots = HashMap::with_capacity(rows.len());
        for row in &rows {
            let snapshot = snapshot_from_row(row)?;
            snapshots.insert(snapshot.record.clone(), snapshot);
        }
        Ok(records
            .iter()
            .map(|record| snapshots.remove(record))
            .collect())
    }
}

#[async_trait]
impl AsyncSnapshotStore for PostgresSnapshotStore {
    type Error = StorageError;

    async fn load(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, Self::Error> {
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                "SELECT namespace, record_key, schema_name, schema_version, revision, payload, updated_at_unix_ms FROM dbproxy_snapshots WHERE namespace = $1 AND record_key = $2",
                &[&record.namespace, &record.key],
            )
            .await?;
        row.as_ref().map(snapshot_from_row).transpose()
    }

    async fn save(&mut self, request: SnapshotWrite) -> Result<SnapshotWriteOutcome, Self::Error> {
        validate_request(&request)?;
        let schema_version = schema_version_to_i64(request.schema_version);
        let expected = revision_to_i64(&request.record, request.expected_revision)?;
        let updated_at = timestamp_to_i64(&request.record, request.updated_at_unix_ms)?;

        let mut client = self.client.lock().await;
        let transaction = client.transaction().await?;

        // 先占用幂等键，再写快照；并发重复请求会等待唯一键冲突并读取第一次结果。
        // Claim the idempotency key before the snapshot mutation; concurrent retries wait on the unique key.
        let claimed = transaction
            .query_opt(
                "INSERT INTO dbproxy_idempotency (request_id, namespace, record_key, schema_name, schema_version, payload, expected_revision, revision) VALUES ($1, $2, $3, $4, $5, $6, $7, 0) ON CONFLICT (request_id) DO NOTHING RETURNING request_id",
                &[
                    &request.request_id,
                    &request.record.namespace,
                    &request.record.key,
                    &request.schema,
                    &schema_version,
                    &request.payload,
                    &expected,
                ],
            )
            .await?;

        if claimed.is_none() {
            let receipt = transaction
                .query_one(
                    "SELECT namespace, record_key, schema_name, schema_version, payload, expected_revision, revision FROM dbproxy_idempotency WHERE request_id = $1",
                    &[&request.request_id],
                )
                .await?;
            if !idempotency_matches(&receipt, &request, schema_version, expected) {
                return Err(StoreError::IdempotencyConflict {
                    request_id: request.request_id,
                }
                .into());
            }
            let revision = revision_from_i64(&request.record, receipt.get(6))?;
            transaction.commit().await?;
            return Ok(SnapshotWriteOutcome::Duplicate { revision });
        }

        let snapshot = transaction
            .query_opt(
                "INSERT INTO dbproxy_snapshots (namespace, record_key, schema_name, schema_version, revision, payload, updated_at_unix_ms) VALUES ($1, $2, $3, $4, 1, $5, $6) ON CONFLICT (namespace, record_key) DO UPDATE SET schema_name = EXCLUDED.schema_name, schema_version = EXCLUDED.schema_version, revision = dbproxy_snapshots.revision + 1, payload = EXCLUDED.payload, updated_at_unix_ms = EXCLUDED.updated_at_unix_ms WHERE $7::BIGINT IS NULL OR dbproxy_snapshots.revision = $7 RETURNING revision",
                &[
                    &request.record.namespace,
                    &request.record.key,
                    &request.schema,
                    &schema_version,
                    &request.payload,
                    &updated_at,
                    &expected,
                ],
            )
            .await?;

        let Some(snapshot) = snapshot else {
            let actual = match transaction
                .query_opt(
                    "SELECT revision FROM dbproxy_snapshots WHERE namespace = $1 AND record_key = $2",
                    &[&request.record.namespace, &request.record.key],
                )
                .await?
            {
                Some(row) => revision_from_i64(&request.record, row.get(0))?,
                None => Revision::ZERO,
            };
            return Err(StoreError::RevisionConflict {
                record: request.record,
                expected: request.expected_revision,
                actual,
            }
            .into());
        };

        let revision = revision_from_i64(&request.record, snapshot.get(0))?;
        transaction
            .execute(
                "UPDATE dbproxy_idempotency SET revision = $2 WHERE request_id = $1",
                &[
                    &request.request_id,
                    &i64::try_from(revision.0).map_err(|_| StorageError::RevisionTooLarge {
                        record: request.record.clone(),
                    })?,
                ],
            )
            .await?;
        transaction.commit().await?;
        Ok(SnapshotWriteOutcome::Applied { revision })
    }
}

#[async_trait]
impl AsyncTransactionalStore for PostgresSnapshotStore {
    type Error = StorageError;

    async fn load_receipt(
        &self,
        operation_id: &str,
        record: &RecordKey,
    ) -> Result<Option<TransactionReceipt>, Self::Error> {
        validate_receipt_lookup(operation_id, record)?;
        let client = self.client.lock().await;
        let receipt = client
            .query_opt(
                "SELECT namespace, record_key, new_revision, result FROM dbproxy_transactions WHERE operation_id = $1",
                &[&operation_id],
            )
            .await?;
        let Some(receipt) = receipt else {
            return Ok(None);
        };
        if receipt.get::<_, String>(0) != record.namespace
            || receipt.get::<_, String>(1) != record.key
        {
            return Err(StoreError::OperationIdConflict {
                operation_id: operation_id.to_string(),
            }
            .into());
        }
        Ok(Some(TransactionReceipt {
            operation_id: operation_id.to_string(),
            record: record.clone(),
            new_revision: revision_from_i64(record, receipt.get(2))?,
            result: receipt.get(3),
        }))
    }

    async fn apply(
        &mut self,
        request: TransactionalWrite,
    ) -> Result<TransactionalWriteOutcome, Self::Error> {
        validate_transaction_request(&request)?;
        let schema_version = schema_version_to_i64(request.schema_version);
        let expected_revision =
            required_revision_to_i64(&request.record, request.expected_revision)?;
        let updated_at_unix_ms = timestamp_to_i64(&request.record, request.updated_at_unix_ms)?;

        let mut client = self.client.lock().await;
        let transaction = client.transaction().await?;

        // 操作收据和快照必须在同一个数据库事务内提交；失败的 CAS 会回滚收据，允许业务修正版本后重试。
        // The operation receipt and snapshot commit together; a failed CAS rolls the receipt back.
        let claimed = transaction
            .query_opt(
                "INSERT INTO dbproxy_transactions (operation_id, namespace, record_key, schema_name, schema_version, expected_revision, payload, result, new_revision, updated_at_unix_ms) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, $9) ON CONFLICT (operation_id) DO NOTHING RETURNING operation_id",
                &[
                    &request.operation_id,
                    &request.record.namespace,
                    &request.record.key,
                    &request.schema,
                    &schema_version,
                    &expected_revision,
                    &request.payload,
                    &request.result,
                    &updated_at_unix_ms,
                ],
            )
            .await?;

        if claimed.is_none() {
            let receipt = transaction
                .query_one(
                    "SELECT namespace, record_key, schema_name, schema_version, expected_revision, payload, result, new_revision, updated_at_unix_ms FROM dbproxy_transactions WHERE operation_id = $1",
                    &[&request.operation_id],
                )
                .await?;
            if !transaction_matches(
                &receipt,
                &request,
                schema_version,
                expected_revision,
                updated_at_unix_ms,
            ) {
                return Err(StoreError::OperationIdConflict {
                    operation_id: request.operation_id,
                }
                .into());
            }
            let new_revision = revision_from_i64(&request.record, receipt.get(7))?;
            let result: Vec<u8> = receipt.get(6);
            transaction.commit().await?;
            return Ok(TransactionalWriteOutcome::Duplicate {
                new_revision,
                result,
            });
        }

        let current = transaction
            .query_opt(
                "SELECT revision FROM dbproxy_snapshots WHERE namespace = $1 AND record_key = $2 FOR UPDATE",
                &[&request.record.namespace, &request.record.key],
            )
            .await?;
        let actual_revision = match &current {
            Some(row) => revision_from_i64(&request.record, row.get(0))?,
            None => Revision::ZERO,
        };
        if actual_revision != request.expected_revision {
            return Err(StoreError::RevisionConflict {
                record: request.record,
                expected: Some(request.expected_revision),
                actual: actual_revision,
            }
            .into());
        }

        let new_revision = Revision(actual_revision.0.checked_add(1).ok_or_else(|| {
            StoreError::RevisionExhausted {
                record: request.record.clone(),
            }
        })?);
        let new_revision_i64 = required_revision_to_i64(&request.record, new_revision)?;

        // 已存在的记录已经被 FOR UPDATE 锁住；首次创建使用 ON CONFLICT DO NOTHING，避免两个创建请求产生唯一键错误。
        // Existing rows are locked by FOR UPDATE; first creation uses ON CONFLICT DO NOTHING to avoid a PK race.
        let snapshot = if current.is_none() {
            transaction
                .query_opt(
                    "INSERT INTO dbproxy_snapshots (namespace, record_key, schema_name, schema_version, revision, payload, updated_at_unix_ms) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (namespace, record_key) DO NOTHING RETURNING revision",
                    &[
                        &request.record.namespace,
                        &request.record.key,
                        &request.schema,
                        &schema_version,
                        &new_revision_i64,
                        &request.payload,
                        &updated_at_unix_ms,
                    ],
                )
                .await?
        } else {
            transaction
                .query_opt(
                    "UPDATE dbproxy_snapshots SET schema_name = $3, schema_version = $4, revision = $5, payload = $6, updated_at_unix_ms = $7 WHERE namespace = $1 AND record_key = $2 RETURNING revision",
                    &[
                        &request.record.namespace,
                        &request.record.key,
                        &request.schema,
                        &schema_version,
                        &new_revision_i64,
                        &request.payload,
                        &updated_at_unix_ms,
                    ],
                )
                .await?
        };

        let Some(snapshot) = snapshot else {
            let actual = transaction
                .query_opt(
                    "SELECT revision FROM dbproxy_snapshots WHERE namespace = $1 AND record_key = $2",
                    &[&request.record.namespace, &request.record.key],
                )
                .await?
                .map(|row| revision_from_i64(&request.record, row.get(0)))
                .transpose()?
                .unwrap_or(Revision::ZERO);
            return Err(StoreError::RevisionConflict {
                record: request.record,
                expected: Some(request.expected_revision),
                actual,
            }
            .into());
        };

        let committed_revision = revision_from_i64(&request.record, snapshot.get(0))?;
        transaction
            .execute(
                "UPDATE dbproxy_transactions SET new_revision = $2 WHERE operation_id = $1",
                &[&request.operation_id, &new_revision_i64],
            )
            .await?;
        transaction.commit().await?;
        Ok(TransactionalWriteOutcome::Applied {
            new_revision: committed_revision,
            result: request.result,
        })
    }
}

#[async_trait]
impl AsyncMultiRecordTransactionStore for PostgresSnapshotStore {
    type Error = StorageError;

    async fn load_multi_receipt(
        &self,
        operation_id: &str,
        records: &[RecordKey],
    ) -> Result<Option<MultiRecordTransactionReceipt>, Self::Error> {
        if operation_id.trim().is_empty() {
            return Err(StoreError::EmptyOperationId.into());
        }
        if records.is_empty() {
            return Err(StoreError::EmptyTransactionRecords.into());
        }
        let expected_records = sorted_unique_records(records)?;
        let client = self.client.lock().await;
        let header = client
            .query_opt(
                "SELECT result, record_count FROM dbproxy_multi_transactions WHERE operation_id = $1",
                &[&operation_id],
            )
            .await?;
        let Some(header) = header else {
            return Ok(None);
        };
        let rows = client
            .query(
                "SELECT namespace, record_key, new_revision FROM dbproxy_multi_transaction_records WHERE operation_id = $1 ORDER BY namespace, record_key",
                &[&operation_id],
            )
            .await?;
        let expected_count: i64 = header.get(1);
        if expected_count < 0
            || rows.len() != expected_records.len()
            || rows.len() as i64 != expected_count
        {
            return Err(StoreError::OperationIdConflict {
                operation_id: operation_id.to_string(),
            }
            .into());
        }
        let mut receipts = Vec::with_capacity(rows.len());
        for (row, expected) in rows.iter().zip(expected_records.iter()) {
            if row.get::<_, String>(0) != expected.namespace
                || row.get::<_, String>(1) != expected.key
            {
                return Err(StoreError::OperationIdConflict {
                    operation_id: operation_id.to_string(),
                }
                .into());
            }
            receipts.push(TransactionRecordReceipt {
                record: expected.clone(),
                new_revision: revision_from_i64(expected, row.get(2))?,
            });
        }
        Ok(Some(MultiRecordTransactionReceipt {
            operation_id: operation_id.to_string(),
            records: receipts,
            result: header.get(0),
        }))
    }

    async fn apply_multi(
        &mut self,
        request: MultiRecordTransactionalWrite,
    ) -> Result<MultiRecordTransactionalWriteOutcome, Self::Error> {
        validate_multi_transaction_request(&request)?;
        let mut request = request;
        request.writes.sort_by(|left, right| {
            left.record
                .namespace
                .cmp(&right.record.namespace)
                .then_with(|| left.record.key.cmp(&right.record.key))
        });

        let mut client = self.client.lock().await;
        let transaction = client.transaction().await?;
        let operation_id = request.operation_id.clone();
        let claimed = transaction
            .query_opt(
                "INSERT INTO dbproxy_multi_transactions (operation_id, result, record_count, updated_at_unix_ms) VALUES ($1, $2, $3, $4) ON CONFLICT (operation_id) DO NOTHING RETURNING operation_id",
                &[
                    &operation_id,
                    &request.result,
                    &(request.writes.len() as i64),
                    &timestamp_to_i64(&request.writes[0].record, request.writes[0].updated_at_unix_ms)?,
                ],
            )
            .await?;

        if claimed.is_none() {
            let header = transaction
                .query_one(
                    "SELECT result, record_count FROM dbproxy_multi_transactions WHERE operation_id = $1",
                    &[&operation_id],
                )
                .await?;
            let rows = transaction
                .query(
                    "SELECT namespace, record_key, schema_name, schema_version, expected_revision, payload, updated_at_unix_ms, new_revision FROM dbproxy_multi_transaction_records WHERE operation_id = $1 ORDER BY namespace, record_key",
                    &[&operation_id],
                )
                .await?;
            let same_header = header.get::<_, i64>(1) == request.writes.len() as i64
                && header.get::<_, Vec<u8>>(0) == request.result;
            let same_records = rows.len() == request.writes.len()
                && rows
                    .iter()
                    .zip(request.writes.iter())
                    .all(|(row, write)| multi_transaction_row_matches(row, write));
            if !same_header || !same_records {
                return Err(StoreError::OperationIdConflict { operation_id }.into());
            }
            let records = rows
                .iter()
                .zip(request.writes.iter())
                .map(|(row, write)| {
                    Ok(TransactionRecordReceipt {
                        record: write.record.clone(),
                        new_revision: revision_from_i64(&write.record, row.get(7))?,
                    })
                })
                .collect::<Result<Vec<_>, StorageError>>()?;
            transaction.commit().await?;
            return Ok(MultiRecordTransactionalWriteOutcome::Duplicate {
                records,
                result: header.get(0),
            });
        }

        let mut revisions = Vec::with_capacity(request.writes.len());
        for write in &request.writes {
            // 用同一个数据库事务锁住每个逻辑记录，且始终按排序后的顺序加锁，避免跨玩家交易死锁。
            // Lock records in deterministic order inside one database transaction to avoid trade deadlocks.
            let lock_key = format!("{}:{}", write.record.namespace, write.record.key);
            transaction
                .query_one(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    &[&lock_key],
                )
                .await?;
            let current = transaction
                .query_opt(
                    "SELECT revision FROM dbproxy_snapshots WHERE namespace = $1 AND record_key = $2 FOR UPDATE",
                    &[&write.record.namespace, &write.record.key],
                )
                .await?;
            let actual = current
                .map(|row| revision_from_i64(&write.record, row.get(0)))
                .transpose()?
                .unwrap_or(Revision::ZERO);
            if actual != write.expected_revision {
                return Err(StoreError::RevisionConflict {
                    record: write.record.clone(),
                    expected: Some(write.expected_revision),
                    actual,
                }
                .into());
            }
            let next =
                Revision(
                    actual
                        .0
                        .checked_add(1)
                        .ok_or_else(|| StoreError::RevisionExhausted {
                            record: write.record.clone(),
                        })?,
                );
            revisions.push(next);
        }

        for (write, revision) in request.writes.iter().zip(revisions.iter()) {
            let schema_version = schema_version_to_i64(write.schema_version);
            let updated_at = timestamp_to_i64(&write.record, write.updated_at_unix_ms)?;
            let new_revision = required_revision_to_i64(&write.record, *revision)?;
            transaction
                .execute(
                    "INSERT INTO dbproxy_snapshots (namespace, record_key, schema_name, schema_version, revision, payload, updated_at_unix_ms) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (namespace, record_key) DO UPDATE SET schema_name = EXCLUDED.schema_name, schema_version = EXCLUDED.schema_version, revision = EXCLUDED.revision, payload = EXCLUDED.payload, updated_at_unix_ms = EXCLUDED.updated_at_unix_ms",
                    &[
                        &write.record.namespace,
                        &write.record.key,
                        &write.schema,
                        &schema_version,
                        &new_revision,
                        &write.payload,
                        &updated_at,
                    ],
                )
                .await?;
            transaction
                .execute(
                    "INSERT INTO dbproxy_multi_transaction_records (operation_id, namespace, record_key, schema_name, schema_version, expected_revision, payload, updated_at_unix_ms, new_revision) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                    &[
                        &operation_id,
                        &write.record.namespace,
                        &write.record.key,
                        &write.schema,
                        &schema_version,
                        &required_revision_to_i64(&write.record, write.expected_revision)?,
                        &write.payload,
                        &updated_at,
                        &new_revision,
                    ],
                )
                .await?;
        }
        transaction.commit().await?;
        let records = request
            .writes
            .iter()
            .zip(revisions)
            .map(|(write, new_revision)| TransactionRecordReceipt {
                record: write.record.clone(),
                new_revision,
            })
            .collect();
        Ok(MultiRecordTransactionalWriteOutcome::Applied {
            records,
            result: request.result,
        })
    }
}

fn sorted_unique_records(records: &[RecordKey]) -> Result<Vec<RecordKey>, StorageError> {
    if records.is_empty() {
        return Err(StoreError::EmptyTransactionRecords.into());
    }
    let mut sorted = records.to_vec();
    sorted.sort_by(|left, right| {
        left.namespace
            .cmp(&right.namespace)
            .then_with(|| left.key.cmp(&right.key))
    });
    for record in &sorted {
        if record.namespace.trim().is_empty() {
            return Err(StoreError::InvalidKey("namespace is empty").into());
        }
        if record.key.trim().is_empty() {
            return Err(StoreError::InvalidKey("key is empty").into());
        }
    }
    for pair in sorted.windows(2) {
        if pair[0] == pair[1] {
            return Err(StoreError::DuplicateTransactionRecord {
                record: pair[0].clone(),
            }
            .into());
        }
    }
    Ok(sorted)
}

fn multi_transaction_row_matches(row: &Row, write: &TransactionalRecordWrite) -> bool {
    let Ok(expected_revision) = i64::try_from(write.expected_revision.0) else {
        return false;
    };
    let Ok(updated_at_unix_ms) = i64::try_from(write.updated_at_unix_ms) else {
        return false;
    };
    row.get::<_, String>(0) == write.record.namespace
        && row.get::<_, String>(1) == write.record.key
        && row.get::<_, String>(2) == write.schema
        && row.get::<_, i64>(3) == i64::from(write.schema_version)
        && row.get::<_, i64>(4) == expected_revision
        && row.get::<_, Vec<u8>>(5) == write.payload
        && row.get::<_, i64>(6) == updated_at_unix_ms
}

/// Redis 快照缓存；只缓存 PostgreSQL 已提交的 `SnapshotEnvelope`。
/// Redis snapshot cache; it only caches PostgreSQL-committed envelopes.
#[derive(Clone)]
pub struct RedisSnapshotCache {
    connection: Arc<Mutex<MultiplexedConnection>>,
}

impl RedisSnapshotCache {
    /// 连接 Redis，不会修改现有键。
    /// Connect to Redis without modifying existing keys.
    pub async fn connect(url: &str) -> Result<Self, StorageError> {
        let client = redis::Client::open(url)?;
        let connection = client.get_multiplexed_async_connection().await?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    /// 生成无碰撞的缓存键；长度前缀允许业务键包含冒号。
    /// Build a collision-free cache key; length prefixes allow colons in business keys.
    pub fn cache_key(record: &RecordKey) -> String {
        format!(
            "dbproxy:snapshot:{}:{}:{}:{}",
            record.namespace.len(),
            record.namespace,
            record.key.len(),
            record.key
        )
    }

    /// 读取缓存；不存在返回 `None`。
    /// Read the cache; return `None` on a miss.
    pub async fn get(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, StorageError> {
        let mut connection = self.connection.lock().await;
        let value: Option<Vec<u8>> = redis::cmd("GET")
            .arg(Self::cache_key(record))
            .query_async(&mut *connection)
            .await?;
        value
            .map(|bytes| {
                bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                    .map(|(snapshot, _)| snapshot)
                    .map_err(|error| StorageError::Codec(error.to_string()))
            })
            .transpose()
    }

    /// 使用一次MGET读取多条缓存，并保持调用方的记录顺序与缺失位置。
    /// Read multiple cache entries with one MGET while preserving order and misses.
    pub async fn get_multi(
        &self,
        records: &[RecordKey],
    ) -> Result<Vec<Option<SnapshotEnvelope>>, StorageError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let keys = records.iter().map(Self::cache_key).collect::<Vec<_>>();
        let mut connection = self.connection.lock().await;
        let values: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
            .arg(keys)
            .query_async(&mut *connection)
            .await?;
        values
            .into_iter()
            .map(|value| {
                value
                    .map(|bytes| {
                        bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                            .map(|(snapshot, _)| snapshot)
                            .map_err(|error| StorageError::Codec(error.to_string()))
                    })
                    .transpose()
            })
            .collect()
    }

    /// 写入缓存；调用方必须在权威存储成功后调用。
    /// Write the cache; callers must invoke this only after the authoritative store succeeds.
    pub async fn put(&self, snapshot: &SnapshotEnvelope) -> Result<(), StorageError> {
        let bytes = bincode::serde::encode_to_vec(snapshot, bincode::config::standard())
            .map_err(|error| StorageError::Codec(error.to_string()))?;
        let mut connection = self.connection.lock().await;
        let _: () = redis::cmd("SET")
            .arg(Self::cache_key(&snapshot.record))
            .arg(bytes)
            .query_async(&mut *connection)
            .await?;
        Ok(())
    }

    /// 删除指定快照缓存；数据库没有记录时由修复流程调用，避免保留幽灵快照。
    /// Delete one snapshot cache entry; repair uses this when the durable record is absent.
    pub async fn delete(&self, record: &RecordKey) -> Result<(), StorageError> {
        let mut connection = self.connection.lock().await;
        let _: u64 = redis::cmd("DEL")
            .arg(Self::cache_key(record))
            .query_async(&mut *connection)
            .await?;
        Ok(())
    }
}

/// PostgreSQL + Redis 的读写组合；权威顺序固定为 PostgreSQL -> Redis。
/// PostgreSQL + Redis composition; the authoritative order is always PostgreSQL -> Redis.
#[derive(Clone)]
pub struct TieredSnapshotStore {
    postgres: PostgresSnapshotStore,
    cache: RedisSnapshotCache,
}

impl TieredSnapshotStore {
    /// 连接两个后端并保证 PostgreSQL schema 已就绪。
    /// Connect both backends and ensure the PostgreSQL schema exists.
    pub async fn connect(postgres_url: &str, redis_url: &str) -> Result<Self, StorageError> {
        Ok(Self {
            postgres: PostgresSnapshotStore::connect(postgres_url).await?,
            cache: RedisSnapshotCache::connect(redis_url).await?,
        })
    }

    /// 从 PostgreSQL 重建一个记录的 Redis 缓存；没有权威记录时删除旧缓存。
    /// Rebuild one Redis entry from PostgreSQL; delete stale cache if no durable record exists.
    ///
    /// 该方法不改变 Revision，也不执行任何业务写入，适合启动恢复、定时修复和故障排空。
    /// It never changes Revision or business state and is suitable for recovery and repair jobs.
    pub async fn repair_cache(&self, record: &RecordKey) -> Result<Option<Revision>, StorageError> {
        let snapshot = self.postgres.load(record).await?;
        match snapshot {
            Some(snapshot) => {
                let revision = snapshot.revision;
                self.cache.put(&snapshot).await?;
                Ok(Some(revision))
            }
            None => {
                self.cache.delete(record).await?;
                Ok(None)
            }
        }
    }

    /// 批量读取缓存，并用一次PostgreSQL查询回源全部未命中记录。
    /// Batch-read cache entries and resolve all misses with one PostgreSQL query.
    pub async fn load_multi(
        &self,
        records: &[RecordKey],
    ) -> Result<Vec<Option<SnapshotEnvelope>>, StorageError> {
        let mut snapshots = match self.cache.get_multi(records).await {
            Ok(snapshots) => snapshots,
            Err(error) => {
                tracing::warn!(%error, record_count = records.len(), "snapshot cache batch read failed; falling back to postgres");
                vec![None; records.len()]
            }
        };
        let misses = records
            .iter()
            .enumerate()
            .filter(|(index, _)| snapshots[*index].is_none())
            .map(|(index, record)| (index, record.clone()))
            .collect::<Vec<_>>();
        if misses.is_empty() {
            return Ok(snapshots);
        }

        let missing_records = misses
            .iter()
            .map(|(_, record)| record.clone())
            .collect::<Vec<_>>();
        let loaded = self.postgres.load_multi(&missing_records).await?;
        for ((request_index, _), snapshot) in misses.into_iter().zip(loaded) {
            if let Some(snapshot) = &snapshot
                && let Err(error) = self.cache.put(snapshot).await
            {
                tracing::warn!(%error, namespace = %snapshot.record.namespace, key = %snapshot.record.key, "snapshot cache batch warmup failed");
            }
            snapshots[request_index] = snapshot;
        }
        Ok(snapshots)
    }
}

#[async_trait]
impl AsyncSnapshotStore for TieredSnapshotStore {
    type Error = StorageError;

    async fn load(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, Self::Error> {
        // Redis是加速层；读取失败必须回源权威PostgreSQL，不能把缓存故障扩大成数据不可用。
        // Redis is an acceleration layer; read failures must fall back to authoritative PostgreSQL.
        let cached = match self.cache.get(record).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                tracing::warn!(%error, namespace = %record.namespace, key = %record.key, "snapshot cache read failed; falling back to postgres");
                None
            }
        };
        if let Some(snapshot) = cached {
            return Ok(Some(snapshot));
        }

        let snapshot = self.postgres.load(record).await?;
        if let Some(snapshot) = &snapshot
            && let Err(error) = self.cache.put(snapshot).await
        {
            tracing::warn!(%error, namespace = %record.namespace, key = %record.key, "snapshot cache warmup failed");
        }
        Ok(snapshot)
    }

    async fn save(&mut self, request: SnapshotWrite) -> Result<SnapshotWriteOutcome, Self::Error> {
        let record = request.record.clone();
        let outcome = self.postgres.save(request).await?;
        let snapshot =
            self.postgres
                .load(&record)
                .await?
                .ok_or_else(|| StorageError::MissingAfterWrite {
                    record: record.clone(),
                })?;
        if let Err(error) = self.cache.put(&snapshot).await {
            return Err(StorageError::CacheSync(error.to_string()));
        }
        Ok(outcome)
    }
}

#[async_trait]
impl AsyncTransactionalStore for TieredSnapshotStore {
    type Error = StorageError;

    async fn load_receipt(
        &self,
        operation_id: &str,
        record: &RecordKey,
    ) -> Result<Option<TransactionReceipt>, Self::Error> {
        self.postgres.load_receipt(operation_id, record).await
    }

    async fn apply(
        &mut self,
        request: TransactionalWrite,
    ) -> Result<TransactionalWriteOutcome, Self::Error> {
        let record = request.record.clone();
        let outcome = self.postgres.apply(request).await?;
        let snapshot =
            self.postgres
                .load(&record)
                .await?
                .ok_or_else(|| StorageError::MissingAfterWrite {
                    record: record.clone(),
                })?;
        if let Err(error) = self.cache.put(&snapshot).await {
            return Err(StorageError::CacheSync(error.to_string()));
        }
        Ok(outcome)
    }
}

#[async_trait]
impl AsyncMultiRecordTransactionStore for TieredSnapshotStore {
    type Error = StorageError;

    async fn load_multi_receipt(
        &self,
        operation_id: &str,
        records: &[RecordKey],
    ) -> Result<Option<MultiRecordTransactionReceipt>, Self::Error> {
        self.postgres
            .load_multi_receipt(operation_id, records)
            .await
    }

    async fn apply_multi(
        &mut self,
        request: MultiRecordTransactionalWrite,
    ) -> Result<MultiRecordTransactionalWriteOutcome, Self::Error> {
        let records = request
            .writes
            .iter()
            .map(|write| write.record.clone())
            .collect::<Vec<_>>();
        let outcome = self.postgres.apply_multi(request).await?;
        for record in records {
            let snapshot = self.postgres.load(&record).await?.ok_or_else(|| {
                StorageError::MissingAfterWrite {
                    record: record.clone(),
                }
            })?;
            if let Err(error) = self.cache.put(&snapshot).await {
                return Err(StorageError::CacheSync(error.to_string()));
            }
        }
        Ok(outcome)
    }
}
