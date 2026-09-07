//! DBProxy 的真实存储适配器。
//!
//! PostgreSQL 是唯一权威写入端；Redis 只保存已经提交的快照缓存。
//! PostgreSQL is the only authoritative write target; Redis caches committed snapshots only.

use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex as StdMutex, Weak},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use redis::{
    Script,
    aio::{ConnectionManager, ConnectionManagerConfig},
};
use thiserror::Error;
use tiangz_dbproxy_core::{
    AsyncMultiRecordTransactionStore, AsyncSnapshotStore, AsyncTransactionalStore,
    MultiRecordTransactionReceipt, MultiRecordTransactionalWrite,
    MultiRecordTransactionalWriteOutcome, RecordKey, Revision, SnapshotEnvelope, SnapshotWrite,
    SnapshotWriteOutcome, StoreError, TransactionReceipt, TransactionRecordReceipt,
    TransactionalRecordWrite, TransactionalWrite, TransactionalWriteOutcome,
};
use tokio::{
    sync::{Mutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore},
    time::{sleep, timeout},
};
use tokio_postgres::{Client, NoTls, Row, Transaction};

mod backlog;
mod cache_repair;
mod latency;
mod postgres_request;
pub use postgres_request::{
    DEFAULT_POSTGRES_CONNECTION_WAIT_TIMEOUT_MS, DEFAULT_POSTGRES_RECONNECT_COOLDOWN_MS,
    PostgresRequestConfig,
};
#[cfg(test)]
mod latency_path_tests;
mod outbox;
mod trade;
pub use latency::{STORAGE_LATENCY_BOUNDS_MS, StorageStageSnapshot};
use latency::{Stage, StorageLatency};

pub use backlog::{
    RedisSnapshotBacklog, RedisSnapshotBacklogStats, SnapshotBacklogAck, SnapshotBacklogLease,
};
pub use cache_repair::{CacheRepairLease, CacheRepairStats, PostgresCacheRepairQueue};
pub use outbox::{
    OutboxLease, OutboxStats, PostgresOutboxQueue, RedisOutboxPublisher, RedisStreamPublisher,
};

const SCHEMA_MIGRATION_BOOTSTRAP: &str = include_str!("../migrations/000_schema_migrations.sql");
const SNAPSHOT_MIGRATION: &str = include_str!("../migrations/001_snapshot.sql");
const TRANSACTION_MIGRATION: &str = include_str!("../migrations/002_transactional.sql");
const MULTI_TRANSACTION_MIGRATION: &str = include_str!("../migrations/003_multi_transactional.sql");
const CACHE_REPAIR_MIGRATION: &str = include_str!("../migrations/004_cache_repair.sql");
const TRADE_OUTBOX_MIGRATION: &str = include_str!("../migrations/005_trade_outbox.sql");
const OPERATION_REGISTRY_MIGRATION: &str = include_str!("../migrations/006_operation_registry.sql");
const HARDENING_MIGRATION: &str = include_str!("../migrations/007_hardening.sql");
mod commit;
mod outbox_admin;
mod relay;
pub use outbox_admin::{OutboxInspection, OutboxSourceStats};
pub use relay::{
    OutboxRoute, PublishError, PublishMessage, PublishReceipt, Publisher,
    redis_endpoint_fingerprint,
};
use tiangz_dbproxy_core::CommitEffects;
const MIGRATION_LOCK_ID: i64 = 8_390_417_203;
pub const SNAPSHOT_PARTITION_COUNT: usize = 32;
pub const DEFAULT_CACHE_FALLBACK_CONCURRENCY: usize = 16;
pub const DEFAULT_CACHE_FALLBACK_TIMEOUT_MS: u64 = 2_000;
pub const DEFAULT_CACHE_OPERATION_TIMEOUT_MS: u64 = 200;
pub const DEFAULT_CACHE_FALLBACK_CIRCUIT_FAILURE_THRESHOLD: u32 = 5;
pub const DEFAULT_CACHE_FALLBACK_CIRCUIT_COOLDOWN_MS: u64 = 5_000;
pub const DEFAULT_CACHE_FALLBACK_LOCK_LEASE_MS: u64 = 3_000;
pub const DEFAULT_CACHE_FALLBACK_LOCK_WAIT_MS: u64 = 1_000;
pub const DEFAULT_CACHE_FALLBACK_LOCK_POLL_MS: u64 = 25;
pub const DEFAULT_CACHE_TTL_MS: u64 = 300_000;
pub const DEFAULT_CACHE_TTL_JITTER_MS: u64 = 30_000;
pub const DEFAULT_CACHE_NEGATIVE_TTL_MS: u64 = 5_000;
pub const DEFAULT_CACHE_STALE_WHILE_REVALIDATE_MS: u64 = 30_000;
pub const DEFAULT_OUTBOX_STREAM_PREFIX: &str = "dbproxy:outbox:";
pub const DEFAULT_REDIS_AOF_ACK_TIMEOUT_MS: u64 = 2_000;
pub const DEFAULT_REDIS_RESPONSE_TIMEOUT_MS: u64 = DEFAULT_REDIS_AOF_ACK_TIMEOUT_MS + 1_000;
pub const DEFAULT_POSTGRES_RECONNECT_TIMEOUT_MS: u64 = 2_000;

static CACHE_FALLBACK_LOCK_SEQUENCE: AtomicU64 = AtomicU64::new(0);

async fn open_redis_connection_manager(url: &str) -> Result<ConnectionManager, StorageError> {
    let client = redis::Client::open(url)?;
    let config = ConnectionManagerConfig::new().set_response_timeout(Some(Duration::from_millis(
        DEFAULT_REDIS_RESPONSE_TIMEOUT_MS,
    )));
    Ok(ConnectionManager::new_with_config(client, config).await?)
}

pub(crate) fn advisory_lock_key(scope: &str, components: &[&str]) -> String {
    let mut key = format!("{}:{scope}:", scope.len());
    for component in components {
        key.push_str(&component.len().to_string());
        key.push(':');
        key.push_str(component);
        key.push(':');
    }
    key
}

/// Cumulative storage-path counters shared by all PostgreSQL/Redis shards.
///
/// The counters deliberately have no record, player, namespace, or request labels.  They are
/// safe to expose through Prometheus without creating an unbounded time-series cardinality.
#[derive(Default)]
pub struct StorageMetrics {
    latency: StorageLatency,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_read_errors: AtomicU64,
    cache_writes: AtomicU64,
    cache_write_errors: AtomicU64,
    postgres_fallbacks: AtomicU64,
    postgres_fallback_errors: AtomicU64,
    postgres_fallback_timeouts: AtomicU64,
    postgres_fallback_circuit_open: AtomicU64,
    cache_fallback_lock_acquired: AtomicU64,
    cache_fallback_lock_contention: AtomicU64,
    cache_fallback_lock_timeouts: AtomicU64,
    cache_fallback_lock_errors: AtomicU64,
    cache_fallback_lock_release_errors: AtomicU64,
    cache_negative_hits: AtomicU64,
    cache_stale_hits: AtomicU64,
    cache_negative_writes: AtomicU64,
    cache_refresh_started: AtomicU64,
    cache_refresh_completed: AtomicU64,
    cache_refresh_errors: AtomicU64,
}

/// Point-in-time copy of [`StorageMetrics`] suitable for exporting from another crate.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StorageMetricsSnapshot {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_read_errors: u64,
    pub cache_writes: u64,
    pub cache_write_errors: u64,
    pub postgres_fallbacks: u64,
    pub postgres_fallback_errors: u64,
    pub postgres_fallback_timeouts: u64,
    pub postgres_fallback_circuit_open: u64,
    pub cache_fallback_lock_acquired: u64,
    pub cache_fallback_lock_contention: u64,
    pub cache_fallback_lock_timeouts: u64,
    pub cache_fallback_lock_errors: u64,
    pub cache_fallback_lock_release_errors: u64,
    pub cache_negative_hits: u64,
    pub cache_stale_hits: u64,
    pub cache_negative_writes: u64,
    pub cache_refresh_started: u64,
    pub cache_refresh_completed: u64,
    pub cache_refresh_errors: u64,
}

impl StorageMetrics {
    /// 低频采集固定阶段；不包含记录键、操作 ID、凭据或 SQL 文本。
    /// Scrapes fixed stages without record keys, operation IDs, credentials or SQL text.
    pub fn latency_snapshot(&self) -> Vec<StorageStageSnapshot> {
        self.latency.snapshot()
    }

    pub fn snapshot(&self) -> StorageMetricsSnapshot {
        StorageMetricsSnapshot {
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            cache_read_errors: self.cache_read_errors.load(Ordering::Relaxed),
            cache_writes: self.cache_writes.load(Ordering::Relaxed),
            cache_write_errors: self.cache_write_errors.load(Ordering::Relaxed),
            postgres_fallbacks: self.postgres_fallbacks.load(Ordering::Relaxed),
            postgres_fallback_errors: self.postgres_fallback_errors.load(Ordering::Relaxed),
            postgres_fallback_timeouts: self.postgres_fallback_timeouts.load(Ordering::Relaxed),
            postgres_fallback_circuit_open: self
                .postgres_fallback_circuit_open
                .load(Ordering::Relaxed),
            cache_fallback_lock_acquired: self.cache_fallback_lock_acquired.load(Ordering::Relaxed),
            cache_fallback_lock_contention: self
                .cache_fallback_lock_contention
                .load(Ordering::Relaxed),
            cache_fallback_lock_timeouts: self.cache_fallback_lock_timeouts.load(Ordering::Relaxed),
            cache_fallback_lock_errors: self.cache_fallback_lock_errors.load(Ordering::Relaxed),
            cache_fallback_lock_release_errors: self
                .cache_fallback_lock_release_errors
                .load(Ordering::Relaxed),
            cache_negative_hits: self.cache_negative_hits.load(Ordering::Relaxed),
            cache_stale_hits: self.cache_stale_hits.load(Ordering::Relaxed),
            cache_negative_writes: self.cache_negative_writes.load(Ordering::Relaxed),
            cache_refresh_started: self.cache_refresh_started.load(Ordering::Relaxed),
            cache_refresh_completed: self.cache_refresh_completed.load(Ordering::Relaxed),
            cache_refresh_errors: self.cache_refresh_errors.load(Ordering::Relaxed),
        }
    }

    fn cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    fn cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    fn cache_read_error(&self) {
        self.cache_read_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn cache_write(&self) {
        self.cache_writes.fetch_add(1, Ordering::Relaxed);
    }

    fn cache_writes(&self, count: usize) {
        self.cache_writes
            .fetch_add(u64::try_from(count).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    fn cache_write_error(&self) {
        self.cache_write_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn postgres_fallback(&self) {
        self.postgres_fallbacks.fetch_add(1, Ordering::Relaxed);
    }

    fn postgres_fallback_error(&self) {
        self.postgres_fallback_errors
            .fetch_add(1, Ordering::Relaxed);
    }

    fn postgres_fallback_timeout(&self) {
        self.postgres_fallback_timeouts
            .fetch_add(1, Ordering::Relaxed);
    }

    fn postgres_fallback_circuit_open(&self) {
        self.postgres_fallback_circuit_open
            .fetch_add(1, Ordering::Relaxed);
    }

    fn cache_fallback_lock_acquired(&self) {
        self.cache_fallback_lock_acquired
            .fetch_add(1, Ordering::Relaxed);
    }

    fn cache_fallback_lock_contention(&self) {
        self.cache_fallback_lock_contention
            .fetch_add(1, Ordering::Relaxed);
    }

    fn cache_fallback_lock_timeout(&self) {
        self.cache_fallback_lock_timeouts
            .fetch_add(1, Ordering::Relaxed);
    }

    fn cache_fallback_lock_error(&self) {
        self.cache_fallback_lock_errors
            .fetch_add(1, Ordering::Relaxed);
    }

    fn cache_fallback_lock_release_error(&self) {
        self.cache_fallback_lock_release_errors
            .fetch_add(1, Ordering::Relaxed);
    }

    fn cache_negative_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
        self.cache_negative_hits.fetch_add(1, Ordering::Relaxed);
    }

    fn cache_stale_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
        self.cache_stale_hits.fetch_add(1, Ordering::Relaxed);
    }

    fn cache_negative_write(&self) {
        self.cache_negative_writes.fetch_add(1, Ordering::Relaxed);
    }

    fn cache_refresh_started(&self) {
        self.cache_refresh_started.fetch_add(1, Ordering::Relaxed);
    }

    fn cache_refresh_completed(&self) {
        self.cache_refresh_completed.fetch_add(1, Ordering::Relaxed);
    }

    fn cache_refresh_error(&self) {
        self.cache_refresh_errors.fetch_add(1, Ordering::Relaxed);
    }
}

const REVISION_AWARE_CACHE_PUT_SCRIPT: &str = r#"
local current = redis.call('GET', KEYS[2])
if current then
    if string.len(current) > string.len(ARGV[2]) then
        return 0
    end
    if string.len(current) == string.len(ARGV[2]) and current > ARGV[2] then
        return 0
    end
end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('SET', KEYS[2], ARGV[2])
redis.call('DEL', KEYS[4])
redis.call('SET', KEYS[3], '1', 'PX', ARGV[3])
redis.call('PEXPIRE', KEYS[1], ARGV[4])
redis.call('PEXPIRE', KEYS[2], ARGV[4])
return 1
"#;

const REVISION_AWARE_CACHE_PUT_MULTI_SCRIPT: &str = r#"
local stored = 0
for index = 1, #ARGV, 4 do
    local key_index = index
    local current = redis.call('GET', KEYS[key_index + 1])
    local incoming_revision = ARGV[index + 1]
    if not current
        or string.len(current) < string.len(incoming_revision)
        or (string.len(current) == string.len(incoming_revision) and current <= incoming_revision)
    then
        redis.call('SET', KEYS[key_index], ARGV[index])
        redis.call('SET', KEYS[key_index + 1], incoming_revision)
        redis.call('DEL', KEYS[key_index + 3])
        redis.call('SET', KEYS[key_index + 2], '1', 'PX', ARGV[index + 2])
        redis.call('PEXPIRE', KEYS[key_index], ARGV[index + 3])
        redis.call('PEXPIRE', KEYS[key_index + 1], ARGV[index + 3])
        stored = stored + 1
    end
end
return stored
"#;

const NEGATIVE_CACHE_PUT_SCRIPT: &str = r#"
local current = redis.call('GET', KEYS[2])
if ARGV[2] ~= '' then
    if current ~= ARGV[2] then
        return 0
    end
    redis.call('DEL', KEYS[1], KEYS[2], KEYS[3])
    redis.call('SET', KEYS[4], '1', 'PX', ARGV[1])
    return 1
end
if redis.call('EXISTS', KEYS[1]) == 1 or current then
    redis.call('DEL', KEYS[4])
    return 0
end
redis.call('DEL', KEYS[3])
redis.call('SET', KEYS[4], '1', 'PX', ARGV[1])
return 1
"#;

const CACHE_DELETE_SCRIPT: &str = "return redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[4])";

const CACHE_FALLBACK_LOCK_RELEASE_SCRIPT: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"#;

/// 存储适配器错误；PostgreSQL 错误不会被包装成“保存成功”。
/// Adapter error; PostgreSQL failures are never reported as successful writes.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error(transparent)]
    Core(#[from] StoreError),
    #[error("postgres error: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    #[error("PostgreSQL connection attempt timed out after {timeout_ms}ms")]
    PostgresConnectTimeout { timeout_ms: u64 },
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("Redis AOF did not acknowledge the write within {timeout_ms}ms")]
    RedisAofNotDurable { timeout_ms: u64 },
    #[error("snapshot cache codec error: {0}")]
    Codec(String),
    #[error("snapshot cache protocol error: {0}")]
    CacheProtocol(String),
    #[error("persistence protocol error: {0}")]
    PersistenceProtocol(String),
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
    #[error("cache fallback gate is closed")]
    CacheFallbackGateClosed,
    #[error("cache fallback timed out after {timeout_ms}ms")]
    CacheFallbackTimeout { timeout_ms: u64 },
    #[error("cache {operation} timed out after {timeout_ms}ms")]
    CacheOperationTimeout {
        operation: &'static str,
        timeout_ms: u64,
    },
    #[error("cache fallback concurrency must be greater than zero")]
    InvalidCacheFallbackConcurrency,
    #[error("cache fallback timeout must be greater than zero")]
    InvalidCacheFallbackTimeout,
    #[error("cache operation timeout must be at least one millisecond")]
    InvalidCacheOperationTimeout,
    #[error(
        "PostgreSQL connection wait timeout must be representable and at least one millisecond"
    )]
    InvalidPostgresConnectionWaitTimeout,
    #[error("PostgreSQL reconnect cooldown must be representable and at least one millisecond")]
    InvalidPostgresReconnectCooldown,
    #[error(
        "PostgreSQL connection queue timed out after {timeout_ms}ms; no SQL sent by this operation"
    )]
    PostgresConnectionWaitTimeout { timeout_ms: u64 },
    #[error("PostgreSQL reconnect is cooling down; retry after {retry_after_ms}ms")]
    PostgresReconnectCooldown { retry_after_ms: u64 },
    #[error("cache fallback circuit failure threshold must be greater than zero")]
    InvalidCacheFallbackCircuitThreshold,
    #[error("cache fallback circuit cooldown must be greater than zero")]
    InvalidCacheFallbackCircuitCooldown,
    #[error("cache fallback circuit is open; retry after {retry_after_ms}ms")]
    CacheFallbackCircuitOpen { retry_after_ms: u64 },
    #[error("cache fallback lock lease must be greater than zero")]
    InvalidCacheFallbackLockLease,
    #[error("cache fallback lock wait must be greater than zero")]
    InvalidCacheFallbackLockWait,
    #[error("cache fallback lock poll interval must be greater than zero")]
    InvalidCacheFallbackLockPoll,
    #[error("cache TTL must be greater than zero")]
    InvalidCacheTtl,
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
    #[error("durable queue lease must be greater than zero")]
    InvalidQueueLease,
    #[error("durable queue worker id is empty")]
    InvalidQueueWorker,
    #[error("durable queue protocol error: {0}")]
    QueueProtocol(String),
    #[error("persisted trade protocol error: {0}")]
    TradeProtocol(String),
    #[error("trade version is too large for {trade_id}")]
    TradeVersionTooLarge { trade_id: String },
    #[error("snapshot partition layout is invalid: {0}")]
    InvalidSnapshotPartitionLayout(String),
    #[error("schema migration {version} is registered as {actual:?}, expected {expected:?}")]
    SchemaMigrationConflict {
        version: i32,
        expected: &'static str,
        actual: String,
    },
}

/// Limits applied when Redis misses force a read from PostgreSQL.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CacheFallbackConfig {
    pub max_concurrent: usize,
    pub timeout: Duration,
}

impl Default for CacheFallbackConfig {
    fn default() -> Self {
        Self {
            max_concurrent: DEFAULT_CACHE_FALLBACK_CONCURRENCY,
            timeout: Duration::from_millis(DEFAULT_CACHE_FALLBACK_TIMEOUT_MS),
        }
    }
}

impl CacheFallbackConfig {
    fn validate(self) -> Result<Self, StorageError> {
        if self.max_concurrent == 0 {
            return Err(StorageError::InvalidCacheFallbackConcurrency);
        }
        if self.timeout.is_zero() {
            return Err(StorageError::InvalidCacheFallbackTimeout);
        }
        Ok(self)
    }

    fn timeout_ms(self) -> u64 {
        u64::try_from(self.timeout.as_millis()).unwrap_or(u64::MAX)
    }
}

/// Circuit-breaker policy for PostgreSQL reads triggered by a Redis miss.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CacheFallbackCircuitConfig {
    pub failure_threshold: u32,
    pub cooldown: Duration,
}

impl Default for CacheFallbackCircuitConfig {
    fn default() -> Self {
        Self {
            failure_threshold: DEFAULT_CACHE_FALLBACK_CIRCUIT_FAILURE_THRESHOLD,
            cooldown: Duration::from_millis(DEFAULT_CACHE_FALLBACK_CIRCUIT_COOLDOWN_MS),
        }
    }
}

impl CacheFallbackCircuitConfig {
    fn validate(self) -> Result<Self, StorageError> {
        if self.failure_threshold == 0 {
            return Err(StorageError::InvalidCacheFallbackCircuitThreshold);
        }
        if self.cooldown.is_zero() {
            return Err(StorageError::InvalidCacheFallbackCircuitCooldown);
        }
        Ok(self)
    }

    fn cooldown_ms(self) -> u64 {
        u64::try_from(self.cooldown.as_millis())
            .unwrap_or(u64::MAX)
            .max(1)
    }
}

/// Redis distributed-lock policy for PostgreSQL cache-fallback reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CacheFallbackLockConfig {
    /// How long an instance owns a lock before Redis releases it automatically.
    pub lease: Duration,
    /// Maximum time to wait for another instance to populate the cache.
    pub wait: Duration,
    /// Delay between cache rechecks while another instance owns the lock.
    pub poll_interval: Duration,
}

impl Default for CacheFallbackLockConfig {
    fn default() -> Self {
        Self {
            lease: Duration::from_millis(DEFAULT_CACHE_FALLBACK_LOCK_LEASE_MS),
            wait: Duration::from_millis(DEFAULT_CACHE_FALLBACK_LOCK_WAIT_MS),
            poll_interval: Duration::from_millis(DEFAULT_CACHE_FALLBACK_LOCK_POLL_MS),
        }
    }
}

impl CacheFallbackLockConfig {
    fn validate(self) -> Result<Self, StorageError> {
        if self.lease.is_zero() {
            return Err(StorageError::InvalidCacheFallbackLockLease);
        }
        if self.wait.is_zero() {
            return Err(StorageError::InvalidCacheFallbackLockWait);
        }
        if self.poll_interval.is_zero() {
            return Err(StorageError::InvalidCacheFallbackLockPoll);
        }
        Ok(self)
    }

    fn lease_ms(self) -> u64 {
        u64::try_from(self.lease.as_millis())
            .unwrap_or(u64::MAX)
            .max(1)
    }
}

/// Redis snapshot lifecycle policy.
///
/// `ttl` is the fresh period.  A deterministic per-record jitter spreads hard expirations,
/// while `stale_while_revalidate` keeps the entry readable after freshness expires until the
/// hard TTL.  Set `negative_ttl` to zero to disable caching of records that do not exist.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotCacheConfig {
    pub ttl: Duration,
    pub ttl_jitter: Duration,
    pub negative_ttl: Duration,
    pub stale_while_revalidate: Duration,
}

impl Default for SnapshotCacheConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_millis(DEFAULT_CACHE_TTL_MS),
            ttl_jitter: Duration::from_millis(DEFAULT_CACHE_TTL_JITTER_MS),
            negative_ttl: Duration::from_millis(DEFAULT_CACHE_NEGATIVE_TTL_MS),
            stale_while_revalidate: Duration::from_millis(DEFAULT_CACHE_STALE_WHILE_REVALIDATE_MS),
        }
    }
}

impl SnapshotCacheConfig {
    fn validate(self) -> Result<Self, StorageError> {
        if self.ttl.is_zero() {
            return Err(StorageError::InvalidCacheTtl);
        }
        Ok(self)
    }

    fn jitter_ms(self, record: &RecordKey) -> u64 {
        let jitter_ms = duration_millis(self.ttl_jitter);
        if jitter_ms == 0 {
            return 0;
        }
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        record.namespace.hash(&mut hasher);
        record.key.hash(&mut hasher);
        hasher.finish() % jitter_ms.saturating_add(1)
    }

    fn fresh_ttl_ms(self, record: &RecordKey) -> u64 {
        duration_millis(self.ttl)
            .saturating_add(self.jitter_ms(record))
            .max(1)
    }

    fn hard_ttl_ms(self, record: &RecordKey) -> u64 {
        self.fresh_ttl_ms(record)
            .saturating_add(duration_millis(self.stale_while_revalidate))
            .max(1)
    }
}

/// Complete Redis read/cache policy for a tiered PostgreSQL + Redis store.
///
/// Keeping these related knobs in one value avoids constructor growth whenever a cache policy is
/// added. Callers can override only the policies they need with struct update syntax.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TieredSnapshotStoreConfig {
    /// 请求分片的连接排队与重连策略；不改变独立维护连接。
    /// Connection policy for request shards, independent of dedicated maintenance connections.
    pub postgres: PostgresRequestConfig,
    /// 缓存单次操作预算，包含连接排队；不缩短 PG 回源或可靠 Redis AOF 等待。
    /// Per-cache-operation budget including connection wait, independent of PG fallback and AOF.
    pub cache_operation_timeout: Duration,
    pub fallback: CacheFallbackConfig,
    pub circuit: CacheFallbackCircuitConfig,
    pub lock: CacheFallbackLockConfig,
    pub cache: SnapshotCacheConfig,
}

impl Default for TieredSnapshotStoreConfig {
    fn default() -> Self {
        Self {
            postgres: PostgresRequestConfig::default(),
            cache_operation_timeout: Duration::from_millis(DEFAULT_CACHE_OPERATION_TIMEOUT_MS),
            fallback: CacheFallbackConfig::default(),
            circuit: CacheFallbackCircuitConfig::default(),
            lock: CacheFallbackLockConfig::default(),
            cache: SnapshotCacheConfig::default(),
        }
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis())
        .unwrap_or(u64::MAX)
        .min(i64::MAX as u64)
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

fn snapshot_from_write(request: &SnapshotWrite, revision: Revision) -> SnapshotEnvelope {
    SnapshotEnvelope {
        record: request.record.clone(),
        schema: request.schema.clone(),
        schema_version: request.schema_version,
        revision,
        payload: request.payload.clone(),
        updated_at_unix_ms: request.updated_at_unix_ms,
    }
}

fn snapshot_write_outcome_revision(outcome: SnapshotWriteOutcome) -> Revision {
    match outcome {
        SnapshotWriteOutcome::Applied { revision }
        | SnapshotWriteOutcome::Duplicate { revision } => revision,
    }
}

fn snapshot_from_transactional_write(
    write: &TransactionalRecordWrite,
    revision: Revision,
) -> SnapshotEnvelope {
    SnapshotEnvelope {
        record: write.record.clone(),
        schema: write.schema.clone(),
        schema_version: write.schema_version,
        revision,
        payload: write.payload.clone(),
        updated_at_unix_ms: write.updated_at_unix_ms,
    }
}

fn snapshot_from_single_transactional_write(
    write: &TransactionalWrite,
    revision: Revision,
) -> SnapshotEnvelope {
    SnapshotEnvelope {
        record: write.record.clone(),
        schema: write.schema.clone(),
        schema_version: write.schema_version,
        revision,
        payload: write.payload.clone(),
        updated_at_unix_ms: write.updated_at_unix_ms,
    }
}

fn idempotency_matches(
    row: &Row,
    request: &SnapshotWrite,
    schema_version: i64,
    expected: Option<i64>,
    updated_at_unix_ms: i64,
) -> bool {
    row.get::<_, String>(0) == request.record.namespace
        && row.get::<_, String>(1) == request.record.key
        && row.get::<_, String>(2) == request.schema
        && row.get::<_, i64>(3) == schema_version
        && row.get::<_, Vec<u8>>(4) == request.payload
        && row.get::<_, Option<i64>>(5) == expected
        && row
            .get::<_, Option<i64>>(7)
            .is_none_or(|persisted| persisted == updated_at_unix_ms)
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

pub(crate) async fn claim_operation(
    transaction: &Transaction<'_>,
    operation_id: &str,
    operation_kind: &'static str,
) -> Result<(), StorageError> {
    transaction
        .execute(
            "INSERT INTO dbproxy_operation_claims (operation_id, operation_kind) VALUES ($1, $2) ON CONFLICT (operation_id) DO NOTHING",
            &[&operation_id, &operation_kind],
        )
        .await?;
    let claimed_kind: String = transaction
        .query_one(
            "SELECT operation_kind FROM dbproxy_operation_claims WHERE operation_id = $1",
            &[&operation_id],
        )
        .await?
        .get(0);
    if claimed_kind != operation_kind {
        return Err(StoreError::OperationIdConflict {
            operation_id: operation_id.to_string(),
        }
        .into());
    }
    Ok(())
}

pub(crate) struct PersistedTransactionalSnapshot {
    pub schema_version: i64,
    pub expected_revision: i64,
    pub new_revision: i64,
    pub updated_at_unix_ms: i64,
}

/// Persist the final snapshot with the CAS predicate repeated in the mutation itself.
///
/// The preceding `FOR UPDATE` protects existing rows. Repeating the predicate here also protects
/// first creation from a concurrent single-record API that does not participate in the advisory
/// lock used by multi-record transactions.
pub(crate) async fn persist_transactional_snapshot(
    transaction: &Transaction<'_>,
    write: &TransactionalRecordWrite,
    revision: Revision,
) -> Result<PersistedTransactionalSnapshot, StorageError> {
    let schema_version = schema_version_to_i64(write.schema_version);
    let expected_revision = required_revision_to_i64(&write.record, write.expected_revision)?;
    let new_revision = required_revision_to_i64(&write.record, revision)?;
    let updated_at_unix_ms = timestamp_to_i64(&write.record, write.updated_at_unix_ms)?;
    let persisted = transaction
        .query_opt(
            r#"
INSERT INTO dbproxy_snapshots
    (namespace, record_key, schema_name, schema_version, revision, payload, updated_at_unix_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (namespace, record_key) DO UPDATE
SET schema_name = EXCLUDED.schema_name,
    schema_version = EXCLUDED.schema_version,
    revision = EXCLUDED.revision,
    payload = EXCLUDED.payload,
    updated_at_unix_ms = EXCLUDED.updated_at_unix_ms
WHERE dbproxy_snapshots.revision = $8
RETURNING revision
"#,
            &[
                &write.record.namespace,
                &write.record.key,
                &write.schema,
                &schema_version,
                &new_revision,
                &write.payload,
                &updated_at_unix_ms,
                &expected_revision,
            ],
        )
        .await?;
    let Some(persisted) = persisted else {
        let actual = transaction
            .query_opt(
                "SELECT revision FROM dbproxy_snapshots WHERE namespace = $1 AND record_key = $2",
                &[&write.record.namespace, &write.record.key],
            )
            .await?
            .map(|row| revision_from_i64(&write.record, row.get(0)))
            .transpose()?
            .unwrap_or(Revision::ZERO);
        return Err(StoreError::RevisionConflict {
            record: write.record.clone(),
            expected: Some(write.expected_revision),
            actual,
        }
        .into());
    };
    let persisted_revision = revision_from_i64(&write.record, persisted.get(0))?;
    if persisted_revision != revision {
        return Err(StorageError::PersistenceProtocol(format!(
            "snapshot mutation returned revision {} instead of {} for {:?}",
            persisted_revision.0, revision.0, write.record
        )));
    }
    Ok(PersistedTransactionalSnapshot {
        schema_version,
        expected_revision,
        new_revision,
        updated_at_unix_ms,
    })
}

pub(crate) struct ReconnectingPostgresClient {
    url: Arc<str>,
    client: Client,
    reconnect_cooldown: Duration,
    retry_at: Option<Instant>,
}

impl ReconnectingPostgresClient {
    async fn connect(url: &str) -> Result<Self, StorageError> {
        Ok(Self {
            url: Arc::from(url),
            client: open_postgres(url).await?,
            reconnect_cooldown: Duration::ZERO,
            retry_at: None,
        })
    }

    pub(crate) async fn ensure_connected(&mut self) -> Result<(), StorageError> {
        if self.client.is_closed() {
            if let Some(remaining) = self
                .retry_at
                .and_then(|at| at.checked_duration_since(Instant::now()))
            {
                return Err(StorageError::PostgresReconnectCooldown {
                    retry_after_ms: duration_millis(remaining),
                });
            }
            let mut attempt = postgres_request::ReconnectAttempt {
                retry_at: &mut self.retry_at,
                cooldown: self.reconnect_cooldown,
                succeeded: false,
            };
            self.client = open_postgres(&self.url).await?;
            attempt.succeeded = true;
        }
        Ok(())
    }

    pub(crate) fn as_client(&self) -> &Client {
        &self.client
    }
}

impl Deref for ReconnectingPostgresClient {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl DerefMut for ReconnectingPostgresClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

async fn open_postgres(url: &str) -> Result<Client, StorageError> {
    let timeout_duration = Duration::from_millis(DEFAULT_POSTGRES_RECONNECT_TIMEOUT_MS);
    let (client, connection) = timeout(timeout_duration, tokio_postgres::connect(url, NoTls))
        .await
        .map_err(|_| StorageError::PostgresConnectTimeout {
            timeout_ms: DEFAULT_POSTGRES_RECONNECT_TIMEOUT_MS,
        })??;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::warn!(%error, "postgres connection stopped; the next operation will reconnect");
        }
    });
    Ok(client)
}

async fn apply_schema_migration(
    transaction: &Transaction<'_>,
    version: i32,
    name: &'static str,
    sql: &str,
) -> Result<(), StorageError> {
    if let Some(row) = transaction
        .query_opt(
            "SELECT name FROM dbproxy_schema_migrations WHERE version = $1",
            &[&version],
        )
        .await?
    {
        let actual: String = row.get(0);
        if actual != name {
            return Err(StorageError::SchemaMigrationConflict {
                version,
                expected: name,
                actual,
            });
        }
        return Ok(());
    }

    transaction.batch_execute(sql).await?;
    transaction
        .execute(
            "INSERT INTO dbproxy_schema_migrations (version, name) VALUES ($1, $2)",
            &[&version, &name],
        )
        .await?;
    Ok(())
}

async fn validate_snapshot_partition_layout(
    transaction: &Transaction<'_>,
) -> Result<(), StorageError> {
    let Some(parent) = transaction
        .query_opt(
            r#"
SELECT relation.relkind::TEXT, pg_get_partkeydef(relation.oid)
FROM pg_class AS relation
WHERE relation.oid = to_regclass('dbproxy_snapshots')
"#,
            &[],
        )
        .await?
    else {
        return Err(StorageError::InvalidSnapshotPartitionLayout(
            "dbproxy_snapshots does not exist".to_string(),
        ));
    };
    let relation_kind: String = parent.get(0);
    let partition_key: Option<String> = parent.get(1);
    if relation_kind != "p" || partition_key.as_deref() != Some("HASH (namespace, record_key)") {
        return Err(StorageError::InvalidSnapshotPartitionLayout(format!(
            "expected a HASH (namespace, record_key) parent, found kind={relation_kind:?}, key={partition_key:?}"
        )));
    }

    let rows = transaction
        .query(
            r#"
SELECT child.relname, pg_get_expr(child.relpartbound, child.oid)
FROM pg_inherits AS inheritance
JOIN pg_class AS child ON child.oid = inheritance.inhrelid
WHERE inheritance.inhparent = to_regclass('dbproxy_snapshots')
"#,
            &[],
        )
        .await?;
    let actual = rows
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<HashSet<_>>();
    let expected = (0..SNAPSHOT_PARTITION_COUNT)
        .map(|remainder| {
            (
                format!("dbproxy_snapshots_p{remainder:02}"),
                format!(
                    "FOR VALUES WITH (modulus {SNAPSHOT_PARTITION_COUNT}, remainder {remainder})"
                ),
            )
        })
        .collect::<HashSet<_>>();
    if actual != expected {
        return Err(StorageError::InvalidSnapshotPartitionLayout(format!(
            "expected {SNAPSHOT_PARTITION_COUNT} canonical hash partitions, found {}",
            actual.len()
        )));
    }
    Ok(())
}

pub(crate) type SharedPostgresClient = Arc<Mutex<ReconnectingPostgresClient>>;

/// PostgreSQL 快照存储。
/// PostgreSQL snapshot store.
///
/// 幂等记录与快照写入在同一个 PostgreSQL 事务中提交。
/// The idempotency receipt and snapshot mutation commit in one PostgreSQL transaction.
#[derive(Clone)]
pub struct PostgresSnapshotStore {
    client: SharedPostgresClient,
    metrics: Arc<StorageMetrics>,
    connection_wait_timeout: Option<Duration>,
}

impl PostgresSnapshotStore {
    /// 连接数据库并执行幂等表与快照表迁移。
    /// Connect and apply the snapshot/idempotency schema.
    pub async fn connect(url: &str) -> Result<Self, StorageError> {
        let store = Self::connect_existing(url).await?;
        store.migrate().await?;
        Ok(store)
    }

    /// 管理查询连接不执行迁移、不启动队列 worker。
    /// Administrative connections do not migrate schemas or start workers.
    pub async fn connect_existing(url: &str) -> Result<Self, StorageError> {
        Ok(Self {
            client: Arc::new(Mutex::new(ReconnectingPostgresClient::connect(url).await?)),
            metrics: Arc::new(StorageMetrics::default()),
            connection_wait_timeout: None,
        })
    }

    /// 请求连接在迁移完成后启用排队预算；迁移和独立维护构造入口保持原语义。
    /// Enable request budgets after migration; legacy maintenance constructors stay unchanged.
    pub async fn connect_with_request_config(
        url: &str,
        config: PostgresRequestConfig,
    ) -> Result<Self, StorageError> {
        let config = config.validate()?;
        let mut store = Self::connect(url).await?;
        store.configure_requests(config).await;
        Ok(store)
    }

    /// 仅在新连接发布给调用者前设置策略，避免克隆之间出现不同预算。
    /// Set policy before exposing a new connection, so public clones share one policy.
    async fn configure_requests(&mut self, config: PostgresRequestConfig) {
        self.connection_wait_timeout = Some(config.connection_wait_timeout);
        self.client.lock().await.reconnect_cooldown = config.reconnect_cooldown;
    }

    /// 记录排队及取消耗时；执行阶段在调用者取得锁后开始。
    /// Observe queueing and cancellation; callers start execution timing after acquisition.
    async fn request_client(
        &self,
    ) -> Result<tokio::sync::MutexGuard<'_, ReconnectingPostgresClient>, StorageError> {
        self.metrics
            .latency
            .measure(
                Stage::PostgresQueue,
                postgres_request::lock_client(&self.client, self.connection_wait_timeout),
            )
            .await
    }

    /// 在全局迁移锁下仅执行尚未登记的 schema migration。
    /// Apply each schema migration once while holding the global migration lock.
    pub async fn migrate(&self) -> Result<(), StorageError> {
        let mut client = self
            .metrics
            .latency
            .measure(Stage::PostgresQueue, self.client.lock())
            .await;
        let _postgres_timer = self.metrics.latency.start(Stage::PostgresOperation);
        client.ensure_connected().await?;
        let transaction = client.transaction().await?;
        // 多个DBProxy进程可能同时启动；事务级 advisory lock 防止DDL在 PostgreSQL 系统目录上竞争。
        // Multiple DBProxy processes may start together; a transaction advisory lock serializes DDL.
        transaction
            .execute("SELECT pg_advisory_xact_lock($1)", &[&MIGRATION_LOCK_ID])
            .await?;
        let migration_registry_exists: bool = transaction
            .query_one(
                "SELECT to_regclass('dbproxy_schema_migrations') IS NOT NULL",
                &[],
            )
            .await?
            .get(0);
        if !migration_registry_exists {
            transaction
                .batch_execute(SCHEMA_MIGRATION_BOOTSTRAP)
                .await?;
        }
        apply_schema_migration(&transaction, 1, "snapshot", SNAPSHOT_MIGRATION).await?;
        validate_snapshot_partition_layout(&transaction).await?;
        apply_schema_migration(&transaction, 2, "transactional", TRANSACTION_MIGRATION).await?;
        apply_schema_migration(
            &transaction,
            3,
            "multi-transactional",
            MULTI_TRANSACTION_MIGRATION,
        )
        .await?;
        apply_schema_migration(&transaction, 4, "cache-repair", CACHE_REPAIR_MIGRATION).await?;
        apply_schema_migration(&transaction, 5, "trade-outbox", TRADE_OUTBOX_MIGRATION).await?;
        apply_schema_migration(
            &transaction,
            6,
            "operation-registry",
            OPERATION_REGISTRY_MIGRATION,
        )
        .await?;
        apply_schema_migration(&transaction, 7, "hardening", HARDENING_MIGRATION).await?;
        apply_schema_migration(
            &transaction,
            8,
            "generic-commit",
            include_str!("../migrations/008_generic_commit.sql"),
        )
        .await?;
        apply_schema_migration(
            &transaction,
            9,
            "outbox-relay",
            include_str!("../migrations/009_outbox_relay.sql"),
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub fn cache_repair_queue(&self) -> PostgresCacheRepairQueue {
        PostgresCacheRepairQueue::new(Arc::clone(&self.client), self.connection_wait_timeout)
    }

    pub fn outbox_queue(&self) -> PostgresOutboxQueue {
        PostgresOutboxQueue::new(Arc::clone(&self.client))
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
        let mut client = self.request_client().await?;
        let _postgres_timer = self.metrics.latency.start(Stage::PostgresOperation);
        client.ensure_connected().await?;
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

    /// Save independent snapshots with one PostgreSQL transaction/commit while preserving a
    /// result for every record. Expected revision and idempotency conflicts only reject their
    /// own entry; a PostgreSQL/protocol failure aborts the complete batch so callers can retry
    /// every request with the same request IDs.
    pub async fn save_batch(
        &mut self,
        requests: &[SnapshotWrite],
    ) -> Result<Vec<Result<SnapshotWriteOutcome, StorageError>>, StorageError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let mut prepared = Vec::with_capacity(requests.len());
        let mut results = std::iter::repeat_with(|| None)
            .take(requests.len())
            .collect::<Vec<Option<Result<SnapshotWriteOutcome, StorageError>>>>();
        for (index, request) in requests.iter().enumerate() {
            let values = (|| -> Result<_, StorageError> {
                validate_request(request)?;
                Ok((
                    schema_version_to_i64(request.schema_version),
                    revision_to_i64(&request.record, request.expected_revision)?,
                    timestamp_to_i64(&request.record, request.updated_at_unix_ms)?,
                ))
            })();
            match values {
                Ok(values) => prepared.push(Some(values)),
                Err(error) => {
                    prepared.push(None);
                    results[index] = Some(Err(error));
                }
            }
        }
        if prepared.iter().all(Option::is_none) {
            return Ok(results
                .into_iter()
                .map(|result| result.expect("invalid batch entry must have a result"))
                .collect());
        }

        let mut client = self.request_client().await?;
        let _postgres_timer = self.metrics.latency.start(Stage::PostgresOperation);
        client.ensure_connected().await?;
        let transaction = client.transaction().await?;
        for (index, values) in prepared.into_iter().enumerate() {
            let Some((schema_version, expected, updated_at)) = values else {
                continue;
            };
            match save_snapshot_in_transaction(
                &transaction,
                &requests[index],
                schema_version,
                expected,
                updated_at,
            )
            .await
            {
                Ok(outcome) => results[index] = Some(Ok(outcome)),
                Err(error @ StorageError::Core(_)) => results[index] = Some(Err(error)),
                Err(error) => return Err(error),
            }
        }
        transaction.commit().await?;
        Ok(results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(StorageError::PersistenceProtocol(
                        "batch save result is missing".to_string(),
                    ))
                })
            })
            .collect())
    }
}

async fn save_snapshot_in_transaction(
    transaction: &Transaction<'_>,
    request: &SnapshotWrite,
    schema_version: i64,
    expected: Option<i64>,
    updated_at: i64,
) -> Result<SnapshotWriteOutcome, StorageError> {
    // Claim first so concurrent retries wait on the unique key and observe the first result.
    let claimed = transaction
        .query_opt(
            "INSERT INTO dbproxy_idempotency (request_id, namespace, record_key, schema_name, schema_version, payload, expected_revision, revision, updated_at_unix_ms) VALUES ($1, $2, $3, $4, $5, $6, $7, 0, $8) ON CONFLICT (request_id) DO NOTHING RETURNING request_id",
            &[
                &request.request_id,
                &request.record.namespace,
                &request.record.key,
                &request.schema,
                &schema_version,
                &request.payload,
                &expected,
                &updated_at,
            ],
        )
        .await?;

    if claimed.is_none() {
        let receipt = transaction
            .query_one(
                "SELECT namespace, record_key, schema_name, schema_version, payload, expected_revision, revision, updated_at_unix_ms FROM dbproxy_idempotency WHERE request_id = $1",
                &[&request.request_id],
            )
            .await?;
        if !idempotency_matches(&receipt, request, schema_version, expected, updated_at) {
            return Err(StoreError::IdempotencyConflict {
                request_id: request.request_id.clone(),
            }
            .into());
        }
        let revision = revision_from_i64(&request.record, receipt.get(6))?;
        cache_repair::enqueue_in_transaction(transaction, &request.record, revision).await?;
        return Ok(SnapshotWriteOutcome::Duplicate { revision });
    }

    // Update first so a non-zero expected revision can reach an existing row without also
    // permitting creation of a missing row. A source-level `SELECT ... WHERE expected = 0`
    // cannot be used here: when the predicate is false PostgreSQL never reaches ON CONFLICT,
    // so valid updates with expected revision 1+ would incorrectly report a conflict.
    let mut snapshot = transaction
        .query_opt(
            r#"
WITH updated AS (
    UPDATE dbproxy_snapshots
    SET schema_name = $3,
        schema_version = $4,
        revision = dbproxy_snapshots.revision + 1,
        payload = $5,
        updated_at_unix_ms = $6
    WHERE namespace = $1
      AND record_key = $2
      AND ($7::BIGINT IS NULL OR revision = $7)
    RETURNING revision
), inserted AS (
    INSERT INTO dbproxy_snapshots (
        namespace, record_key, schema_name, schema_version,
        revision, payload, updated_at_unix_ms
    )
    SELECT $1, $2, $3, $4, 1, $5, $6
    WHERE NOT EXISTS (SELECT 1 FROM updated)
      AND ($7::BIGINT IS NULL OR $7 = 0)
    ON CONFLICT (namespace, record_key) DO NOTHING
    RETURNING revision
)
SELECT revision FROM updated
UNION ALL
SELECT revision FROM inserted
"#,
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

    // A blind create can lose a race after the UPDATE snapshot but before INSERT. Blind writes
    // allow either create or update, so finish that rare path with an unconditional UPSERT.
    if snapshot.is_none() && expected.is_none() {
        snapshot = transaction
            .query_opt(
                "INSERT INTO dbproxy_snapshots (namespace, record_key, schema_name, schema_version, revision, payload, updated_at_unix_ms) VALUES ($1, $2, $3, $4, 1, $5, $6) ON CONFLICT (namespace, record_key) DO UPDATE SET schema_name = EXCLUDED.schema_name, schema_version = EXCLUDED.schema_version, revision = dbproxy_snapshots.revision + 1, payload = EXCLUDED.payload, updated_at_unix_ms = EXCLUDED.updated_at_unix_ms RETURNING revision",
                &[
                    &request.record.namespace,
                    &request.record.key,
                    &request.schema,
                    &schema_version,
                    &request.payload,
                    &updated_at,
                ],
            )
            .await?;
    }

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
        // A single-record save would roll back its newly claimed idempotency row with the whole
        // transaction. A shared batch must remove only this failed claim before continuing.
        transaction
            .execute(
                "DELETE FROM dbproxy_idempotency WHERE request_id = $1 AND revision = 0",
                &[&request.request_id],
            )
            .await?;
        return Err(StoreError::RevisionConflict {
            record: request.record.clone(),
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
    cache_repair::enqueue_in_transaction(transaction, &request.record, revision).await?;
    Ok(SnapshotWriteOutcome::Applied { revision })
}

#[async_trait]
impl AsyncSnapshotStore for PostgresSnapshotStore {
    type Error = StorageError;

    async fn load(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, Self::Error> {
        let mut client = self.request_client().await?;
        let _postgres_timer = self.metrics.latency.start(Stage::PostgresOperation);
        client.ensure_connected().await?;
        let row = client
            .query_opt(
                "SELECT namespace, record_key, schema_name, schema_version, revision, payload, updated_at_unix_ms FROM dbproxy_snapshots WHERE namespace = $1 AND record_key = $2",
                &[&record.namespace, &record.key],
            )
            .await?;
        row.as_ref().map(snapshot_from_row).transpose()
    }

    async fn save(&mut self, request: SnapshotWrite) -> Result<SnapshotWriteOutcome, Self::Error> {
        self.save_batch(std::slice::from_ref(&request))
            .await?
            .pop()
            .ok_or_else(|| {
                StorageError::PersistenceProtocol("single save result is missing".to_string())
            })?
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
        let mut client = self.request_client().await?;
        let _postgres_timer = self.metrics.latency.start(Stage::PostgresOperation);
        client.ensure_connected().await?;
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

        let mut client = self.request_client().await?;
        let _postgres_timer = self.metrics.latency.start(Stage::PostgresOperation);
        client.ensure_connected().await?;
        let transaction = client.transaction().await?;
        claim_operation(&transaction, &request.operation_id, "single").await?;

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
            cache_repair::enqueue_in_transaction(&transaction, &request.record, new_revision)
                .await?;
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
        cache_repair::enqueue_in_transaction(&transaction, &request.record, committed_revision)
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
        let mut client = self.request_client().await?;
        let _postgres_timer = self.metrics.latency.start(Stage::PostgresOperation);
        client.ensure_connected().await?;
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
        self.commit_records(request, CommitEffects::default()).await
    }
}

impl PostgresSnapshotStore {
    /// 原子保存记录与通用效果；兼容普通多记录事务和原始回执查询。
    /// Atomically commit records and opaque effects, preserving existing receipts.
    pub async fn commit_records(
        &mut self,
        request: MultiRecordTransactionalWrite,
        effects: CommitEffects,
    ) -> Result<MultiRecordTransactionalWriteOutcome, StorageError> {
        let effects = effects.normalize()?;
        validate_multi_transaction_request(&request)?;
        let mut request = request;
        request.writes.sort_by(|left, right| {
            left.record
                .namespace
                .cmp(&right.record.namespace)
                .then_with(|| left.record.key.cmp(&right.record.key))
        });

        let mut client = self.request_client().await?;
        let _postgres_timer = self.metrics.latency.start(Stage::PostgresOperation);
        client.ensure_connected().await?;
        let transaction = client.transaction().await?;
        let operation_id = request.operation_id.clone();
        claim_operation(&transaction, &operation_id, "multi").await?;
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
            commit::verify_retry(&transaction, &operation_id, &effects).await?;
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
            for record in &records {
                cache_repair::enqueue_in_transaction(
                    &transaction,
                    &record.record,
                    record.new_revision,
                )
                .await?;
            }
            transaction.commit().await?;
            return Ok(MultiRecordTransactionalWriteOutcome::Duplicate {
                records,
                result: header.get(0),
            });
        }

        commit::lock_partitions(&transaction, &effects).await?;
        let mut revisions = Vec::with_capacity(request.writes.len());
        for write in &request.writes {
            // 用同一个数据库事务锁住每个逻辑记录，且始终按排序后的顺序加锁，避免跨玩家交易死锁。
            // Lock records in deterministic order inside one database transaction to avoid trade deadlocks.
            let lock_key = advisory_lock_key(
                "record",
                &[write.record.namespace.as_str(), write.record.key.as_str()],
            );
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
            let persisted = persist_transactional_snapshot(&transaction, write, *revision).await?;
            transaction
                .execute(
                    "INSERT INTO dbproxy_multi_transaction_records (operation_id, namespace, record_key, schema_name, schema_version, expected_revision, payload, updated_at_unix_ms, new_revision) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                    &[
                        &operation_id,
                        &write.record.namespace,
                        &write.record.key,
                        &write.schema,
                        &persisted.schema_version,
                        &persisted.expected_revision,
                        &write.payload,
                        &persisted.updated_at_unix_ms,
                        &persisted.new_revision,
                    ],
                )
                .await?;
            cache_repair::enqueue_in_transaction(&transaction, &write.record, *revision).await?;
        }
        commit::persist(&transaction, &operation_id, &effects).await?;
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

#[derive(Clone, Copy, Debug)]
enum CacheFallbackCircuitState {
    Closed {
        consecutive_failures: u32,
        generation: u64,
    },
    Open {
        opened_at: std::time::Instant,
        generation: u64,
    },
    HalfOpen {
        generation: u64,
    },
}

#[derive(Clone, Copy, Debug)]
enum CacheFallbackCircuitPermit {
    Closed { generation: u64 },
    HalfOpen { generation: u64 },
}

/// Small in-process circuit breaker around PostgreSQL cache-fallback reads.
///
/// Each TieredSnapshotStore owns one breaker for its connection shard. The semaphore still caps
/// concurrent reads globally within that shard; this state machine prevents repeated failures from
/// immediately consuming even that bounded capacity.
struct CacheFallbackCircuit {
    state: StdMutex<CacheFallbackCircuitState>,
    failure_threshold: u32,
    cooldown: Duration,
    cooldown_ms: u64,
}

impl CacheFallbackCircuit {
    fn new(config: CacheFallbackCircuitConfig) -> Result<Self, StorageError> {
        let config = config.validate()?;
        Ok(Self {
            state: StdMutex::new(CacheFallbackCircuitState::Closed {
                consecutive_failures: 0,
                generation: 0,
            }),
            failure_threshold: config.failure_threshold,
            cooldown: config.cooldown,
            cooldown_ms: config.cooldown_ms(),
        })
    }

    fn allow(&self) -> Result<CacheFallbackCircuitPermit, StorageError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match *state {
            CacheFallbackCircuitState::Closed { generation, .. } => {
                Ok(CacheFallbackCircuitPermit::Closed { generation })
            }
            CacheFallbackCircuitState::Open {
                opened_at,
                generation,
            } => {
                let elapsed = opened_at.elapsed();
                if elapsed >= self.cooldown {
                    *state = CacheFallbackCircuitState::HalfOpen { generation };
                    Ok(CacheFallbackCircuitPermit::HalfOpen { generation })
                } else {
                    let remaining = self.cooldown.saturating_sub(elapsed);
                    let retry_after_ms = u64::try_from(remaining.as_millis()).unwrap_or(u64::MAX);
                    Err(StorageError::CacheFallbackCircuitOpen { retry_after_ms })
                }
            }
            CacheFallbackCircuitState::HalfOpen { .. } => {
                Err(StorageError::CacheFallbackCircuitOpen {
                    retry_after_ms: self.cooldown_ms,
                })
            }
        }
    }

    fn succeeded(&self, permit: CacheFallbackCircuitPermit) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match (permit, *state) {
            (
                CacheFallbackCircuitPermit::Closed {
                    generation: permit_generation,
                },
                CacheFallbackCircuitState::Closed { generation, .. },
            ) if permit_generation == generation => {
                *state = CacheFallbackCircuitState::Closed {
                    consecutive_failures: 0,
                    generation,
                };
            }
            (
                CacheFallbackCircuitPermit::HalfOpen {
                    generation: permit_generation,
                },
                CacheFallbackCircuitState::HalfOpen { generation },
            ) if permit_generation == generation => {
                *state = CacheFallbackCircuitState::Closed {
                    consecutive_failures: 0,
                    generation: generation.saturating_add(1),
                };
            }
            _ => {}
        }
    }

    fn failed(&self, permit: CacheFallbackCircuitPermit) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match (permit, *state) {
            (
                CacheFallbackCircuitPermit::HalfOpen {
                    generation: permit_generation,
                },
                CacheFallbackCircuitState::HalfOpen { generation },
            ) if permit_generation == generation => {
                *state = CacheFallbackCircuitState::Open {
                    opened_at: std::time::Instant::now(),
                    generation: generation.saturating_add(1),
                };
            }
            (
                CacheFallbackCircuitPermit::Closed {
                    generation: permit_generation,
                },
                CacheFallbackCircuitState::Closed {
                    consecutive_failures,
                    generation,
                },
            ) if permit_generation == generation => {
                let consecutive_failures = consecutive_failures.saturating_add(1);
                if consecutive_failures >= self.failure_threshold {
                    *state = CacheFallbackCircuitState::Open {
                        opened_at: std::time::Instant::now(),
                        generation: generation.saturating_add(1),
                    };
                } else {
                    *state = CacheFallbackCircuitState::Closed {
                        consecutive_failures,
                        generation,
                    };
                }
            }
            _ => {}
        }
    }
}

/// Coordinates cache misses so one hot record has at most one PostgreSQL read in flight.
///
/// The key map stores weak references: completed keys do not retain an ever-growing lock map.
/// A shared semaphore also bounds fallback reads across unrelated keys when Redis is unavailable.
struct CacheReadCoordinator {
    cache_operation_timeout: Duration,
    metrics: Arc<StorageMetrics>,
    key_locks: Mutex<HashMap<RecordKey, Weak<Mutex<()>>>>,
    fallback_slots: Arc<Semaphore>,
    refresh_slots: Arc<Semaphore>,
    fallback_timeout: Duration,
    fallback_timeout_ms: u64,
    fallback_circuit: CacheFallbackCircuit,
    fallback_lock: CacheFallbackLockConfig,
}

impl CacheReadCoordinator {
    fn new_with_circuit_and_lock(
        config: CacheFallbackConfig,
        circuit_config: CacheFallbackCircuitConfig,
        lock_config: CacheFallbackLockConfig,
    ) -> Result<Self, StorageError> {
        let config = config.validate()?;
        let lock_config = lock_config.validate()?;
        Ok(Self {
            key_locks: Mutex::new(HashMap::new()),
            cache_operation_timeout: Duration::from_millis(DEFAULT_CACHE_OPERATION_TIMEOUT_MS),
            metrics: Arc::new(StorageMetrics::default()),
            fallback_slots: Arc::new(Semaphore::new(config.max_concurrent)),
            refresh_slots: Arc::new(Semaphore::new(config.max_concurrent)),
            fallback_timeout: config.timeout,
            fallback_timeout_ms: config.timeout_ms(),
            fallback_circuit: CacheFallbackCircuit::new(circuit_config)?,
            fallback_lock: lock_config,
        })
    }

    async fn acquire_fallback(&self) -> Result<OwnedSemaphorePermit, StorageError> {
        let _timer = self.metrics.latency.start(Stage::FallbackCapacity);
        timeout(
            self.fallback_timeout,
            Arc::clone(&self.fallback_slots).acquire_owned(),
        )
        .await
        .map_err(|_| self.timeout_error())?
        .map_err(|_| StorageError::CacheFallbackGateClosed)
    }

    fn try_acquire_refresh(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.refresh_slots).try_acquire_owned().ok()
    }

    fn timeout(&self) -> Duration {
        self.fallback_timeout
    }

    fn timeout_error(&self) -> StorageError {
        StorageError::CacheFallbackTimeout {
            timeout_ms: self.fallback_timeout_ms,
        }
    }

    fn cache_timeout_error(&self, operation: &'static str) -> StorageError {
        StorageError::CacheOperationTimeout {
            operation,
            timeout_ms: duration_millis(self.cache_operation_timeout),
        }
    }

    fn allow_fallback(&self) -> Result<CacheFallbackCircuitPermit, StorageError> {
        self.fallback_circuit.allow()
    }

    fn fallback_succeeded(&self, permit: CacheFallbackCircuitPermit) {
        self.fallback_circuit.succeeded(permit);
    }

    fn fallback_failed(&self, permit: CacheFallbackCircuitPermit) {
        self.fallback_circuit.failed(permit);
    }

    fn fallback_lock_config(&self) -> CacheFallbackLockConfig {
        self.fallback_lock
    }

    async fn acquire_key(&self, record: &RecordKey) -> OwnedMutexGuard<()> {
        let _timer = self.metrics.latency.start(Stage::FallbackKey);
        let lock = {
            let mut key_locks = self.key_locks.lock().await;
            key_locks.retain(|_, lock| lock.strong_count() > 0);
            if let Some(lock) = key_locks.get(record).and_then(Weak::upgrade) {
                lock
            } else {
                let lock = Arc::new(Mutex::new(()));
                key_locks.insert(record.clone(), Arc::downgrade(&lock));
                lock
            }
        };
        lock.lock_owned().await
    }
}

/// Redis 快照缓存；只缓存 PostgreSQL 已提交的 `SnapshotEnvelope`。
/// Redis snapshot cache; it only caches PostgreSQL-committed envelopes.
#[derive(Clone)]
pub struct RedisSnapshotCache {
    connection: Arc<Mutex<ConnectionManager>>,
    metrics: Arc<StorageMetrics>,
    policy: SnapshotCacheConfig,
}

#[derive(Clone, Debug)]
enum CacheLookup {
    Miss,
    Negative,
    Fresh(SnapshotEnvelope),
    Stale(SnapshotEnvelope),
}

#[derive(Clone, Debug)]
struct CacheFallbackLock {
    key: String,
    token: String,
}

enum CacheFallbackLockOutcome {
    Acquired(CacheFallbackLock),
    CacheFilled(SnapshotEnvelope),
    NegativeFilled,
    Unavailable,
}

impl RedisSnapshotCache {
    /// 连接 Redis，不会修改现有键。
    /// Connect to Redis without modifying existing keys.
    pub async fn connect(url: &str) -> Result<Self, StorageError> {
        Self::connect_with_metrics(url, Arc::new(StorageMetrics::default())).await
    }

    pub async fn connect_with_metrics(
        url: &str,
        metrics: Arc<StorageMetrics>,
    ) -> Result<Self, StorageError> {
        Self::connect_with_metrics_and_policy(url, metrics, SnapshotCacheConfig::default()).await
    }

    pub async fn connect_with_metrics_and_policy(
        url: &str,
        metrics: Arc<StorageMetrics>,
        policy: SnapshotCacheConfig,
    ) -> Result<Self, StorageError> {
        let policy = policy.validate()?;
        let connection = open_redis_connection_manager(url).await?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            metrics,
            policy,
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

    fn revision_key(record: &RecordKey) -> String {
        format!("{}:revision", Self::cache_key(record))
    }

    fn freshness_key(record: &RecordKey) -> String {
        format!("{}:fresh-until", Self::cache_key(record))
    }

    fn negative_key(record: &RecordKey) -> String {
        format!("{}:negative", Self::cache_key(record))
    }

    fn fallback_lock_key(record: &RecordKey) -> String {
        format!("{}:fallback-lock", Self::cache_key(record))
    }

    fn fallback_lock_token() -> String {
        let sequence = CACHE_FALLBACK_LOCK_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        format!("dbproxy-{}-{timestamp}-{sequence}", std::process::id())
    }

    fn revision_token(revision: Revision) -> String {
        format!("{:020}", revision.0)
    }

    fn decode_lookup(values: &[Option<Vec<u8>>]) -> Result<CacheLookup, StorageError> {
        if values.len() != 3 {
            return Err(StorageError::CacheProtocol(format!(
                "cache lookup returned {} entries instead of 3",
                values.len()
            )));
        }
        let Some(bytes) = &values[0] else {
            return if values[2].is_some() {
                Ok(CacheLookup::Negative)
            } else {
                Ok(CacheLookup::Miss)
            };
        };
        let snapshot = bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map(|(snapshot, _)| snapshot)
            .map_err(|error| StorageError::Codec(error.to_string()))?;
        // Freshness is a short-lived Redis marker rather than an application-clock timestamp.
        // Missing markers include both naturally stale entries and entries from older versions.
        let stale = values[1].is_none();
        if stale {
            Ok(CacheLookup::Stale(snapshot))
        } else {
            Ok(CacheLookup::Fresh(snapshot))
        }
    }

    fn observe_lookup(&self, result: &Result<CacheLookup, StorageError>) {
        match result {
            Ok(CacheLookup::Fresh(_)) => self.metrics.cache_hit(),
            Ok(CacheLookup::Stale(_)) => self.metrics.cache_stale_hit(),
            Ok(CacheLookup::Negative) => self.metrics.cache_negative_hit(),
            Ok(CacheLookup::Miss) => self.metrics.cache_miss(),
            Err(_) => self.metrics.cache_read_error(),
        }
    }

    async fn lookup_unobserved(&self, record: &RecordKey) -> Result<CacheLookup, StorageError> {
        let mut connection = self.connection.lock().await;
        let keys = [
            Self::cache_key(record),
            Self::freshness_key(record),
            Self::negative_key(record),
        ];
        let values: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
            .arg(&keys)
            .query_async(&mut *connection)
            .await?;
        Self::decode_lookup(&values)
    }

    async fn lookup(&self, record: &RecordKey) -> Result<CacheLookup, StorageError> {
        let result = self.lookup_unobserved(record).await;
        self.observe_lookup(&result);
        result
    }

    async fn lookup_multi_unobserved(
        &self,
        records: &[RecordKey],
    ) -> Result<Vec<CacheLookup>, StorageError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let keys = records
            .iter()
            .flat_map(|record| {
                [
                    Self::cache_key(record),
                    Self::freshness_key(record),
                    Self::negative_key(record),
                ]
            })
            .collect::<Vec<_>>();
        let mut connection = self.connection.lock().await;
        let values: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
            .arg(keys)
            .query_async(&mut *connection)
            .await?;
        if values.len() != records.len().saturating_mul(3) {
            return Err(StorageError::CacheProtocol(format!(
                "MGET returned {} entries for {} records",
                values.len(),
                records.len()
            )));
        }
        values
            .as_chunks::<3>()
            .0
            .iter()
            .map(|chunk| Self::decode_lookup(chunk))
            .collect::<Result<Vec<_>, StorageError>>()
    }

    async fn lookup_multi(&self, records: &[RecordKey]) -> Result<Vec<CacheLookup>, StorageError> {
        let result = self.lookup_multi_unobserved(records).await;
        match &result {
            Ok(lookups) => {
                for lookup in lookups {
                    match lookup {
                        CacheLookup::Fresh(_) => self.metrics.cache_hit(),
                        CacheLookup::Stale(_) => self.metrics.cache_stale_hit(),
                        CacheLookup::Negative => self.metrics.cache_negative_hit(),
                        CacheLookup::Miss => self.metrics.cache_miss(),
                    }
                }
            }
            Err(_) => self.metrics.cache_read_error(),
        }
        result
    }

    /// 读取缓存；不存在返回 `None`。
    /// Read the cache; return `None` on a miss.
    pub async fn get(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, StorageError> {
        Ok(match self.lookup(record).await? {
            CacheLookup::Fresh(snapshot) | CacheLookup::Stale(snapshot) => Some(snapshot),
            CacheLookup::Negative | CacheLookup::Miss => None,
        })
    }

    /// 使用一次MGET读取多条缓存，并保持调用方的记录顺序与缺失位置。
    /// Read multiple cache entries with one MGET while preserving order and misses.
    pub async fn get_multi(
        &self,
        records: &[RecordKey],
    ) -> Result<Vec<Option<SnapshotEnvelope>>, StorageError> {
        Ok(self
            .lookup_multi(records)
            .await?
            .into_iter()
            .map(|lookup| match lookup {
                CacheLookup::Fresh(snapshot) | CacheLookup::Stale(snapshot) => Some(snapshot),
                CacheLookup::Negative | CacheLookup::Miss => None,
            })
            .collect())
    }

    async fn try_acquire_fallback_lock(
        &self,
        record: &RecordKey,
        token: &str,
        lease_ms: u64,
    ) -> Result<bool, StorageError> {
        let mut connection = self.connection.lock().await;
        let result: Option<String> = redis::cmd("SET")
            .arg(Self::fallback_lock_key(record))
            .arg(token)
            .arg("NX")
            .arg("PX")
            .arg(lease_ms)
            .query_async(&mut *connection)
            .await?;
        Ok(result.is_some())
    }

    async fn release_fallback_lock(&self, lock: &CacheFallbackLock) -> Result<(), StorageError> {
        let mut connection = self.connection.lock().await;
        let _: i64 = Script::new(CACHE_FALLBACK_LOCK_RELEASE_SCRIPT)
            .key(&lock.key)
            .arg(&lock.token)
            .invoke_async(&mut *connection)
            .await?;
        Ok(())
    }

    /// 写入缓存；调用方必须在权威存储成功后调用。
    /// Write the cache; callers must invoke this only after the authoritative store succeeds.
    pub async fn put(&self, snapshot: &SnapshotEnvelope) -> Result<(), StorageError> {
        let result = async {
            let bytes = bincode::serde::encode_to_vec(snapshot, bincode::config::standard())
                .map_err(|error| StorageError::Codec(error.to_string()))?;
            let revision = Self::revision_token(snapshot.revision);
            let fresh_ttl = self.policy.fresh_ttl_ms(&snapshot.record);
            let hard_ttl = self.policy.hard_ttl_ms(&snapshot.record);
            let mut connection = self.connection.lock().await;
            let _: i64 = Script::new(REVISION_AWARE_CACHE_PUT_SCRIPT)
                .key(Self::cache_key(&snapshot.record))
                .key(Self::revision_key(&snapshot.record))
                .key(Self::freshness_key(&snapshot.record))
                .key(Self::negative_key(&snapshot.record))
                .arg(bytes)
                .arg(revision)
                .arg(fresh_ttl)
                .arg(hard_ttl)
                .invoke_async(&mut *connection)
                .await?;
            Ok(())
        }
        .await;
        if result.is_ok() {
            self.metrics.cache_write();
        } else {
            self.metrics.cache_write_error();
        }
        result
    }

    /// Write a revision-aware cache batch with one Redis script invocation.
    pub async fn put_multi(&self, snapshots: &[SnapshotEnvelope]) -> Result<(), StorageError> {
        if snapshots.is_empty() {
            return Ok(());
        }
        let mut prepared = Vec::with_capacity(snapshots.len());
        for snapshot in snapshots {
            prepared.push((
                bincode::serde::encode_to_vec(snapshot, bincode::config::standard())
                    .map_err(|error| StorageError::Codec(error.to_string()))?,
                Self::revision_token(snapshot.revision),
                self.policy.fresh_ttl_ms(&snapshot.record),
                self.policy.hard_ttl_ms(&snapshot.record),
            ));
        }
        let result = async {
            let script = Script::new(REVISION_AWARE_CACHE_PUT_MULTI_SCRIPT);
            let mut invocation = script.prepare_invoke();
            for snapshot in snapshots {
                invocation
                    .key(Self::cache_key(&snapshot.record))
                    .key(Self::revision_key(&snapshot.record))
                    .key(Self::freshness_key(&snapshot.record))
                    .key(Self::negative_key(&snapshot.record));
            }
            for (bytes, revision, fresh_ttl, hard_ttl) in prepared {
                invocation
                    .arg(bytes)
                    .arg(revision)
                    .arg(fresh_ttl)
                    .arg(hard_ttl);
            }
            let _: i64 = {
                let mut connection = self.connection.lock().await;
                invocation.invoke_async(&mut *connection).await?
            };
            Ok(())
        }
        .await;
        if result.is_ok() {
            self.metrics.cache_writes(snapshots.len());
        } else {
            self.metrics.cache_write_error();
        }
        result
    }

    pub async fn put_negative(&self, record: &RecordKey) -> Result<(), StorageError> {
        self.put_negative_if_revision(record, None).await
    }

    async fn put_negative_if_revision(
        &self,
        record: &RecordKey,
        expected_revision: Option<Revision>,
    ) -> Result<(), StorageError> {
        if self.policy.negative_ttl.is_zero() {
            return Ok(());
        }
        let result = async {
            let mut connection = self.connection.lock().await;
            let stored: i64 = Script::new(NEGATIVE_CACHE_PUT_SCRIPT)
                .key(Self::cache_key(record))
                .key(Self::revision_key(record))
                .key(Self::freshness_key(record))
                .key(Self::negative_key(record))
                .arg(duration_millis(self.policy.negative_ttl).max(1))
                .arg(
                    expected_revision
                        .map(Self::revision_token)
                        .unwrap_or_default(),
                )
                .invoke_async(&mut *connection)
                .await?;
            Ok(stored == 1)
        }
        .await;
        match result {
            Ok(true) => self.metrics.cache_negative_write(),
            Ok(false) => {}
            Err(_) => self.metrics.cache_write_error(),
        }
        result.map(|_| ())
    }

    /// 删除指定快照缓存；数据库没有记录时由修复流程调用，避免保留幽灵快照。
    /// Delete one snapshot cache entry; repair uses this when the durable record is absent.
    pub async fn delete(&self, record: &RecordKey) -> Result<(), StorageError> {
        let result = async {
            let mut connection = self.connection.lock().await;
            let _: i64 = Script::new(CACHE_DELETE_SCRIPT)
                .key(Self::cache_key(record))
                .key(Self::revision_key(record))
                .key(Self::freshness_key(record))
                .key(Self::negative_key(record))
                .invoke_async(&mut *connection)
                .await?;
            Ok(())
        }
        .await;
        if result.is_err() {
            self.metrics.cache_write_error();
        }
        result
    }
}

/// PostgreSQL + Redis 的读写组合；权威顺序固定为 PostgreSQL -> Redis。
/// PostgreSQL + Redis composition; the authoritative order is always PostgreSQL -> Redis.
#[derive(Clone)]
pub struct TieredSnapshotStore {
    postgres: PostgresSnapshotStore,
    cache: RedisSnapshotCache,
    read_coordinator: Arc<CacheReadCoordinator>,
    metrics: Arc<StorageMetrics>,
    refreshing: Arc<StdMutex<HashSet<RecordKey>>>,
}

impl TieredSnapshotStore {
    /// 连接两个后端并保证 PostgreSQL schema 已就绪。
    /// Connect both backends and ensure the PostgreSQL schema exists.
    pub async fn connect(postgres_url: &str, redis_url: &str) -> Result<Self, StorageError> {
        Self::connect_with_config(
            postgres_url,
            redis_url,
            TieredSnapshotStoreConfig::default(),
            Arc::new(StorageMetrics::default()),
        )
        .await
    }

    /// Connect with explicit cache policies and shared storage telemetry.
    pub async fn connect_with_config(
        postgres_url: &str,
        redis_url: &str,
        config: TieredSnapshotStoreConfig,
        metrics: Arc<StorageMetrics>,
    ) -> Result<Self, StorageError> {
        if config.cache_operation_timeout < Duration::from_millis(1) {
            return Err(StorageError::InvalidCacheOperationTimeout);
        }
        let mut postgres =
            PostgresSnapshotStore::connect_with_request_config(postgres_url, config.postgres)
                .await?;
        // 启动迁移不混入请求路径计时；所有请求分片共享同一组指标。
        // Exclude startup migration and aggregate every request shard into shared telemetry.
        postgres.metrics = Arc::clone(&metrics);
        let cache = RedisSnapshotCache::connect_with_metrics_and_policy(
            redis_url,
            Arc::clone(&metrics),
            config.cache,
        )
        .await?;
        let mut coordinator = CacheReadCoordinator::new_with_circuit_and_lock(
            config.fallback,
            config.circuit,
            config.lock,
        )?;
        coordinator.metrics = Arc::clone(&metrics);
        coordinator.cache_operation_timeout = config.cache_operation_timeout;
        Ok(Self {
            postgres,
            cache,
            read_coordinator: Arc::new(coordinator),
            metrics,
            refreshing: Arc::new(StdMutex::new(HashSet::new())),
        })
    }

    fn record_fallback_error(&self, error: &StorageError) {
        self.metrics.postgres_fallback_error();
        match error {
            StorageError::CacheFallbackTimeout { .. } => {
                self.metrics.postgres_fallback_timeout();
            }
            StorageError::CacheFallbackCircuitOpen { .. } => {
                self.metrics.postgres_fallback_circuit_open();
            }
            _ => {}
        }
    }

    fn cache_connection_unavailable(error: &StorageError) -> bool {
        matches!(
            error,
            StorageError::Redis(_) | StorageError::CacheOperationTimeout { .. }
        )
    }

    pub fn cache_repair_queue(&self) -> PostgresCacheRepairQueue {
        self.postgres.cache_repair_queue()
    }

    pub fn outbox_queue(&self) -> PostgresOutboxQueue {
        self.postgres.outbox_queue()
    }

    /// Persist an independent snapshot batch with one PostgreSQL commit, then synchronize all
    /// committed cache entries with one Redis script and one repair acknowledgement statement.
    pub async fn save_batch(
        &mut self,
        requests: Vec<SnapshotWrite>,
    ) -> Result<Vec<Result<SnapshotWriteOutcome, StorageError>>, StorageError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let mut outcomes = self.postgres.save_batch(&requests).await?;
        let mut snapshots = Vec::with_capacity(requests.len());
        let mut duplicate_indexes = Vec::new();
        for (index, (request, outcome)) in requests.iter().zip(outcomes.iter()).enumerate() {
            match outcome {
                Ok(outcome @ SnapshotWriteOutcome::Applied { .. }) => snapshots.push(
                    snapshot_from_write(request, snapshot_write_outcome_revision(*outcome)),
                ),
                Ok(SnapshotWriteOutcome::Duplicate { .. }) => duplicate_indexes.push(index),
                Err(_) => {}
            }
        }

        if !duplicate_indexes.is_empty() {
            let records = duplicate_indexes
                .iter()
                .map(|index| requests[*index].record.clone())
                .collect::<Vec<_>>();
            let loaded = self.postgres.load_multi(&records).await?;
            for (index, snapshot) in duplicate_indexes.into_iter().zip(loaded) {
                match snapshot {
                    Some(snapshot) => snapshots.push(snapshot),
                    None => {
                        outcomes[index] = Err(StorageError::MissingAfterWrite {
                            record: requests[index].record.clone(),
                        });
                    }
                }
            }
        }
        self.synchronize_committed_cache_multi(&snapshots).await;
        Ok(outcomes)
    }

    async fn lookup_cache(
        &self,
        record: &RecordKey,
        observed: bool,
    ) -> Result<CacheLookup, StorageError> {
        let _timer = self.metrics.latency.start(Stage::CacheLookup);
        let result = if observed {
            timeout(
                self.read_coordinator.cache_operation_timeout,
                self.cache.lookup(record),
            )
            .await
        } else {
            timeout(
                self.read_coordinator.cache_operation_timeout,
                self.cache.lookup_unobserved(record),
            )
            .await
        };
        match result {
            Ok(result) => result,
            Err(_) => {
                self.metrics.cache_read_error();
                Err(self.read_coordinator.cache_timeout_error("lookup"))
            }
        }
    }

    async fn lookup_cache_multi(
        &self,
        records: &[RecordKey],
        observed: bool,
    ) -> Result<Vec<CacheLookup>, StorageError> {
        let _timer = self.metrics.latency.start(Stage::CacheLookup);
        let result = if observed {
            timeout(
                self.read_coordinator.cache_operation_timeout,
                self.cache.lookup_multi(records),
            )
            .await
        } else {
            timeout(
                self.read_coordinator.cache_operation_timeout,
                self.cache.lookup_multi_unobserved(records),
            )
            .await
        };
        match result {
            Ok(result) => result,
            Err(_) => {
                self.metrics.cache_read_error();
                Err(self.read_coordinator.cache_timeout_error("batch lookup"))
            }
        }
    }

    async fn put_cache(&self, snapshot: &SnapshotEnvelope) -> Result<(), StorageError> {
        let _timer = self.metrics.latency.start(Stage::CacheWrite);
        match timeout(
            self.read_coordinator.cache_operation_timeout,
            self.cache.put(snapshot),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                self.metrics.cache_write_error();
                Err(self.read_coordinator.cache_timeout_error("write"))
            }
        }
    }

    async fn put_cache_multi(&self, snapshots: &[SnapshotEnvelope]) -> Result<(), StorageError> {
        let _timer = self.metrics.latency.start(Stage::CacheWrite);
        match timeout(
            self.read_coordinator.cache_operation_timeout,
            self.cache.put_multi(snapshots),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                self.metrics.cache_write_error();
                Err(self.read_coordinator.cache_timeout_error("batch write"))
            }
        }
    }

    /// Best-effort fast path after a durable commit. The PostgreSQL repair row was inserted in
    /// the same transaction, so Redis failure never turns a committed write into an ambiguous
    /// client error; a worker will retry it later.
    async fn synchronize_committed_cache(&self, snapshot: &SnapshotEnvelope) {
        let _timer = self.metrics.latency.start(Stage::CommittedCacheSync);
        if let Err(error) = self.put_cache(snapshot).await {
            tracing::warn!(
                %error,
                namespace = %snapshot.record.namespace,
                key = %snapshot.record.key,
                revision = snapshot.revision.0,
                "snapshot cache update deferred to durable repair queue"
            );
            return;
        }
        if let Err(error) = self
            .metrics
            .latency
            .measure(
                Stage::RepairAck,
                self.postgres
                    .cache_repair_queue()
                    .acknowledge_cached(&snapshot.record, snapshot.revision),
            )
            .await
        {
            tracing::warn!(
                %error,
                namespace = %snapshot.record.namespace,
                key = %snapshot.record.key,
                revision = snapshot.revision.0,
                "cache was updated but durable repair acknowledgement failed"
            );
        }
    }

    /// Best-effort cache synchronization for a committed record batch. PostgreSQL repair rows
    /// remain durable until both the revision-aware Redis script and the bulk acknowledgement
    /// succeed, so an interrupted fast path is recovered by the normal worker.
    async fn synchronize_committed_cache_multi(&self, snapshots: &[SnapshotEnvelope]) {
        if snapshots.is_empty() {
            return;
        }
        let _timer = self.metrics.latency.start(Stage::CommittedCacheSync);
        if let Err(error) = self.put_cache_multi(snapshots).await {
            tracing::warn!(
                %error,
                record_count = snapshots.len(),
                "snapshot cache batch update deferred to durable repair queue"
            );
            return;
        }
        if let Err(error) = self
            .metrics
            .latency
            .measure(
                Stage::RepairAck,
                self.postgres
                    .cache_repair_queue()
                    .acknowledge_cached_multi(snapshots),
            )
            .await
        {
            tracing::warn!(
                %error,
                record_count = snapshots.len(),
                "snapshot cache batch was updated but durable repair acknowledgement failed"
            );
        }
    }

    async fn put_negative_cache(
        &self,
        record: &RecordKey,
        expected_revision: Option<Revision>,
    ) -> Result<(), StorageError> {
        let _timer = self.metrics.latency.start(Stage::CacheWrite);
        match timeout(
            self.read_coordinator.cache_operation_timeout,
            self.cache
                .put_negative_if_revision(record, expected_revision),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                self.metrics.cache_write_error();
                Err(self.read_coordinator.cache_timeout_error("negative write"))
            }
        }
    }

    async fn delete_cache(&self, record: &RecordKey) -> Result<(), StorageError> {
        let _timer = self.metrics.latency.start(Stage::CacheWrite);
        match timeout(
            self.read_coordinator.cache_operation_timeout,
            self.cache.delete(record),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                self.metrics.cache_write_error();
                Err(self.read_coordinator.cache_timeout_error("delete"))
            }
        }
    }

    async fn acquire_fallback_lock(&self, record: &RecordKey) -> CacheFallbackLockOutcome {
        self.acquire_fallback_lock_with_wait(
            record,
            self.read_coordinator.fallback_lock_config().wait,
        )
        .await
    }

    async fn acquire_fallback_lock_with_wait(
        &self,
        record: &RecordKey,
        max_wait: Duration,
    ) -> CacheFallbackLockOutcome {
        let _timer = self.metrics.latency.start(Stage::FallbackLease);
        let config = self.read_coordinator.fallback_lock_config();
        let lock = CacheFallbackLock {
            key: RedisSnapshotCache::fallback_lock_key(record),
            token: RedisSnapshotCache::fallback_lock_token(),
        };
        let started = Instant::now();
        loop {
            let remaining_before_attempt = max_wait.saturating_sub(started.elapsed());
            if remaining_before_attempt.is_zero() {
                self.metrics.cache_fallback_lock_timeout();
                return CacheFallbackLockOutcome::Unavailable;
            }
            let attempt = timeout(
                self.read_coordinator
                    .cache_operation_timeout
                    .min(remaining_before_attempt),
                self.cache
                    .try_acquire_fallback_lock(record, &lock.token, config.lease_ms()),
            )
            .await;
            match attempt {
                Err(_) => {
                    self.metrics.cache_fallback_lock_error();
                    tracing::debug!(namespace = %record.namespace, key = %record.key, "distributed cache fallback lock command timed out; proceeding without lock");
                    return CacheFallbackLockOutcome::Unavailable;
                }
                Ok(Err(error)) => {
                    self.metrics.cache_fallback_lock_error();
                    tracing::debug!(%error, namespace = %record.namespace, key = %record.key, "distributed cache fallback lock unavailable; proceeding without lock");
                    return CacheFallbackLockOutcome::Unavailable;
                }
                Ok(Ok(true)) => {
                    self.metrics.cache_fallback_lock_acquired();
                    return CacheFallbackLockOutcome::Acquired(lock);
                }
                Ok(Ok(false)) => {
                    self.metrics.cache_fallback_lock_contention();
                }
            }

            let remaining_before_recheck = max_wait.saturating_sub(started.elapsed());
            if remaining_before_recheck.is_zero() {
                self.metrics.cache_fallback_lock_timeout();
                return CacheFallbackLockOutcome::Unavailable;
            }
            let recheck = timeout(
                self.read_coordinator
                    .cache_operation_timeout
                    .min(remaining_before_recheck),
                self.cache.lookup_unobserved(record),
            )
            .await;
            match recheck {
                Err(_) => {
                    self.metrics.cache_fallback_lock_error();
                    tracing::debug!(namespace = %record.namespace, key = %record.key, "distributed cache fallback lock recheck timed out; proceeding without lock");
                    return CacheFallbackLockOutcome::Unavailable;
                }
                Ok(Err(error)) => {
                    self.metrics.cache_fallback_lock_error();
                    tracing::debug!(%error, namespace = %record.namespace, key = %record.key, "distributed cache fallback lock recheck failed; proceeding without lock");
                    return CacheFallbackLockOutcome::Unavailable;
                }
                Ok(Ok(CacheLookup::Fresh(snapshot) | CacheLookup::Stale(snapshot))) => {
                    return CacheFallbackLockOutcome::CacheFilled(snapshot);
                }
                Ok(Ok(CacheLookup::Negative)) => return CacheFallbackLockOutcome::NegativeFilled,
                Ok(Ok(CacheLookup::Miss)) => {}
            }

            let elapsed = started.elapsed();
            if elapsed >= max_wait {
                self.metrics.cache_fallback_lock_timeout();
                return CacheFallbackLockOutcome::Unavailable;
            }
            let remaining = max_wait.saturating_sub(elapsed);
            sleep(remaining.min(config.poll_interval)).await;
        }
    }

    async fn release_fallback_lock(&self, lock: CacheFallbackLock) {
        let _timer = self.metrics.latency.start(Stage::FallbackRelease);
        match timeout(
            self.read_coordinator.cache_operation_timeout,
            self.cache.release_fallback_lock(&lock),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                self.metrics.cache_fallback_lock_release_error();
                tracing::warn!(%error, key = %lock.key, "failed to release distributed cache fallback lock; Redis TTL will recover it");
            }
            Err(_) => {
                self.metrics.cache_fallback_lock_release_error();
                tracing::warn!(key = %lock.key, "distributed cache fallback lock release timed out; Redis TTL will recover it");
            }
        }
    }

    async fn release_fallback_locks(&self, locks: Vec<CacheFallbackLock>) {
        for lock in locks {
            self.release_fallback_lock(lock).await;
        }
    }

    fn schedule_cache_refresh(&self, record: &RecordKey) {
        let Some(refresh_slot) = self.read_coordinator.try_acquire_refresh() else {
            return;
        };
        let should_start = {
            let mut refreshing = self
                .refreshing
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            refreshing.insert(record.clone())
        };
        if !should_start {
            return;
        }
        self.metrics.cache_refresh_started();
        let store = self.clone();
        let record = record.clone();
        tokio::spawn(async move {
            let _refresh_slot = refresh_slot;
            match store.refresh_cache(&record).await {
                Ok(()) => store.metrics.cache_refresh_completed(),
                Err(error) => {
                    store.metrics.cache_refresh_error();
                    tracing::debug!(%error, namespace = %record.namespace, key = %record.key, "stale snapshot refresh failed")
                }
            }
            let mut refreshing = store
                .refreshing
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            refreshing.remove(&record);
        });
    }

    async fn refresh_cache(&self, record: &RecordKey) -> Result<(), StorageError> {
        let _fallback_slot = self.read_coordinator.acquire_fallback().await?;
        let _key_guard = self.read_coordinator.acquire_key(record).await;
        let mut stale_revision = match self.lookup_cache(record, false).await {
            Ok(CacheLookup::Fresh(_) | CacheLookup::Negative) => return Ok(()),
            Ok(CacheLookup::Stale(snapshot)) => Some(snapshot.revision),
            Ok(CacheLookup::Miss) | Err(_) => None,
        };

        let mut distributed_lock = match self.acquire_fallback_lock(record).await {
            CacheFallbackLockOutcome::Acquired(lock) => Some(lock),
            CacheFallbackLockOutcome::CacheFilled(_) | CacheFallbackLockOutcome::NegativeFilled => {
                return Ok(());
            }
            CacheFallbackLockOutcome::Unavailable => None,
        };
        if distributed_lock.is_some() {
            match self.lookup_cache(record, false).await {
                Ok(CacheLookup::Fresh(_) | CacheLookup::Negative) => {
                    if let Some(lock) = distributed_lock.take() {
                        self.release_fallback_lock(lock).await;
                    }
                    return Ok(());
                }
                Ok(CacheLookup::Stale(snapshot)) => stale_revision = Some(snapshot.revision),
                Ok(CacheLookup::Miss) | Err(_) => stale_revision = None,
            }
        }

        let circuit_permit = match self.read_coordinator.allow_fallback() {
            Ok(permit) => permit,
            Err(error) => {
                if let Some(lock) = distributed_lock.take() {
                    self.release_fallback_lock(lock).await;
                }
                self.record_fallback_error(&error);
                return Err(error);
            }
        };
        self.metrics.postgres_fallback();
        let loaded =
            match timeout(self.read_coordinator.timeout(), self.postgres.load(record)).await {
                Ok(Ok(snapshot)) => {
                    self.read_coordinator.fallback_succeeded(circuit_permit);
                    snapshot
                }
                Ok(Err(error)) => {
                    self.read_coordinator.fallback_failed(circuit_permit);
                    self.record_fallback_error(&error);
                    if let Some(lock) = distributed_lock.take() {
                        self.release_fallback_lock(lock).await;
                    }
                    return Err(error);
                }
                Err(_) => {
                    self.read_coordinator.fallback_failed(circuit_permit);
                    let error = self.read_coordinator.timeout_error();
                    self.record_fallback_error(&error);
                    if let Some(lock) = distributed_lock.take() {
                        self.release_fallback_lock(lock).await;
                    }
                    return Err(error);
                }
            };
        let cache_result = match loaded {
            Some(snapshot) => self.put_cache(&snapshot).await,
            None => self.put_negative_cache(record, stale_revision).await,
        };
        if let Some(lock) = distributed_lock.take() {
            self.release_fallback_lock(lock).await;
        }
        cache_result
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
                self.put_cache(&snapshot).await?;
                Ok(Some(revision))
            }
            None => {
                self.delete_cache(record).await?;
                Ok(None)
            }
        }
    }

    /// 批量读取缓存，并用一次PostgreSQL查询回源全部未命中记录。
    /// Batch-read cache entries and resolve all misses with one PostgreSQL query.
    /// Concurrent callers recheck under per-record gates so a hot miss is not fanned out to PostgreSQL.
    pub async fn load_multi(
        &self,
        records: &[RecordKey],
    ) -> Result<Vec<Option<SnapshotEnvelope>>, StorageError> {
        let mut cache_available = true;
        let lookups = match self.lookup_cache_multi(records, true).await {
            Ok(lookups) => lookups,
            Err(error) => {
                cache_available = !Self::cache_connection_unavailable(&error);
                tracing::warn!(%error, record_count = records.len(), "snapshot cache batch read failed; falling back to postgres");
                vec![CacheLookup::Miss; records.len()]
            }
        };
        let mut snapshots = vec![None; records.len()];
        let mut missing_indices = Vec::new();
        for (index, lookup) in lookups.into_iter().enumerate() {
            match lookup {
                CacheLookup::Fresh(snapshot) => snapshots[index] = Some(snapshot),
                CacheLookup::Stale(snapshot) => {
                    snapshots[index] = Some(snapshot);
                    self.schedule_cache_refresh(&records[index]);
                }
                CacheLookup::Negative => {}
                CacheLookup::Miss => missing_indices.push(index),
            }
        }
        let misses = missing_indices
            .into_iter()
            .map(|index| (index, records[index].clone()))
            .collect::<Vec<_>>();
        if misses.is_empty() {
            return Ok(snapshots);
        }

        // Acquire the shared fallback slot before key locks. Keeping one ordering prevents a
        // multi-record read from holding a key while waiting for capacity another read owns.
        let _fallback_slot = match self.read_coordinator.acquire_fallback().await {
            Ok(slot) => slot,
            Err(error) => {
                self.metrics.postgres_fallback();
                self.record_fallback_error(&error);
                return Err(error);
            }
        };
        let mut lock_records = misses
            .iter()
            .map(|(_, record)| record.clone())
            .collect::<Vec<_>>();
        lock_records.sort_by(|left, right| {
            left.namespace
                .cmp(&right.namespace)
                .then_with(|| left.key.cmp(&right.key))
        });
        lock_records.dedup();
        let mut _key_guards = Vec::with_capacity(lock_records.len());
        for record in &lock_records {
            _key_guards.push(self.read_coordinator.acquire_key(record).await);
        }

        let missing_records = misses
            .iter()
            .map(|(_, record)| record.clone())
            .collect::<Vec<_>>();
        let mut still_missing = misses.clone();
        if cache_available {
            match self.lookup_cache_multi(&missing_records, false).await {
                Ok(refreshed) if refreshed.len() == missing_records.len() => {
                    still_missing.clear();
                    for ((request_index, record), lookup) in misses.iter().zip(refreshed) {
                        match lookup {
                            CacheLookup::Fresh(snapshot) => {
                                snapshots[*request_index] = Some(snapshot)
                            }
                            CacheLookup::Stale(snapshot) => {
                                snapshots[*request_index] = Some(snapshot);
                                self.schedule_cache_refresh(record);
                            }
                            CacheLookup::Negative => {}
                            CacheLookup::Miss => {
                                still_missing.push((*request_index, record.clone()));
                            }
                        }
                    }
                }
                Ok(refreshed) => {
                    tracing::warn!(
                        expected = missing_records.len(),
                        actual = refreshed.len(),
                        "snapshot cache batch recheck returned a mismatched result"
                    );
                }
                Err(error) => {
                    cache_available &= !Self::cache_connection_unavailable(&error);
                    tracing::warn!(%error, record_count = missing_records.len(), "snapshot cache batch recheck failed; falling back to postgres");
                }
            }
        }
        if still_missing.is_empty() {
            return Ok(snapshots);
        }

        let recheck_targets = still_missing.clone();
        let durable_records = recheck_targets
            .iter()
            .map(|(_, record)| record.clone())
            .collect::<Vec<_>>();
        let mut distributed_lock_records = durable_records.clone();
        distributed_lock_records.sort_by(|left, right| {
            left.namespace
                .cmp(&right.namespace)
                .then_with(|| left.key.cmp(&right.key))
        });
        distributed_lock_records.dedup();
        let mut distributed_locks = Vec::with_capacity(distributed_lock_records.len());
        let lock_started = Instant::now();
        let lock_wait = self.read_coordinator.fallback_lock_config().wait;
        let mut lock_coordination_failed = false;
        if cache_available {
            for record in &distributed_lock_records {
                let remaining = lock_wait.saturating_sub(lock_started.elapsed());
                if remaining.is_zero() {
                    self.metrics.cache_fallback_lock_timeout();
                    lock_coordination_failed = true;
                    break;
                }
                match self
                    .acquire_fallback_lock_with_wait(record, remaining)
                    .await
                {
                    CacheFallbackLockOutcome::Acquired(lock) => distributed_locks.push(lock),
                    CacheFallbackLockOutcome::CacheFilled(_)
                    | CacheFallbackLockOutcome::NegativeFilled => {}
                    CacheFallbackLockOutcome::Unavailable => {
                        lock_coordination_failed = true;
                        break;
                    }
                }
            }
        }
        if lock_coordination_failed && !distributed_locks.is_empty() {
            tracing::debug!(
                lock_count = distributed_locks.len(),
                "batch fallback lock coordination stopped; leases will expire by Redis TTL"
            );
            distributed_locks.clear();
        }

        // A different DBProxy instance may have filled one of the entries while we waited for
        // its lease. Recheck after all locks are acquired before touching PostgreSQL.
        if cache_available {
            match self.lookup_cache_multi(&durable_records, false).await {
                Ok(refreshed) if refreshed.len() == durable_records.len() => {
                    still_missing.clear();
                    for ((request_index, record), lookup) in recheck_targets.iter().zip(refreshed) {
                        match lookup {
                            CacheLookup::Fresh(snapshot) => {
                                snapshots[*request_index] = Some(snapshot)
                            }
                            CacheLookup::Stale(snapshot) => {
                                snapshots[*request_index] = Some(snapshot);
                                self.schedule_cache_refresh(record);
                            }
                            CacheLookup::Negative => {}
                            CacheLookup::Miss => {
                                still_missing.push((*request_index, record.clone()));
                            }
                        }
                    }
                }
                Ok(refreshed) => {
                    tracing::warn!(
                        expected = durable_records.len(),
                        actual = refreshed.len(),
                        "snapshot cache distributed-lock recheck returned a mismatched result"
                    );
                }
                Err(error) => {
                    cache_available &= !Self::cache_connection_unavailable(&error);
                    tracing::debug!(%error, record_count = durable_records.len(), "snapshot cache distributed-lock recheck failed; falling back to postgres");
                }
            }
        }
        if still_missing.is_empty() {
            self.release_fallback_locks(distributed_locks).await;
            return Ok(snapshots);
        }

        let durable_records = still_missing
            .iter()
            .map(|(_, record)| record.clone())
            .collect::<Vec<_>>();
        let circuit_permit = match self.read_coordinator.allow_fallback() {
            Ok(permit) => permit,
            Err(error) => {
                self.release_fallback_locks(distributed_locks).await;
                self.metrics.postgres_fallback();
                self.record_fallback_error(&error);
                return Err(error);
            }
        };
        self.metrics.postgres_fallback();
        let loaded = match timeout(
            self.read_coordinator.timeout(),
            self.postgres.load_multi(&durable_records),
        )
        .await
        {
            Ok(Ok(loaded)) => {
                self.read_coordinator.fallback_succeeded(circuit_permit);
                loaded
            }
            Ok(Err(error)) => {
                self.read_coordinator.fallback_failed(circuit_permit);
                self.record_fallback_error(&error);
                self.release_fallback_locks(distributed_locks).await;
                return Err(error);
            }
            Err(_) => {
                self.read_coordinator.fallback_failed(circuit_permit);
                let error = self.read_coordinator.timeout_error();
                self.record_fallback_error(&error);
                self.release_fallback_locks(distributed_locks).await;
                return Err(error);
            }
        };
        for ((request_index, record), snapshot) in still_missing.into_iter().zip(loaded) {
            if cache_available {
                if let Some(snapshot) = &snapshot
                    && let Err(error) = self.put_cache(snapshot).await
                {
                    tracing::warn!(%error, namespace = %snapshot.record.namespace, key = %snapshot.record.key, "snapshot cache batch warmup failed");
                } else if snapshot.is_none()
                    && let Err(error) = self.put_negative_cache(&record, None).await
                {
                    tracing::warn!(%error, namespace = %record.namespace, key = %record.key, "negative snapshot cache batch warmup failed");
                }
            }
            snapshots[request_index] = snapshot;
        }
        self.release_fallback_locks(distributed_locks).await;
        Ok(snapshots)
    }
}

#[async_trait]
impl AsyncSnapshotStore for TieredSnapshotStore {
    type Error = StorageError;

    async fn load(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, Self::Error> {
        // Redis是加速层；读取失败必须回源权威PostgreSQL，不能把缓存故障扩大成数据不可用。
        // Redis is an acceleration layer; read failures must fall back to authoritative PostgreSQL.
        let mut cache_available = true;
        let cached = match self.lookup_cache(record, true).await {
            Ok(lookup) => lookup,
            Err(error) => {
                cache_available = !Self::cache_connection_unavailable(&error);
                tracing::warn!(%error, namespace = %record.namespace, key = %record.key, "snapshot cache read failed; falling back to postgres");
                CacheLookup::Miss
            }
        };
        match cached {
            CacheLookup::Fresh(snapshot) => return Ok(Some(snapshot)),
            CacheLookup::Stale(snapshot) => {
                self.schedule_cache_refresh(record);
                return Ok(Some(snapshot));
            }
            CacheLookup::Negative => return Ok(None),
            CacheLookup::Miss => {}
        }

        // Serialize the fallback path for this record and cap unrelated misses globally.
        let _fallback_slot = match self.read_coordinator.acquire_fallback().await {
            Ok(slot) => slot,
            Err(error) => {
                self.metrics.postgres_fallback();
                self.record_fallback_error(&error);
                return Err(error);
            }
        };
        let _key_guard = self.read_coordinator.acquire_key(record).await;
        let cached = if cache_available {
            match self.lookup_cache(record, false).await {
                Ok(lookup) => lookup,
                Err(error) => {
                    cache_available &= !Self::cache_connection_unavailable(&error);
                    tracing::warn!(%error, namespace = %record.namespace, key = %record.key, "snapshot cache recheck failed; falling back to postgres");
                    CacheLookup::Miss
                }
            }
        } else {
            CacheLookup::Miss
        };
        match cached {
            CacheLookup::Fresh(snapshot) => return Ok(Some(snapshot)),
            CacheLookup::Stale(snapshot) => {
                self.schedule_cache_refresh(record);
                return Ok(Some(snapshot));
            }
            CacheLookup::Negative => return Ok(None),
            CacheLookup::Miss => {}
        }

        let mut distributed_lock = if cache_available {
            match self.acquire_fallback_lock(record).await {
                CacheFallbackLockOutcome::Acquired(lock) => Some(lock),
                CacheFallbackLockOutcome::CacheFilled(snapshot) => return Ok(Some(snapshot)),
                CacheFallbackLockOutcome::NegativeFilled => return Ok(None),
                CacheFallbackLockOutcome::Unavailable => None,
            }
        } else {
            None
        };
        if distributed_lock.is_some() {
            let cached = match self.lookup_cache(record, false).await {
                Ok(lookup) => lookup,
                Err(error) => {
                    cache_available &= !Self::cache_connection_unavailable(&error);
                    tracing::debug!(%error, namespace = %record.namespace, key = %record.key, "distributed-lock cache recheck failed; falling back to postgres");
                    CacheLookup::Miss
                }
            };
            match cached {
                CacheLookup::Fresh(snapshot) => {
                    if let Some(lock) = distributed_lock.take() {
                        self.release_fallback_lock(lock).await;
                    }
                    return Ok(Some(snapshot));
                }
                CacheLookup::Stale(snapshot) => {
                    if let Some(lock) = distributed_lock.take() {
                        self.release_fallback_lock(lock).await;
                    }
                    self.schedule_cache_refresh(record);
                    return Ok(Some(snapshot));
                }
                CacheLookup::Negative => {
                    if let Some(lock) = distributed_lock.take() {
                        self.release_fallback_lock(lock).await;
                    }
                    return Ok(None);
                }
                CacheLookup::Miss => {}
            }
        }

        let circuit_permit = match self.read_coordinator.allow_fallback() {
            Ok(permit) => permit,
            Err(error) => {
                if let Some(lock) = distributed_lock.take() {
                    self.release_fallback_lock(lock).await;
                }
                self.metrics.postgres_fallback();
                self.record_fallback_error(&error);
                return Err(error);
            }
        };
        self.metrics.postgres_fallback();
        let fallback_result =
            match timeout(self.read_coordinator.timeout(), self.postgres.load(record)).await {
                Ok(Ok(snapshot)) => {
                    self.read_coordinator.fallback_succeeded(circuit_permit);
                    Ok(snapshot)
                }
                Ok(Err(error)) => {
                    self.read_coordinator.fallback_failed(circuit_permit);
                    self.record_fallback_error(&error);
                    Err(error)
                }
                Err(_) => {
                    self.read_coordinator.fallback_failed(circuit_permit);
                    let error = self.read_coordinator.timeout_error();
                    self.record_fallback_error(&error);
                    Err(error)
                }
            };
        let snapshot = match fallback_result {
            Ok(snapshot) => snapshot,
            Err(error) => {
                if let Some(lock) = distributed_lock.take() {
                    self.release_fallback_lock(lock).await;
                }
                return Err(error);
            }
        };
        if cache_available {
            if let Some(snapshot) = &snapshot
                && let Err(error) = self.put_cache(snapshot).await
            {
                tracing::warn!(%error, namespace = %record.namespace, key = %record.key, "snapshot cache warmup failed");
            } else if snapshot.is_none()
                && let Err(error) = self.put_negative_cache(record, None).await
            {
                tracing::warn!(%error, namespace = %record.namespace, key = %record.key, "negative snapshot cache warmup failed");
            }
        }
        if let Some(lock) = distributed_lock.take() {
            self.release_fallback_lock(lock).await;
        }
        Ok(snapshot)
    }

    async fn save(&mut self, request: SnapshotWrite) -> Result<SnapshotWriteOutcome, Self::Error> {
        self.save_batch(vec![request]).await?.pop().ok_or_else(|| {
            StorageError::PersistenceProtocol("single tiered save result is missing".to_string())
        })?
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
        let committed_write = request.clone();
        let outcome = self.postgres.apply(request).await?;
        let (revision, duplicate) = match &outcome {
            TransactionalWriteOutcome::Applied { new_revision, .. } => (*new_revision, false),
            TransactionalWriteOutcome::Duplicate { new_revision, .. } => (*new_revision, true),
        };
        let snapshot = if duplicate {
            self.postgres
                .load(&committed_write.record)
                .await?
                .ok_or_else(|| StorageError::MissingAfterWrite {
                    record: committed_write.record.clone(),
                })?
        } else {
            snapshot_from_single_transactional_write(&committed_write, revision)
        };
        self.synchronize_committed_cache(&snapshot).await;
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
        self.commit_records(request, CommitEffects::default()).await
    }
}

impl TieredSnapshotStore {
    /// 提交后缓存失败由持久修复队列收敛，不重做领域决策。
    /// Durable repair handles cache failure after an authoritative commit.
    pub async fn commit_records(
        &mut self,
        request: MultiRecordTransactionalWrite,
        effects: CommitEffects,
    ) -> Result<MultiRecordTransactionalWriteOutcome, StorageError> {
        let committed_writes = request.writes.clone();
        let outcome = self.postgres.commit_records(request, effects).await?;
        let (records, duplicate) = match &outcome {
            MultiRecordTransactionalWriteOutcome::Applied { records, .. } => (records, false),
            MultiRecordTransactionalWriteOutcome::Duplicate { records, .. } => (records, true),
        };
        let snapshots = if duplicate {
            let keys = committed_writes
                .iter()
                .map(|write| write.record.clone())
                .collect::<Vec<_>>();
            self.postgres
                .load_multi(&keys)
                .await?
                .into_iter()
                .zip(keys)
                .map(|(snapshot, record)| {
                    snapshot.ok_or(StorageError::MissingAfterWrite { record })
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            let revisions = records
                .iter()
                .map(|record| (record.record.clone(), record.new_revision))
                .collect::<HashMap<_, _>>();
            committed_writes
                .iter()
                .map(|write| {
                    let revision = revisions.get(&write.record).copied().ok_or_else(|| {
                        StorageError::PersistenceProtocol(format!(
                            "multi transaction result is missing {:?}",
                            write.record
                        ))
                    })?;
                    Ok(snapshot_from_transactional_write(write, revision))
                })
                .collect::<Result<Vec<_>, StorageError>>()?
        };
        self.synchronize_committed_cache_multi(&snapshots).await;
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tiangz_dbproxy_core::RecordKey;
    use tokio::sync::Notify;

    #[tokio::test]
    async fn cache_read_coordinator_serializes_same_record() {
        let coordinator = Arc::new(
            CacheReadCoordinator::new_with_circuit_and_lock(
                CacheFallbackConfig::default(),
                CacheFallbackCircuitConfig::default(),
                CacheFallbackLockConfig::default(),
            )
            .unwrap(),
        );
        let record = RecordKey::new("tests", "hot-key").unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let first_coordinator = Arc::clone(&coordinator);
        let first_record = record.clone();
        let first_entered = Arc::clone(&entered);
        let first_release = Arc::clone(&release);
        let first = tokio::spawn(async move {
            let _fallback = first_coordinator.acquire_fallback().await.unwrap();
            let _guard = first_coordinator.acquire_key(&first_record).await;
            first_entered.notify_one();
            first_release.notified().await;
        });

        entered.notified().await;
        let second_coordinator = Arc::clone(&coordinator);
        let second_record = record.clone();
        let second_entered = Arc::new(Notify::new());
        let second_entered_signal = Arc::clone(&second_entered);
        let second = tokio::spawn(async move {
            let _fallback = second_coordinator.acquire_fallback().await.unwrap();
            let _guard = second_coordinator.acquire_key(&second_record).await;
            second_entered_signal.notify_one();
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(50), second_entered.notified())
                .await
                .is_err(),
            "a concurrent miss for one record must wait for the first loader"
        );
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), second_entered.notified())
            .await
            .expect("the second loader should proceed after the first releases the key");
        first.await.unwrap();
        second.await.unwrap();
        let snapshot = coordinator.metrics.latency_snapshot();
        let key = snapshot
            .iter()
            .find(|sample| sample.stage == "fallback_key_wait")
            .unwrap();
        assert_eq!(key.buckets.iter().sum::<u64>(), 2);
        assert_eq!(key.in_flight, 0);
    }

    #[tokio::test]
    async fn cache_read_coordinator_times_out_when_all_slots_are_busy() {
        let config = CacheFallbackConfig {
            max_concurrent: 1,
            timeout: Duration::from_millis(20),
        };
        let coordinator = CacheReadCoordinator::new_with_circuit_and_lock(
            config,
            CacheFallbackCircuitConfig::default(),
            CacheFallbackLockConfig::default(),
        )
        .unwrap();
        let _held = coordinator.acquire_fallback().await.unwrap();
        assert!(matches!(
            coordinator.acquire_fallback().await,
            Err(StorageError::CacheFallbackTimeout { timeout_ms: 20 })
        ));
        let snapshot = coordinator.metrics.latency_snapshot();
        let capacity = snapshot
            .iter()
            .find(|sample| sample.stage == "fallback_capacity_wait")
            .unwrap();
        assert_eq!(capacity.buckets.iter().sum::<u64>(), 2);
        assert_eq!(capacity.in_flight, 0);
        assert!(capacity.sum_micros >= 20_000);
        assert_eq!(
            snapshot
                .iter()
                .find(|sample| sample.stage == "postgres_operation")
                .unwrap()
                .buckets
                .iter()
                .sum::<u64>(),
            0
        );
    }

    #[test]
    fn cache_fallback_config_rejects_zero_limits() {
        assert!(matches!(
            CacheReadCoordinator::new_with_circuit_and_lock(
                CacheFallbackConfig {
                    max_concurrent: 0,
                    timeout: Duration::from_secs(1),
                },
                CacheFallbackCircuitConfig::default(),
                CacheFallbackLockConfig::default(),
            ),
            Err(StorageError::InvalidCacheFallbackConcurrency)
        ));
        assert!(matches!(
            CacheReadCoordinator::new_with_circuit_and_lock(
                CacheFallbackConfig {
                    max_concurrent: 1,
                    timeout: Duration::ZERO,
                },
                CacheFallbackCircuitConfig::default(),
                CacheFallbackLockConfig::default(),
            ),
            Err(StorageError::InvalidCacheFallbackTimeout)
        ));
    }

    #[test]
    fn storage_metrics_snapshot_is_cumulative() {
        let metrics = StorageMetrics::default();
        metrics.cache_hit();
        metrics.cache_miss();
        metrics.cache_read_error();
        metrics.cache_write();
        metrics.cache_write_error();
        metrics.postgres_fallback();
        metrics.postgres_fallback_error();
        metrics.postgres_fallback_timeout();
        metrics.postgres_fallback_circuit_open();
        metrics.cache_negative_hit();
        metrics.cache_stale_hit();
        metrics.cache_negative_write();
        metrics.cache_refresh_started();
        metrics.cache_refresh_completed();
        metrics.cache_refresh_error();
        assert_eq!(
            metrics.snapshot(),
            StorageMetricsSnapshot {
                cache_hits: 3,
                cache_misses: 1,
                cache_read_errors: 1,
                cache_writes: 1,
                cache_write_errors: 1,
                postgres_fallbacks: 1,
                postgres_fallback_errors: 1,
                postgres_fallback_timeouts: 1,
                postgres_fallback_circuit_open: 1,
                cache_fallback_lock_acquired: 0,
                cache_fallback_lock_contention: 0,
                cache_fallback_lock_timeouts: 0,
                cache_fallback_lock_errors: 0,
                cache_fallback_lock_release_errors: 0,
                cache_negative_hits: 1,
                cache_stale_hits: 1,
                cache_negative_writes: 1,
                cache_refresh_started: 1,
                cache_refresh_completed: 1,
                cache_refresh_errors: 1,
            }
        );
    }

    #[test]
    fn cache_fallback_circuit_rejects_zero_limits() {
        assert!(matches!(
            CacheFallbackCircuit::new(CacheFallbackCircuitConfig {
                failure_threshold: 0,
                cooldown: Duration::from_secs(1),
            }),
            Err(StorageError::InvalidCacheFallbackCircuitThreshold)
        ));
        assert!(matches!(
            CacheFallbackCircuit::new(CacheFallbackCircuitConfig {
                failure_threshold: 1,
                cooldown: Duration::ZERO,
            }),
            Err(StorageError::InvalidCacheFallbackCircuitCooldown)
        ));
    }

    #[test]
    fn cache_fallback_circuit_opens_and_recovers() {
        let circuit = CacheFallbackCircuit::new(CacheFallbackCircuitConfig {
            failure_threshold: 2,
            cooldown: Duration::from_millis(10),
        })
        .unwrap();

        let first = circuit.allow().unwrap();
        circuit.failed(first);
        let second = circuit.allow().unwrap();
        circuit.failed(second);
        assert!(matches!(
            circuit.allow(),
            Err(StorageError::CacheFallbackCircuitOpen { .. })
        ));

        std::thread::sleep(Duration::from_millis(20));
        let probe = circuit.allow().unwrap();
        assert!(matches!(probe, CacheFallbackCircuitPermit::HalfOpen { .. }));
        circuit.succeeded(probe);
        assert!(matches!(
            circuit.allow(),
            Ok(CacheFallbackCircuitPermit::Closed { .. })
        ));
    }

    #[test]
    fn stale_circuit_permit_cannot_close_a_new_generation() {
        let circuit = CacheFallbackCircuit::new(CacheFallbackCircuitConfig {
            failure_threshold: 1,
            cooldown: Duration::from_secs(1),
        })
        .unwrap();
        let first = circuit.allow().unwrap();
        let stale = circuit.allow().unwrap();
        circuit.failed(first);
        circuit.succeeded(stale);
        assert!(matches!(
            circuit.allow(),
            Err(StorageError::CacheFallbackCircuitOpen { .. })
        ));
    }

    #[test]
    fn cache_fallback_lock_config_rejects_zero_limits() {
        let defaults = CacheFallbackLockConfig::default();
        assert!(matches!(
            CacheFallbackLockConfig {
                lease: Duration::ZERO,
                ..defaults
            }
            .validate(),
            Err(StorageError::InvalidCacheFallbackLockLease)
        ));
        assert!(matches!(
            CacheFallbackLockConfig {
                wait: Duration::ZERO,
                ..defaults
            }
            .validate(),
            Err(StorageError::InvalidCacheFallbackLockWait)
        ));
        assert!(matches!(
            CacheFallbackLockConfig {
                poll_interval: Duration::ZERO,
                ..defaults
            }
            .validate(),
            Err(StorageError::InvalidCacheFallbackLockPoll)
        ));
    }

    #[test]
    fn cache_fallback_lock_keys_and_tokens_are_unique() {
        let first = RecordKey::new("tests", "first").unwrap();
        let second = RecordKey::new("tests", "second").unwrap();
        assert_ne!(
            RedisSnapshotCache::fallback_lock_key(&first),
            RedisSnapshotCache::fallback_lock_key(&second)
        );
        assert_ne!(
            RedisSnapshotCache::fallback_lock_token(),
            RedisSnapshotCache::fallback_lock_token()
        );
    }

    #[test]
    fn advisory_lock_keys_preserve_scope_and_component_boundaries() {
        assert_ne!(
            advisory_lock_key("record", &["a:b", "c"]),
            advisory_lock_key("record", &["a", "b:c"])
        );
        assert_ne!(
            advisory_lock_key("record", &["trade", "42"]),
            advisory_lock_key("trade", &["trade:42"])
        );
    }

    #[test]
    fn snapshot_cache_policy_validates_and_applies_stable_jitter() {
        assert!(matches!(
            SnapshotCacheConfig {
                ttl: Duration::ZERO,
                ..SnapshotCacheConfig::default()
            }
            .validate(),
            Err(StorageError::InvalidCacheTtl)
        ));

        let policy = SnapshotCacheConfig {
            ttl: Duration::from_millis(100),
            ttl_jitter: Duration::from_millis(25),
            negative_ttl: Duration::ZERO,
            stale_while_revalidate: Duration::from_millis(40),
        };
        let record = RecordKey::new("tests", "jitter").unwrap();
        let fresh = policy.fresh_ttl_ms(&record);
        assert!((100..=125).contains(&fresh));
        assert_eq!(fresh, policy.fresh_ttl_ms(&record));
        assert_eq!(policy.hard_ttl_ms(&record), fresh + 40);
    }

    #[test]
    fn cache_lookup_supports_negative_fresh_stale_and_legacy_entries() {
        let snapshot = SnapshotEnvelope {
            record: RecordKey::new("tests", "lookup").unwrap(),
            schema: "tests.snapshot".to_string(),
            schema_version: 1,
            revision: Revision(7),
            payload: vec![1, 2, 3],
            updated_at_unix_ms: 123,
        };
        let bytes = bincode::serde::encode_to_vec(&snapshot, bincode::config::standard()).unwrap();

        assert!(matches!(
            RedisSnapshotCache::decode_lookup(&[Some(bytes.clone()), None, None]).unwrap(),
            CacheLookup::Stale(value) if value == snapshot
        ));
        assert!(matches!(
            RedisSnapshotCache::decode_lookup(&[Some(bytes.clone()), Some(vec![b'1']), None]).unwrap(),
            CacheLookup::Fresh(value) if value == snapshot
        ));
        assert!(matches!(
            RedisSnapshotCache::decode_lookup(&[None, None, Some(vec![b'1'])]).unwrap(),
            CacheLookup::Negative
        ));
        assert!(matches!(
            RedisSnapshotCache::decode_lookup(&[None, None, None]).unwrap(),
            CacheLookup::Miss
        ));
    }
}
