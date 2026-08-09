//! Redis-backed durable backlog for ordinary snapshots.
//! Redis 持久普通快照积压队列。
//!
//! 该模块与 RedisSnapshotCache 分离：缓存丢失只会降低读取性能，backlog 丢失则代表
//! 尚未落 PostgreSQL 的普通快照无法恢复。因此本地和生产 Redis 都必须启用持久化，
//! 并由部署层监控 AOF/RDB 状态。
//! This module is intentionally separate from RedisSnapshotCache. Losing a cache only
//! reduces read performance; losing a backlog can lose a snapshot that has not reached
//! PostgreSQL yet. Deployments must enable Redis persistence and monitor its durability.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use redis::Script;
use redis::aio::MultiplexedConnection;
use tiangz_dbproxy_core::{RecordKey, SnapshotWrite, StoreError};
use tokio::sync::Mutex;

use crate::StorageError;

const PENDING_KEY: &str = "dbproxy:snapshot-backlog:pending";
const PROCESSING_KEY: &str = "dbproxy:snapshot-backlog:processing";
const LEASES_KEY: &str = "dbproxy:snapshot-backlog:leases";
const LEASE_SEQUENCE_KEY: &str = "dbproxy:snapshot-backlog:lease-sequence";
const ENTRY_PREFIX: &str = "dbproxy:snapshot-backlog:entry:";
const RECLAIM_LIMIT: i64 = 128;

const CLAIM_SCRIPT: &str = r#"
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1], 'LIMIT', 0, ARGV[4])
for _, member in ipairs(expired) do
    redis.call('ZREM', KEYS[2], member)
    redis.call('HDEL', KEYS[3], member)
    if redis.call('EXISTS', ARGV[3] .. member) == 1 then
        redis.call('ZADD', KEYS[1], ARGV[1], member)
    end
end

for _ = 1, ARGV[4] do
    local item = redis.call('ZPOPMIN', KEYS[1], 1)
    if #item == 0 then
        return {}
    end
    local member = item[1]
    local payload = redis.call('GET', ARGV[3] .. member)
    if payload then
        local lease = tostring(redis.call('INCR', KEYS[4]))
        redis.call('ZADD', KEYS[2], ARGV[2], member)
        redis.call('HSET', KEYS[3], member, lease)
        return { member, lease, payload }
    end
end
return {}
"#;

const ACK_SCRIPT: &str = r#"
if redis.call('HGET', KEYS[2], ARGV[2]) ~= ARGV[1] then
    return 0
end

redis.call('HDEL', KEYS[2], ARGV[2])
redis.call('ZREM', KEYS[1], ARGV[2])
local current = redis.call('GET', ARGV[4] .. ARGV[2])
if not current then
    redis.call('ZREM', KEYS[3], ARGV[2])
    return 1
end
if current == ARGV[3] then
    redis.call('DEL', ARGV[4] .. ARGV[2])
    redis.call('ZREM', KEYS[3], ARGV[2])
    return 1
end

redis.call('ZADD', KEYS[3], ARGV[5], ARGV[2])
return 2
"#;

const RELEASE_SCRIPT: &str = r#"
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then
    return 0
end
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[1], ARGV[1])
if redis.call('EXISTS', ARGV[4] .. ARGV[1]) == 1 then
    redis.call('ZADD', KEYS[3], ARGV[3], ARGV[1])
end
return 1
"#;

const RENEW_SCRIPT: &str = r#"
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then
    return 0
end
if redis.call('EXISTS', ARGV[4] .. ARGV[1]) == 0 then
    return 0
end
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
return 1
"#;

/// Redis 中一次被领取的普通快照；只有持有有效 lease 的消费者才能确认或释放。
/// One claimed ordinary snapshot; only the valid lease holder may acknowledge or release it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotBacklogLease {
    pub request: SnapshotWrite,
    member: String,
    lease_id: String,
    encoded: Vec<u8>,
}

/// ACK 的结果，用于区分正常删除、被更新快照替代和 lease 已失效。
/// ACK outcome distinguishing removal, supersession by a newer snapshot, and lease loss.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotBacklogAck {
    Removed,
    Superseded,
    LeaseLost,
}

/// Redis AOF-backed ordinary snapshot backlog.
/// 基于 Redis AOF 的普通快照持久积压队列。
#[derive(Clone)]
pub struct RedisSnapshotBacklog {
    connection: Arc<Mutex<MultiplexedConnection>>,
}

impl RedisSnapshotBacklog {
    /// 连接 Redis；不会自动改变 Redis 的持久化配置。
    /// Connect to Redis; persistence configuration remains a deployment responsibility.
    pub async fn connect(url: &str) -> Result<Self, StorageError> {
        let client = redis::Client::open(url)?;
        let connection = client.get_multiplexed_async_connection().await?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    /// 返回稳定的记录成员名；长度前缀避免业务键包含冒号时发生碰撞。
    /// Build a stable record member; length prefixes avoid collisions when keys contain colons.
    pub fn member(record: &RecordKey) -> String {
        format!(
            "{}:{}:{}:{}",
            record.namespace.len(),
            record.namespace,
            record.key.len(),
            record.key
        )
    }

    fn entry_key(member: &str) -> String {
        format!("{ENTRY_PREFIX}{member}")
    }

    fn now_unix_ms() -> Result<i64, StorageError> {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| StorageError::BacklogClock(error.to_string()))?
            .as_millis();
        i64::try_from(millis).map_err(|_| StorageError::BacklogTimestampTooLarge)
    }

    fn lease_deadline(now: i64, lease_ms: u64) -> Result<i64, StorageError> {
        let lease =
            i64::try_from(lease_ms).map_err(|_| StorageError::BacklogLeaseTooLarge { lease_ms })?;
        now.checked_add(lease)
            .ok_or(StorageError::BacklogTimestampTooLarge)
    }

    fn encode(request: &SnapshotWrite) -> Result<Vec<u8>, StorageError> {
        bincode::serde::encode_to_vec(request, bincode::config::standard())
            .map_err(|error| StorageError::Codec(error.to_string()))
    }

    fn decode(bytes: &[u8]) -> Result<SnapshotWrite, StorageError> {
        bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map(|(request, _)| request)
            .map_err(|error| StorageError::Codec(error.to_string()))
    }

    fn validate(request: &SnapshotWrite) -> Result<(), StorageError> {
        if request.request_id.trim().is_empty() {
            return Err(StoreError::EmptyRequestId.into());
        }
        if request.record.namespace.trim().is_empty() {
            return Err(StoreError::InvalidKey("namespace is empty").into());
        }
        if request.record.key.trim().is_empty() {
            return Err(StoreError::InvalidKey("key is empty").into());
        }
        if request.expected_revision.is_some() {
            return Err(StoreError::QueuedSnapshotRequiresUnconditionalWrite {
                record: request.record.clone(),
            }
            .into());
        }
        Ok(())
    }

    /// 写入或替换一条普通快照；同一 RecordKey 永远只保留最新请求。
    /// Enqueue or replace one ordinary snapshot; one RecordKey keeps only its newest request.
    pub async fn enqueue(&self, request: SnapshotWrite) -> Result<(), StorageError> {
        Self::validate(&request)?;
        let member = Self::member(&request.record);
        let encoded = Self::encode(&request)?;
        let score = Self::now_unix_ms()?;
        let entry_key = Self::entry_key(&member);
        let script = Script::new(
            "redis.call('SET', KEYS[1], ARGV[1]); redis.call('ZADD', KEYS[2], ARGV[2], ARGV[3]); return 1",
        );
        let mut connection = self.connection.lock().await;
        let _: i64 = script
            .key(entry_key)
            .key(PENDING_KEY)
            .arg(encoded)
            .arg(score)
            .arg(member)
            .invoke_async(&mut *connection)
            .await?;
        Ok(())
    }

    /// 领取一条积压并设置 lease；会先把过期 lease 重新放回 pending。
    /// Claim one backlog item with a lease; expired leases are reclaimed first.
    pub async fn claim(&self, lease_ms: u64) -> Result<Option<SnapshotBacklogLease>, StorageError> {
        if lease_ms == 0 {
            return Err(StorageError::InvalidBacklogLease);
        }
        let now = Self::now_unix_ms()?;
        let deadline = Self::lease_deadline(now, lease_ms)?;
        let script = Script::new(CLAIM_SCRIPT);
        let mut connection = self.connection.lock().await;
        let values: Vec<Vec<u8>> = script
            .key(PENDING_KEY)
            .key(PROCESSING_KEY)
            .key(LEASES_KEY)
            .key(LEASE_SEQUENCE_KEY)
            .arg(now)
            .arg(deadline)
            .arg(ENTRY_PREFIX)
            .arg(RECLAIM_LIMIT)
            .invoke_async(&mut *connection)
            .await?;
        if values.is_empty() {
            return Ok(None);
        }
        if values.len() != 3 {
            return Err(StorageError::BacklogProtocol(
                "claim returned an invalid tuple".to_string(),
            ));
        }
        let member = String::from_utf8(values[0].clone())
            .map_err(|error| StorageError::BacklogProtocol(error.to_string()))?;
        let lease_id = String::from_utf8(values[1].clone())
            .map_err(|error| StorageError::BacklogProtocol(error.to_string()))?;
        let encoded = values[2].clone();
        let request = Self::decode(&encoded)?;
        if Self::member(&request.record) != member {
            return Err(StorageError::BacklogProtocol(
                "claim record member does not match payload".to_string(),
            ));
        }
        Ok(Some(SnapshotBacklogLease {
            request,
            member,
            lease_id,
            encoded,
        }))
    }

    /// 延长 lease，避免慢数据库写入期间被另一个消费者重新领取。
    /// Renew a lease so a slow database write is not reclaimed by another consumer.
    pub async fn renew(
        &self,
        lease: &SnapshotBacklogLease,
        lease_ms: u64,
    ) -> Result<bool, StorageError> {
        if lease_ms == 0 {
            return Err(StorageError::InvalidBacklogLease);
        }
        let now = Self::now_unix_ms()?;
        let deadline = Self::lease_deadline(now, lease_ms)?;
        let script = Script::new(RENEW_SCRIPT);
        let mut connection = self.connection.lock().await;
        let result: i64 = script
            .key(PROCESSING_KEY)
            .key(LEASES_KEY)
            .arg(&lease.member)
            .arg(&lease.lease_id)
            .arg(deadline)
            .arg(ENTRY_PREFIX)
            .invoke_async(&mut *connection)
            .await?;
        Ok(result == 1)
    }

    /// ACK 成功写入；如果期间有更新快照，旧项不会删除新项，而是重新进入 pending。
    /// Acknowledge a successful write; a newer replacement stays pending instead of being deleted.
    pub async fn ack(
        &self,
        lease: &SnapshotBacklogLease,
    ) -> Result<SnapshotBacklogAck, StorageError> {
        let now = Self::now_unix_ms()?;
        let script = Script::new(ACK_SCRIPT);
        let mut connection = self.connection.lock().await;
        let result: i64 = script
            .key(PROCESSING_KEY)
            .key(LEASES_KEY)
            .key(PENDING_KEY)
            .arg(&lease.lease_id)
            .arg(&lease.member)
            .arg(&lease.encoded)
            .arg(ENTRY_PREFIX)
            .arg(now)
            .invoke_async(&mut *connection)
            .await?;
        match result {
            0 => Ok(SnapshotBacklogAck::LeaseLost),
            1 => Ok(SnapshotBacklogAck::Removed),
            2 => Ok(SnapshotBacklogAck::Superseded),
            _ => Err(StorageError::BacklogProtocol(
                "ack returned an invalid status".to_string(),
            )),
        }
    }

    /// 主动释放 lease，通常用于数据库写入失败后的快速重试；失败时也会等待 lease 过期自动恢复。
    /// Release a lease for immediate retry after a database failure; expiry remains the fallback.
    pub async fn release(&self, lease: &SnapshotBacklogLease) -> Result<bool, StorageError> {
        let now = Self::now_unix_ms()?;
        let script = Script::new(RELEASE_SCRIPT);
        let mut connection = self.connection.lock().await;
        let result: i64 = script
            .key(PROCESSING_KEY)
            .key(LEASES_KEY)
            .key(PENDING_KEY)
            .arg(&lease.member)
            .arg(&lease.lease_id)
            .arg(now)
            .arg(ENTRY_PREFIX)
            .invoke_async(&mut *connection)
            .await?;
        Ok(result == 1)
    }
}
