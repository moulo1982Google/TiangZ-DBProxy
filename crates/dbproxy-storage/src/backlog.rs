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
use redis::aio::ConnectionManager;
use tiangz_dbproxy_core::{RecordKey, SnapshotWrite, StoreError};
use tokio::sync::Mutex;

use crate::{DEFAULT_REDIS_AOF_ACK_TIMEOUT_MS, StorageError, open_redis_connection_manager};

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

/// Point-in-time depth information for the durable Redis snapshot backlog.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RedisSnapshotBacklogStats {
    pub pending: u64,
    pub processing: u64,
    pub oldest_pending_age_ms: Option<u64>,
}

/// Redis AOF-backed ordinary snapshot backlog.
/// 基于 Redis AOF 的普通快照持久积压队列。
#[derive(Clone)]
pub struct RedisSnapshotBacklog {
    enqueue_connection: Arc<Mutex<ConnectionManager>>,
    worker_connection: Arc<Mutex<ConnectionManager>>,
    stats_connection: Arc<Mutex<ConnectionManager>>,
}

impl RedisSnapshotBacklog {
    /// 连接 Redis；不会自动改变 Redis 的持久化配置。
    /// Connect to Redis; persistence configuration remains a deployment responsibility.
    pub async fn connect(url: &str) -> Result<Self, StorageError> {
        // Keep AOF acknowledgement, lease processing, and observability independent. A slow
        // WAITAOF or a reconnect in one role must not hold up either of the other two roles.
        let (enqueue_connection, worker_connection, stats_connection) = tokio::try_join!(
            open_redis_connection_manager(url),
            open_redis_connection_manager(url),
            open_redis_connection_manager(url),
        )?;
        Ok(Self {
            enqueue_connection: Arc::new(Mutex::new(enqueue_connection)),
            worker_connection: Arc::new(Mutex::new(worker_connection)),
            stats_connection: Arc::new(Mutex::new(stats_connection)),
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
        let mut connection = self.enqueue_connection.lock().await;
        let _: i64 = script
            .key(entry_key)
            .key(PENDING_KEY)
            .arg(encoded)
            .arg(score)
            .arg(member)
            .invoke_async(&mut *connection)
            .await?;
        wait_for_local_aof(&mut connection).await?;
        Ok(())
    }

    /// 原子接收一批普通快照；同一批只产生一次 Redis 往返。批内记录必须已经去重。
    /// Atomically accept one snapshot batch with a single Redis round trip. Records must be unique.
    pub async fn enqueue_multi(&self, requests: &[SnapshotWrite]) -> Result<(), StorageError> {
        let mut prepared = Vec::with_capacity(requests.len());
        for request in requests {
            Self::validate(request)?;
            let member = Self::member(&request.record);
            prepared.push((Self::entry_key(&member), Self::encode(request)?, member));
        }
        let score = Self::now_unix_ms()?;
        let script = Script::new(
            r#"
local score = ARGV[1]
for index = 2, #ARGV, 3 do
    redis.call('SET', ARGV[index], ARGV[index + 1])
    redis.call('ZADD', KEYS[1], score, ARGV[index + 2])
end
return (#ARGV - 1) / 3
"#,
        );
        let mut invocation = script.prepare_invoke();
        invocation.key(PENDING_KEY).arg(score);
        for (entry_key, encoded, member) in prepared {
            invocation.arg(entry_key).arg(encoded).arg(member);
        }
        let mut connection = self.enqueue_connection.lock().await;
        let accepted: i64 = invocation.invoke_async(&mut *connection).await?;
        if accepted != i64::try_from(requests.len()).unwrap_or(i64::MAX) {
            return Err(StorageError::BacklogProtocol(
                "batch enqueue returned an invalid count".to_string(),
            ));
        }
        wait_for_local_aof(&mut connection).await?;
        Ok(())
    }

    /// Read backlog depth and the age of the oldest pending item without mutating the queue.
    pub async fn stats(&self) -> Result<RedisSnapshotBacklogStats, StorageError> {
        let now = Self::now_unix_ms()?;
        let mut connection = self.stats_connection.lock().await;
        let pending: i64 = redis::cmd("ZCARD")
            .arg(PENDING_KEY)
            .query_async(&mut *connection)
            .await?;
        let processing: i64 = redis::cmd("ZCARD")
            .arg(PROCESSING_KEY)
            .query_async(&mut *connection)
            .await?;
        let oldest_values: Vec<String> = redis::cmd("ZRANGE")
            .arg(PENDING_KEY)
            .arg(0)
            .arg(0)
            .arg("WITHSCORES")
            .query_async(&mut *connection)
            .await?;
        let pending = u64::try_from(pending)
            .map_err(|_| StorageError::BacklogProtocol("pending depth is negative".to_string()))?;
        let processing = u64::try_from(processing).map_err(|_| {
            StorageError::BacklogProtocol("processing depth is negative".to_string())
        })?;
        let oldest_pending_age_ms = match oldest_values.as_slice() {
            [] => None,
            [_, score] => {
                let score = score.parse::<i64>().map_err(|error| {
                    StorageError::BacklogProtocol(format!("invalid pending score: {error}"))
                })?;
                Some(now.saturating_sub(score).max(0) as u64)
            }
            _ => {
                return Err(StorageError::BacklogProtocol(
                    "ZRANGE returned an invalid score tuple".to_string(),
                ));
            }
        };
        Ok(RedisSnapshotBacklogStats {
            pending,
            processing,
            oldest_pending_age_ms,
        })
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
        let mut connection = self.worker_connection.lock().await;
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
        let mut connection = self.worker_connection.lock().await;
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
        let mut connection = self.worker_connection.lock().await;
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
        let mut connection = self.worker_connection.lock().await;
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

async fn wait_for_local_aof(connection: &mut ConnectionManager) -> Result<(), StorageError> {
    let timeout_ms = i64::try_from(DEFAULT_REDIS_AOF_ACK_TIMEOUT_MS).unwrap_or(i64::MAX);
    let (local, _replicas): (i64, i64) = redis::cmd("WAITAOF")
        .arg(1)
        .arg(0)
        .arg(timeout_ms)
        .query_async(connection)
        .await?;
    if local < 1 {
        return Err(StorageError::RedisAofNotDurable {
            timeout_ms: DEFAULT_REDIS_AOF_ACK_TIMEOUT_MS,
        });
    }
    Ok(())
}
