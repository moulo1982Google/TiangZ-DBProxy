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
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use redis::Script;
use redis::aio::ConnectionManager;
use tiangz_dbproxy_core::{RecordKey, SnapshotWrite, StoreError};
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::{DEFAULT_REDIS_AOF_ACK_TIMEOUT_MS, StorageError, open_redis_connection_manager};

const PENDING_KEY: &str = "dbproxy:snapshot-backlog:pending";
const PROCESSING_KEY: &str = "dbproxy:snapshot-backlog:processing";
const LEASES_KEY: &str = "dbproxy:snapshot-backlog:leases";
const LEASE_SEQUENCE_KEY: &str = "dbproxy:snapshot-backlog:lease-sequence";
const ENTRY_PREFIX: &str = "dbproxy:snapshot-backlog:entry:";
const RECLAIM_LIMIT: i64 = 128;

// 批内顺序写入，同一记录后写覆盖先写，与逐条入队的合并语义一致。
// Writes in batch order; a later write to the same record replaces the earlier one, matching per-call coalescing.
const ENQUEUE_BATCH_SCRIPT: &str = r#"
local score = ARGV[1]
for index = 2, #ARGV, 3 do
    redis.call('SET', ARGV[index], ARGV[index + 1])
    redis.call('ZADD', KEYS[1], score, ARGV[index + 2])
end
return (#ARGV - 1) / 3
"#;

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

const CLAIM_MULTI_SCRIPT: &str = r#"
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1], 'LIMIT', 0, ARGV[4])
for _, member in ipairs(expired) do
    redis.call('ZREM', KEYS[2], member)
    redis.call('HDEL', KEYS[3], member)
    if redis.call('EXISTS', ARGV[3] .. member) == 1 then
        redis.call('ZADD', KEYS[1], ARGV[1], member)
    end
end

local claimed = {}
local claim_limit = tonumber(ARGV[5])
local scan_limit = tonumber(ARGV[4]) + claim_limit
for _ = 1, scan_limit do
    if (#claimed / 3) >= claim_limit then
        break
    end
    local item = redis.call('ZPOPMIN', KEYS[1], 1)
    if #item == 0 then
        break
    end
    local member = item[1]
    local payload = redis.call('GET', ARGV[3] .. member)
    if payload then
        local lease = tostring(redis.call('INCR', KEYS[4]))
        redis.call('ZADD', KEYS[2], ARGV[2], member)
        redis.call('HSET', KEYS[3], member, lease)
        table.insert(claimed, member)
        table.insert(claimed, lease)
        table.insert(claimed, payload)
    end
end
return claimed
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

const ACK_MULTI_SCRIPT: &str = r#"
local results = {}
local entry_prefix = ARGV[1]
local now = ARGV[2]
for index = 3, #ARGV, 3 do
    local lease = ARGV[index]
    local member = ARGV[index + 1]
    local encoded = ARGV[index + 2]
    if redis.call('HGET', KEYS[2], member) ~= lease then
        table.insert(results, 0)
    else
        redis.call('HDEL', KEYS[2], member)
        redis.call('ZREM', KEYS[1], member)
        local current = redis.call('GET', entry_prefix .. member)
        if not current then
            redis.call('ZREM', KEYS[3], member)
            table.insert(results, 1)
        elseif current == encoded then
            redis.call('DEL', entry_prefix .. member)
            redis.call('ZREM', KEYS[3], member)
            table.insert(results, 1)
        else
            redis.call('ZADD', KEYS[3], now, member)
            table.insert(results, 2)
        end
    end
end
return results
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

const RELEASE_MULTI_SCRIPT: &str = r#"
local results = {}
local entry_prefix = ARGV[1]
local now = ARGV[2]
for index = 3, #ARGV, 2 do
    local member = ARGV[index]
    local lease = ARGV[index + 1]
    if redis.call('HGET', KEYS[2], member) ~= lease then
        table.insert(results, 0)
    else
        redis.call('HDEL', KEYS[2], member)
        redis.call('ZREM', KEYS[1], member)
        if redis.call('EXISTS', entry_prefix .. member) == 1 then
            redis.call('ZADD', KEYS[3], now, member)
        end
        table.insert(results, 1)
    end
end
return results
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
    enqueue: EnqueueBatcher,
    worker_connection: Arc<Mutex<ConnectionManager>>,
    stats_connection: Arc<Mutex<ConnectionManager>>,
}

/// 入队组提交参数。排队上限与期限保证过载时快速拒绝，而不是执行调用方早已放弃的请求。
/// Enqueue group-commit limits. The queue bound and deadline reject fast under overload instead of
/// executing requests the caller has long abandoned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EnqueueBatchConfig {
    /// 等待写入的批次上限（每次入队调用算一个）。 / Maximum queued submissions (one per enqueue call).
    pub queue_capacity: usize,
    /// 一次写入与一次WAITAOF合并的记录上限。 / Records merged into one write and one WAITAOF.
    pub max_batch_records: usize,
    /// 从接收到开始写入的最长排队时间；超过则不写入并返回可重试错误。
    /// Longest wait from acceptance to write start; beyond it nothing is written and a retryable error returns.
    pub max_queue_wait: Duration,
    /// 入队何时算成功。 / When an enqueue counts as accepted.
    pub ack: EnqueueAck,
}

/// 入队确认档位，由部署配置统一选择。 / Enqueue acknowledgement level, chosen once per deployment.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum EnqueueAck {
    /// 等本地AOF落盘后才确认；Redis崩溃也不丢已确认写入。 / Acknowledge after local AOF fsync; acknowledged writes survive a Redis crash.
    #[default]
    Aof,
    /// 写入Redis内存即确认；按appendfsync everysec，Redis崩溃可能丢失约1秒内已确认的写入，正常重启不丢。
    /// Acknowledge once in Redis memory; with appendfsync everysec a Redis crash may lose about the last second of
    /// acknowledged writes, while a clean restart loses nothing.
    Memory,
}

impl Default for EnqueueBatchConfig {
    fn default() -> Self {
        // 排队期限加上WAITAOF超时须小于常见客户端5秒超时，调用方收到的是明确结果而不是超时。
        // Queue deadline plus WAITAOF timeout stay below the usual 5 s client timeout, so callers get an answer, not a timeout.
        Self {
            queue_capacity: 4096,
            max_batch_records: 512,
            max_queue_wait: Duration::from_millis(2_000),
            ack: EnqueueAck::Aof,
        }
    }
}

struct EnqueueEntry {
    entry_key: String,
    encoded: Vec<u8>,
    member: String,
}

struct EnqueueJob {
    entries: Vec<EnqueueEntry>,
    accepted_at: Instant,
    reply: oneshot::Sender<Result<(), StorageError>>,
}

/// 把同一时刻的入队合并为一次Redis写入和一次WAITAOF。 / Writes concurrent enqueues with one Redis write and one WAITAOF.
#[async_trait]
trait EnqueueSink: Send + 'static {
    /// 写入全部记录并等待本连接此前的写入进入本地AOF。 / Write every record and wait until this connection's writes reach local AOF.
    async fn write(&mut self, entries: &[&EnqueueEntry]) -> Result<(), StorageError>;
}

struct RedisEnqueueSink {
    connection: ConnectionManager,
    script: Script,
    ack: EnqueueAck,
}

#[async_trait]
impl EnqueueSink for RedisEnqueueSink {
    async fn write(&mut self, entries: &[&EnqueueEntry]) -> Result<(), StorageError> {
        let score = RedisSnapshotBacklog::now_unix_ms()?;
        let mut invocation = self.script.prepare_invoke();
        invocation.key(PENDING_KEY).arg(score);
        for entry in entries {
            invocation
                .arg(&entry.entry_key)
                .arg(&entry.encoded)
                .arg(&entry.member);
        }
        let accepted: i64 = invocation.invoke_async(&mut self.connection).await?;
        if accepted != i64::try_from(entries.len()).unwrap_or(i64::MAX) {
            return Err(StorageError::BacklogProtocol(
                "batch enqueue returned an invalid count".to_string(),
            ));
        }
        match self.ack {
            // WAITAOF覆盖本连接此前的全部写入，所以一次等待确认整批。 / WAITAOF covers every prior write of this connection, so one wait acknowledges the batch.
            EnqueueAck::Aof => wait_for_local_aof(&mut self.connection).await,
            EnqueueAck::Memory => Ok(()),
        }
    }
}

/// 组提交入口：调用方只提交并等待自己的结果；唯一的后台任务独占写入连接。
/// Group-commit entry: callers submit and await their own result; one background task owns the write connection.
#[derive(Clone)]
struct EnqueueBatcher {
    sender: mpsc::Sender<EnqueueJob>,
    capacity: usize,
}

impl EnqueueBatcher {
    fn spawn<S: EnqueueSink>(sink: S, config: EnqueueBatchConfig) -> Self {
        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        tokio::spawn(run_enqueue_batcher(receiver, sink, config));
        Self {
            sender,
            capacity: config.queue_capacity,
        }
    }

    async fn submit(&self, entries: Vec<EnqueueEntry>) -> Result<(), StorageError> {
        let (reply, result) = oneshot::channel();
        self.sender
            .try_send(EnqueueJob {
                entries,
                accepted_at: Instant::now(),
                reply,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => StorageError::BacklogEnqueueOverloaded {
                    capacity: self.capacity,
                },
                mpsc::error::TrySendError::Closed(_) => StorageError::BacklogEnqueueStopped,
            })?;
        result
            .await
            .map_err(|_| StorageError::BacklogEnqueueStopped)?
    }
}

/// 写入进行中（包括等待落盘）到达的请求自然积累，下一轮一次写完；超期或已放弃的请求不写入。
/// Requests arriving while a write (including its fsync wait) is in progress accumulate and are written together
/// next round; expired or abandoned requests are never written.
async fn run_enqueue_batcher<S: EnqueueSink>(
    mut jobs: mpsc::Receiver<EnqueueJob>,
    mut sink: S,
    config: EnqueueBatchConfig,
) {
    while let Some(first) = jobs.recv().await {
        let mut records = first.entries.len();
        let mut batch = vec![first];
        while records < config.max_batch_records {
            match jobs.try_recv() {
                Ok(job) => {
                    records += job.entries.len();
                    batch.push(job);
                }
                Err(_) => break,
            }
        }
        let now = Instant::now();
        let mut live = Vec::with_capacity(batch.len());
        for job in batch {
            if job.reply.is_closed() {
                continue;
            }
            let waited = now.duration_since(job.accepted_at);
            if waited > config.max_queue_wait {
                let waited_ms = u64::try_from(waited.as_millis()).unwrap_or(u64::MAX);
                let _ = job
                    .reply
                    .send(Err(StorageError::BacklogEnqueueDeadlineExceeded {
                        waited_ms,
                    }));
                continue;
            }
            live.push(job);
        }
        if live.is_empty() {
            continue;
        }
        let entries: Vec<&EnqueueEntry> = live.iter().flat_map(|job| job.entries.iter()).collect();
        // 写入与确认对整批共享同一个结果；失败时结果未知，调用方按原请求号重试。
        // Write and acknowledgement share one outcome across the batch; on failure the outcome is unknown and callers retry with their request IDs.
        let outcome = sink
            .write(&entries)
            .await
            .map_err(|error| error.to_string());
        for job in live {
            let _ = job
                .reply
                .send(outcome.clone().map_err(StorageError::BacklogEnqueueFailed));
        }
    }
}

impl RedisSnapshotBacklog {
    /// 连接 Redis；不会自动改变 Redis 的持久化配置。
    /// Connect to Redis; persistence configuration remains a deployment responsibility.
    pub async fn connect(url: &str) -> Result<Self, StorageError> {
        Self::connect_with_config(url, EnqueueBatchConfig::default()).await
    }

    /// 使用指定组提交参数连接。 / Connect with explicit group-commit limits.
    pub async fn connect_with_config(
        url: &str,
        config: EnqueueBatchConfig,
    ) -> Result<Self, StorageError> {
        if config.queue_capacity == 0 || config.max_batch_records == 0 {
            return Err(StorageError::BacklogProtocol(
                "enqueue queue capacity and batch size must be positive".to_string(),
            ));
        }
        // Keep AOF acknowledgement, lease processing, and observability independent. A slow
        // WAITAOF or a reconnect in one role must not hold up either of the other two roles.
        let (enqueue_connection, worker_connection, stats_connection) = tokio::try_join!(
            open_redis_connection_manager(url),
            open_redis_connection_manager(url),
            open_redis_connection_manager(url),
        )?;
        let sink = RedisEnqueueSink {
            connection: enqueue_connection,
            script: Script::new(ENQUEUE_BATCH_SCRIPT),
            ack: config.ack,
        };
        Ok(Self {
            enqueue: EnqueueBatcher::spawn(sink, config),
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
        self.enqueue.submit(vec![Self::prepare(&request)?]).await
    }

    /// 原子接收一批普通快照；与同一时刻的其他入队合并为一次写入和一次AOF确认。批内记录必须已经去重。
    /// Atomically accept one snapshot batch, merged with concurrent enqueues into one write and one AOF
    /// acknowledgement. Records must be unique.
    pub async fn enqueue_multi(&self, requests: &[SnapshotWrite]) -> Result<(), StorageError> {
        let entries = requests
            .iter()
            .map(Self::prepare)
            .collect::<Result<Vec<_>, _>>()?;
        self.enqueue.submit(entries).await
    }

    fn prepare(request: &SnapshotWrite) -> Result<EnqueueEntry, StorageError> {
        Self::validate(request)?;
        let member = Self::member(&request.record);
        Ok(EnqueueEntry {
            entry_key: Self::entry_key(&member),
            encoded: Self::encode(request)?,
            member,
        })
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

    /// Claim up to `max_items` with one Redis script invocation.
    pub async fn claim_multi(
        &self,
        lease_ms: u64,
        max_items: usize,
    ) -> Result<Vec<SnapshotBacklogLease>, StorageError> {
        if lease_ms == 0 {
            return Err(StorageError::InvalidBacklogLease);
        }
        if max_items == 0 {
            return Err(StorageError::BacklogProtocol(
                "batch claim size is zero".to_string(),
            ));
        }
        let now = Self::now_unix_ms()?;
        let deadline = Self::lease_deadline(now, lease_ms)?;
        let claim_limit = i64::try_from(max_items).map_err(|_| {
            StorageError::BacklogProtocol("batch claim size is too large".to_string())
        })?;
        let script = Script::new(CLAIM_MULTI_SCRIPT);
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
            .arg(claim_limit)
            .invoke_async(&mut *connection)
            .await?;
        let (tuples, remainder) = values.as_chunks::<3>();
        if !remainder.is_empty() {
            return Err(StorageError::BacklogProtocol(
                "batch claim returned an invalid tuple list".to_string(),
            ));
        }
        let mut leases = Vec::with_capacity(values.len() / 3);
        for tuple in tuples {
            let member = String::from_utf8(tuple[0].clone())
                .map_err(|error| StorageError::BacklogProtocol(error.to_string()))?;
            let lease_id = String::from_utf8(tuple[1].clone())
                .map_err(|error| StorageError::BacklogProtocol(error.to_string()))?;
            let encoded = tuple[2].clone();
            let request = Self::decode(&encoded)?;
            if Self::member(&request.record) != member {
                return Err(StorageError::BacklogProtocol(
                    "batch claim record member does not match payload".to_string(),
                ));
            }
            leases.push(SnapshotBacklogLease {
                request,
                member,
                lease_id,
                encoded,
            });
        }
        Ok(leases)
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

    /// Acknowledge a committed lease batch in one Redis round trip.
    pub async fn ack_multi(
        &self,
        leases: &[SnapshotBacklogLease],
    ) -> Result<Vec<SnapshotBacklogAck>, StorageError> {
        if leases.is_empty() {
            return Ok(Vec::new());
        }
        let now = Self::now_unix_ms()?;
        let script = Script::new(ACK_MULTI_SCRIPT);
        let mut invocation = script.prepare_invoke();
        invocation
            .key(PROCESSING_KEY)
            .key(LEASES_KEY)
            .key(PENDING_KEY)
            .arg(ENTRY_PREFIX)
            .arg(now);
        for lease in leases {
            invocation
                .arg(&lease.lease_id)
                .arg(&lease.member)
                .arg(&lease.encoded);
        }
        let mut connection = self.worker_connection.lock().await;
        let values: Vec<i64> = invocation.invoke_async(&mut *connection).await?;
        if values.len() != leases.len() {
            return Err(StorageError::BacklogProtocol(
                "batch ack returned an invalid result count".to_string(),
            ));
        }
        values
            .into_iter()
            .map(|result| match result {
                0 => Ok(SnapshotBacklogAck::LeaseLost),
                1 => Ok(SnapshotBacklogAck::Removed),
                2 => Ok(SnapshotBacklogAck::Superseded),
                _ => Err(StorageError::BacklogProtocol(
                    "batch ack returned an invalid status".to_string(),
                )),
            })
            .collect()
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

    /// Release a failed lease batch for immediate retry in one Redis round trip.
    pub async fn release_multi(
        &self,
        leases: &[SnapshotBacklogLease],
    ) -> Result<Vec<bool>, StorageError> {
        if leases.is_empty() {
            return Ok(Vec::new());
        }
        let now = Self::now_unix_ms()?;
        let script = Script::new(RELEASE_MULTI_SCRIPT);
        let mut invocation = script.prepare_invoke();
        invocation
            .key(PROCESSING_KEY)
            .key(LEASES_KEY)
            .key(PENDING_KEY)
            .arg(ENTRY_PREFIX)
            .arg(now);
        for lease in leases {
            invocation.arg(&lease.member).arg(&lease.lease_id);
        }
        let mut connection = self.worker_connection.lock().await;
        let values: Vec<i64> = invocation.invoke_async(&mut *connection).await?;
        if values.len() != leases.len() {
            return Err(StorageError::BacklogProtocol(
                "batch release returned an invalid result count".to_string(),
            ));
        }
        Ok(values.into_iter().map(|value| value == 1).collect())
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

#[cfg(test)]
mod enqueue_batcher_tests {
    use super::*;
    use std::sync::Mutex as StdMutex;
    use tokio::sync::Semaphore;
    use tokio::time::sleep;

    /// 可阻塞、可在指定批次失败的替身写入端。 / Gated sink double that can fail a chosen batch.
    #[derive(Clone)]
    struct FakeSink {
        batches: Arc<StdMutex<Vec<Vec<String>>>>,
        gate: Arc<Semaphore>,
        fail_batch: Option<usize>,
    }

    #[async_trait]
    impl EnqueueSink for FakeSink {
        async fn write(&mut self, entries: &[&EnqueueEntry]) -> Result<(), StorageError> {
            self.gate.acquire().await.expect("gate open").forget();
            let index = {
                let mut batches = self.batches.lock().unwrap();
                batches.push(entries.iter().map(|entry| entry.member.clone()).collect());
                batches.len()
            };
            if self.fail_batch == Some(index) {
                return Err(StorageError::BacklogProtocol(
                    "injected write failure".to_string(),
                ));
            }
            Ok(())
        }
    }

    fn sink(fail_batch: Option<usize>) -> FakeSink {
        FakeSink {
            batches: Arc::default(),
            gate: Arc::new(Semaphore::new(0)),
            fail_batch,
        }
    }

    fn entry(name: &str) -> Vec<EnqueueEntry> {
        vec![EnqueueEntry {
            entry_key: format!("entry:{name}"),
            encoded: name.as_bytes().to_vec(),
            member: name.to_string(),
        }]
    }

    fn config(queue_capacity: usize, max_queue_wait_ms: u64) -> EnqueueBatchConfig {
        EnqueueBatchConfig {
            queue_capacity,
            max_batch_records: 512,
            max_queue_wait: Duration::from_millis(max_queue_wait_ms),
            ack: EnqueueAck::Aof,
        }
    }

    fn batches(sink: &FakeSink) -> Vec<Vec<String>> {
        sink.batches.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn requests_arriving_during_a_write_share_the_next_write() {
        let sink = sink(None);
        let batcher = EnqueueBatcher::spawn(sink.clone(), config(4096, 10_000));
        let first = tokio::spawn({
            let batcher = batcher.clone();
            async move { batcher.submit(entry("first")).await }
        });
        sleep(Duration::from_millis(50)).await;
        let waiting: Vec<_> = (0..50)
            .map(|i| {
                let batcher = batcher.clone();
                tokio::spawn(async move { batcher.submit(entry(&format!("p{i}"))).await })
            })
            .collect();
        sleep(Duration::from_millis(50)).await;
        sink.gate.add_permits(10);
        first.await.unwrap().unwrap();
        for handle in waiting {
            handle.await.unwrap().unwrap();
        }
        let written = batches(&sink);
        // 第一次写入期间到达的50个请求只用一次写入和一次AOF确认。 / The 50 requests that arrived during the first write use one write and one AOF wait.
        assert_eq!(written.len(), 2);
        assert_eq!(written[0], vec!["first"]);
        assert_eq!(written[1].len(), 50);
    }

    #[tokio::test]
    async fn expired_requests_are_rejected_without_being_written() {
        let sink = sink(None);
        let batcher = EnqueueBatcher::spawn(sink.clone(), config(4096, 50));
        let first = tokio::spawn({
            let batcher = batcher.clone();
            async move { batcher.submit(entry("first")).await }
        });
        sleep(Duration::from_millis(30)).await;
        let stale: Vec<_> = (0..3)
            .map(|i| {
                let batcher = batcher.clone();
                tokio::spawn(async move { batcher.submit(entry(&format!("stale{i}"))).await })
            })
            .collect();
        sleep(Duration::from_millis(150)).await;
        sink.gate.add_permits(10);
        first.await.unwrap().unwrap();
        for handle in stale {
            assert!(matches!(
                handle.await.unwrap(),
                Err(StorageError::BacklogEnqueueDeadlineExceeded { .. })
            ));
        }
        batcher.submit(entry("fresh")).await.unwrap();
        assert_eq!(
            batches(&sink),
            vec![vec!["first".to_string()], vec!["fresh".to_string()]]
        );
    }

    #[tokio::test]
    async fn a_full_queue_rejects_immediately_instead_of_waiting() {
        let sink = sink(None);
        let batcher = EnqueueBatcher::spawn(sink.clone(), config(1, 10_000));
        let first = tokio::spawn({
            let batcher = batcher.clone();
            async move { batcher.submit(entry("first")).await }
        });
        sleep(Duration::from_millis(30)).await;
        let queued = tokio::spawn({
            let batcher = batcher.clone();
            async move { batcher.submit(entry("queued")).await }
        });
        sleep(Duration::from_millis(30)).await;
        let rejected = tokio::time::timeout(
            Duration::from_millis(200),
            batcher.submit(entry("rejected")),
        )
        .await
        .expect("overload must not wait");
        assert!(matches!(
            rejected,
            Err(StorageError::BacklogEnqueueOverloaded { capacity: 1 })
        ));
        sink.gate.add_permits(10);
        first.await.unwrap().unwrap();
        queued.await.unwrap().unwrap();
        assert!(
            !batches(&sink)
                .iter()
                .flatten()
                .any(|member| member == "rejected")
        );
    }

    #[tokio::test]
    async fn a_failed_write_fails_its_whole_batch_and_later_batches_continue() {
        let sink = sink(Some(2));
        let batcher = EnqueueBatcher::spawn(sink.clone(), config(4096, 10_000));
        let first = tokio::spawn({
            let batcher = batcher.clone();
            async move { batcher.submit(entry("first")).await }
        });
        sleep(Duration::from_millis(30)).await;
        let failing: Vec<_> = ["b", "c"]
            .iter()
            .map(|name| {
                let batcher = batcher.clone();
                let name = name.to_string();
                tokio::spawn(async move { batcher.submit(entry(&name)).await })
            })
            .collect();
        sleep(Duration::from_millis(30)).await;
        sink.gate.add_permits(10);
        first.await.unwrap().unwrap();
        for handle in failing {
            match handle.await.unwrap() {
                Err(StorageError::BacklogEnqueueFailed(message)) => {
                    assert!(message.contains("injected write failure"))
                }
                other => panic!("expected shared batch failure, got {other:?}"),
            }
        }
        batcher.submit(entry("after")).await.unwrap();
        assert_eq!(batches(&sink).len(), 3);
    }

    #[tokio::test]
    async fn abandoned_requests_are_not_written() {
        let sink = sink(None);
        let batcher = EnqueueBatcher::spawn(sink.clone(), config(4096, 10_000));
        let first = tokio::spawn({
            let batcher = batcher.clone();
            async move { batcher.submit(entry("first")).await }
        });
        sleep(Duration::from_millis(30)).await;
        let abandoned = tokio::spawn({
            let batcher = batcher.clone();
            async move { batcher.submit(entry("abandoned")).await }
        });
        sleep(Duration::from_millis(30)).await;
        abandoned.abort();
        let kept = tokio::spawn({
            let batcher = batcher.clone();
            async move { batcher.submit(entry("kept")).await }
        });
        sleep(Duration::from_millis(30)).await;
        sink.gate.add_permits(10);
        first.await.unwrap().unwrap();
        kept.await.unwrap().unwrap();
        assert_eq!(
            batches(&sink),
            vec![vec!["first".to_string()], vec!["kept".to_string()]]
        );
    }

    #[tokio::test]
    async fn zero_limits_are_rejected_before_connecting() {
        let invalid = EnqueueBatchConfig {
            queue_capacity: 0,
            ..EnqueueBatchConfig::default()
        };
        assert!(matches!(
            RedisSnapshotBacklog::connect_with_config("redis://127.0.0.1:1/0", invalid).await,
            Err(StorageError::BacklogProtocol(_))
        ));
    }
}
