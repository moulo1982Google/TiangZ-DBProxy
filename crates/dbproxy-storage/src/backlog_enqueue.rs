//! 入队组提交、期限与写入连接所有权。
//! Enqueue group commit, deadlines and write-connection ownership.
use super::{ENQUEUE_BATCH_SCRIPT, FENCE_SEQUENCE_KEY, PENDING_KEY, RedisSnapshotBacklog};
use crate::redis_durability::redis_result;
use crate::{RedisDurabilityConfig, StorageError, StorageMetrics, latency::Stage};
use async_trait::async_trait;
use redis::{Script, aio::MultiplexedConnection};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::{Instant, timeout_at},
};

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
    /// 从组提交入口起，排队、连接、写入、确认共享的预算。
    /// Shared budget from batcher acceptance through queue, connect, write and acknowledgement.
    pub total_timeout: Duration,
    pub durability: RedisDurabilityConfig,
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
        // 仅限制本地入队阶段；客户端网络、准入和重试仍受 SDK 独立的总预算约束。
        // Bounds this local enqueue stage; network, admission and retries retain the SDK's total budget.
        Self {
            queue_capacity: 4096,
            max_batch_records: 512,
            max_queue_wait: Duration::from_millis(2_000),
            total_timeout: Duration::from_millis(4_500),
            durability: RedisDurabilityConfig::default(),
            ack: EnqueueAck::Aof,
        }
    }
}

impl EnqueueBatchConfig {
    pub fn validate(self) -> Result<(), StorageError> {
        self.durability.validate()?;
        if self.queue_capacity == 0 || self.max_batch_records == 0 {
            return Err(StorageError::BacklogProtocol(
                "enqueue queue capacity and batch size must be positive".into(),
            ));
        }
        if self.max_queue_wait < Duration::from_millis(1)
            || self.total_timeout > Duration::from_secs(60)
            || self.total_timeout <= self.durability.response_timeout
            || self.max_queue_wait >= self.total_timeout
            || (self.ack == EnqueueAck::Aof
                && self.max_queue_wait
                    >= self
                        .total_timeout
                        .saturating_sub(self.durability.aof_ack_timeout))
        {
            return Err(StorageError::InvalidRedisDurabilityBudget(
                "enqueue requires positive queue wait, Redis I/O < total <= 60000ms, and queue + AOF < total",
            ));
        }
        Ok(())
    }
}

pub(super) struct EnqueueEntry {
    pub(super) entry_key: String,
    pub(super) encoded: Vec<u8>,
    pub(super) member: String,
}

const QUEUED: u8 = 0;
const WRITING: u8 = 1;
const CANCELLED: u8 = 2;
const TIMED_OUT: u8 = 4;

struct EnqueueState {
    phase: AtomicU8,
    accepted_at: Instant,
    metrics: Arc<StorageMetrics>,
}
impl EnqueueState {
    fn new(metrics: Arc<StorageMetrics>) -> Self {
        metrics.latency.enter(Stage::EnqueueQueue);
        Self {
            phase: AtomicU8::new(QUEUED),
            accepted_at: Instant::now(),
            metrics,
        }
    }
    fn leave_queue(&self, next: u8) -> bool {
        if self
            .phase
            .compare_exchange(QUEUED, next, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        self.metrics
            .latency
            .leave(Stage::EnqueueQueue, self.accepted_at.elapsed());
        true
    }
}
impl Drop for EnqueueState {
    fn drop(&mut self) {
        // receiver 关闭或任务在开始前被丢弃，也必须释放队列观测作用域。
        // Receiver shutdown or dropping an unstarted job must close the observed queue scope.
        self.leave_queue(CANCELLED);
    }
}

struct EnqueueJob {
    entries: Vec<EnqueueEntry>,
    state: Arc<EnqueueState>,
    reply: oneshot::Sender<Result<(), StorageError>>,
}

/// 把同一时刻的入队合并为一次Redis写入和一次WAITAOF。 / Writes concurrent enqueues with one Redis write and one WAITAOF.
#[async_trait]
pub(super) trait EnqueueSink: Send + 'static {
    /// 写入全部记录并等待本连接此前的写入进入本地AOF。 / Write every record and wait until this connection's writes reach local AOF.
    async fn write(
        &mut self,
        entries: &[&EnqueueEntry],
        deadline: Instant,
    ) -> Result<(), StorageError>;
}

pub(super) struct RedisEnqueueSink {
    connection: Option<MultiplexedConnection>,
    client: redis::Client,
    script: Script,
    config: EnqueueBatchConfig,
    metrics: Arc<StorageMetrics>,
}

impl RedisEnqueueSink {
    pub(super) async fn connect(
        url: &str,
        config: EnqueueBatchConfig,
        metrics: Arc<StorageMetrics>,
    ) -> Result<Self, StorageError> {
        let client = redis::Client::open(url)?;
        let connection = client
            .get_multiplexed_async_connection_with_config(&config.durability.connection_config())
            .await?;
        Ok(Self {
            connection: Some(connection),
            client,
            script: Script::new(ENQUEUE_BATCH_SCRIPT),
            config,
            metrics,
        })
    }
}

#[async_trait]
impl EnqueueSink for RedisEnqueueSink {
    async fn write(
        &mut self,
        entries: &[&EnqueueEntry],
        deadline: Instant,
    ) -> Result<(), StorageError> {
        if Instant::now() >= deadline {
            return Err(StorageError::RedisDurabilityDeadlineExceeded);
        }
        // 连接只在完整确认后归还；失败或 Future 取消后不得在新连接确认旧写入。
        // Return the connection only after full ACK; failure/cancellation cannot ACK old writes on a new socket.
        let timer = self.metrics.latency.start(Stage::EnqueueWrite);
        let mut connection = match self.connection.take() {
            Some(connection) => connection,
            None => redis_result(
                self.client
                    .get_multiplexed_async_connection_with_config(
                        &self.config.durability.connection_config(),
                    )
                    .await,
                &self.metrics,
                Stage::EnqueueWrite,
            )?,
        };
        let score = RedisSnapshotBacklog::now_unix_ms()?;
        let mut invocation = self.script.prepare_invoke();
        invocation
            .key(PENDING_KEY)
            .key(FENCE_SEQUENCE_KEY)
            .arg(score);
        for entry in entries {
            invocation
                .arg(&entry.entry_key)
                .arg(&entry.encoded)
                .arg(&entry.member);
        }
        if Instant::now() >= deadline {
            return Err(StorageError::RedisDurabilityDeadlineExceeded);
        }
        let accepted: i64 = redis_result(
            invocation.invoke_async(&mut connection).await,
            &self.metrics,
            Stage::EnqueueWrite,
        )?;
        if accepted != i64::try_from(entries.len()).unwrap_or(i64::MAX) {
            return Err(StorageError::BacklogProtocol(
                "batch enqueue returned an invalid count".to_string(),
            ));
        }
        drop(timer);
        match self.config.ack {
            // WAITAOF覆盖本连接此前的全部写入，所以一次等待确认整批。 / WAITAOF covers every prior write of this connection, so one wait acknowledges the batch.
            EnqueueAck::Aof => {
                self.config
                    .durability
                    .wait_for_local_aof(&mut connection, deadline, &self.metrics, Stage::EnqueueAof)
                    .await?
            }
            EnqueueAck::Memory => (),
        }
        self.connection = Some(connection);
        Ok(())
    }
}

/// 组提交入口：调用方只提交并等待自己的结果；唯一的后台任务独占写入连接。
/// Group-commit entry: callers submit and await their own result; one background task owns the write connection.
#[derive(Clone)]
pub(super) struct EnqueueBatcher {
    sender: mpsc::Sender<EnqueueJob>,
    capacity: usize,
    config: EnqueueBatchConfig,
    metrics: Arc<StorageMetrics>,
}

impl EnqueueBatcher {
    #[cfg(test)]
    fn spawn<S: EnqueueSink>(sink: S, config: EnqueueBatchConfig) -> Self {
        Self::spawn_with_metrics(sink, config, Arc::new(StorageMetrics::default()))
    }

    pub(super) fn spawn_with_metrics<S: EnqueueSink>(
        sink: S,
        config: EnqueueBatchConfig,
        metrics: Arc<StorageMetrics>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        // 在租户后端连接时创建；保留该 span，日志能看出属于哪个租户。
        // Created while the tenant's backend connects; keep that span for its log lines.
        tokio::spawn(tracing::Instrument::in_current_span(run_enqueue_batcher(
            receiver,
            sink,
            config,
            metrics.clone(),
        )));
        Self {
            sender,
            capacity: config.queue_capacity,
            config,
            metrics,
        }
    }

    pub(super) async fn submit(&self, entries: Vec<EnqueueEntry>) -> Result<(), StorageError> {
        let _timer = self.metrics.latency.start(Stage::EnqueueTotal);
        let permit = self.sender.try_reserve().map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => StorageError::BacklogEnqueueOverloaded {
                capacity: self.capacity,
            },
            mpsc::error::TrySendError::Closed(_) => StorageError::BacklogEnqueueStopped,
        })?;
        let state = Arc::new(EnqueueState::new(self.metrics.clone()));
        let accepted_at = state.accepted_at;
        let (reply, mut result) = oneshot::channel();
        permit.send(EnqueueJob {
            entries,
            state: state.clone(),
            reply,
        });
        match timeout_at(accepted_at + self.config.max_queue_wait, &mut result).await {
            Ok(outcome) => return outcome.map_err(|_| StorageError::BacklogEnqueueStopped)?,
            Err(_) if state.leave_queue(CANCELLED) => {
                self.metrics.latency.timed_out(Stage::EnqueueQueue);
                return Err(StorageError::BacklogEnqueueDeadlineExceeded {
                    waited_ms: accepted_at.elapsed().as_millis() as u64,
                });
            }
            Err(_) => (),
        }
        match timeout_at(accepted_at + self.config.total_timeout, result).await {
            Ok(outcome) => outcome.map_err(|_| StorageError::BacklogEnqueueStopped)?,
            Err(_) => {
                record_total_timeout(&state, &self.metrics);
                Err(StorageError::BacklogEnqueueFailed(
                    StorageError::RedisDurabilityDeadlineExceeded.to_string(),
                ))
            }
        }
    }
}

fn record_total_timeout(state: &EnqueueState, metrics: &StorageMetrics) {
    if state.phase.fetch_or(TIMED_OUT, Ordering::AcqRel) & TIMED_OUT == 0 {
        metrics.latency.timed_out(Stage::EnqueueTotal);
    }
}

/// 写入进行中（包括等待落盘）到达的请求自然积累，下一轮一次写完；超期或已放弃的请求不写入。
/// Requests arriving while a write (including its fsync wait) is in progress accumulate and are written together
/// next round; expired or abandoned requests are never written.
async fn run_enqueue_batcher<S: EnqueueSink>(
    mut jobs: mpsc::Receiver<EnqueueJob>,
    mut sink: S,
    config: EnqueueBatchConfig,
    metrics: Arc<StorageMetrics>,
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
        let mut live = Vec::with_capacity(batch.len());
        for job in batch {
            let now = Instant::now();
            // 与调用方排队超时竞态时，只有成功取得 QUEUED 的一方可以决定写入。
            // Only the winner of the queue-timeout race may authorize a write.
            if !job.state.leave_queue(WRITING) {
                continue;
            }
            if job.reply.is_closed() {
                continue;
            }
            let waited = now.duration_since(job.state.accepted_at);
            if waited >= config.max_queue_wait {
                metrics.latency.timed_out(Stage::EnqueueQueue);
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
        let deadline = live
            .iter()
            .map(|job| job.state.accepted_at + config.total_timeout)
            .min()
            .expect("live batch");
        // 写入与确认对整批共享同一个结果；失败时结果未知，调用方按原请求号重试。
        // Write and acknowledgement share one outcome across the batch; on failure the outcome is unknown and callers retry with their request IDs.
        let outcome = if Instant::now() >= deadline {
            Err(StorageError::RedisDurabilityDeadlineExceeded)
        } else {
            timeout_at(deadline, sink.write(&entries, deadline))
                .await
                .unwrap_or(Err(StorageError::RedisDurabilityDeadlineExceeded))
        };
        // total 指标按逻辑请求计数；原子标志合并 worker 与调用方的超时竞态。
        // Total timeouts count logical requests; an atomic flag merges worker/caller timeout races.
        let timed_out = matches!(outcome, Err(StorageError::RedisDurabilityDeadlineExceeded));
        let outcome = outcome.map_err(|error| error.to_string());
        for job in live {
            if timed_out {
                record_total_timeout(&job.state, &metrics);
            }
            let _ = job
                .reply
                .send(outcome.clone().map_err(StorageError::BacklogEnqueueFailed));
        }
    }
}

#[cfg(test)]
#[path = "backlog_enqueue_tests.rs"]
mod budget_tests;
