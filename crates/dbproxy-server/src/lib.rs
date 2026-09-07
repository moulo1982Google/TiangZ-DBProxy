//! DBProxy 网络服务实现。
//! DBProxy network service implementation.
//!
//! 服务端只调度通用快照、记录事务和交易持久化原语。游戏 Repository、Entity 生命周期与业务校验
//! 必须留在 TiangZ。The server only dispatches generic snapshots, record transactions, and trade persistence primitives;
//! game repositories, entity lifecycle, and business validation stay in TiangZ.

pub mod config;
mod memory_backend;
mod observability;
pub mod relay_config;
pub mod relay_metrics;

pub use memory_backend::MemoryBackend;
pub use observability::{DbProxyMetrics, ObservabilityServer};

use std::{
    collections::{HashMap, HashSet},
    fmt,
    hash::{Hash, Hasher},
    io,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use thiserror::Error;
use tiangz_dbproxy_core::CommitEffects;
use tiangz_dbproxy_core::{
    AsyncMultiRecordTransactionStore, AsyncSnapshotStore, AsyncTradeStore, AsyncTransactionalStore,
    MultiRecordTransactionalWrite, MultiRecordTransactionalWriteOutcome, RecordKey, Revision,
    SnapshotEnvelope, SnapshotWrite, SnapshotWriteOutcome, StoreError, TradeEnvelope, TradeReceipt,
    TradeTransaction, TradeTransactionOutcome, TransactionReceipt, TransactionalRecordWrite,
    TransactionalWrite, TransactionalWriteOutcome,
};
use tiangz_dbproxy_protocol::{
    DEFAULT_MAX_FRAME_BYTES, DEFAULT_MAX_PAYLOAD_BYTES, MAX_AUTH_TOKEN_BYTES,
    MAX_CLIENT_NAME_BYTES, PROTOCOL_FINGERPRINT, PROTOCOL_VERSION, ProtocolError,
    is_compatible_protocol_fingerprint, read_message, wire, write_message,
};
use tiangz_dbproxy_storage::{
    CacheRepairAcknowledgements, CacheRepairStats, DEFAULT_OUTBOX_STREAM_PREFIX, OutboxStats,
    PostgresCacheRepairQueue, PostgresOutboxQueue, PostgresSnapshotStore, RedisOutboxPublisher,
    RedisSnapshotBacklog, RedisSnapshotBacklogStats, SnapshotBacklogAck, StorageError,
    StorageMetrics, TieredSnapshotStore, TieredSnapshotStoreConfig,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinSet,
    time::{sleep, timeout},
};

use observability::{
    BacklogMetricResult, DurableQueueMetricKind, DurableQueueMetricResult, HandshakeRejection,
    RpcOperation,
};

const BACKLOG_FLUSH_BATCH_SIZE: usize = 64;

#[derive(Debug, Error)]
pub enum BackendError {
    #[error("invalid backend configuration: {0}")]
    InvalidConfig(&'static str),
    #[error(transparent)]
    Core(#[from] StoreError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("backend worker failed: {0}")]
    Worker(String),
}

/// 网络层依赖的最小后端接口。实现不能把业务对象泄漏到 DBProxy。
/// Minimal backend used by the network layer; implementations must remain business-agnostic.
#[async_trait]
pub trait DbProxyBackend: Send + Sync + 'static {
    async fn commit_records(
        &self,
        request: MultiRecordTransactionalWrite,
        effects: CommitEffects,
    ) -> Result<MultiRecordTransactionalWriteOutcome, BackendError> {
        let _ = (request, effects);
        Err(BackendError::InvalidConfig(
            "generic commits are not supported by this backend",
        ))
    }
    async fn load(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, BackendError>;
    async fn load_multi(
        &self,
        records: &[RecordKey],
    ) -> Result<Vec<Option<SnapshotEnvelope>>, BackendError> {
        let mut snapshots = Vec::with_capacity(records.len());
        for record in records {
            snapshots.push(self.load(record).await?);
        }
        Ok(snapshots)
    }
    async fn save(&self, request: SnapshotWrite) -> Result<SnapshotWriteOutcome, BackendError>;
    async fn save_multi(
        &self,
        requests: Vec<SnapshotWrite>,
    ) -> Result<Vec<Result<SnapshotWriteOutcome, BackendError>>, BackendError> {
        let mut outcomes = Vec::with_capacity(requests.len());
        for request in requests {
            outcomes.push(self.save(request).await);
        }
        Ok(outcomes)
    }
    async fn enqueue_snapshot(&self, request: SnapshotWrite) -> Result<(), BackendError>;
    async fn enqueue_multi_snapshot(
        &self,
        requests: Vec<SnapshotWrite>,
    ) -> Result<Vec<Result<(), BackendError>>, BackendError> {
        let mut outcomes = Vec::with_capacity(requests.len());
        for request in requests {
            outcomes.push(self.enqueue_snapshot(request).await);
        }
        Ok(outcomes)
    }
    async fn apply_transaction(
        &self,
        request: TransactionalWrite,
    ) -> Result<TransactionalWriteOutcome, BackendError>;
    async fn load_transaction(
        &self,
        operation_id: &str,
        record: &RecordKey,
    ) -> Result<Option<TransactionReceipt>, BackendError>;
    async fn apply_multi_transaction(
        &self,
        request: MultiRecordTransactionalWrite,
    ) -> Result<MultiRecordTransactionalWriteOutcome, BackendError> {
        let _ = request;
        Err(BackendError::InvalidConfig(
            "multi-record transactions are not supported by this backend",
        ))
    }
    async fn load_multi_transaction(
        &self,
        operation_id: &str,
        records: &[RecordKey],
    ) -> Result<Option<tiangz_dbproxy_core::MultiRecordTransactionReceipt>, BackendError> {
        let _ = (operation_id, records);
        Err(BackendError::InvalidConfig(
            "multi-record transactions are not supported by this backend",
        ))
    }
    async fn load_trade(&self, trade_id: &str) -> Result<Option<TradeEnvelope>, BackendError> {
        let _ = trade_id;
        Err(BackendError::InvalidConfig(
            "trade transactions are not supported by this backend",
        ))
    }
    async fn apply_trade_transaction(
        &self,
        request: TradeTransaction,
    ) -> Result<TradeTransactionOutcome, BackendError> {
        let _ = request;
        Err(BackendError::InvalidConfig(
            "trade transactions are not supported by this backend",
        ))
    }
    async fn load_trade_transaction(
        &self,
        operation_id: &str,
        trade_id: &str,
    ) -> Result<Option<TradeReceipt>, BackendError> {
        let _ = (operation_id, trade_id);
        Err(BackendError::InvalidConfig(
            "trade transactions are not supported by this backend",
        ))
    }
}

/// 真实 PostgreSQL/Redis 后端。每个 shard 使用独立数据库连接，并按 RecordKey 稳定路由，
/// 避免整个服务被单个 `tokio-postgres::Client` 的事务锁串行化。
/// Real PostgreSQL/Redis backend. Stable record sharding avoids one global client lock.
pub struct StorageBackend {
    shards: Vec<TieredSnapshotStore>,
    backlog: RedisSnapshotBacklog,
    cache_repairs: PostgresCacheRepairQueue,
    cache_acknowledgements: CacheRepairAcknowledgements,
    outbox: PostgresOutboxQueue,
    outbox_publishers: HashMap<String, Arc<dyn tiangz_dbproxy_storage::Publisher>>,
    outbox_publish_timeout: Duration,
    pub outbox_relay_metrics: Arc<relay_metrics::RelayMetrics>,
    metrics: Arc<StorageMetrics>,
}

/// Connection layout and cache policies for the real storage backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StorageBackendConfig {
    pub shard_count: usize,
    pub tiered: TieredSnapshotStoreConfig,
}

impl StorageBackend {
    /// 创建固定数量的连接分片。分片数是启动配置，运行时不能热改。
    /// Create a fixed connection-shard count; changing it requires a service restart.
    pub async fn connect(
        postgres_url: &str,
        redis_url: &str,
        shard_count: usize,
    ) -> Result<Self, BackendError> {
        Self::connect_with_config(
            postgres_url,
            redis_url,
            StorageBackendConfig {
                shard_count,
                tiered: TieredSnapshotStoreConfig::default(),
            },
        )
        .await
    }

    /// Create fixed connection shards with explicit cache policies.
    pub async fn connect_with_config(
        postgres_url: &str,
        redis_url: &str,
        config: StorageBackendConfig,
    ) -> Result<Self, BackendError> {
        Self::connect_with_redis_urls(postgres_url, redis_url, redis_url, config).await
    }

    /// Connect with separate Redis endpoints for durable queues and the disposable snapshot cache.
    /// Keeping them equal preserves the original single-Redis deployment.
    pub async fn connect_with_redis_urls(
        postgres_url: &str,
        redis_url: &str,
        cache_redis_url: &str,
        config: StorageBackendConfig,
    ) -> Result<Self, BackendError> {
        Self::connect_with_outbox(
            postgres_url,
            redis_url,
            cache_redis_url,
            config,
            &relay_config::ResolvedOutboxRelay::default(),
        )
        .await
    }

    /// 路由必须先注册再接流量；旧配置保留 legacy Publisher。
    /// Registers immutable routes before serving traffic, preserving the legacy publisher.
    pub async fn connect_with_outbox(
        postgres_url: &str,
        redis_url: &str,
        cache_redis_url: &str,
        config: StorageBackendConfig,
        relay: &relay_config::ResolvedOutboxRelay,
    ) -> Result<Self, BackendError> {
        if config.shard_count == 0 {
            return Err(BackendError::InvalidConfig("storage shard count is zero"));
        }
        let metrics = Arc::new(StorageMetrics::default());
        let cache_acknowledgements = CacheRepairAcknowledgements::default();
        let mut shards = Vec::with_capacity(config.shard_count);
        for _ in 0..config.shard_count {
            shards.push(
                TieredSnapshotStore::connect_with_config(
                    postgres_url,
                    cache_redis_url,
                    config.tiered,
                    Arc::clone(&metrics),
                )
                .await?,
            );
        }
        // Queue polling uses one dedicated PostgreSQL connection so background maintenance never
        // holds the mutex of a request shard. Both queues share it because claims are short.
        let maintenance = PostgresSnapshotStore::connect(postgres_url).await?;
        let cache_repairs = maintenance.cache_repair_queue();
        let outbox = maintenance.outbox_queue();
        outbox
            .ensure_unregistered_routes(&relay.disabled_routes)
            .await?;
        let mut outbox_publishers: HashMap<String, Arc<dyn tiangz_dbproxy_storage::Publisher>> =
            HashMap::new();
        for (id, url) in std::iter::once(("legacy", redis_url)).chain(
            relay
                .publishers
                .iter()
                .map(|p| (p.id.as_str(), p.url.as_str())),
        ) {
            outbox
                .register_publisher(
                    id,
                    &tiangz_dbproxy_storage::redis_endpoint_fingerprint(url)?,
                )
                .await?;
            outbox_publishers.insert(
                id.to_string(),
                Arc::new(RedisOutboxPublisher::connect(url, DEFAULT_OUTBOX_STREAM_PREFIX).await?),
            );
        }
        for route in &relay.routes {
            outbox.register_route(route).await?;
        }
        for id in outbox.required_publishers().await? {
            if !outbox_publishers.contains_key(&id) {
                return Err(BackendError::InvalidConfig(
                    "a registered route or pending event requires a missing publisher",
                ));
            }
        }
        for shard in &mut shards {
            shard.defer_cache_acknowledgements(cache_acknowledgements.clone());
        }
        Ok(Self {
            shards,
            backlog: RedisSnapshotBacklog::connect(redis_url).await?,
            cache_repairs,
            cache_acknowledgements,
            outbox,
            outbox_publishers,
            outbox_publish_timeout: Duration::from_millis(relay.publish_timeout_ms),
            outbox_relay_metrics: Arc::new(relay_metrics::RelayMetrics::new(&relay.routes)),
            metrics,
        })
    }

    pub fn metrics(&self) -> &StorageMetrics {
        &self.metrics
    }

    pub async fn backlog_stats(&self) -> Result<RedisSnapshotBacklogStats, BackendError> {
        Ok(self.backlog.stats().await?)
    }

    pub async fn cache_repair_stats(&self) -> Result<CacheRepairStats, BackendError> {
        Ok(self.cache_repairs.stats().await?)
    }

    pub async fn outbox_stats(&self) -> Result<OutboxStats, BackendError> {
        Ok(self.outbox.stats().await?)
    }

    fn shard_index(&self, record: &RecordKey) -> usize {
        let mut hasher = StableHasher::default();
        record.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()
    }

    fn shard(&self, record: &RecordKey) -> TieredSnapshotStore {
        self.shards[self.shard_index(record)].clone()
    }

    fn shard_for_operation(&self, operation_id: &str) -> TieredSnapshotStore {
        let mut hasher = StableHasher::default();
        operation_id.hash(&mut hasher);
        self.shards[(hasher.finish() as usize) % self.shards.len()].clone()
    }

    /// 处理一条 Redis backlog。数据库成功但 ACK 失败时不能伪装成完全成功；lease 到期后
    /// 会以原 request_id 重试并命中 PostgreSQL 幂等记录。
    /// Process one durable backlog item. A failed ACK is recovered by lease expiry and idempotency.
    pub async fn process_backlog_once(
        &self,
        lease_ms: u64,
    ) -> Result<BacklogProcessOutcome, BackendError> {
        let Some(lease) = self.backlog.claim(lease_ms).await? else {
            return Ok(BacklogProcessOutcome::Empty);
        };
        match self.save(lease.request.clone()).await {
            Ok(_) => {
                let ack = self.backlog.ack(&lease).await?;
                Ok(BacklogProcessOutcome::Committed(ack))
            }
            Err(error) => {
                if let Err(release_error) = self.backlog.release(&lease).await {
                    tracing::error!(%release_error, "failed to release snapshot backlog lease");
                }
                Err(error)
            }
        }
    }

    /// Drain multiple ordinary snapshots with one Redis claim, one shard-aware save batch, and
    /// batched ACK/release commands. Failed entries are immediately released while successful
    /// entries retain their original idempotency IDs.
    pub async fn process_backlog_batch(
        &self,
        lease_ms: u64,
        max_items: usize,
    ) -> Result<Vec<SnapshotBacklogAck>, BackendError> {
        let leases = self.backlog.claim_multi(lease_ms, max_items).await?;
        if leases.is_empty() {
            return Ok(Vec::new());
        }
        let requests = leases
            .iter()
            .map(|lease| lease.request.clone())
            .collect::<Vec<_>>();
        let outcomes = match self.save_multi(requests).await {
            Ok(outcomes) => outcomes,
            Err(error) => {
                if let Err(release_error) = self.backlog.release_multi(&leases).await {
                    tracing::error!(%release_error, "failed to release snapshot backlog batch after storage failure");
                }
                return Err(error);
            }
        };
        if outcomes.len() != leases.len() {
            if let Err(release_error) = self.backlog.release_multi(&leases).await {
                tracing::error!(%release_error, "failed to release malformed snapshot backlog batch");
            }
            return Err(BackendError::Worker(
                "snapshot backlog batch returned an invalid result count".to_string(),
            ));
        }

        let mut committed = Vec::new();
        let mut failed = Vec::new();
        let mut first_error = None;
        for (lease, outcome) in leases.into_iter().zip(outcomes) {
            match outcome {
                Ok(_) => committed.push(lease),
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    failed.push(lease);
                }
            }
        }
        let acknowledgements = self.backlog.ack_multi(&committed).await?;
        if !failed.is_empty()
            && let Err(release_error) = self.backlog.release_multi(&failed).await
        {
            tracing::error!(%release_error, record_count = failed.len(), "failed to release snapshot backlog batch entries");
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(acknowledgements)
    }

    pub async fn process_cache_repair_once(
        &self,
        worker_id: &str,
        policy: RetryWorkerPolicy,
    ) -> Result<DurableQueueProcessOutcome, BackendError> {
        let policy = policy.validate()?;
        // 每轮先批量清理成功提示，再处理一条持久修复，避免提示持续到达饿死恢复。
        // Flush one bounded hint batch, then service a durable repair even under continuous writes.
        self.cache_acknowledgements
            .flush(&self.cache_repairs, &self.metrics)
            .await?;
        let Some(lease) = self.cache_repairs.claim(worker_id, policy.lease_ms).await? else {
            return Ok(DurableQueueProcessOutcome::Empty);
        };
        match self.shard(&lease.record).repair_cache(&lease.record).await {
            Ok(_) => {
                if self.cache_repairs.acknowledge(&lease).await? {
                    Ok(DurableQueueProcessOutcome::Committed)
                } else {
                    Ok(DurableQueueProcessOutcome::LeaseLost)
                }
            }
            Err(error) => {
                let dead_lettered =
                    lease.attempt_count.saturating_add(1) >= u64::from(policy.max_attempts);
                let retry_delay = policy.retry_delay_ms(lease.attempt_count);
                if !self
                    .cache_repairs
                    .fail(&lease, &error.to_string(), retry_delay, policy.max_attempts)
                    .await?
                {
                    return Ok(DurableQueueProcessOutcome::LeaseLost);
                }
                if dead_lettered {
                    Ok(DurableQueueProcessOutcome::DeadLettered)
                } else {
                    Ok(DurableQueueProcessOutcome::RetryScheduled)
                }
            }
        }
    }

    pub async fn process_outbox_once(
        &self,
        worker_id: &str,
        policy: RetryWorkerPolicy,
    ) -> Result<DurableQueueProcessOutcome, BackendError> {
        let policy = policy.validate()?;
        let Some(lease) = self.outbox.claim(worker_id, policy.lease_ms).await? else {
            return Ok(DurableQueueProcessOutcome::Empty);
        };
        let publisher = self
            .outbox_publishers
            .get(&lease.publisher_id)
            .ok_or(BackendError::InvalidConfig("outbox publisher is missing"))?;
        let message = tiangz_dbproxy_storage::PublishMessage {
            event: &lease.event,
            destination: &lease.destination,
            operation_id: &lease.operation_id,
            trade_id: &lease.trade_id,
        };
        let started = Instant::now();
        let published = timeout(self.outbox_publish_timeout, publisher.publish(message)).await;
        let status = match &published {
            Ok(Ok(_)) => "success",
            Ok(Err(_)) => "error",
            Err(_) => "timeout",
        };
        self.outbox_relay_metrics.record(
            &lease.producer,
            &lease.publisher_id,
            status,
            started.elapsed().as_secs_f64(),
        );
        let publication =
            published.unwrap_or(Err(tiangz_dbproxy_storage::PublishError::Transient(
                "publication deadline exceeded; result may be unknown",
            )));
        match publication {
            Ok(_) => {
                if self.outbox.acknowledge(&lease).await? {
                    Ok(DurableQueueProcessOutcome::Committed)
                } else {
                    self.outbox_relay_metrics.record(
                        &lease.producer,
                        &lease.publisher_id,
                        "lease_lost",
                        0.0,
                    );
                    Ok(DurableQueueProcessOutcome::LeaseLost)
                }
            }
            Err(error) => {
                let max_attempts =
                    if matches!(error, tiangz_dbproxy_storage::PublishError::Permanent(_)) {
                        1
                    } else {
                        policy.max_attempts
                    };
                let dead_lettered =
                    lease.attempt_count.saturating_add(1) >= u64::from(max_attempts);
                let retry_delay =
                    policy.outbox_retry_delay_ms(&lease.event.event_id, lease.attempt_count);
                if !self
                    .outbox
                    .fail(&lease, &error.to_string(), retry_delay, max_attempts)
                    .await?
                {
                    self.outbox_relay_metrics.record(
                        &lease.producer,
                        &lease.publisher_id,
                        "lease_lost",
                        0.0,
                    );
                    return Ok(DurableQueueProcessOutcome::LeaseLost);
                }
                self.outbox_relay_metrics.record(
                    &lease.producer,
                    &lease.publisher_id,
                    if dead_lettered { "dead" } else { "retry" },
                    0.0,
                );
                if dead_lettered {
                    Ok(DurableQueueProcessOutcome::DeadLettered)
                } else {
                    Ok(DurableQueueProcessOutcome::RetryScheduled)
                }
            }
        }
    }
}

#[async_trait]
impl DbProxyBackend for StorageBackend {
    async fn load(&self, record: &RecordKey) -> Result<Option<SnapshotEnvelope>, BackendError> {
        Ok(self.shard(record).load(record).await?)
    }

    async fn load_multi(
        &self,
        records: &[RecordKey],
    ) -> Result<Vec<Option<SnapshotEnvelope>>, BackendError> {
        let mut groups = vec![Vec::new(); self.shards.len()];
        for (request_index, record) in records.iter().enumerate() {
            groups[self.shard_index(record)].push((request_index, record.clone()));
        }

        let mut workers = JoinSet::new();
        for (shard_index, group) in groups.into_iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            let shard = self.shards[shard_index].clone();
            workers.spawn(async move {
                let records = group
                    .iter()
                    .map(|(_, record)| record.clone())
                    .collect::<Vec<_>>();
                let snapshots = shard.load_multi(&records).await?;
                let loaded = group
                    .into_iter()
                    .zip(snapshots)
                    .map(|((request_index, _), snapshot)| (request_index, snapshot))
                    .collect::<Vec<_>>();
                Ok::<_, StorageError>(loaded)
            });
        }

        let mut snapshots = vec![None; records.len()];
        while let Some(result) = workers.join_next().await {
            let loaded = result.map_err(|error| BackendError::Worker(error.to_string()))??;
            for (request_index, snapshot) in loaded {
                snapshots[request_index] = snapshot;
            }
        }
        Ok(snapshots)
    }

    async fn save(&self, request: SnapshotWrite) -> Result<SnapshotWriteOutcome, BackendError> {
        let mut store = self.shard(&request.record);
        Ok(store.save(request).await?)
    }

    async fn save_multi(
        &self,
        requests: Vec<SnapshotWrite>,
    ) -> Result<Vec<Result<SnapshotWriteOutcome, BackendError>>, BackendError> {
        let request_count = requests.len();
        let mut groups = (0..self.shards.len())
            .map(|_| Vec::new())
            .collect::<Vec<_>>();
        for (request_index, request) in requests.into_iter().enumerate() {
            groups[self.shard_index(&request.record)].push((request_index, request));
        }

        let mut workers = JoinSet::new();
        for (shard_index, group) in groups.into_iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            let mut shard = self.shards[shard_index].clone();
            workers.spawn(async move {
                let request_indexes = group
                    .iter()
                    .map(|(request_index, _)| *request_index)
                    .collect::<Vec<_>>();
                let requests = group
                    .into_iter()
                    .map(|(_, request)| request)
                    .collect::<Vec<_>>();
                let outcomes = shard
                    .save_batch(requests)
                    .await
                    .map_err(BackendError::from)?;
                Ok::<_, BackendError>(
                    request_indexes
                        .into_iter()
                        .zip(
                            outcomes
                                .into_iter()
                                .map(|outcome| outcome.map_err(BackendError::from)),
                        )
                        .collect::<Vec<_>>(),
                )
            });
        }

        let mut outcomes = std::iter::repeat_with(|| None)
            .take(request_count)
            .collect::<Vec<_>>();
        while let Some(result) = workers.join_next().await {
            for (request_index, outcome) in
                result.map_err(|error| BackendError::Worker(error.to_string()))??
            {
                outcomes[request_index] = Some(outcome);
            }
        }
        outcomes
            .into_iter()
            .map(|outcome| {
                outcome
                    .ok_or_else(|| BackendError::Worker("batch save result is missing".to_string()))
            })
            .collect()
    }

    async fn enqueue_snapshot(&self, request: SnapshotWrite) -> Result<(), BackendError> {
        Ok(self.backlog.enqueue(request).await?)
    }

    async fn enqueue_multi_snapshot(
        &self,
        requests: Vec<SnapshotWrite>,
    ) -> Result<Vec<Result<(), BackendError>>, BackendError> {
        self.backlog.enqueue_multi(&requests).await?;
        Ok(requests.into_iter().map(|_| Ok(())).collect())
    }

    async fn apply_transaction(
        &self,
        request: TransactionalWrite,
    ) -> Result<TransactionalWriteOutcome, BackendError> {
        let mut store = self.shard(&request.record);
        Ok(store.apply(request).await?)
    }

    async fn load_transaction(
        &self,
        operation_id: &str,
        record: &RecordKey,
    ) -> Result<Option<TransactionReceipt>, BackendError> {
        Ok(self
            .shard(record)
            .load_receipt(operation_id, record)
            .await?)
    }

    async fn apply_multi_transaction(
        &self,
        request: MultiRecordTransactionalWrite,
    ) -> Result<MultiRecordTransactionalWriteOutcome, BackendError> {
        let mut store = self.shard_for_operation(&request.operation_id);
        Ok(store.apply_multi(request).await?)
    }

    async fn commit_records(
        &self,
        request: MultiRecordTransactionalWrite,
        effects: CommitEffects,
    ) -> Result<MultiRecordTransactionalWriteOutcome, BackendError> {
        let mut store = self.shard_for_operation(&request.operation_id);
        Ok(store.commit_records(request, effects).await?)
    }

    async fn load_multi_transaction(
        &self,
        operation_id: &str,
        records: &[RecordKey],
    ) -> Result<Option<tiangz_dbproxy_core::MultiRecordTransactionReceipt>, BackendError> {
        Ok(self
            .shard_for_operation(operation_id)
            .load_multi_receipt(operation_id, records)
            .await?)
    }

    async fn load_trade(&self, trade_id: &str) -> Result<Option<TradeEnvelope>, BackendError> {
        Ok(self
            .shard_for_operation(trade_id)
            .load_trade(trade_id)
            .await?)
    }

    async fn apply_trade_transaction(
        &self,
        request: TradeTransaction,
    ) -> Result<TradeTransactionOutcome, BackendError> {
        let mut store = self.shard_for_operation(&request.operation_id);
        Ok(store.apply_trade(request).await?)
    }

    async fn load_trade_transaction(
        &self,
        operation_id: &str,
        trade_id: &str,
    ) -> Result<Option<TradeReceipt>, BackendError> {
        Ok(self
            .shard_for_operation(operation_id)
            .load_trade_receipt(operation_id, trade_id)
            .await?)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BacklogProcessOutcome {
    Empty,
    Committed(SnapshotBacklogAck),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryWorkerPolicy {
    pub lease_ms: u64,
    pub base_retry_delay_ms: u64,
    pub max_retry_delay_ms: u64,
    pub max_attempts: u32,
}

impl RetryWorkerPolicy {
    /// 对同一事件/次数稳定的半幅抖动，避免整批事件同步重试。
    /// Stable per-event jitter prevents synchronized retry waves.
    fn outbox_retry_delay_ms(self, event_id: &str, attempt: u64) -> u64 {
        use sha2::{Digest, Sha256};
        let cap = self.retry_delay_ms(attempt);
        let floor = cap.div_ceil(2);
        let digest = Sha256::digest(format!("{event_id}/{attempt}").as_bytes());
        let entropy = u64::from_le_bytes(digest[..8].try_into().expect("eight digest bytes"));
        floor + entropy % (cap - floor + 1)
    }
    fn validate(self) -> Result<Self, BackendError> {
        if self.lease_ms == 0
            || self.base_retry_delay_ms == 0
            || self.max_retry_delay_ms < self.base_retry_delay_ms
            || self.max_attempts == 0
        {
            return Err(BackendError::InvalidConfig(
                "durable retry worker policy is invalid",
            ));
        }
        Ok(self)
    }

    fn retry_delay_ms(self, previous_attempts: u64) -> u64 {
        let exponent = u32::try_from(previous_attempts.min(20)).unwrap_or(20);
        self.base_retry_delay_ms
            .saturating_mul(1_u64 << exponent)
            .min(self.max_retry_delay_ms)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableQueueProcessOutcome {
    Empty,
    Committed,
    RetryScheduled,
    DeadLettered,
    LeaseLost,
}

#[derive(Clone, Copy)]
enum DurableWorkerKind {
    CacheRepair,
    Outbox,
}

/// 持续消费普通快照积压。停机只停止领取新项；已领取项要么完成 ACK，要么由 lease 回收。
/// Continuously consume ordinary snapshots. Shutdown stops new claims; leases recover interrupted work.
pub async fn run_backlog_worker(
    backend: Arc<StorageBackend>,
    lease_ms: u64,
    idle_delay: Duration,
    failure_delay: Duration,
    shutdown: watch::Receiver<bool>,
) {
    run_backlog_worker_observed(backend, lease_ms, idle_delay, failure_delay, shutdown, None).await;
}

/// 运行带Prometheus观测的积压消费者；指标不得影响Lease或ACK语义。
/// Run an observed backlog consumer without changing lease or ACK semantics.
pub async fn run_backlog_worker_observed(
    backend: Arc<StorageBackend>,
    lease_ms: u64,
    idle_delay: Duration,
    failure_delay: Duration,
    mut shutdown: watch::Receiver<bool>,
    metrics: Option<Arc<DbProxyMetrics>>,
) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        let started_at = Instant::now();
        match backend
            .process_backlog_batch(lease_ms, BACKLOG_FLUSH_BATCH_SIZE)
            .await
        {
            Ok(acknowledgements) if !acknowledgements.is_empty() => {
                if let Some(metrics) = &metrics {
                    metrics.backlog_finished(BacklogMetricResult::Committed, started_at.elapsed());
                }
                continue;
            }
            Ok(_) => {
                if let Some(metrics) = &metrics {
                    metrics.backlog_finished(BacklogMetricResult::Empty, started_at.elapsed());
                }
                tokio::select! {
                    _ = sleep(idle_delay) => {}
                    _ = shutdown.changed() => return,
                }
            }
            Err(error) => {
                if let Some(metrics) = &metrics {
                    metrics.backlog_finished(BacklogMetricResult::Failure, started_at.elapsed());
                }
                tracing::error!(%error, "snapshot backlog flush failed");
                tokio::select! {
                    _ = sleep(failure_delay) => {}
                    _ = shutdown.changed() => return,
                }
            }
        }
    }
}

pub async fn run_cache_repair_worker_observed(
    backend: Arc<StorageBackend>,
    worker_id: String,
    policy: RetryWorkerPolicy,
    idle_delay: Duration,
    shutdown: watch::Receiver<bool>,
    metrics: Option<Arc<DbProxyMetrics>>,
) {
    run_durable_queue_worker(
        backend,
        DurableWorkerKind::CacheRepair,
        worker_id,
        policy,
        idle_delay,
        shutdown,
        metrics,
    )
    .await;
}

pub async fn run_outbox_worker_observed(
    backend: Arc<StorageBackend>,
    worker_id: String,
    policy: RetryWorkerPolicy,
    idle_delay: Duration,
    shutdown: watch::Receiver<bool>,
    metrics: Option<Arc<DbProxyMetrics>>,
) {
    run_durable_queue_worker(
        backend,
        DurableWorkerKind::Outbox,
        worker_id,
        policy,
        idle_delay,
        shutdown,
        metrics,
    )
    .await;
}

async fn run_durable_queue_worker(
    backend: Arc<StorageBackend>,
    kind: DurableWorkerKind,
    worker_id: String,
    policy: RetryWorkerPolicy,
    idle_delay: Duration,
    mut shutdown: watch::Receiver<bool>,
    metrics: Option<Arc<DbProxyMetrics>>,
) {
    let metric_kind = match kind {
        DurableWorkerKind::CacheRepair => DurableQueueMetricKind::CacheRepair,
        DurableWorkerKind::Outbox => DurableQueueMetricKind::Outbox,
    };
    let queue_name = match kind {
        DurableWorkerKind::CacheRepair => "cache repair",
        DurableWorkerKind::Outbox => "outbox",
    };
    loop {
        if *shutdown.borrow() {
            return;
        }
        let outcome = match kind {
            DurableWorkerKind::CacheRepair => {
                backend.process_cache_repair_once(&worker_id, policy).await
            }
            DurableWorkerKind::Outbox => backend.process_outbox_once(&worker_id, policy).await,
        };
        let (metric_result, should_idle) = match outcome {
            Ok(DurableQueueProcessOutcome::Committed) => {
                (DurableQueueMetricResult::Committed, false)
            }
            Ok(DurableQueueProcessOutcome::RetryScheduled) => {
                (DurableQueueMetricResult::RetryScheduled, false)
            }
            Ok(DurableQueueProcessOutcome::DeadLettered) => {
                tracing::error!(worker = %worker_id, queue = queue_name, "durable queue item moved to dead letter");
                (DurableQueueMetricResult::DeadLettered, false)
            }
            Ok(DurableQueueProcessOutcome::LeaseLost) => {
                (DurableQueueMetricResult::LeaseLost, false)
            }
            Ok(DurableQueueProcessOutcome::Empty) => (DurableQueueMetricResult::Empty, true),
            Err(error) => {
                tracing::error!(%error, worker = %worker_id, queue = queue_name, "durable queue worker failed");
                (DurableQueueMetricResult::Failure, true)
            }
        };
        if let Some(metrics) = &metrics {
            metrics.durable_queue_finished(metric_kind, metric_result);
        }
        if should_idle {
            let delay = if matches!(metric_result, DurableQueueMetricResult::Failure) {
                Duration::from_millis(policy.base_retry_delay_ms)
            } else {
                idle_delay
            };
            tokio::select! {
                _ = sleep(delay) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }
}

/// Periodically refresh storage counters and durable backlog gauges for the Prometheus endpoint.
///
/// Storage counters are maintained in the storage layer so every connection shard contributes to
/// one aggregate. Backlog depth is sampled instead of attached to each request, which keeps the
/// request path free of extra Redis round trips.
pub async fn run_storage_metrics_poller(
    backend: Arc<StorageBackend>,
    metrics: Arc<DbProxyMetrics>,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        metrics.storage_metrics_updated(backend.metrics().snapshot());
        metrics.storage_latencies_updated(backend.metrics().latency_snapshot());
        match backend.backlog_stats().await {
            Ok(stats) => {
                metrics.redis_dependency_updated(true);
                metrics.backlog_depth_updated(
                    stats.pending,
                    stats.processing,
                    stats.oldest_pending_age_ms,
                );
            }
            Err(error) => {
                metrics.redis_dependency_updated(false);
                tracing::warn!(%error, "failed to sample snapshot backlog metrics");
            }
        }
        let postgres_healthy = match backend.cache_repair_stats().await {
            Ok(stats) => {
                metrics.cache_repair_depth_updated(
                    stats.pending,
                    stats.processing,
                    stats.dead_lettered,
                    stats.oldest_age_ms,
                );
                match backend.outbox_stats().await {
                    Ok(stats) => {
                        metrics.outbox_depth_updated(
                            stats.pending,
                            stats.processing,
                            stats.dead_lettered,
                            stats.oldest_age_ms,
                        );
                        true
                    }
                    Err(error) => {
                        tracing::warn!(%error, "failed to sample outbox metrics");
                        false
                    }
                }
            }
            Err(error) => {
                // Both queues share one maintenance connection. Avoid a second reconnect attempt
                // in the same poll when PostgreSQL is already known to be unavailable.
                tracing::warn!(%error, "failed to sample cache repair metrics");
                false
            }
        };
        metrics.postgres_dependency_updated(postgres_healthy);
        if postgres_healthy {
            match backend.outbox.source_stats().await {
                Ok(stats) => backend.outbox_relay_metrics.depths(stats),
                Err(error) => tracing::warn!(%error,"failed to sample outbox source metrics"),
            }
        }
        tokio::select! {
            _ = sleep(interval) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

/// TCP 服务配置。认证令牌必须通过部署密钥注入，禁止使用仓库中的本地示例密码。
/// TCP server settings. Inject the auth token as a deployment secret, never from sample credentials.
#[derive(Clone)]
pub struct ServerConfig {
    pub listen_addr: SocketAddr,
    pub auth_token: String,
    pub max_frame_bytes: usize,
    pub max_payload_bytes: usize,
    pub handshake_timeout: Duration,
    pub shutdown_grace: Duration,
    pub metrics: Arc<DbProxyMetrics>,
}

impl fmt::Debug for ServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerConfig")
            .field("listen_addr", &self.listen_addr)
            .field("auth_token", &"[REDACTED]")
            .field("max_frame_bytes", &self.max_frame_bytes)
            .field("max_payload_bytes", &self.max_payload_bytes)
            .field("handshake_timeout", &self.handshake_timeout)
            .field("shutdown_grace", &self.shutdown_grace)
            .field("metrics", &"[PROMETHEUS]")
            .finish()
    }
}

impl ServerConfig {
    pub fn new(listen_addr: SocketAddr, auth_token: impl Into<String>) -> Self {
        Self {
            listen_addr,
            auth_token: auth_token.into(),
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
            handshake_timeout: Duration::from_secs(5),
            shutdown_grace: Duration::from_secs(5),
            metrics: Arc::new(DbProxyMetrics::default()),
        }
    }

    fn validate(&self) -> Result<(), ServerError> {
        if !(16..=MAX_AUTH_TOKEN_BYTES).contains(&self.auth_token.len()) {
            return Err(ServerError::InvalidConfig(
                "auth token length is outside 16..=512 bytes",
            ));
        }
        if self.max_frame_bytes == 0 {
            return Err(ServerError::InvalidConfig("max frame bytes is zero"));
        }
        if self.max_payload_bytes == 0 {
            return Err(ServerError::InvalidConfig("max payload bytes is zero"));
        }
        if self.max_payload_bytes > self.max_frame_bytes {
            return Err(ServerError::InvalidConfig(
                "max payload bytes exceeds max frame bytes",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("invalid server configuration: {0}")]
    InvalidConfig(&'static str),
    #[error(transparent)]
    Io(#[from] io::Error),
}

pub struct DbProxyServer {
    listener: TcpListener,
    config: Arc<ServerConfig>,
    backend: Arc<dyn DbProxyBackend>,
}

impl DbProxyServer {
    pub async fn bind(
        config: ServerConfig,
        backend: Arc<dyn DbProxyBackend>,
    ) -> Result<Self, ServerError> {
        config.validate()?;
        let listener = TcpListener::bind(config.listen_addr).await?;
        Ok(Self {
            listener,
            config: Arc::new(config),
            backend,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, ServerError> {
        Ok(self.listener.local_addr()?)
    }

    /// 接收连接直到收到 shutdown；随后通知连接任务并在有限窗口内等待退出。
    /// Accept until shutdown, then signal connection tasks and wait within a bounded grace period.
    pub async fn serve(self, mut shutdown: watch::Receiver<bool>) -> Result<(), ServerError> {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                accepted = self.listener.accept() => {
                    let (stream, peer) = accepted?;
                    self.config.metrics.connection_opened();
                    let config = Arc::clone(&self.config);
                    let backend = Arc::clone(&self.backend);
                    let connection_shutdown = shutdown.clone();
                    connections.spawn(async move {
                        let result = handle_connection(
                            stream,
                            Arc::clone(&config),
                            backend,
                            connection_shutdown,
                        ).await;
                        config.metrics.connection_closed();
                        if let Err(error) = result {
                            config.metrics.connection_failed();
                            tracing::warn!(%peer, %error, "DBProxy connection closed with an error");
                        }
                    });
                }
            }
            while let Some(joined) = connections.try_join_next() {
                if let Err(error) = joined {
                    tracing::error!(%error, "DBProxy connection task panicked");
                }
            }
        }

        let grace = self.config.shutdown_grace;
        if timeout(grace, async {
            while let Some(joined) = connections.join_next().await {
                if let Err(error) = joined {
                    tracing::error!(%error, "DBProxy connection task panicked during shutdown");
                }
            }
        })
        .await
        .is_err()
        {
            connections.abort_all();
            tracing::warn!(?grace, "DBProxy connection shutdown grace expired");
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
enum ConnectionError {
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error("connection closed before the handshake")]
    ClosedBeforeHandshake,
    #[error("handshake timed out")]
    HandshakeTimeout,
    #[error("first frame was not a handshake")]
    MissingHandshake,
}

async fn handle_connection(
    mut stream: TcpStream,
    config: Arc<ServerConfig>,
    backend: Arc<dyn DbProxyBackend>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ConnectionError> {
    stream.set_nodelay(true).map_err(ProtocolError::from)?;
    let first = timeout(
        config.handshake_timeout,
        read_message::<_, wire::ClientFrame>(&mut stream, config.max_frame_bytes),
    )
    .await
    .map_err(|_| ConnectionError::HandshakeTimeout)??
    .ok_or(ConnectionError::ClosedBeforeHandshake)?;
    let wire::client_frame::Body::Hello(hello) =
        first.body.ok_or(ConnectionError::MissingHandshake)?
    else {
        return Err(ConnectionError::MissingHandshake);
    };

    if hello.protocol_version != PROTOCOL_VERSION
        || !is_compatible_protocol_fingerprint(&hello.protocol_fingerprint)
    {
        config
            .metrics
            .handshake_rejected(HandshakeRejection::ProtocolMismatch);
        write_hello_rejection(
            &mut stream,
            config.max_frame_bytes,
            wire::ErrorCode::ProtocolMismatch,
            "DBProxy protocol version or fingerprint does not match",
        )
        .await?;
        return Ok(());
    }
    if hello.auth_token.len() > MAX_AUTH_TOKEN_BYTES {
        config
            .metrics
            .handshake_rejected(HandshakeRejection::Unauthorized);
        write_hello_rejection(
            &mut stream,
            config.max_frame_bytes,
            wire::ErrorCode::Unauthorized,
            "DBProxy authentication failed",
        )
        .await?;
        return Ok(());
    }
    if !constant_time_token_eq(config.auth_token.as_bytes(), hello.auth_token.as_bytes()) {
        config
            .metrics
            .handshake_rejected(HandshakeRejection::Unauthorized);
        write_hello_rejection(
            &mut stream,
            config.max_frame_bytes,
            wire::ErrorCode::Unauthorized,
            "DBProxy authentication failed",
        )
        .await?;
        return Ok(());
    }
    if hello.client_name.trim().is_empty() || hello.client_name.len() > MAX_CLIENT_NAME_BYTES {
        config
            .metrics
            .handshake_rejected(HandshakeRejection::InvalidClient);
        write_hello_rejection(
            &mut stream,
            config.max_frame_bytes,
            wire::ErrorCode::InvalidRequest,
            "DBProxy client name is empty or too long",
        )
        .await?;
        return Ok(());
    }

    let accepted = wire::ServerFrame {
        body: Some(wire::server_frame::Body::Hello(wire::ServerHello {
            supports_outbox_relay: true,
            protocol_version: PROTOCOL_VERSION,
            protocol_fingerprint: hello.protocol_fingerprint.clone(),
            accepted: true,
            error: None,
        })),
    };
    write_message(&mut stream, &accepted, config.max_frame_bytes).await?;
    tracing::debug!(client_name = %hello.client_name, "DBProxy client authenticated");

    loop {
        let frame = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
                continue;
            }
            frame = read_message::<_, wire::ClientFrame>(&mut stream, config.max_frame_bytes) => frame?,
        };
        let Some(frame) = frame else {
            return Ok(());
        };
        let Some(wire::client_frame::Body::Request(request)) = frame.body else {
            return Err(ConnectionError::MissingHandshake);
        };
        let response = dispatch(
            request,
            backend.as_ref(),
            &config.metrics,
            config.max_payload_bytes,
        )
        .await;
        write_message(&mut stream, &response, config.max_frame_bytes).await?;
    }
}

async fn write_hello_rejection(
    stream: &mut TcpStream,
    maximum: usize,
    code: wire::ErrorCode,
    message: &str,
) -> Result<(), ProtocolError> {
    write_message(
        stream,
        &wire::ServerFrame {
            body: Some(wire::server_frame::Body::Hello(wire::ServerHello {
                supports_outbox_relay: false,
                protocol_version: PROTOCOL_VERSION,
                protocol_fingerprint: PROTOCOL_FINGERPRINT.to_string(),
                accepted: false,
                error: Some(wire_error(code, message, None)),
            })),
        },
        maximum,
    )
    .await
}

async fn dispatch(
    request: wire::RequestEnvelope,
    backend: &dyn DbProxyBackend,
    metrics: &DbProxyMetrics,
    max_payload_bytes: usize,
) -> wire::ServerFrame {
    let rpc_id = request.rpc_id;
    let operation = RpcOperation::from_body(request.body.as_ref());
    let record_count = RpcOperation::record_count(request.body.as_ref());
    let payload_bytes = RpcOperation::payload_bytes(request.body.as_ref());
    let started_at = Instant::now();
    metrics.request_started();
    let result = dispatch_body(request.body, backend, max_payload_bytes).await;
    let error_code = result.as_ref().err().map(|failure| failure.code);
    metrics.request_finished(
        operation,
        record_count,
        payload_bytes,
        started_at.elapsed(),
        error_code,
    );
    tracing::debug!(
        rpc_id,
        operation = operation.name(),
        record_count,
        result = if error_code.is_some() { "error" } else { "ok" },
        duration_ms = started_at.elapsed().as_secs_f64() * 1000.0,
        "DBProxy RPC completed"
    );
    let response = match result {
        Ok(body) => wire::ResponseEnvelope {
            rpc_id,
            error: None,
            body: Some(body),
        },
        Err(failure) => wire::ResponseEnvelope {
            rpc_id,
            error: Some(wire_error(
                failure.code,
                &failure.public_message,
                failure.actual_revision,
            )),
            body: None,
        },
    };
    wire::ServerFrame {
        body: Some(wire::server_frame::Body::Response(response)),
    }
}

async fn dispatch_body(
    body: Option<wire::request_envelope::Body>,
    backend: &dyn DbProxyBackend,
    max_payload_bytes: usize,
) -> Result<wire::response_envelope::Body, RpcFailure> {
    match body.ok_or_else(|| RpcFailure::invalid("request body is missing"))? {
        wire::request_envelope::Body::LoadSnapshot(request) => {
            let record = request
                .record
                .ok_or_else(|| RpcFailure::invalid("load_snapshot.record is missing"))?
                .try_into()
                .map_err(RpcFailure::from_protocol)?;
            let snapshot = backend
                .load(&record)
                .await
                .map_err(RpcFailure::from_backend)?;
            Ok(wire::response_envelope::Body::LoadSnapshot(
                wire::LoadSnapshotResponse {
                    snapshot: snapshot.as_ref().map(Into::into),
                },
            ))
        }
        wire::request_envelope::Body::LoadMultiSnapshot(request) => {
            if request.records.is_empty() {
                return Err(RpcFailure::invalid("load_multi_snapshot.records is empty"));
            }
            if request.records.len() > tiangz_dbproxy_protocol::MAX_BATCH_LOAD_RECORDS {
                return Err(RpcFailure::invalid(
                    "load_multi_snapshot.records exceeds the record limit",
                ));
            }
            let records = request
                .records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<RecordKey>, _>>()
                .map_err(RpcFailure::from_protocol)?;
            if records.iter().collect::<HashSet<_>>().len() != records.len() {
                return Err(RpcFailure::invalid(
                    "load_multi_snapshot.records contains duplicates",
                ));
            }
            let snapshots = backend
                .load_multi(&records)
                .await
                .map_err(RpcFailure::from_backend)?;
            if snapshots.len() != records.len() {
                return Err(RpcFailure::internal(
                    "backend returned a mismatched batch load result",
                ));
            }
            Ok(wire::response_envelope::Body::LoadMultiSnapshot(
                wire::LoadMultiSnapshotResponse {
                    entries: snapshots
                        .iter()
                        .map(|snapshot| wire::LoadMultiSnapshotEntry {
                            snapshot: snapshot.as_ref().map(Into::into),
                        })
                        .collect(),
                },
            ))
        }
        wire::request_envelope::Body::SaveSnapshot(request) => {
            validate_payload_size(
                "save_snapshot.payload",
                request.payload.len(),
                max_payload_bytes,
            )?;
            let request = request.try_into().map_err(RpcFailure::from_protocol)?;
            let outcome = backend
                .save(request)
                .await
                .map_err(RpcFailure::from_backend)?;
            let (disposition, revision) = snapshot_outcome(outcome);
            Ok(wire::response_envelope::Body::SaveSnapshot(
                wire::SaveSnapshotResponse {
                    disposition: disposition.into(),
                    revision: revision.0,
                },
            ))
        }
        wire::request_envelope::Body::SaveMultiSnapshot(request) => {
            if request.writes.is_empty() {
                return Err(RpcFailure::invalid("save_multi_snapshot.writes is empty"));
            }
            if request.writes.len() > tiangz_dbproxy_protocol::MAX_BATCH_SNAPSHOT_WRITES {
                return Err(RpcFailure::invalid(
                    "save_multi_snapshot.writes exceeds the record limit",
                ));
            }
            let writes = request
                .writes
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<SnapshotWrite>, _>>()
                .map_err(RpcFailure::from_protocol)?;
            validate_snapshot_write_batch(&writes)?;
            validate_snapshot_payloads(&writes, max_payload_bytes)?;
            let expected_count = writes.len();
            let outcomes = backend
                .save_multi(writes)
                .await
                .map_err(RpcFailure::from_backend)?;
            if outcomes.len() != expected_count {
                return Err(RpcFailure::internal(
                    "backend returned a mismatched batch save result",
                ));
            }
            let entries = outcomes
                .into_iter()
                .map(|outcome| match outcome {
                    Ok(outcome) => {
                        let (disposition, revision) = snapshot_outcome(outcome);
                        wire::SaveMultiSnapshotEntry {
                            result: Some(wire::SaveSnapshotResponse {
                                disposition: disposition.into(),
                                revision: revision.0,
                            }),
                            error: None,
                        }
                    }
                    Err(error) => wire::SaveMultiSnapshotEntry {
                        result: None,
                        error: Some(RpcFailure::from_backend(error).into_wire()),
                    },
                })
                .collect::<Vec<_>>();
            Ok(wire::response_envelope::Body::SaveMultiSnapshot(
                wire::SaveMultiSnapshotResponse { entries },
            ))
        }
        wire::request_envelope::Body::EnqueueSnapshot(request) => {
            let request: SnapshotWrite = request
                .write
                .ok_or_else(|| RpcFailure::invalid("enqueue_snapshot.write is missing"))?
                .try_into()
                .map_err(RpcFailure::from_protocol)?;
            validate_payload_size(
                "enqueue_snapshot.payload",
                request.payload.len(),
                max_payload_bytes,
            )?;
            backend
                .enqueue_snapshot(request)
                .await
                .map_err(RpcFailure::from_backend)?;
            Ok(wire::response_envelope::Body::EnqueueSnapshot(
                wire::EnqueueSnapshotResponse { accepted: true },
            ))
        }
        wire::request_envelope::Body::EnqueueMultiSnapshot(request) => {
            if request.writes.is_empty() {
                return Err(RpcFailure::invalid(
                    "enqueue_multi_snapshot.writes is empty",
                ));
            }
            if request.writes.len() > tiangz_dbproxy_protocol::MAX_BATCH_SNAPSHOT_WRITES {
                return Err(RpcFailure::invalid(
                    "enqueue_multi_snapshot.writes exceeds the record limit",
                ));
            }
            let writes = request
                .writes
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<SnapshotWrite>, _>>()
                .map_err(RpcFailure::from_protocol)?;
            validate_snapshot_write_batch(&writes)?;
            validate_snapshot_payloads(&writes, max_payload_bytes)?;
            let expected_count = writes.len();
            let outcomes = backend
                .enqueue_multi_snapshot(writes)
                .await
                .map_err(RpcFailure::from_backend)?;
            if outcomes.len() != expected_count {
                return Err(RpcFailure::internal(
                    "backend returned a mismatched batch enqueue result",
                ));
            }
            let entries = outcomes
                .into_iter()
                .map(|outcome| match outcome {
                    Ok(()) => wire::EnqueueMultiSnapshotEntry {
                        accepted: true,
                        error: None,
                    },
                    Err(error) => wire::EnqueueMultiSnapshotEntry {
                        accepted: false,
                        error: Some(RpcFailure::from_backend(error).into_wire()),
                    },
                })
                .collect::<Vec<_>>();
            Ok(wire::response_envelope::Body::EnqueueMultiSnapshot(
                wire::EnqueueMultiSnapshotResponse { entries },
            ))
        }
        wire::request_envelope::Body::ApplyTransaction(request) => {
            validate_payload_size(
                "apply_transaction.payload",
                request.payload.len(),
                max_payload_bytes,
            )?;
            validate_payload_size(
                "apply_transaction.result",
                request.result.len(),
                max_payload_bytes,
            )?;
            let request = request.try_into().map_err(RpcFailure::from_protocol)?;
            let outcome = backend
                .apply_transaction(request)
                .await
                .map_err(RpcFailure::from_backend)?;
            let (disposition, revision, result) = transaction_outcome(outcome);
            Ok(wire::response_envelope::Body::ApplyTransaction(
                wire::ApplyTransactionResponse {
                    disposition: disposition.into(),
                    new_revision: revision.0,
                    result,
                },
            ))
        }
        wire::request_envelope::Body::LoadTransaction(request) => {
            validate_text_field(
                "load_transaction.operation_id",
                &request.operation_id,
                tiangz_dbproxy_protocol::MAX_IDEMPOTENCY_KEY_BYTES,
            )?;
            let record = request
                .record
                .ok_or_else(|| RpcFailure::invalid("load_transaction.record is missing"))?
                .try_into()
                .map_err(RpcFailure::from_protocol)?;
            let receipt = backend
                .load_transaction(&request.operation_id, &record)
                .await
                .map_err(RpcFailure::from_backend)?;
            Ok(wire::response_envelope::Body::LoadTransaction(
                wire::LoadTransactionResponse {
                    receipt: receipt.map(transaction_receipt),
                },
            ))
        }
        wire::request_envelope::Body::CommitRecords(request) => {
            validate_text_field(
                "commit.operation_id",
                &request.operation_id,
                tiangz_dbproxy_protocol::MAX_IDEMPOTENCY_KEY_BYTES,
            )?;
            if request.writes.is_empty()
                || request.writes.len() > tiangz_dbproxy_protocol::MAX_TRANSACTION_RECORDS
                || request.appends.len() > tiangz_dbproxy_protocol::MAX_TRANSACTION_RECORDS
                || request.outbox_events.len() > tiangz_dbproxy_protocol::MAX_OUTBOX_EVENTS
            {
                return Err(RpcFailure::invalid("commit collection size exceeds limits"));
            }
            let writes = request
                .writes
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()
                .map_err(RpcFailure::from_protocol)?;
            validate_transaction_payloads(&writes, max_payload_bytes)?;
            validate_payload_size("commit.result", request.result.len(), max_payload_bytes)?;
            let appends = request
                .appends
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<tiangz_dbproxy_core::AppendRecord>, _>>()
                .map_err(RpcFailure::from_protocol)?;
            let events = request
                .outbox_events
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<tiangz_dbproxy_core::OutboxEvent>, _>>()
                .map_err(RpcFailure::from_protocol)?;
            for append in &appends {
                validate_payload_size("commit.append", append.payload.len(), max_payload_bytes)?;
            }
            for event in &events {
                validate_payload_size("commit.event", event.payload.len(), max_payload_bytes)?;
            }
            let outcome = backend
                .commit_records(
                    MultiRecordTransactionalWrite {
                        operation_id: request.operation_id,
                        writes,
                        result: request.result,
                    },
                    CommitEffects {
                        appends,
                        outbox_events: events,
                    },
                )
                .await
                .map_err(RpcFailure::from_backend)?;
            Ok(wire::response_envelope::Body::CommitRecords(
                multi_transaction_outcome(outcome),
            ))
        }
        wire::request_envelope::Body::ApplyMultiTransaction(request) => {
            validate_text_field(
                "apply_multi_transaction.operation_id",
                &request.operation_id,
                tiangz_dbproxy_protocol::MAX_IDEMPOTENCY_KEY_BYTES,
            )?;
            if request.writes.is_empty() {
                return Err(RpcFailure::invalid(
                    "apply_multi_transaction.writes is empty",
                ));
            }
            if request.writes.len() > tiangz_dbproxy_protocol::MAX_TRANSACTION_RECORDS {
                return Err(RpcFailure::invalid(
                    "apply_multi_transaction.writes exceeds the record limit",
                ));
            }
            let writes = request
                .writes
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()
                .map_err(RpcFailure::from_protocol)?;
            validate_transaction_payloads(&writes, max_payload_bytes)?;
            validate_payload_size(
                "apply_multi_transaction.result",
                request.result.len(),
                max_payload_bytes,
            )?;
            let outcome = backend
                .apply_multi_transaction(MultiRecordTransactionalWrite {
                    operation_id: request.operation_id,
                    writes,
                    result: request.result,
                })
                .await
                .map_err(RpcFailure::from_backend)?;
            Ok(wire::response_envelope::Body::ApplyMultiTransaction(
                multi_transaction_outcome(outcome),
            ))
        }
        wire::request_envelope::Body::LoadMultiTransaction(request) => {
            validate_text_field(
                "load_multi_transaction.operation_id",
                &request.operation_id,
                tiangz_dbproxy_protocol::MAX_IDEMPOTENCY_KEY_BYTES,
            )?;
            if request.records.is_empty() {
                return Err(RpcFailure::invalid(
                    "load_multi_transaction.records is empty",
                ));
            }
            if request.records.len() > tiangz_dbproxy_protocol::MAX_TRANSACTION_RECORDS {
                return Err(RpcFailure::invalid(
                    "load_multi_transaction.records exceeds the record limit",
                ));
            }
            let records = request
                .records
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()
                .map_err(RpcFailure::from_protocol)?;
            let receipt = backend
                .load_multi_transaction(&request.operation_id, &records)
                .await
                .map_err(RpcFailure::from_backend)?;
            Ok(wire::response_envelope::Body::LoadMultiTransaction(
                wire::LoadMultiTransactionResponse {
                    receipt: receipt.map(multi_transaction_receipt),
                },
            ))
        }
        wire::request_envelope::Body::ApplyTradeTransaction(request) => {
            if let Some(transition) = &request.transition {
                validate_payload_size(
                    "apply_trade_transaction.transition.payload",
                    transition.payload.len(),
                    max_payload_bytes,
                )?;
            }
            for write in &request.writes {
                validate_payload_size(
                    "apply_trade_transaction.write.payload",
                    write.payload.len(),
                    max_payload_bytes,
                )?;
            }
            for posting in &request.ledger_postings {
                validate_payload_size(
                    "apply_trade_transaction.ledger_posting.metadata",
                    posting.metadata.len(),
                    max_payload_bytes,
                )?;
            }
            for event in &request.outbox_events {
                validate_payload_size(
                    "apply_trade_transaction.outbox_event.payload",
                    event.payload.len(),
                    max_payload_bytes,
                )?;
            }
            validate_payload_size(
                "apply_trade_transaction.result",
                request.result.len(),
                max_payload_bytes,
            )?;
            let request: TradeTransaction =
                request.try_into().map_err(RpcFailure::from_protocol)?;
            let outcome = backend
                .apply_trade_transaction(request)
                .await
                .map_err(RpcFailure::from_backend)?;
            Ok(wire::response_envelope::Body::ApplyTradeTransaction(
                trade_transaction_outcome(outcome),
            ))
        }
        wire::request_envelope::Body::LoadTrade(request) => {
            validate_text_field(
                "load_trade.trade_id",
                &request.trade_id,
                tiangz_dbproxy_protocol::MAX_TRADE_ID_BYTES,
            )?;
            let trade = backend
                .load_trade(&request.trade_id)
                .await
                .map_err(RpcFailure::from_backend)?;
            Ok(wire::response_envelope::Body::LoadTrade(
                wire::LoadTradeResponse {
                    trade: trade.as_ref().map(Into::into),
                },
            ))
        }
        wire::request_envelope::Body::LoadTradeTransaction(request) => {
            validate_text_field(
                "load_trade_transaction.operation_id",
                &request.operation_id,
                tiangz_dbproxy_protocol::MAX_IDEMPOTENCY_KEY_BYTES,
            )?;
            validate_text_field(
                "load_trade_transaction.trade_id",
                &request.trade_id,
                tiangz_dbproxy_protocol::MAX_TRADE_ID_BYTES,
            )?;
            let receipt = backend
                .load_trade_transaction(&request.operation_id, &request.trade_id)
                .await
                .map_err(RpcFailure::from_backend)?;
            Ok(wire::response_envelope::Body::LoadTradeTransaction(
                wire::LoadTradeTransactionResponse {
                    receipt: receipt.as_ref().map(Into::into),
                },
            ))
        }
    }
}

fn trade_transaction_outcome(
    outcome: TradeTransactionOutcome,
) -> wire::ApplyTradeTransactionResponse {
    let (disposition, receipt) = match outcome {
        TradeTransactionOutcome::Applied(receipt) => (wire::WriteDisposition::Applied, receipt),
        TradeTransactionOutcome::Duplicate(receipt) => (wire::WriteDisposition::Duplicate, receipt),
    };
    wire::ApplyTradeTransactionResponse {
        disposition: disposition.into(),
        receipt: Some((&receipt).into()),
    }
}

fn multi_transaction_record_receipt(
    receipt: tiangz_dbproxy_core::TransactionRecordReceipt,
) -> wire::MultiTransactionRecordReceipt {
    wire::MultiTransactionRecordReceipt {
        record: Some((&receipt.record).into()),
        new_revision: receipt.new_revision.0,
    }
}

fn multi_transaction_receipt(
    receipt: tiangz_dbproxy_core::MultiRecordTransactionReceipt,
) -> wire::MultiTransactionReceipt {
    wire::MultiTransactionReceipt {
        operation_id: receipt.operation_id,
        records: receipt
            .records
            .into_iter()
            .map(multi_transaction_record_receipt)
            .collect(),
        result: receipt.result,
    }
}

fn multi_transaction_outcome(
    outcome: MultiRecordTransactionalWriteOutcome,
) -> wire::ApplyMultiTransactionResponse {
    let (disposition, records, result) = match outcome {
        MultiRecordTransactionalWriteOutcome::Applied { records, result } => {
            (wire::WriteDisposition::Applied, records, result)
        }
        MultiRecordTransactionalWriteOutcome::Duplicate { records, result } => {
            (wire::WriteDisposition::Duplicate, records, result)
        }
    };
    wire::ApplyMultiTransactionResponse {
        disposition: disposition.into(),
        records: records
            .into_iter()
            .map(multi_transaction_record_receipt)
            .collect(),
        result,
    }
}

fn transaction_receipt(receipt: TransactionReceipt) -> wire::TransactionReceipt {
    wire::TransactionReceipt {
        operation_id: receipt.operation_id,
        record: Some((&receipt.record).into()),
        new_revision: receipt.new_revision.0,
        result: receipt.result,
    }
}

fn snapshot_outcome(outcome: SnapshotWriteOutcome) -> (wire::WriteDisposition, Revision) {
    match outcome {
        SnapshotWriteOutcome::Applied { revision } => (wire::WriteDisposition::Applied, revision),
        SnapshotWriteOutcome::Duplicate { revision } => {
            (wire::WriteDisposition::Duplicate, revision)
        }
    }
}

fn validate_snapshot_write_batch(writes: &[SnapshotWrite]) -> Result<(), RpcFailure> {
    if writes
        .iter()
        .map(|write| &write.record)
        .collect::<HashSet<_>>()
        .len()
        != writes.len()
    {
        return Err(RpcFailure::invalid(
            "snapshot batch contains duplicate records",
        ));
    }
    if writes
        .iter()
        .map(|write| write.request_id.as_str())
        .collect::<HashSet<_>>()
        .len()
        != writes.len()
    {
        return Err(RpcFailure::invalid(
            "snapshot batch contains duplicate request ids",
        ));
    }
    Ok(())
}

fn validate_snapshot_payloads(
    writes: &[SnapshotWrite],
    max_payload_bytes: usize,
) -> Result<(), RpcFailure> {
    for write in writes {
        validate_payload_size("snapshot payload", write.payload.len(), max_payload_bytes)?;
    }
    Ok(())
}

fn validate_transaction_payloads(
    writes: &[TransactionalRecordWrite],
    max_payload_bytes: usize,
) -> Result<(), RpcFailure> {
    for write in writes {
        validate_payload_size(
            "transaction payload",
            write.payload.len(),
            max_payload_bytes,
        )?;
    }
    Ok(())
}

fn validate_payload_size(field: &str, length: usize, maximum: usize) -> Result<(), RpcFailure> {
    if length > maximum {
        return Err(RpcFailure::invalid(format!(
            "{field} exceeds maxPayloadBytes ({length} > {maximum})"
        )));
    }
    Ok(())
}

fn validate_text_field(field: &str, value: &str, maximum: usize) -> Result<(), RpcFailure> {
    if value.trim().is_empty() || value.len() > maximum {
        return Err(RpcFailure::invalid(format!(
            "{field} is empty or exceeds {maximum} bytes"
        )));
    }
    Ok(())
}

fn transaction_outcome(
    outcome: TransactionalWriteOutcome,
) -> (wire::WriteDisposition, Revision, Vec<u8>) {
    match outcome {
        TransactionalWriteOutcome::Applied {
            new_revision,
            result,
        } => (wire::WriteDisposition::Applied, new_revision, result),
        TransactionalWriteOutcome::Duplicate {
            new_revision,
            result,
        } => (wire::WriteDisposition::Duplicate, new_revision, result),
    }
}

struct RpcFailure {
    code: wire::ErrorCode,
    public_message: String,
    actual_revision: Option<u64>,
}

impl RpcFailure {
    fn into_wire(self) -> wire::RpcError {
        wire_error(self.code, &self.public_message, self.actual_revision)
    }

    fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: wire::ErrorCode::InvalidRequest,
            public_message: message.into(),
            actual_revision: None,
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            code: wire::ErrorCode::Internal,
            public_message: message.into(),
            actual_revision: None,
        }
    }

    fn from_protocol(error: ProtocolError) -> Self {
        match error {
            ProtocolError::Store(error) => Self::from_store(error),
            ProtocolError::MissingField(field) => Self::invalid(format!("missing field: {field}")),
            ProtocolError::InvalidField(field) => Self::invalid(format!("invalid field: {field}")),
            other => {
                tracing::error!(%other, "unexpected request conversion error");
                Self {
                    code: wire::ErrorCode::Internal,
                    public_message: "request conversion failed".to_string(),
                    actual_revision: None,
                }
            }
        }
    }

    fn from_backend(error: BackendError) -> Self {
        match error {
            BackendError::InvalidConfig(message) => {
                tracing::error!(%message, "invalid DBProxy backend configuration reached RPC dispatch");
                Self {
                    code: wire::ErrorCode::Internal,
                    public_message: "DBProxy backend is misconfigured".to_string(),
                    actual_revision: None,
                }
            }
            BackendError::Core(error) | BackendError::Storage(StorageError::Core(error)) => {
                Self::from_store(error)
            }
            BackendError::Storage(error) => {
                tracing::error!(%error, "DBProxy storage operation failed");
                Self {
                    code: wire::ErrorCode::StorageUnavailable,
                    public_message: "storage operation failed; retry with the same idempotency key"
                        .to_string(),
                    actual_revision: None,
                }
            }
            BackendError::Worker(error) => {
                tracing::error!(%error, "DBProxy backend worker failed");
                Self::internal("DBProxy backend worker failed")
            }
        }
    }

    fn from_store(error: StoreError) -> Self {
        match error {
            StoreError::InvalidKey(_)
            | StoreError::EmptyRequestId
            | StoreError::EmptyOperationId
            | StoreError::EmptyTransactionRecords
            | StoreError::EmptyTradeId
            | StoreError::EmptyTradeRecords
            | StoreError::DuplicateTransactionRecord { .. }
            | StoreError::InvalidTradeStateTransition { .. }
            | StoreError::InvalidLedgerPosting(_)
            | StoreError::DuplicateLedgerPosting { .. }
            | StoreError::UnbalancedLedger { .. }
            | StoreError::InvalidOutboxEvent(_)
            | StoreError::DuplicateOutboxEvent { .. }
            | StoreError::QueuedSnapshotRequiresUnconditionalWrite { .. } => {
                Self::invalid(error.to_string())
            }
            StoreError::IdempotencyConflict { .. } => Self {
                code: wire::ErrorCode::IdempotencyConflict,
                public_message: error.to_string(),
                actual_revision: None,
            },
            StoreError::OperationIdConflict { .. } | StoreError::AppendRecordConflict { .. } => {
                Self {
                    code: wire::ErrorCode::OperationConflict,
                    public_message: error.to_string(),
                    actual_revision: None,
                }
            }
            StoreError::RevisionConflict { actual, .. } => Self {
                code: wire::ErrorCode::RevisionConflict,
                public_message: error.to_string(),
                actual_revision: Some(actual.0),
            },
            StoreError::TradeVersionConflict { actual, .. } => Self {
                code: wire::ErrorCode::TradeConflict,
                public_message: error.to_string(),
                actual_revision: Some(actual.0),
            },
            StoreError::TradeStateConflict { .. } => Self {
                code: wire::ErrorCode::TradeConflict,
                public_message: error.to_string(),
                actual_revision: None,
            },
            StoreError::LedgerPostingConflict { .. } => Self {
                code: wire::ErrorCode::LedgerConflict,
                public_message: error.to_string(),
                actual_revision: None,
            },
            StoreError::OutboxEventConflict { .. } => Self {
                code: wire::ErrorCode::OutboxConflict,
                public_message: error.to_string(),
                actual_revision: None,
            },
            StoreError::RevisionExhausted { .. } | StoreError::TradeVersionExhausted { .. } => {
                tracing::error!(%error, "DBProxy revision exhausted");
                Self {
                    code: wire::ErrorCode::Internal,
                    public_message: "revision exhausted".to_string(),
                    actual_revision: None,
                }
            }
        }
    }
}

fn wire_error(
    code: wire::ErrorCode,
    message: &str,
    actual_revision: Option<u64>,
) -> wire::RpcError {
    wire::RpcError {
        code: code.into(),
        message: message.to_string(),
        actual_revision,
    }
}

/// 比较固定密钥时遍历两侧最大长度，避免在第一个不同字节提前返回。
/// Compare the full maximum length so mismatches do not return at the first differing byte.
fn constant_time_token_eq(expected: &[u8], actual: &[u8]) -> bool {
    let mut difference = expected.len() ^ actual.len();
    let maximum = expected.len().max(actual.len());
    for index in 0..maximum {
        let left = expected.get(index).copied().unwrap_or_default();
        let right = actual.get(index).copied().unwrap_or_default();
        difference |= usize::from(left ^ right);
    }
    difference == 0
}

struct StableHasher(u64);

impl Default for StableHasher {
    fn default() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }
}

impl Hasher for StableHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn postgres_admission_errors_are_retryable_without_claiming_transaction_outcome() {
        for error in [
            super::StorageError::PostgresConnectionWaitTimeout { timeout_ms: 500 },
            super::StorageError::PostgresReconnectCooldown {
                retry_after_ms: 500,
            },
        ] {
            let failure = super::RpcFailure::from_backend(super::BackendError::Storage(error));
            assert_eq!(failure.code, super::wire::ErrorCode::StorageUnavailable);
            assert!(failure.public_message.contains("same idempotency key"));
            assert_eq!(failure.actual_revision, None);
        }
    }
    use super::*;

    #[test]
    fn token_comparison_handles_equal_and_different_lengths() {
        assert!(constant_time_token_eq(
            b"abcdefghijklmnop",
            b"abcdefghijklmnop"
        ));
        assert!(!constant_time_token_eq(
            b"abcdefghijklmnop",
            b"abcdefghijklmnoq"
        ));
        assert!(!constant_time_token_eq(b"abcdefghijklmnop", b"abc"));
    }

    #[test]
    fn text_fields_are_bounded_before_backend_dispatch() {
        assert!(validate_text_field("operation_id", "valid", 5).is_ok());
        assert!(validate_text_field("operation_id", "", 5).is_err());
        assert!(validate_text_field("operation_id", "123456", 5).is_err());
    }

    #[test]
    fn stable_hasher_is_repeatable() {
        let record = RecordKey::new("player", "1001").unwrap();
        let mut first = StableHasher::default();
        let mut second = StableHasher::default();
        record.hash(&mut first);
        record.hash(&mut second);
        assert_eq!(first.finish(), second.finish());
    }

    #[test]
    fn server_config_debug_redacts_the_auth_token() {
        let token = "secret-server-token";
        let config = ServerConfig::new("127.0.0.1:7800".parse().unwrap(), token);
        let debug = format!("{config:?}");
        assert!(!debug.contains(token));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn server_rejects_short_or_oversized_tokens() {
        let mut config = ServerConfig::new("127.0.0.1:7800".parse().unwrap(), "short");
        assert!(matches!(
            config.validate(),
            Err(ServerError::InvalidConfig(_))
        ));
        config.auth_token = "x".repeat(MAX_AUTH_TOKEN_BYTES + 1);
        assert!(matches!(
            config.validate(),
            Err(ServerError::InvalidConfig(_))
        ));
    }

    #[test]
    fn server_rejects_invalid_payload_limits() {
        let mut config = ServerConfig::new("127.0.0.1:7800".parse().unwrap(), "0123456789abcdef");
        config.max_payload_bytes = 0;
        assert!(matches!(
            config.validate(),
            Err(ServerError::InvalidConfig("max payload bytes is zero"))
        ));

        config.max_payload_bytes = config.max_frame_bytes + 1;
        assert!(matches!(
            config.validate(),
            Err(ServerError::InvalidConfig(
                "max payload bytes exceeds max frame bytes"
            ))
        ));
    }

    #[test]
    fn durable_queue_retry_policy_is_bounded() {
        let policy = RetryWorkerPolicy {
            lease_ms: 30_000,
            base_retry_delay_ms: 1_000,
            max_retry_delay_ms: 60_000,
            max_attempts: 20,
        };
        assert!(policy.validate().is_ok());
        assert_eq!(policy.retry_delay_ms(0), 1_000);
        assert_eq!(policy.retry_delay_ms(1), 2_000);
        assert_eq!(policy.retry_delay_ms(6), 60_000);
        assert_eq!(policy.retry_delay_ms(u64::MAX), 60_000);
        for attempt in [0, 1, 6, u64::MAX] {
            let delays = (0..100)
                .map(|id| policy.outbox_retry_delay_ms(&format!("event-{id}"), attempt))
                .collect::<HashSet<_>>();
            assert!(delays.len() > 1);
            for delay in delays {
                assert!(
                    (policy.retry_delay_ms(attempt).div_ceil(2)..=policy.retry_delay_ms(attempt))
                        .contains(&delay)
                );
            }
        }

        let invalid = RetryWorkerPolicy {
            max_retry_delay_ms: 999,
            ..policy
        };
        assert!(invalid.validate().is_err());
    }

    #[tokio::test]
    async fn dispatch_rejects_oversized_snapshot_payload() {
        let backend = MemoryBackend::new(1).unwrap();
        let result = dispatch_body(
            Some(wire::request_envelope::Body::SaveSnapshot(
                wire::SaveSnapshotRequest {
                    request_id: "request-1".to_string(),
                    record: None,
                    schema: "player".to_string(),
                    schema_version: 1,
                    payload: vec![0; 4],
                    expected_revision: None,
                    updated_at_unix_ms: 1,
                },
            )),
            &backend,
            3,
        )
        .await;
        let error = result.unwrap_err();
        assert_eq!(error.code, wire::ErrorCode::InvalidRequest);
        assert!(error.public_message.contains("maxPayloadBytes"));
    }
}
