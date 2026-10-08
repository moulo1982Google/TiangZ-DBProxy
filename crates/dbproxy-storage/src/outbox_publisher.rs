//! Redis Streams publication with same-connection AOF confirmation.

#[cfg(test)]
#[path = "outbox_publisher_tests.rs"]
mod publisher_tests;

use crate::redis_durability::redis_result;
use crate::{OutboxLease, RedisDurabilityConfig, StorageError, StorageMetrics, latency::Stage};
use redis::aio::MultiplexedConnection;
use std::{sync::Arc, time::Duration};
use tiangz_dbproxy_core::OutboxEvent;
use tokio::sync::Mutex;
use tokio::time::{Instant, timeout_at};

#[derive(Clone)]
pub struct RedisOutboxPublisher {
    connection: Arc<Mutex<Option<MultiplexedConnection>>>,
    client: redis::Client,
    stream_prefix: Arc<str>,
    durability: RedisDurabilityConfig,
    publish_timeout: Duration,
    metrics: Arc<StorageMetrics>,
}

impl RedisOutboxPublisher {
    pub async fn connect(url: &str, stream_prefix: &str) -> Result<Self, StorageError> {
        Self::connect_with_config(
            url,
            stream_prefix,
            RedisDurabilityConfig::default(),
            Duration::from_secs(5),
            Arc::new(StorageMetrics::default()),
        )
        .await
    }

    pub async fn connect_with_config(
        url: &str,
        stream_prefix: &str,
        durability: RedisDurabilityConfig,
        publish_timeout: Duration,
        metrics: Arc<StorageMetrics>,
    ) -> Result<Self, StorageError> {
        durability.validate()?;
        if publish_timeout <= durability.response_timeout
            || publish_timeout > Duration::from_secs(60)
        {
            return Err(StorageError::InvalidRedisDurabilityBudget(
                "outbox requires Redis I/O < publish timeout <= 60000ms",
            ));
        }
        if stream_prefix.trim().is_empty() {
            return Err(StorageError::QueueProtocol(
                "outbox stream prefix is empty".to_string(),
            ));
        }
        let client = redis::Client::open(url)?;
        let connection = client
            .get_multiplexed_async_connection_with_config(&durability.connection_config())
            .await?;
        Ok(Self {
            connection: Arc::new(Mutex::new(Some(connection))),
            client,
            stream_prefix: Arc::from(stream_prefix),
            durability,
            publish_timeout,
            metrics,
        })
    }

    /// Publish at least once. Consumers must deduplicate by event_id.
    pub async fn publish(&self, lease: &OutboxLease) -> Result<String, StorageError> {
        let stream = if tiangz_dbproxy_core::EventEnvelope::from_outbox(&lease.event)?.is_some() {
            lease.destination.clone()
        } else {
            format!("{}{}", self.stream_prefix, lease.event.topic)
        };
        self.publish_to(&lease.event, &stream, &lease.operation_id, &lease.trade_id)
            .await
    }

    async fn publish_to(
        &self,
        event: &OutboxEvent,
        stream: &str,
        operation_id: &str,
        trade_id: &str,
    ) -> Result<String, StorageError> {
        let _timer = self.metrics.latency.start(Stage::OutboxTotal);
        let deadline = Instant::now() + self.publish_timeout;
        let result = match timeout_at(
            deadline,
            self.publish_before(event, stream, operation_id, trade_id, deadline),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(_) => Err(StorageError::RedisDurabilityDeadlineExceeded),
        };
        if matches!(result, Err(StorageError::RedisDurabilityDeadlineExceeded)) {
            self.metrics.latency.timed_out(Stage::OutboxTotal);
        }
        result
    }

    async fn publish_before(
        &self,
        event: &OutboxEvent,
        stream: &str,
        operation_id: &str,
        trade_id: &str,
        deadline: Instant,
    ) -> Result<String, StorageError> {
        let messages = [crate::PublishMessage {
            event,
            destination: stream,
            operation_id,
            trade_id,
        }];
        let mut ids = self.publish_many_before(&messages, deadline).await?;
        Ok(ids.remove(0))
    }

    async fn publish_many_to(
        &self,
        messages: &[crate::PublishMessage<'_>],
    ) -> Result<Vec<String>, StorageError> {
        let _timer = self.metrics.latency.start(Stage::OutboxTotal);
        let deadline = Instant::now() + self.publish_timeout;
        let result = match timeout_at(deadline, self.publish_many_before(messages, deadline)).await
        {
            Ok(result) => result,
            Err(_) => Err(StorageError::RedisDurabilityDeadlineExceeded),
        };
        if matches!(result, Err(StorageError::RedisDurabilityDeadlineExceeded)) {
            self.metrics.latency.timed_out(Stage::OutboxTotal);
        }
        result
    }

    async fn publish_many_before(
        &self,
        messages: &[crate::PublishMessage<'_>],
        deadline: Instant,
    ) -> Result<Vec<String>, StorageError> {
        let mut slot = self.connection.lock().await;
        if Instant::now() >= deadline {
            return Err(StorageError::RedisDurabilityDeadlineExceeded);
        }
        let timer = self.metrics.latency.start(Stage::OutboxWrite);
        if slot.is_none() {
            *slot = Some(redis_result(
                self.client
                    .get_multiplexed_async_connection_with_config(
                        &self.durability.connection_config(),
                    )
                    .await,
                &self.metrics,
                Stage::OutboxWrite,
            )?);
        }
        // Take ownership before awaiting: cancellation discards the connection instead of
        // acknowledging XADD on a possibly reconnected, unrelated WAITAOF connection.
        let mut connection = slot.take().expect("initialized connection");
        let mut pipeline = redis::pipe();
        for message in messages {
            pipeline.add_command(xadd_command(*message)?);
        }
        if Instant::now() >= deadline {
            return Err(StorageError::RedisDurabilityDeadlineExceeded);
        }
        let stream_ids: Vec<String> = redis_result(
            pipeline.query_async(&mut connection).await,
            &self.metrics,
            Stage::OutboxWrite,
        )?;
        if stream_ids.len() != messages.len() {
            return Err(StorageError::QueueProtocol(
                "outbox receipt count differs from batch".into(),
            ));
        }
        drop(timer);
        self.durability
            .wait_for_local_aof(&mut connection, deadline, &self.metrics, Stage::OutboxAof)
            .await?;
        *slot = Some(connection);
        Ok(stream_ids)
    }
}

fn xadd_command(message: crate::PublishMessage<'_>) -> Result<redis::Cmd, StorageError> {
    let crate::PublishMessage {
        event,
        destination: stream,
        operation_id,
        trade_id,
    } = message;
    let mut command = redis::cmd("XADD");
    command
        .arg(stream)
        .arg("*")
        .arg("event_id")
        .arg(&event.event_id)
        .arg("operation_id")
        .arg(operation_id)
        .arg("trade_id")
        .arg(trade_id)
        .arg("partition_key")
        .arg(&event.partition_key)
        .arg("occurred_at_unix_ms")
        .arg(event.occurred_at_unix_ms)
        .arg("payload")
        .arg(&event.payload);
    if tiangz_dbproxy_core::EventEnvelope::from_outbox(event)?.is_some() {
        command.arg("event").arg(&event.payload);
    }
    Ok(command)
}

/// 首个内置 Publisher；旧类型名保留兼容，Relay 不依赖 Redis 的具体方法。
/// First built-in publisher; the old concrete name remains source-compatible.
pub type RedisStreamPublisher = RedisOutboxPublisher;

#[async_trait::async_trait]
impl crate::Publisher for RedisStreamPublisher {
    async fn publish(
        &self,
        message: crate::PublishMessage<'_>,
    ) -> Result<crate::PublishReceipt, crate::PublishError> {
        tiangz_dbproxy_core::EventEnvelope::from_outbox(message.event)
            .map_err(|_| crate::PublishError::Permanent("invalid envelope"))?;
        self.publish_to(
            message.event,
            message.destination,
            message.operation_id,
            message.trade_id,
        )
        .await
        .map(|message_id| crate::PublishReceipt { message_id })
        .map_err(|_| crate::PublishError::Transient("Redis send or AOF confirmation failed"))
    }

    async fn publish_batch(
        &self,
        messages: &[crate::PublishMessage<'_>],
    ) -> Vec<Result<crate::PublishReceipt, crate::PublishError>> {
        if messages.is_empty() {
            return Vec::new();
        }
        if messages.len() > 64 {
            return messages
                .iter()
                .map(|_| {
                    Err(crate::PublishError::Transient(
                        "publication batch exceeds 64 items",
                    ))
                })
                .collect();
        }
        // An invalid envelope cannot cause unrelated, valid events to be dead-lettered.
        let mut results = Vec::with_capacity(messages.len());
        let mut valid = Vec::new();
        let mut indices = Vec::new();
        for (index, message) in messages.iter().enumerate() {
            if tiangz_dbproxy_core::EventEnvelope::from_outbox(message.event).is_err() {
                results.push(Err(crate::PublishError::Permanent("invalid envelope")));
            } else {
                results.push(Err(crate::PublishError::Transient(
                    "Redis send or AOF confirmation failed",
                )));
                valid.push(*message);
                indices.push(index);
            }
        }
        if valid.is_empty() {
            return results;
        }
        // Keep the controlled connection until all XADDs and their one shared WAITAOF finish.
        // Partial writes or an unknown confirmation return no success and require republication.
        if let Ok(ids) = self.publish_many_to(&valid).await {
            for (index, message_id) in indices.into_iter().zip(ids) {
                results[index] = Ok(crate::PublishReceipt { message_id });
            }
        }
        results
    }
}
