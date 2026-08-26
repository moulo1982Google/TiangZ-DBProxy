//! PostgreSQL outbox queue and Redis Streams publisher.

use std::sync::Arc;

use redis::aio::ConnectionManager;
use tiangz_dbproxy_core::OutboxEvent;
use tokio::sync::Mutex;

use crate::{
    DEFAULT_REDIS_AOF_ACK_TIMEOUT_MS, SharedPostgresClient, StorageError,
    open_redis_connection_manager,
};

const MAX_ERROR_CHARS: usize = 1_024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxLease {
    pub event: OutboxEvent,
    pub operation_id: String,
    pub trade_id: String,
    pub attempt_count: u64,
    lease_owner: String,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OutboxStats {
    pub pending: u64,
    pub processing: u64,
    pub dead_lettered: u64,
    pub oldest_age_ms: Option<u64>,
}

#[derive(Clone)]
pub struct PostgresOutboxQueue {
    client: SharedPostgresClient,
}

impl PostgresOutboxQueue {
    pub(crate) fn new(client: SharedPostgresClient) -> Self {
        Self { client }
    }

    pub async fn claim(
        &self,
        worker_id: &str,
        lease_ms: u64,
    ) -> Result<Option<OutboxLease>, StorageError> {
        validate_worker(worker_id, lease_ms)?;
        let lease_ms = i64::try_from(lease_ms)
            .map_err(|_| StorageError::QueueProtocol("lease duration is too large".to_string()))?;
        let mut client = self.client.lock().await;
        client.ensure_connected().await?;
        let row = client
            .query_opt(
                r#"
WITH candidate AS (
    SELECT current_event.event_id
    FROM dbproxy_outbox AS current_event
    WHERE current_event.published_at IS NULL
      AND current_event.dead_lettered_at IS NULL
      AND current_event.available_at <= clock_timestamp()
      AND (current_event.lease_until IS NULL OR current_event.lease_until <= clock_timestamp())
      AND NOT EXISTS (
          SELECT 1
          FROM dbproxy_outbox AS prior_event
          WHERE prior_event.topic = current_event.topic
            AND prior_event.partition_key = current_event.partition_key
            AND prior_event.published_at IS NULL
            AND (prior_event.created_at, prior_event.event_id)
                < (current_event.created_at, current_event.event_id)
      )
    ORDER BY current_event.created_at, current_event.event_id
    FOR UPDATE OF current_event SKIP LOCKED
    LIMIT 1
)
UPDATE dbproxy_outbox AS event
SET lease_owner = $1,
    lease_until = clock_timestamp() + ($2::BIGINT * interval '1 millisecond')
FROM candidate
WHERE event.event_id = candidate.event_id
RETURNING event.event_id, event.operation_id, event.trade_id, event.topic,
          event.partition_key, event.payload, event.occurred_at_unix_ms, event.attempt_count
"#,
                &[&worker_id, &lease_ms],
            )
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let occurred_at = u64::try_from(row.get::<_, i64>(6))
            .map_err(|_| StorageError::QueueProtocol("outbox timestamp is negative".to_string()))?;
        let attempt_count = u64::try_from(row.get::<_, i64>(7)).map_err(|_| {
            StorageError::QueueProtocol("outbox attempt count is negative".to_string())
        })?;
        Ok(Some(OutboxLease {
            event: OutboxEvent {
                event_id: row.get(0),
                topic: row.get(3),
                partition_key: row.get(4),
                payload: row.get(5),
                occurred_at_unix_ms: occurred_at,
            },
            operation_id: row.get(1),
            trade_id: row.get(2),
            attempt_count,
            lease_owner: worker_id.to_string(),
        }))
    }

    pub async fn acknowledge(&self, lease: &OutboxLease) -> Result<bool, StorageError> {
        let mut client = self.client.lock().await;
        client.ensure_connected().await?;
        let updated = client
            .execute(
                r#"
UPDATE dbproxy_outbox
SET published_at = clock_timestamp(), lease_owner = NULL, lease_until = NULL, last_error = NULL
WHERE event_id = $1 AND lease_owner = $2 AND published_at IS NULL
"#,
                &[&lease.event.event_id, &lease.lease_owner],
            )
            .await?;
        Ok(updated == 1)
    }

    pub async fn fail(
        &self,
        lease: &OutboxLease,
        error: &str,
        retry_delay_ms: u64,
        max_attempts: u32,
    ) -> Result<bool, StorageError> {
        if max_attempts == 0 {
            return Err(StorageError::QueueProtocol(
                "maximum attempts must be greater than zero".to_string(),
            ));
        }
        let delay = i64::try_from(retry_delay_ms)
            .map_err(|_| StorageError::QueueProtocol("retry delay is too large".to_string()))?;
        let error: String = error.chars().take(MAX_ERROR_CHARS).collect();
        let maximum = i64::from(max_attempts);
        let mut client = self.client.lock().await;
        client.ensure_connected().await?;
        let updated = client
            .execute(
                r#"
UPDATE dbproxy_outbox
SET attempt_count = attempt_count + 1,
    last_error = $3,
    lease_owner = NULL,
    lease_until = NULL,
    available_at = CASE
        WHEN attempt_count + 1 >= $5 THEN available_at
        ELSE clock_timestamp() + ($4::BIGINT * interval '1 millisecond')
    END,
    dead_lettered_at = CASE
        WHEN attempt_count + 1 >= $5 THEN clock_timestamp()
        ELSE NULL
    END
WHERE event_id = $1 AND lease_owner = $2 AND published_at IS NULL
"#,
                &[
                    &lease.event.event_id,
                    &lease.lease_owner,
                    &error,
                    &delay,
                    &maximum,
                ],
            )
            .await?;
        Ok(updated == 1)
    }

    /// Requeue one inspected dead letter after an operator has fixed the root cause.
    pub async fn requeue_dead_letter(&self, event_id: &str) -> Result<bool, StorageError> {
        if event_id.trim().is_empty() {
            return Err(StorageError::QueueProtocol(
                "outbox event id is empty".to_string(),
            ));
        }
        let mut client = self.client.lock().await;
        client.ensure_connected().await?;
        let updated = client
            .execute(
                r#"
UPDATE dbproxy_outbox
SET attempt_count = 0,
    available_at = clock_timestamp(),
    lease_owner = NULL,
    lease_until = NULL,
    last_error = NULL,
    dead_lettered_at = NULL
WHERE event_id = $1 AND published_at IS NULL AND dead_lettered_at IS NOT NULL
"#,
                &[&event_id],
            )
            .await?;
        Ok(updated == 1)
    }

    pub async fn stats(&self) -> Result<OutboxStats, StorageError> {
        let mut client = self.client.lock().await;
        client.ensure_connected().await?;
        let row = client
            .query_one(
                r#"
SELECT
    COUNT(*) FILTER (WHERE published_at IS NULL AND dead_lettered_at IS NULL AND (lease_until IS NULL OR lease_until <= clock_timestamp())),
    COUNT(*) FILTER (WHERE published_at IS NULL AND dead_lettered_at IS NULL AND lease_until > clock_timestamp()),
    COUNT(*) FILTER (WHERE dead_lettered_at IS NOT NULL),
    (EXTRACT(EPOCH FROM (clock_timestamp() - MIN(created_at) FILTER (WHERE published_at IS NULL AND dead_lettered_at IS NULL))) * 1000)::DOUBLE PRECISION
FROM dbproxy_outbox
"#,
                &[],
            )
            .await?;
        Ok(OutboxStats {
            pending: count(row.get(0), "outbox pending count")?,
            processing: count(row.get(1), "outbox processing count")?,
            dead_lettered: count(row.get(2), "outbox dead-letter count")?,
            oldest_age_ms: age(row.get(3)),
        })
    }
}

#[derive(Clone)]
pub struct RedisOutboxPublisher {
    connection: Arc<Mutex<ConnectionManager>>,
    stream_prefix: Arc<str>,
}

impl RedisOutboxPublisher {
    pub async fn connect(url: &str, stream_prefix: &str) -> Result<Self, StorageError> {
        if stream_prefix.trim().is_empty() {
            return Err(StorageError::QueueProtocol(
                "outbox stream prefix is empty".to_string(),
            ));
        }
        let connection = open_redis_connection_manager(url).await?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            stream_prefix: Arc::from(stream_prefix),
        })
    }

    /// Publish at least once. Consumers must deduplicate by event_id.
    pub async fn publish(&self, lease: &OutboxLease) -> Result<String, StorageError> {
        let stream = format!("{}{}", self.stream_prefix, lease.event.topic);
        let mut connection = self.connection.lock().await;
        let stream_id: String = redis::cmd("XADD")
            .arg(stream)
            .arg("*")
            .arg("event_id")
            .arg(&lease.event.event_id)
            .arg("operation_id")
            .arg(&lease.operation_id)
            .arg("trade_id")
            .arg(&lease.trade_id)
            .arg("partition_key")
            .arg(&lease.event.partition_key)
            .arg("occurred_at_unix_ms")
            .arg(lease.event.occurred_at_unix_ms)
            .arg("payload")
            .arg(&lease.event.payload)
            .query_async(&mut *connection)
            .await?;
        let timeout_ms = i64::try_from(DEFAULT_REDIS_AOF_ACK_TIMEOUT_MS).unwrap_or(i64::MAX);
        let (local, _replicas): (i64, i64) = redis::cmd("WAITAOF")
            .arg(1)
            .arg(0)
            .arg(timeout_ms)
            .query_async(&mut *connection)
            .await?;
        if local < 1 {
            return Err(StorageError::RedisAofNotDurable {
                timeout_ms: DEFAULT_REDIS_AOF_ACK_TIMEOUT_MS,
            });
        }
        Ok(stream_id)
    }
}

fn validate_worker(worker_id: &str, lease_ms: u64) -> Result<(), StorageError> {
    if worker_id.trim().is_empty() {
        return Err(StorageError::InvalidQueueWorker);
    }
    if lease_ms == 0 {
        return Err(StorageError::InvalidQueueLease);
    }
    Ok(())
}

fn count(value: i64, field: &str) -> Result<u64, StorageError> {
    u64::try_from(value).map_err(|_| StorageError::QueueProtocol(format!("{field} is negative")))
}

fn age(value: Option<f64>) -> Option<u64> {
    value.map(|milliseconds| milliseconds.max(0.0).min(u64::MAX as f64) as u64)
}
