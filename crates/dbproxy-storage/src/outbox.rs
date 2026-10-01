//! PostgreSQL outbox leases, fencing and retry state.

use crate::{SharedPostgresClient, StorageError};
use tiangz_dbproxy_core::OutboxEvent;

const MAX_ERROR_CHARS: usize = 1_024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxLease {
    pub event: OutboxEvent,
    pub operation_id: String,
    pub trade_id: String,
    pub attempt_count: u64,
    pub producer: String,
    pub publisher_id: String,
    pub destination: String,
    pub lease_token: i64,
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
    pub(crate) client: SharedPostgresClient,
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
        self.claim_for_publisher(worker_id, lease_ms, None).await
    }

    /// 可选限制 Publisher，供独立验收或专用 worker 使用，不跨来源修改队列。
    /// Optional publisher scoping for isolated acceptance or dedicated workers.
    pub async fn claim_for_publisher(
        &self,
        worker_id: &str,
        lease_ms: u64,
        publisher: Option<&str>,
    ) -> Result<Option<OutboxLease>, StorageError> {
        Ok(self
            .claim_batch_for_publisher(worker_id, lease_ms, publisher, 1)
            .await?
            .pop())
    }

    /// At most one head per (publisher, destination, partition). Dead letters still block followers.
    /// Only the count is bounded here; callers must also budget payloads and worker concurrency.
    pub async fn claim_batch_for_publisher(
        &self,
        worker_id: &str,
        lease_ms: u64,
        publisher: Option<&str>,
        maximum: usize,
    ) -> Result<Vec<OutboxLease>, StorageError> {
        validate_worker(worker_id, lease_ms)?;
        if !(1..=64).contains(&maximum) {
            return Err(StorageError::QueueProtocol(
                "outbox claim batch must contain 1..=64 items".into(),
            ));
        }
        let maximum = maximum as i64;
        let lease_ms = i64::try_from(lease_ms)
            .map_err(|_| StorageError::QueueProtocol("lease duration is too large".to_string()))?;
        let mut client = self.client.lock().await;
        client.ensure_connected().await?;
        let rows = client
            .query(
                r#"
WITH candidate AS (
    SELECT current_event.event_id
    FROM dbproxy_outbox AS current_event
    WHERE current_event.published_at IS NULL
      AND current_event.dead_lettered_at IS NULL
      AND current_event.available_at <= clock_timestamp()
      AND ($3::TEXT IS NULL OR current_event.publisher_id = $3)
      AND (current_event.lease_until IS NULL OR current_event.lease_until <= clock_timestamp())
      AND NOT EXISTS (
          SELECT 1
          FROM dbproxy_outbox AS prior_event
          WHERE prior_event.publisher_id = current_event.publisher_id
            AND prior_event.destination = current_event.destination
            AND prior_event.partition_key = current_event.partition_key
            AND prior_event.published_at IS NULL
            AND prior_event.enqueue_order < current_event.enqueue_order
      )
    ORDER BY current_event.enqueue_order
    FOR UPDATE OF current_event SKIP LOCKED
    LIMIT $4
)
UPDATE dbproxy_outbox AS event
SET lease_owner = $1,
    lease_token = event.lease_token + 1,
    expired_leases = event.expired_leases + CASE WHEN event.lease_until IS NOT NULL THEN 1 ELSE 0 END,
    lease_until = clock_timestamp() + ($2::BIGINT * interval '1 millisecond')
FROM candidate
WHERE event.event_id = candidate.event_id
RETURNING event.event_id, event.operation_id, event.trade_id, event.topic,
          event.partition_key, event.payload, event.occurred_at_unix_ms, event.attempt_count,
          event.producer, event.publisher_id, event.destination, event.lease_token
"#,
                &[&worker_id, &lease_ms, &publisher, &maximum],
            )
            .await?;
        rows.into_iter()
            .map(|row| {
                let occurred_at = u64::try_from(row.get::<_, i64>(6)).map_err(|_| {
                    StorageError::QueueProtocol("outbox timestamp is negative".to_string())
                })?;
                let attempt_count = u64::try_from(row.get::<_, i64>(7)).map_err(|_| {
                    StorageError::QueueProtocol("outbox attempt count is negative".to_string())
                })?;
                Ok(OutboxLease {
                    event: OutboxEvent {
                        event_id: row.get(0),
                        topic: row.get(3),
                        partition_key: row.get(4),
                        payload: row.get(5),
                        occurred_at_unix_ms: occurred_at,
                    },
                    operation_id: row.get(1),
                    // Empty for generic commits; legacy trade deliveries retain their original ID.
                    trade_id: row.get::<_, Option<String>>(2).unwrap_or_default(),
                    attempt_count,
                    producer: row.get(8),
                    publisher_id: row.get(9),
                    destination: row.get(10),
                    lease_token: row.get(11),
                    lease_owner: worker_id.to_string(),
                })
            })
            .collect()
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
  AND lease_token = $3 AND lease_until > clock_timestamp()
"#,
                &[
                    &lease.event.event_id,
                    &lease.lease_owner,
                    &lease.lease_token,
                ],
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
  AND lease_token = $6 AND lease_until > clock_timestamp()
"#,
                &[
                    &lease.event.event_id,
                    &lease.lease_owner,
                    &error,
                    &delay,
                    &maximum,
                    &lease.lease_token,
                ],
            )
            .await?;
        Ok(updated == 1)
    }

    /// Requeue one inspected dead letter after an operator has fixed the root cause.
    pub async fn requeue_dead_letter(&self, event_id: &str) -> Result<bool, StorageError> {
        self.retry_dead_letter(
            event_id,
            "legacy-api",
            "Explicit retry through the compatibility API",
        )
        .await
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
