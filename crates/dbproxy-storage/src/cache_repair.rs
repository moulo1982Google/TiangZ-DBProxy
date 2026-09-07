//! PostgreSQL-backed durable cache-repair queue.

use tiangz_dbproxy_core::{RecordKey, Revision};
use tokio_postgres::Transaction;

use crate::{SharedPostgresClient, StorageError, required_revision_to_i64, revision_from_i64};

const MAX_ERROR_CHARS: usize = 1_024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheRepairLease {
    pub record: RecordKey,
    pub target_revision: Revision,
    pub attempt_count: u64,
    lease_owner: String,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheRepairStats {
    pub pending: u64,
    pub processing: u64,
    pub dead_lettered: u64,
    pub oldest_age_ms: Option<u64>,
}

#[derive(Clone)]
pub struct PostgresCacheRepairQueue {
    client: SharedPostgresClient,
    acknowledgement_wait: Option<std::time::Duration>,
}

impl PostgresCacheRepairQueue {
    pub(crate) fn new(
        client: SharedPostgresClient,
        acknowledgement_wait: Option<std::time::Duration>,
    ) -> Self {
        Self {
            client,
            acknowledgement_wait,
        }
    }

    pub async fn enqueue(
        &self,
        record: &RecordKey,
        target_revision: Revision,
    ) -> Result<(), StorageError> {
        let target_revision = required_revision_to_i64(record, target_revision)?;
        let mut client = self.client.lock().await;
        client.ensure_connected().await?;
        enqueue_sql(client.as_client(), record, target_revision).await
    }

    pub async fn acknowledge_cached(
        &self,
        record: &RecordKey,
        cached_revision: Revision,
    ) -> Result<bool, StorageError> {
        let cached_revision = required_revision_to_i64(record, cached_revision)?;
        let mut client =
            crate::postgres_request::lock_client(&self.client, self.acknowledgement_wait).await?;
        client.ensure_connected().await?;
        let removed = client
            .execute(
                "DELETE FROM dbproxy_cache_repairs WHERE namespace = $1 AND record_key = $2 AND target_revision <= $3",
                &[&record.namespace, &record.key, &cached_revision],
            )
            .await?;
        Ok(removed == 1)
    }

    /// Remove repair targets covered by a successfully written cache batch in one statement.
    pub async fn acknowledge_cached_multi(
        &self,
        snapshots: &[tiangz_dbproxy_core::SnapshotEnvelope],
    ) -> Result<u64, StorageError> {
        let targets: Vec<_> = snapshots
            .iter()
            .map(|snapshot| (snapshot.record.clone(), snapshot.revision))
            .collect();
        self.acknowledge_cached_revisions(&targets).await
    }

    pub(crate) async fn acknowledge_cached_revisions(
        &self,
        targets: &[(RecordKey, Revision)],
    ) -> Result<u64, StorageError> {
        if targets.is_empty() {
            return Ok(0);
        }
        let namespaces = targets
            .iter()
            .map(|(record, _)| record.namespace.clone())
            .collect::<Vec<_>>();
        let keys = targets
            .iter()
            .map(|(record, _)| record.key.clone())
            .collect::<Vec<_>>();
        let revisions = targets
            .iter()
            .map(|(record, revision)| required_revision_to_i64(record, *revision))
            .collect::<Result<Vec<_>, _>>()?;
        let mut client =
            crate::postgres_request::lock_client(&self.client, self.acknowledgement_wait).await?;
        client.ensure_connected().await?;
        Ok(client
            .execute(
                r#"
DELETE FROM dbproxy_cache_repairs AS repair
USING unnest($1::TEXT[], $2::TEXT[], $3::BIGINT[])
    AS cached(namespace, record_key, revision)
WHERE repair.namespace = cached.namespace
  AND repair.record_key = cached.record_key
  AND repair.target_revision <= cached.revision
"#,
                &[&namespaces, &keys, &revisions],
            )
            .await?)
    }

    pub async fn claim(
        &self,
        worker_id: &str,
        lease_ms: u64,
    ) -> Result<Option<CacheRepairLease>, StorageError> {
        validate_worker(worker_id, lease_ms)?;
        let lease_ms = i64::try_from(lease_ms)
            .map_err(|_| StorageError::QueueProtocol("lease duration is too large".to_string()))?;
        let mut client = self.client.lock().await;
        client.ensure_connected().await?;
        let row = client
            .query_opt(
                r#"
WITH candidate AS (
    SELECT namespace, record_key
    FROM dbproxy_cache_repairs
    WHERE dead_lettered_at IS NULL
      AND available_at <= clock_timestamp()
      AND (lease_until IS NULL OR lease_until <= clock_timestamp())
    ORDER BY requested_at, namespace, record_key
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE dbproxy_cache_repairs AS repair
SET lease_owner = $1,
    lease_until = clock_timestamp() + ($2::BIGINT * interval '1 millisecond')
FROM candidate
WHERE repair.namespace = candidate.namespace
  AND repair.record_key = candidate.record_key
RETURNING repair.namespace, repair.record_key, repair.target_revision, repair.attempt_count
"#,
                &[&worker_id, &lease_ms],
            )
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let record = RecordKey::new(row.get::<_, String>(0), row.get::<_, String>(1))?;
        let attempt_count = u64::try_from(row.get::<_, i64>(3)).map_err(|_| {
            StorageError::QueueProtocol("cache repair attempt count is negative".to_string())
        })?;
        Ok(Some(CacheRepairLease {
            target_revision: revision_from_i64(&record, row.get(2))?,
            record,
            attempt_count,
            lease_owner: worker_id.to_string(),
        }))
    }

    /// ACK only the exact claimed target. A concurrent newer enqueue remains pending.
    pub async fn acknowledge(&self, lease: &CacheRepairLease) -> Result<bool, StorageError> {
        let target_revision = required_revision_to_i64(&lease.record, lease.target_revision)?;
        let mut client = self.client.lock().await;
        client.ensure_connected().await?;
        let removed = client
            .execute(
                "DELETE FROM dbproxy_cache_repairs WHERE namespace = $1 AND record_key = $2 AND target_revision = $3 AND lease_owner = $4",
                &[
                    &lease.record.namespace,
                    &lease.record.key,
                    &target_revision,
                    &lease.lease_owner,
                ],
            )
            .await?;
        Ok(removed == 1)
    }

    pub async fn fail(
        &self,
        lease: &CacheRepairLease,
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
        let target_revision = required_revision_to_i64(&lease.record, lease.target_revision)?;
        let error = bounded_error(error);
        let maximum = i64::from(max_attempts);
        let mut client = self.client.lock().await;
        client.ensure_connected().await?;
        let updated = client
            .execute(
                r#"
UPDATE dbproxy_cache_repairs
SET attempt_count = attempt_count + 1,
    last_error = $5,
    lease_owner = NULL,
    lease_until = NULL,
    available_at = CASE
        WHEN attempt_count + 1 >= $7 THEN available_at
        ELSE clock_timestamp() + ($6::BIGINT * interval '1 millisecond')
    END,
    dead_lettered_at = CASE
        WHEN attempt_count + 1 >= $7 THEN clock_timestamp()
        ELSE NULL
    END
WHERE namespace = $1
  AND record_key = $2
  AND target_revision = $3
  AND lease_owner = $4
"#,
                &[
                    &lease.record.namespace,
                    &lease.record.key,
                    &target_revision,
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
    pub async fn requeue_dead_letter(&self, record: &RecordKey) -> Result<bool, StorageError> {
        let mut client = self.client.lock().await;
        client.ensure_connected().await?;
        let updated = client
            .execute(
                r#"
UPDATE dbproxy_cache_repairs
SET attempt_count = 0,
    available_at = clock_timestamp(),
    lease_owner = NULL,
    lease_until = NULL,
    last_error = NULL,
    dead_lettered_at = NULL
WHERE namespace = $1 AND record_key = $2 AND dead_lettered_at IS NOT NULL
"#,
                &[&record.namespace, &record.key],
            )
            .await?;
        Ok(updated == 1)
    }

    pub async fn stats(&self) -> Result<CacheRepairStats, StorageError> {
        let mut client = self.client.lock().await;
        client.ensure_connected().await?;
        let row = client
            .query_one(
                r#"
SELECT
    COUNT(*) FILTER (WHERE dead_lettered_at IS NULL AND (lease_until IS NULL OR lease_until <= clock_timestamp())),
    COUNT(*) FILTER (WHERE dead_lettered_at IS NULL AND lease_until > clock_timestamp()),
    COUNT(*) FILTER (WHERE dead_lettered_at IS NOT NULL),
    (EXTRACT(EPOCH FROM (clock_timestamp() - MIN(requested_at) FILTER (WHERE dead_lettered_at IS NULL))) * 1000)::DOUBLE PRECISION
FROM dbproxy_cache_repairs
"#,
                &[],
            )
            .await?;
        Ok(CacheRepairStats {
            pending: count(row.get(0), "cache repair pending count")?,
            processing: count(row.get(1), "cache repair processing count")?,
            dead_lettered: count(row.get(2), "cache repair dead-letter count")?,
            oldest_age_ms: age(row.get(3)),
        })
    }
}

pub(crate) async fn enqueue_in_transaction(
    transaction: &Transaction<'_>,
    record: &RecordKey,
    target_revision: Revision,
) -> Result<(), StorageError> {
    let target_revision = required_revision_to_i64(record, target_revision)?;
    enqueue_sql(transaction, record, target_revision).await
}

async fn enqueue_sql<C>(
    client: &C,
    record: &RecordKey,
    target_revision: i64,
) -> Result<(), StorageError>
where
    C: tokio_postgres::GenericClient + Sync,
{
    client
        .execute(
            r#"
INSERT INTO dbproxy_cache_repairs (namespace, record_key, target_revision)
VALUES ($1, $2, $3)
ON CONFLICT (namespace, record_key) DO UPDATE
SET target_revision = GREATEST(dbproxy_cache_repairs.target_revision, EXCLUDED.target_revision),
    requested_at = clock_timestamp(),
    available_at = clock_timestamp(),
    attempt_count = 0,
    lease_owner = NULL,
    lease_until = NULL,
    last_error = NULL,
    dead_lettered_at = NULL
"#,
            &[&record.namespace, &record.key, &target_revision],
        )
        .await?;
    Ok(())
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

fn bounded_error(error: &str) -> String {
    error.chars().take(MAX_ERROR_CHARS).collect()
}

fn count(value: i64, field: &str) -> Result<u64, StorageError> {
    u64::try_from(value).map_err(|_| StorageError::QueueProtocol(format!("{field} is negative")))
}

fn age(value: Option<f64>) -> Option<u64> {
    value.map(|milliseconds| milliseconds.max(0.0).min(u64::MAX as f64) as u64)
}
