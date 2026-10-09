//! Ordinary snapshot receipts only; transaction receipts and business rows are never deleted.
use std::time::Duration;

use crate::{PostgresSnapshotStore, StorageError};

pub const RECEIPT_CLEANUP_BATCH_SIZE: u64 = 500;
/// 普通回执默认保留 24 小时。 / Ordinary receipts are kept for 24 hours by default.
pub const DEFAULT_RECEIPT_RETENTION: Duration = Duration::from_secs(24 * 3600);
/// 下限 1 小时：必须长于调用方用同一请求号重试的最长窗口。
/// Floor of one hour: it must outlast the longest same-request-ID retry window of any caller.
pub const MIN_RECEIPT_RETENTION: Duration = Duration::from_secs(3600);
/// 上限一年，只防止配置错误。 / One-year ceiling, only to reject misconfiguration.
pub const MAX_RECEIPT_RETENTION: Duration = Duration::from_secs(8760 * 3600);

/// Reject retention outside [`MIN_RECEIPT_RETENTION`, `MAX_RECEIPT_RETENTION`] before any deletion.
pub fn validate_receipt_retention(retention: Duration) -> Result<Duration, StorageError> {
    if retention < MIN_RECEIPT_RETENTION || retention > MAX_RECEIPT_RETENTION {
        return Err(StorageError::InvalidReceiptRetention {
            seconds: retention.as_secs(),
        });
    }
    Ok(retention)
}

impl PostgresSnapshotStore {
    /// Delete at most 500 receipts strictly older than `retention` (measured from the database
    /// insert time `recorded_at`), skipping locked receipts. Retention outside the allowed range
    /// is rejected without touching the database.
    /// Call on a dedicated maintenance connection, not a request shard.
    pub async fn cleanup_expired_receipts(&self, retention: Duration) -> Result<u64, StorageError> {
        let retention = validate_receipt_retention(retention)?;
        let mut client = self
            .client
            .lock_for("cleanup_expired_receipts", None, 0, None)
            .await
            .expect("unbounded maintenance lock");
        client.ensure_connected().await?;
        let transaction = client.transaction().await?;
        transaction
            .batch_execute("SET LOCAL statement_timeout='2s'; SET LOCAL lock_timeout='100ms'")
            .await?;
        let deleted = transaction
            .execute(
                include_str!("receipt_cleanup.sql"),
                &[&retention.as_secs_f64()],
            )
            .await?;
        transaction.commit().await?;
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The worker paces on `deleted == RECEIPT_CLEANUP_BATCH_SIZE`; the SQL LIMIT must match it.
    #[test]
    fn cleanup_sql_limit_matches_batch_constant() {
        let sql = include_str!("receipt_cleanup.sql");
        assert!(
            sql.contains(&format!("LIMIT {RECEIPT_CLEANUP_BATCH_SIZE}\n")),
            "receipt_cleanup.sql LIMIT must equal RECEIPT_CLEANUP_BATCH_SIZE ({RECEIPT_CLEANUP_BATCH_SIZE})"
        );
        assert_eq!(sql.matches("LIMIT ").count(), 1);
        // Retention is bound as a parameter, never a hard-coded interval.
        assert!(sql.contains("make_interval(secs => $1::double precision)"));
        assert!(!sql.contains("interval '"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
    }

    #[test]
    fn retention_bounds_are_enforced() {
        assert_eq!(DEFAULT_RECEIPT_RETENTION, Duration::from_secs(86_400));
        for ok in [
            MIN_RECEIPT_RETENTION,
            DEFAULT_RECEIPT_RETENTION,
            Duration::from_secs(168 * 3600),
            MAX_RECEIPT_RETENTION,
        ] {
            assert_eq!(validate_receipt_retention(ok).unwrap(), ok);
        }
        for bad in [
            Duration::ZERO,
            Duration::from_secs(3599),
            MAX_RECEIPT_RETENTION + Duration::from_secs(1),
        ] {
            assert!(matches!(
                validate_receipt_retention(bad),
                Err(StorageError::InvalidReceiptRetention { .. })
            ));
        }
    }
}
