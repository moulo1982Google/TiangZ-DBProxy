//! 多记录事务回执退休：超过保留期删除事务头与回执记录，只把操作号标为已退休。
//! 之后查询或重提这个操作号返回“已过期”，不会被当成“没执行过”。
//! Multi-record transaction receipt retirement: after the retention the header and its records
//! are deleted and the operation claim is marked retired, so a late lookup or replay is answered
//! `OperationExpired`, never "not executed". Receipts carrying commit effects are kept.
//!
//! 退休标记再保留一段时间（默认 7 天）后删除：之后用同一个操作号查询得到“没有回执”，重提会被当作新操作。
//! Retired markers are deleted after a second retention (7 days by default). After that a lookup of
//! the id finds no receipt and a replay is treated as a new operation; expected revisions still
//! reject a replay whose records have moved on.
use std::time::Duration;

use crate::{PostgresSnapshotStore, StorageError};

pub const OPERATION_RETIREMENT_BATCH_SIZE: u64 = 500;
/// 默认保留 30 天。 / Multi-record receipts are kept for 30 days by default.
pub const DEFAULT_OPERATION_RECEIPT_RETENTION: Duration = Duration::from_secs(30 * 86_400);
/// 下限 1 天：必须长于业务未决操作的最长存活时间，过短会让未决操作卡在“结果未知”。
/// Floor of one day: it must outlast the longest-lived unresolved business operation, otherwise
/// that operation can only ever be answered "expired" (outcome unknown).
pub const MIN_OPERATION_RECEIPT_RETENTION: Duration = Duration::from_secs(86_400);
/// 上限十年，只防止配置错误。 / Ten-year ceiling, only to reject misconfiguration.
pub const MAX_OPERATION_RECEIPT_RETENTION: Duration = Duration::from_secs(3650 * 86_400);

/// 退休标记默认保留 7 天。 / Retired markers are kept for 7 days by default.
pub const DEFAULT_RETIRED_OPERATION_RETENTION: Duration = Duration::from_secs(7 * 86_400);
/// 下限 1 天、上限十年，与回执保留期相同。 / Same one-day floor and ten-year ceiling as receipts.
pub const MIN_RETIRED_OPERATION_RETENTION: Duration = Duration::from_secs(86_400);
pub const MAX_RETIRED_OPERATION_RETENTION: Duration = Duration::from_secs(3650 * 86_400);

/// Reject marker retention outside [`MIN_RETIRED_OPERATION_RETENTION`, `MAX_RETIRED_OPERATION_RETENTION`].
pub fn validate_retired_operation_retention(retention: Duration) -> Result<Duration, StorageError> {
    if retention < MIN_RETIRED_OPERATION_RETENTION || retention > MAX_RETIRED_OPERATION_RETENTION {
        return Err(StorageError::InvalidRetiredOperationRetention {
            seconds: retention.as_secs(),
        });
    }
    Ok(retention)
}

/// Reject retention outside [`MIN_OPERATION_RECEIPT_RETENTION`, `MAX_OPERATION_RECEIPT_RETENTION`].
pub fn validate_operation_receipt_retention(retention: Duration) -> Result<Duration, StorageError> {
    if retention < MIN_OPERATION_RECEIPT_RETENTION || retention > MAX_OPERATION_RECEIPT_RETENTION {
        return Err(StorageError::InvalidOperationReceiptRetention {
            seconds: retention.as_secs(),
        });
    }
    Ok(retention)
}

impl PostgresSnapshotStore {
    /// Retire at most 500 multi-record receipts whose claim is strictly older than `retention`
    /// (server time `claimed_at`), skipping claims locked by an in-flight replay and receipts
    /// with commit effects. Returns the number of operations retired.
    /// Call on a dedicated maintenance connection, not a request shard.
    pub async fn retire_expired_operations(
        &self,
        retention: Duration,
    ) -> Result<u64, StorageError> {
        let retention = validate_operation_receipt_retention(retention)?;
        let mut client = self
            .client
            .lock_for("retire_expired_operations", None, 0, None)
            .await
            .expect("unbounded maintenance lock");
        client.ensure_connected().await?;
        let transaction = client.transaction().await?;
        transaction
            .batch_execute("SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='100ms'")
            .await?;
        let retired = transaction
            .execute(
                include_str!("operation_retirement.sql"),
                &[&retention.as_secs_f64()],
            )
            .await?;
        transaction.commit().await?;
        Ok(retired)
    }

    /// Delete at most 500 retired multi-record markers whose retirement is strictly older than
    /// `retention` (server time `retired_at`), skipping markers locked by a late replay.
    /// Returns the number of markers deleted. Call on a dedicated maintenance connection.
    pub async fn purge_retired_operations(&self, retention: Duration) -> Result<u64, StorageError> {
        let retention = validate_retired_operation_retention(retention)?;
        let mut client = self
            .client
            .lock_for("purge_retired_operations", None, 0, None)
            .await
            .expect("unbounded maintenance lock");
        client.ensure_connected().await?;
        let transaction = client.transaction().await?;
        transaction
            .batch_execute("SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='100ms'")
            .await?;
        let purged = transaction
            .execute(
                include_str!("operation_claim_purge.sql"),
                &[&retention.as_secs_f64()],
            )
            .await?;
        transaction.commit().await?;
        Ok(purged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retirement_sql_matches_batch_constant_and_guards() {
        let sql = include_str!("operation_retirement.sql");
        assert!(sql.contains(&format!("LIMIT {OPERATION_RETIREMENT_BATCH_SIZE}\n")));
        assert_eq!(sql.matches("LIMIT ").count(), 1);
        assert!(sql.contains("make_interval(secs => $1::double precision)"));
        assert!(!sql.contains("interval '"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
        assert!(sql.contains("claim.operation_kind = 'multi'"));
        assert!(sql.contains("dbproxy_multi_transaction_effects"));
        assert!(
            sql.contains("claimed_at <"),
            "age must use the server-side claim time"
        );
        assert!(
            !sql.contains("updated_at_unix_ms"),
            "client timestamps must not decide age"
        );
    }

    #[test]
    fn purge_sql_matches_batch_constant_and_guards() {
        let sql = include_str!("operation_claim_purge.sql");
        assert!(sql.contains(&format!(
            "LIMIT {OPERATION_RETIREMENT_BATCH_SIZE}
"
        )));
        assert_eq!(sql.matches("LIMIT ").count(), 1);
        assert!(sql.contains("make_interval(secs => $1::double precision)"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
        assert!(sql.contains("claim.operation_kind = 'multi'"));
        assert!(sql.contains("claim.retired_at IS NOT NULL"));
        assert!(
            sql.contains("retired_at <"),
            "age must use the server-side retirement time"
        );
    }

    #[test]
    fn retired_marker_retention_bounds_are_enforced() {
        assert_eq!(
            DEFAULT_RETIRED_OPERATION_RETENTION,
            Duration::from_secs(7 * 86_400)
        );
        for ok in [
            MIN_RETIRED_OPERATION_RETENTION,
            DEFAULT_RETIRED_OPERATION_RETENTION,
            MAX_RETIRED_OPERATION_RETENTION,
        ] {
            assert_eq!(validate_retired_operation_retention(ok).unwrap(), ok);
        }
        for bad in [
            Duration::ZERO,
            Duration::from_secs(86_399),
            MAX_RETIRED_OPERATION_RETENTION + Duration::from_secs(1),
        ] {
            assert!(matches!(
                validate_retired_operation_retention(bad),
                Err(StorageError::InvalidRetiredOperationRetention { .. })
            ));
        }
    }

    #[test]
    fn retention_bounds_are_enforced() {
        assert_eq!(
            DEFAULT_OPERATION_RECEIPT_RETENTION,
            Duration::from_secs(30 * 86_400)
        );
        for ok in [
            MIN_OPERATION_RECEIPT_RETENTION,
            DEFAULT_OPERATION_RECEIPT_RETENTION,
            MAX_OPERATION_RECEIPT_RETENTION,
        ] {
            assert_eq!(validate_operation_receipt_retention(ok).unwrap(), ok);
        }
        for bad in [
            Duration::ZERO,
            Duration::from_secs(86_399),
            MAX_OPERATION_RECEIPT_RETENTION + Duration::from_secs(1),
        ] {
            assert!(matches!(
                validate_operation_receipt_retention(bad),
                Err(StorageError::InvalidOperationReceiptRetention { .. })
            ));
        }
    }
}
