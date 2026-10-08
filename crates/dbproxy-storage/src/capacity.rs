//! 独立只读容量采样，不迁移、不启动 worker、不清理数据。
//! Dedicated read-only capacity sampling without migrations, workers or data cleanup.

use std::time::Duration;

use serde::Serialize;
use thiserror::Error;
use tokio::{task::JoinHandle, time::timeout};
use tokio_postgres::{Config, NoTls, Transaction};

const CATALOG_SQL: &str = include_str!("capacity.sql");
const MAX_RELATIONS: usize = 10_000;
const AGE_STATEMENT_TIMEOUT_MS: u64 = 250;

/// 固定清单既限定输出，也避免业务身份成为指标维度。
/// A fixed inventory bounds output and never exposes business identities.
const TABLES: &[(&str, &str, Option<&str>)] = &[
    ("dbproxy_snapshots", "authoritative_state", None),
    ("dbproxy_idempotency", "cas_receipt", None),
    ("dbproxy_transactions", "transaction_receipt", None),
    ("dbproxy_multi_transactions", "transaction_receipt", None),
    (
        "dbproxy_multi_transaction_records",
        "receipt_write_set",
        None,
    ),
    (
        "dbproxy_multi_transaction_effects",
        "immutable_effects",
        None,
    ),
    (
        "dbproxy_operation_claims",
        "operation_identity",
        Some("claimed_at"),
    ),
    ("dbproxy_trades", "trade_state", None),
    ("dbproxy_trade_operations", "trade_receipt", None),
    ("dbproxy_trade_operation_records", "receipt_write_set", None),
    ("dbproxy_ledger_postings", "ledger_fact", None),
    ("dbproxy_append_records", "append_fact", None),
    (
        "dbproxy_outbox",
        "delivery_responsibility",
        Some("created_at"),
    ),
    (
        "dbproxy_cache_repairs",
        "repair_responsibility",
        Some("requested_at"),
    ),
    ("dbproxy_outbox_publishers", "delivery_config", None),
    ("dbproxy_outbox_routes", "delivery_config", None),
    (
        "dbproxy_outbox_admin_audit",
        "audit_fact",
        Some("created_at"),
    ),
    (
        "dbproxy_schema_migrations",
        "schema_metadata",
        Some("applied_at"),
    ),
];

#[derive(Clone, Debug)]
pub struct CapacityOptions {
    pub schema: String,
    pub timeout: Duration,
    pub include_server_age: bool,
}

impl Default for CapacityOptions {
    fn default() -> Self {
        Self {
            schema: "public".into(),
            timeout: Duration::from_secs(10),
            include_server_age: false,
        }
    }
}

#[derive(Debug, Error)]
pub enum CapacityError {
    #[error(
        "invalid capacity options: schema must be 1..63 bytes without NUL; timeout must be 1..60000ms"
    )]
    InvalidOptions,
    #[error("capacity observation exceeded its total time budget")]
    Timeout,
    #[error("requested PostgreSQL schema does not exist")]
    MissingSchema,
    #[error("capacity observation exceeds the 10000 relation limit")]
    TooManyRelations,
    #[error("capacity PostgreSQL operation failed (SQLSTATE {sqlstate})")]
    Postgres { sqlstate: String },
}

impl From<tokio_postgres::Error> for CapacityError {
    /// 仅公开稳定错误码，不把服务端文本或连接凭据带进 CLI 日志。
    /// Exposes only a stable code, never server text or connection credentials.
    fn from(error: tokio_postgres::Error) -> Self {
        Self::Postgres {
            sqlstate: sqlstate(&error),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct CapacitySnapshot {
    pub format_version: u32,
    pub schema: String,
    pub sampled_at_unix_ms: i64,
    pub server_version_num: String,
    pub read_only: bool,
    pub tables: Vec<TableCapacity>,
}

#[derive(Debug, Serialize)]
pub struct TableCapacity {
    pub table: &'static str,
    pub purpose: &'static str,
    pub status: &'static str,
    pub physical_relations: u32,
    pub unknown_estimate_relations: u32,
    pub estimated_rows: Option<f64>,
    pub table_bytes: Option<u64>,
    pub index_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
    pub oldest_server_time: OldestServerTime,
}

#[derive(Debug, Serialize)]
pub struct OldestServerTime {
    pub column: Option<&'static str>,
    pub status: &'static str,
    pub unix_ms: Option<i64>,
    pub sqlstate: Option<String>,
}

struct ConnectionTask(JoinHandle<()>);

impl Drop for ConnectionTask {
    /// 超时或调用方取消也终止本次专用连接，不遗留后台查询驱动任务。
    /// Timeout and caller cancellation also stop this dedicated connection driver.
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// 一次调用共用总期限，默认只读 catalog；可选年龄扫描各有服务端期限。
/// One deadline covers the call; optional age scans also have server-side limits.
pub async fn sample_capacity(
    config: &Config,
    options: &CapacityOptions,
) -> Result<CapacitySnapshot, CapacityError> {
    if options.schema.is_empty()
        || options.schema.len() > 63
        || options.schema.contains('\0')
        || options.timeout < Duration::from_millis(1)
        || options.timeout > Duration::from_secs(60)
    {
        return Err(CapacityError::InvalidOptions);
    }
    timeout(options.timeout, sample_inner(config, options))
        .await
        .map_err(|_| CapacityError::Timeout)?
}

/// 连接与事务仅服务本次样本；不会复用业务池或自动迁移。
/// The connection and transaction serve only this sample, with no pool or migration.
async fn sample_inner(
    config: &Config,
    options: &CapacityOptions,
) -> Result<CapacitySnapshot, CapacityError> {
    let mut config = config.clone();
    config.application_name("tiangz-dbproxy-capacity");
    let (mut client, connection) = config.connect(NoTls).await?;
    let _connection = ConnectionTask(tokio::spawn(async move {
        let _ = connection.await;
    }));
    let tx = client.build_transaction().read_only(true).start().await?;
    tx.batch_execute("SET LOCAL search_path TO pg_catalog; SET LOCAL row_security TO off")
        .await?;
    set_statement_timeout(&tx, options.timeout.as_millis() as u64).await?;
    let metadata = tx.query_one("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname=$1), (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp())*1000)::bigint, pg_catalog.current_setting('server_version_num'), pg_catalog.current_setting('transaction_read_only')", &[&options.schema]).await?;
    if !metadata.get::<_, bool>(0) {
        return Err(CapacityError::MissingSchema);
    }
    let names: Vec<_> = TABLES.iter().map(|(name, _, _)| *name).collect();
    let rows = tx.query(CATALOG_SQL, &[&options.schema, &names]).await?;
    if rows.len() > MAX_RELATIONS {
        return Err(CapacityError::TooManyRelations);
    }
    let mut tables = Vec::with_capacity(TABLES.len());
    for &(name, purpose, column) in TABLES {
        let measurements = rows
            .iter()
            .filter(|r| r.get::<_, &str>(0) == name)
            .collect::<Vec<_>>();
        let root_kind = measurements
            .first()
            .and_then(|r| r.get::<_, Option<&str>>(1));
        let supported = matches!(root_kind, Some("r" | "p"));
        let mut table = TableCapacity {
            table: name,
            purpose,
            status: if supported {
                "measured"
            } else if root_kind.is_none() {
                "missing"
            } else {
                "unsupported-relation"
            },
            physical_relations: 0,
            unknown_estimate_relations: 0,
            estimated_rows: supported.then_some(0.0),
            table_bytes: supported.then_some(0),
            index_bytes: supported.then_some(0),
            total_bytes: supported.then_some(0),
            oldest_server_time: OldestServerTime {
                column,
                status: if column.is_some() {
                    "not-requested"
                } else {
                    "no-server-clock"
                },
                unix_ms: None,
                sqlstate: None,
            },
        };
        for row in measurements {
            match row.get::<_, Option<&str>>(2) {
                Some("r") => {
                    table.physical_relations += 1;
                    let estimate = row
                        .get::<_, Option<f64>>(3)
                        .filter(|n| n.is_finite() && *n >= 0.0);
                    if estimate.is_none() {
                        table.unknown_estimate_relations += 1;
                    }
                    table.estimated_rows = table.estimated_rows.zip(estimate).map(|(a, b)| a + b);
                    table.table_bytes = sum_bytes(table.table_bytes, row.get(4));
                    table.index_bytes = sum_bytes(table.index_bytes, row.get(5));
                }
                Some("p") | None => {}
                Some(_) => {
                    table.status = "unsupported-descendant";
                }
            }
        }
        if table.status != "measured" {
            table.estimated_rows = None;
            table.table_bytes = None;
            table.index_bytes = None;
        }
        table.total_bytes = table
            .table_bytes
            .zip(table.index_bytes)
            .and_then(|(a, b)| a.checked_add(b));
        if table.status == "measured" && table.total_bytes.is_none() {
            table.status = "unavailable";
        }
        if !supported {
            table.oldest_server_time.status = table.status;
        }
        if options.include_server_age
            && table.status == "measured"
            && let Some(column) = column
        {
            table.oldest_server_time =
                sample_oldest(&tx, &options.schema, name, column, options.timeout).await?;
        }
        tables.push(table);
    }
    tx.rollback().await?;
    Ok(CapacitySnapshot {
        format_version: 1,
        schema: options.schema.clone(),
        sampled_at_unix_ms: metadata.get(1),
        server_version_num: metadata.get(2),
        read_only: metadata.get::<_, &str>(3) == "on",
        tables,
    })
}

/// 字节统计缺失或溢出时保留未知，不伪装成零。
/// Missing or overflowing size observations remain unknown, never zero.
fn sum_bytes(current: Option<u64>, next: Option<i64>) -> Option<u64> {
    current
        .zip(next.and_then(|n| u64::try_from(n).ok()))
        .and_then(|(a, b)| a.checked_add(b))
}

/// schema 是唯一动态标识符，双引号转义后使用；表/列仅来自固定清单。
/// Quotes the only dynamic identifier; table and column names come from the fixed inventory.
fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

/// 局部 statement_timeout 限制服务端执行，不依赖 Future 丢弃取消 SQL。
/// A transaction-local statement limit bounds server execution independently of dropped futures.
async fn set_statement_timeout(tx: &Transaction<'_>, millis: u64) -> Result<(), CapacityError> {
    tx.query_one(
        "SELECT pg_catalog.set_config('statement_timeout', $1, true)",
        &[&format!("{millis}ms")],
    )
    .await?;
    Ok(())
}

/// 可选全表时间查询，用 savepoint 隔离超时/权限错误，保留其他容量数据。
/// Isolates optional age-scan timeout/permission errors with a savepoint.
async fn sample_oldest(
    tx: &Transaction<'_>,
    schema: &str,
    table: &'static str,
    column: &'static str,
    total: Duration,
) -> Result<OldestServerTime, CapacityError> {
    set_statement_timeout(tx, AGE_STATEMENT_TIMEOUT_MS.min(total.as_millis() as u64)).await?;
    tx.batch_execute("SAVEPOINT capacity_age").await?;
    let result = tx
        .query_one(
            &format!(
                "SELECT (EXTRACT(EPOCH FROM MIN({column}))*1000)::bigint FROM {}.{table}",
                quote_identifier(schema)
            ),
            &[],
        )
        .await;
    let sample = match result {
        Ok(row) => {
            let unix_ms = row.get::<_, Option<i64>>(0);
            OldestServerTime {
                column: Some(column),
                status: if unix_ms.is_some() {
                    "measured"
                } else {
                    "empty"
                },
                unix_ms,
                sqlstate: None,
            }
        }
        Err(error) => {
            tx.batch_execute("ROLLBACK TO SAVEPOINT capacity_age")
                .await?;
            let code = sqlstate(&error);
            OldestServerTime {
                column: Some(column),
                status: if code == "57014" {
                    "query-timeout"
                } else {
                    "query-error"
                },
                unix_ms: None,
                sqlstate: Some(code),
            }
        }
    };
    tx.batch_execute("RELEASE SAVEPOINT capacity_age").await?;
    Ok(sample)
}

/// 连接层错误没有 SQLSTATE；不记录其可能含环境信息的原始文本。
/// Connection errors lack SQLSTATE; their potentially sensitive text is omitted.
fn sqlstate(error: &tokio_postgres::Error) -> String {
    error
        .code()
        .map(|c| c.code())
        .unwrap_or("unavailable")
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[path = "postgres.rs"]
    mod postgres;
    use tokio::{io::AsyncReadExt, net::TcpListener};

    #[tokio::test]
    async fn total_deadline_closes_unresponsive_postgres_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = Config::new();
        config
            .host("127.0.0.1")
            .port(listener.local_addr().unwrap().port())
            .user("capacity-fixture");
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut received = Vec::new();
            timeout(Duration::from_secs(2), stream.read_to_end(&mut received))
                .await
                .unwrap()
                .unwrap();
            assert!(!received.is_empty());
        });
        assert!(matches!(
            sample_capacity(
                &config,
                &CapacityOptions {
                    timeout: Duration::from_millis(50),
                    ..Default::default()
                }
            )
            .await,
            Err(CapacityError::Timeout)
        ));
        timeout(Duration::from_secs(2), peer)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn invalid_options_fail_before_connecting() {
        for schema in [String::new(), "x".repeat(64), "bad\0name".into()] {
            assert!(matches!(
                sample_capacity(
                    &Config::new(),
                    &CapacityOptions {
                        schema,
                        ..Default::default()
                    }
                )
                .await,
                Err(CapacityError::InvalidOptions)
            ));
        }
        for budget in [Duration::ZERO, Duration::from_secs(61)] {
            assert!(matches!(
                sample_capacity(
                    &Config::new(),
                    &CapacityOptions {
                        timeout: budget,
                        ..Default::default()
                    }
                )
                .await,
                Err(CapacityError::InvalidOptions)
            ));
        }
    }

    #[test]
    fn unknown_sizes_never_become_zero_and_schema_is_quoted() {
        assert_eq!(sum_bytes(Some(4), Some(7)), Some(11));
        assert_eq!(sum_bytes(Some(4), None), None);
        assert_eq!(sum_bytes(None, Some(7)), None);
        assert_eq!(sum_bytes(Some(4), Some(-1)), None);
        assert_eq!(sum_bytes(Some(u64::MAX), Some(1)), None);
        assert_eq!(
            quote_identifier("space \"; drop schema public; --"),
            "\"space \"\"; drop schema public; --\""
        );
    }
}
