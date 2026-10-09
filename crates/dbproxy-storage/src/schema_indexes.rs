//! 启动时核对全库索引，不读取业务行，也不自动修复漂移。
//! Validate schema indexes at startup without scanning data or silently repairing drift.

use tokio_postgres::GenericClient;

use crate::{SNAPSHOT_PARTITION_COUNT, StorageError};

struct RequiredIndex {
    table: &'static str,
    name: Option<&'static str>,
    columns: &'static [&'static str],
    unique: bool,
    predicate: &'static str,
}

const PRIMARY_KEYS: &[(&str, &[&str])] = &[
    ("dbproxy_schema_migrations", &["version"]),
    ("dbproxy_snapshots", &["namespace", "record_key"]),
    ("dbproxy_idempotency", &["request_id"]),
    ("dbproxy_transactions", &["operation_id"]),
    ("dbproxy_multi_transactions", &["operation_id"]),
    (
        "dbproxy_multi_transaction_records",
        &["operation_id", "namespace", "record_key"],
    ),
    ("dbproxy_cache_repairs", &["namespace", "record_key"]),
    ("dbproxy_trades", &["trade_id"]),
    ("dbproxy_trade_operations", &["operation_id"]),
    (
        "dbproxy_trade_operation_records",
        &["operation_id", "namespace", "record_key"],
    ),
    ("dbproxy_ledger_postings", &["posting_id"]),
    ("dbproxy_outbox", &["event_id"]),
    ("dbproxy_operation_claims", &["operation_id"]),
    ("dbproxy_multi_transaction_effects", &["operation_id"]),
    ("dbproxy_append_records", &["namespace", "record_key"]),
    ("dbproxy_outbox_publishers", &["publisher_id"]),
    ("dbproxy_outbox_routes", &["route_key"]),
    ("dbproxy_outbox_admin_audit", &["id"]),
];

// 名称属于迁移契约；主键名称由 PG 决定，只校验结构。
// Named secondary indexes are migration contracts; primary keys are checked structurally.
const SECONDARY: &[RequiredIndex] = &[
    index(
        "dbproxy_idempotency",
        "dbproxy_idempotency_retention",
        &["recorded_at", "request_id"],
        "",
    ),
    index(
        "dbproxy_cache_repairs",
        "dbproxy_cache_repairs_unleased_order",
        &["requested_at", "namespace", "record_key", "available_at"],
        "dead_lettered_at IS NULL AND lease_until IS NULL",
    ),
    index(
        "dbproxy_cache_repairs",
        "dbproxy_cache_repairs_expired",
        &["lease_until", "requested_at"],
        "dead_lettered_at IS NULL AND lease_until IS NOT NULL",
    ),
    index(
        "dbproxy_cache_repairs",
        "dbproxy_cache_repairs_leased_order",
        &[
            "requested_at",
            "namespace",
            "record_key",
            "lease_until",
            "available_at",
        ],
        "dead_lettered_at IS NULL AND lease_until IS NOT NULL",
    ),
    index(
        "dbproxy_multi_transaction_records",
        "dbproxy_multi_transaction_records_lookup",
        &["operation_id"],
        "",
    ),
    index(
        "dbproxy_cache_repairs",
        "dbproxy_cache_repairs_ready",
        &["available_at", "requested_at"],
        "dead_lettered_at IS NULL",
    ),
    index(
        "dbproxy_cache_repairs",
        "dbproxy_cache_repairs_dead_lettered",
        &["dead_lettered_at"],
        "dead_lettered_at IS NOT NULL",
    ),
    index(
        "dbproxy_trade_operations",
        "dbproxy_trade_operations_trade",
        &["trade_id", "new_trade_version"],
        "",
    ),
    index(
        "dbproxy_ledger_postings",
        "dbproxy_ledger_postings_trade",
        &["trade_id", "posting_id"],
        "",
    ),
    index(
        "dbproxy_ledger_postings",
        "dbproxy_ledger_postings_account",
        &["account_id", "asset", "posting_id"],
        "",
    ),
    index(
        "dbproxy_ledger_postings",
        "dbproxy_ledger_postings_operation",
        &["operation_id", "posting_id"],
        "",
    ),
    index(
        "dbproxy_outbox",
        "dbproxy_outbox_ready",
        &["available_at", "occurred_at_unix_ms"],
        "published_at IS NULL AND dead_lettered_at IS NULL",
    ),
    index(
        "dbproxy_outbox",
        "dbproxy_outbox_dead_lettered",
        &["dead_lettered_at"],
        "dead_lettered_at IS NOT NULL",
    ),
    index(
        "dbproxy_outbox",
        "dbproxy_outbox_order",
        &[
            "publisher_id",
            "destination",
            "partition_key",
            "enqueue_order",
        ],
        "published_at IS NULL",
    ),
    index(
        "dbproxy_outbox",
        "dbproxy_outbox_operation",
        &["operation_id", "event_id"],
        "",
    ),
    index(
        "dbproxy_append_records",
        "dbproxy_append_records_operation",
        &["operation_id"],
        "",
    ),
    RequiredIndex {
        table: "dbproxy_outbox_routes",
        name: Some("dbproxy_outbox_routes_producer_route_version_key"),
        columns: &["producer", "route_version"],
        unique: true,
        predicate: "",
    },
];

const fn index(
    table: &'static str,
    name: &'static str,
    columns: &'static [&'static str],
    predicate: &'static str,
) -> RequiredIndex {
    RequiredIndex {
        table,
        name: Some(name),
        columns,
        unique: false,
        predicate,
    }
}

struct ActualIndex {
    table: String,
    name: String,
    columns: Vec<String>,
    primary: bool,
    unique: bool,
    usable: bool,
    predicate: String,
    default_order: bool,
    attached_to_snapshot_primary: bool,
}

// 仅用于本模块列出的简单 IS NULL / AND 谓词，不作为任意 SQL 等价判断器。
// Normalize only the simple IS NULL / AND predicates declared above, not arbitrary SQL.
fn normalized_predicate(value: &str) -> String {
    value
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '(' && *c != ')')
        .flat_map(char::to_lowercase)
        .collect()
}

/// 同一次系统目录快照核对所有必需索引，报告第一个不匹配对象。
/// Check required indexes in one catalog snapshot and identify the first mismatch.
pub(crate) async fn validate(client: &(impl GenericClient + Sync)) -> Result<(), StorageError> {
    let mut tables: Vec<String> = PRIMARY_KEYS
        .iter()
        .map(|(table, _)| (*table).to_string())
        .collect();
    tables.extend((0..SNAPSHOT_PARTITION_COUNT).map(|n| format!("dbproxy_snapshots_p{n:02}")));
    let rows = client
        .query(
            r#"
SELECT t.relname::TEXT, ix.relname::TEXT,
       ARRAY(SELECT pg_get_indexdef(i.indexrelid, n, true)
             FROM generate_series(1, i.indnkeyatts) n ORDER BY n),
       i.indisprimary, i.indisunique,
       i.indisvalid AND i.indisready AND i.indislive AND i.indimmediate
           AND am.amname = 'btree' AND i.indexprs IS NULL
           AND i.indnatts = i.indnkeyatts AS usable,
       COALESCE(pg_get_expr(i.indpred, i.indrelid), ''),
       NOT EXISTS (SELECT 1 FROM unnest(i.indoption) opt WHERE opt <> 0),
       EXISTS (SELECT 1 FROM pg_inherits inh JOIN pg_index parent ON parent.indexrelid=inh.inhparent
               WHERE inh.inhrelid=i.indexrelid AND parent.indisprimary
                 AND parent.indrelid=to_regclass('dbproxy_snapshots'))
FROM unnest($1::TEXT[]) requested(name)
JOIN pg_class t ON t.oid=to_regclass(requested.name)
JOIN pg_index i ON i.indrelid=t.oid
JOIN pg_class ix ON ix.oid=i.indexrelid
JOIN pg_am am ON am.oid=ix.relam
"#,
            &[&tables],
        )
        .await?;
    let actual: Vec<ActualIndex> = rows
        .into_iter()
        .map(|row| ActualIndex {
            table: row.get(0),
            name: row.get(1),
            columns: row.get(2),
            primary: row.get(3),
            unique: row.get(4),
            usable: row.get(5),
            predicate: row.get(6),
            default_order: row.get(7),
            attached_to_snapshot_primary: row.get(8),
        })
        .collect();
    for (table, columns) in PRIMARY_KEYS {
        require(
            &actual,
            &RequiredIndex {
                table,
                name: None,
                columns,
                unique: true,
                predicate: "",
            },
        )?;
    }
    for spec in SECONDARY {
        require(&actual, spec)?;
    }
    for n in 0..SNAPSHOT_PARTITION_COUNT {
        let table = format!("dbproxy_snapshots_p{n:02}");
        let valid = actual.iter().any(|i| {
            i.table == table
                && i.primary
                && i.unique
                && i.usable
                && i.default_order
                && i.columns == ["namespace", "record_key"]
                && i.predicate.is_empty()
                && i.attached_to_snapshot_primary
        });
        if !valid {
            return Err(StorageError::InvalidSchemaIndex(format!(
                "{table}: missing, invalid or detached primary index (namespace, record_key)"
            )));
        }
    }
    Ok(())
}

fn require(actual: &[ActualIndex], spec: &RequiredIndex) -> Result<(), StorageError> {
    let candidate = actual
        .iter()
        .find(|i| i.table == spec.table && spec.name.map_or(i.primary, |name| i.name == name));
    if candidate.is_some_and(|i| {
        i.usable
            && i.default_order
            && i.unique == spec.unique
            && i.columns == spec.columns
            && normalized_predicate(&i.predicate) == normalized_predicate(spec.predicate)
    }) {
        return Ok(());
    }
    Err(StorageError::InvalidSchemaIndex(format!(
        "{}: {} must be a valid, ready {}btree index on ({}){}",
        spec.table,
        spec.name.unwrap_or("PRIMARY KEY"),
        if spec.unique { "unique " } else { "" },
        spec.columns.join(", "),
        if spec.predicate.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", spec.predicate)
        }
    )))
}
