//! 通用追加和事件写入；调用者必须传入同一个快照事务。
//! Generic append/outbox effects inside the caller's snapshot transaction.
use crate::{StorageError, advisory_lock_key, timestamp_to_i64};
use tiangz_dbproxy_core::{CommitEffects, StoreError};
use tokio_postgres::Transaction;

pub(crate) fn encode(effects: &CommitEffects) -> Result<Vec<u8>, StorageError> {
    bincode::serde::encode_to_vec(effects, bincode::config::standard())
        .map_err(|e| StorageError::PersistenceProtocol(e.to_string()))
}

pub(crate) async fn verify_retry(
    tx: &Transaction<'_>,
    id: &str,
    effects: &CommitEffects,
) -> Result<(), StorageError> {
    let row = tx
        .query_opt(
            "SELECT payload FROM dbproxy_multi_transaction_effects WHERE operation_id = $1",
            &[&id],
        )
        .await?;
    let matches = match row {
        Some(row) => row.get::<_, Vec<u8>>(0) == encode(effects)?,
        None => effects.is_empty(),
    };
    if !matches {
        return Err(StoreError::OperationIdConflict {
            operation_id: id.to_string(),
        }
        .into());
    }
    Ok(())
}

pub(crate) async fn lock_partitions(
    tx: &Transaction<'_>,
    effects: &CommitEffects,
) -> Result<(), StorageError> {
    let mut routes = Vec::new();
    for event in &effects.outbox_events {
        let route = if tiangz_dbproxy_core::EventEnvelope::from_outbox(event)?.is_some() {
            let row = tx
                .query_opt(
                    "SELECT publisher_id,destination FROM dbproxy_outbox_routes WHERE route_key=$1",
                    &[&event.topic],
                )
                .await?
                .ok_or(StoreError::InvalidOutboxEvent(
                    "unknown outbox source or route version",
                ))?;
            advisory_lock_key(
                "relay-destination",
                &[&row.get::<_, String>(0), &row.get::<_, String>(1)],
            )
        } else {
            event.topic.clone()
        };
        routes.push((route, event.partition_key.clone()));
    }
    routes.sort();
    routes.dedup();
    // Relay uses the resolved destination scope; legacy uses its one-to-one topic scope.
    for (lock_scope, partition) in routes {
        let key = advisory_lock_key("outbox-partition", &[&lock_scope, &partition]);
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&key],
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn persist(
    tx: &Transaction<'_>,
    id: &str,
    effects: &CommitEffects,
) -> Result<(), StorageError> {
    if effects.is_empty() {
        return Ok(());
    }
    tx.execute(
        "INSERT INTO dbproxy_multi_transaction_effects(operation_id, payload) VALUES ($1,$2)",
        &[&id, &encode(effects)?],
    )
    .await?;
    for record in &effects.appends {
        let row = tx.query_opt(
            "INSERT INTO dbproxy_append_records(namespace,record_key,operation_id,schema_name,schema_version,payload,occurred_at_unix_ms) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(namespace,record_key) DO NOTHING RETURNING record_key",
            &[&record.record.namespace, &record.record.key, &id, &record.schema, &i64::from(record.schema_version), &record.payload, &timestamp_to_i64(&record.record, record.occurred_at_unix_ms)?],
        ).await?;
        if row.is_none() {
            return Err(StoreError::AppendRecordConflict {
                record: record.record.clone(),
            }
            .into());
        }
    }
    for event in &effects.outbox_events {
        let timestamp = i64::try_from(event.occurred_at_unix_ms)
            .map_err(|_| StorageError::PersistenceProtocol("event timestamp exceeds i64".into()))?;
        let row = tx.query_opt(
            "INSERT INTO dbproxy_outbox(event_id,operation_id,topic,partition_key,payload,occurred_at_unix_ms) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(event_id) DO NOTHING RETURNING event_id",
            &[&event.event_id, &id, &event.topic, &event.partition_key, &event.payload, &timestamp],
        ).await?;
        if row.is_none() {
            return Err(StoreError::OutboxEventConflict {
                event_id: event.event_id.clone(),
            }
            .into());
        }
    }
    Ok(())
}
