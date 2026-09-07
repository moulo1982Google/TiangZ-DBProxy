//! PostgreSQL implementation of the atomic trade contract.

use async_trait::async_trait;
use tiangz_dbproxy_core::{
    AsyncTradeStore, LedgerPosting, OutboxEvent, RecordKey, Revision, StoreError, TradeEnvelope,
    TradeReceipt, TradeState, TradeTransaction, TradeTransactionOutcome, TransactionRecordReceipt,
    TransactionalRecordWrite, normalize_trade_transaction,
};
use tokio_postgres::{GenericClient, Row};

use crate::{
    PostgresSnapshotStore, StorageError, TieredSnapshotStore, advisory_lock_key, cache_repair,
    claim_operation, persist_transactional_snapshot, revision_from_i64,
};

fn state_to_i16(state: TradeState) -> i16 {
    match state {
        TradeState::Proposed => 1,
        TradeState::Escrowed => 2,
        TradeState::Settled => 3,
        TradeState::Cancelled => 4,
    }
}

fn state_from_i16(value: i16) -> Result<TradeState, StorageError> {
    match value {
        1 => Ok(TradeState::Proposed),
        2 => Ok(TradeState::Escrowed),
        3 => Ok(TradeState::Settled),
        4 => Ok(TradeState::Cancelled),
        _ => Err(StorageError::TradeProtocol(format!(
            "persisted trade state is invalid: {value}"
        ))),
    }
}

fn trade_version_to_i64(trade_id: &str, version: Revision) -> Result<i64, StorageError> {
    i64::try_from(version.0).map_err(|_| StorageError::TradeVersionTooLarge {
        trade_id: trade_id.to_string(),
    })
}

fn trade_version_from_i64(trade_id: &str, value: i64) -> Result<Revision, StorageError> {
    u64::try_from(value)
        .map(Revision)
        .map_err(|_| StorageError::TradeProtocol(format!("trade {trade_id} has negative version")))
}

fn receipt_from_parts(
    operation_id: &str,
    trade_id: &str,
    header: &Row,
    record_rows: &[Row],
    ledger_rows: &[Row],
    outbox_rows: &[Row],
) -> Result<TradeReceipt, StorageError> {
    let persisted_counts = [
        ("record", header.get::<_, i64>(6), record_rows.len()),
        ("ledger", header.get::<_, i64>(7), ledger_rows.len()),
        ("outbox", header.get::<_, i64>(8), outbox_rows.len()),
    ];
    for (name, expected, actual) in persisted_counts {
        if usize::try_from(expected).ok() != Some(actual) {
            return Err(StorageError::TradeProtocol(format!(
                "trade operation {operation_id} has inconsistent {name} count"
            )));
        }
    }
    let records = record_rows
        .iter()
        .map(|row| {
            let record = RecordKey::new(row.get::<_, String>(0), row.get::<_, String>(1))?;
            Ok(TransactionRecordReceipt {
                new_revision: revision_from_i64(&record, row.get(2))?,
                record,
            })
        })
        .collect::<Result<Vec<_>, StorageError>>()?;
    Ok(TradeReceipt {
        operation_id: operation_id.to_string(),
        trade_id: trade_id.to_string(),
        new_trade_version: trade_version_from_i64(trade_id, header.get(9))?,
        state: state_from_i16(header.get(3))?,
        records,
        ledger_posting_ids: ledger_rows.iter().map(|row| row.get(0)).collect(),
        outbox_event_ids: outbox_rows.iter().map(|row| row.get(0)).collect(),
        result: header.get(5),
    })
}

async fn load_receipt_parts<C>(
    client: &C,
    operation_id: &str,
) -> Result<Option<(Row, Vec<Row>, Vec<Row>, Vec<Row>)>, StorageError>
where
    C: GenericClient + Sync,
{
    let header = client
        .query_opt(
            "SELECT trade_id, expected_version, expected_state, next_state, trade_payload, result, record_count, ledger_count, outbox_count, new_trade_version, updated_at_unix_ms FROM dbproxy_trade_operations WHERE operation_id = $1",
            &[&operation_id],
        )
        .await?;
    let Some(header) = header else {
        return Ok(None);
    };
    let records = client
        .query(
            "SELECT namespace, record_key, new_revision, schema_name, schema_version, expected_revision, payload, updated_at_unix_ms FROM dbproxy_trade_operation_records WHERE operation_id = $1 ORDER BY namespace, record_key",
            &[&operation_id],
        )
        .await?;
    let ledger = client
        .query(
            "SELECT posting_id, account_id, asset, amount, metadata FROM dbproxy_ledger_postings WHERE operation_id = $1 ORDER BY posting_id",
            &[&operation_id],
        )
        .await?;
    let outbox = client
        .query(
            "SELECT event_id, topic, partition_key, payload, occurred_at_unix_ms FROM dbproxy_outbox WHERE operation_id = $1 ORDER BY event_id",
            &[&operation_id],
        )
        .await?;
    Ok(Some((header, records, ledger, outbox)))
}

fn receipt_matches(
    header: &Row,
    records: &[Row],
    ledger: &[Row],
    outbox: &[Row],
    request: &TradeTransaction,
) -> bool {
    let expected_version = i64::try_from(request.transition.expected_version.0).ok();
    let expected_new_version = request
        .transition
        .expected_version
        .0
        .checked_add(1)
        .and_then(|version| i64::try_from(version).ok());
    let expected_state = request.transition.expected_state.map(state_to_i16);
    let updated_at = i64::try_from(request.transition.updated_at_unix_ms).ok();
    let header_matches = header.get::<_, String>(0) == request.transition.trade_id
        && Some(header.get::<_, i64>(1)) == expected_version
        && header.get::<_, Option<i16>>(2) == expected_state
        && header.get::<_, i16>(3) == state_to_i16(request.transition.next_state)
        && header.get::<_, Vec<u8>>(4) == request.transition.payload
        && header.get::<_, Vec<u8>>(5) == request.result
        && header.get::<_, i64>(6) == request.writes.len() as i64
        && header.get::<_, i64>(7) == request.ledger_postings.len() as i64
        && header.get::<_, i64>(8) == request.outbox_events.len() as i64
        && Some(header.get::<_, i64>(9)) == expected_new_version
        && Some(header.get::<_, i64>(10)) == updated_at;
    let records_match = records.len() == request.writes.len()
        && records
            .iter()
            .zip(&request.writes)
            .all(|(row, write)| trade_record_matches(row, write));
    let ledger_matches = ledger.len() == request.ledger_postings.len()
        && ledger
            .iter()
            .zip(&request.ledger_postings)
            .all(|(row, posting)| ledger_row_matches(row, posting));
    let outbox_matches = outbox.len() == request.outbox_events.len()
        && outbox
            .iter()
            .zip(&request.outbox_events)
            .all(|(row, event)| outbox_row_matches(row, event));
    header_matches && records_match && ledger_matches && outbox_matches
}

fn trade_record_matches(row: &Row, write: &TransactionalRecordWrite) -> bool {
    let Ok(expected) = i64::try_from(write.expected_revision.0) else {
        return false;
    };
    let Some(new_revision) = write
        .expected_revision
        .0
        .checked_add(1)
        .and_then(|revision| i64::try_from(revision).ok())
    else {
        return false;
    };
    let Ok(updated_at) = i64::try_from(write.updated_at_unix_ms) else {
        return false;
    };
    row.get::<_, String>(0) == write.record.namespace
        && row.get::<_, String>(1) == write.record.key
        && row.get::<_, i64>(2) == new_revision
        && row.get::<_, String>(3) == write.schema
        && row.get::<_, i64>(4) == i64::from(write.schema_version)
        && row.get::<_, i64>(5) == expected
        && row.get::<_, Vec<u8>>(6) == write.payload
        && row.get::<_, i64>(7) == updated_at
}

fn ledger_row_matches(row: &Row, posting: &LedgerPosting) -> bool {
    row.get::<_, String>(0) == posting.posting_id
        && row.get::<_, String>(1) == posting.account_id
        && row.get::<_, String>(2) == posting.asset
        && row.get::<_, i64>(3) == posting.amount
        && row.get::<_, Vec<u8>>(4) == posting.metadata
}

fn outbox_row_matches(row: &Row, event: &OutboxEvent) -> bool {
    i64::try_from(event.occurred_at_unix_ms).is_ok_and(|occurred_at| {
        row.get::<_, String>(0) == event.event_id
            && row.get::<_, String>(1) == event.topic
            && row.get::<_, String>(2) == event.partition_key
            && row.get::<_, Vec<u8>>(3) == event.payload
            && row.get::<_, i64>(4) == occurred_at
    })
}

#[async_trait]
impl AsyncTradeStore for PostgresSnapshotStore {
    type Error = StorageError;

    async fn load_trade(&self, trade_id: &str) -> Result<Option<TradeEnvelope>, Self::Error> {
        if trade_id.trim().is_empty() {
            return Err(StoreError::EmptyTradeId.into());
        }
        let mut client = self
            .metrics
            .latency
            .measure(super::Stage::PostgresQueue, self.client.lock())
            .await;
        let _postgres_timer = self.metrics.latency.start(super::Stage::PostgresOperation);
        client.ensure_connected().await?;
        let row = client
            .query_opt(
                "SELECT version, state, payload, updated_at_unix_ms FROM dbproxy_trades WHERE trade_id = $1",
                &[&trade_id],
            )
            .await?;
        row.map(|row| {
            let updated_at = u64::try_from(row.get::<_, i64>(3)).map_err(|_| {
                StorageError::TradeProtocol(format!("trade {trade_id} has negative timestamp"))
            })?;
            Ok(TradeEnvelope {
                trade_id: trade_id.to_string(),
                version: trade_version_from_i64(trade_id, row.get(0))?,
                state: state_from_i16(row.get(1))?,
                payload: row.get(2),
                updated_at_unix_ms: updated_at,
            })
        })
        .transpose()
    }

    async fn load_trade_receipt(
        &self,
        operation_id: &str,
        trade_id: &str,
    ) -> Result<Option<TradeReceipt>, Self::Error> {
        if operation_id.trim().is_empty() {
            return Err(StoreError::EmptyOperationId.into());
        }
        if trade_id.trim().is_empty() {
            return Err(StoreError::EmptyTradeId.into());
        }
        let mut client = self
            .metrics
            .latency
            .measure(super::Stage::PostgresQueue, self.client.lock())
            .await;
        let _postgres_timer = self.metrics.latency.start(super::Stage::PostgresOperation);
        client.ensure_connected().await?;
        let Some((header, records, ledger, outbox)) =
            load_receipt_parts(client.as_client(), operation_id).await?
        else {
            return Ok(None);
        };
        if header.get::<_, String>(0) != trade_id {
            return Err(StoreError::OperationIdConflict {
                operation_id: operation_id.to_string(),
            }
            .into());
        }
        Ok(Some(receipt_from_parts(
            operation_id,
            trade_id,
            &header,
            &records,
            &ledger,
            &outbox,
        )?))
    }

    async fn apply_trade(
        &mut self,
        request: TradeTransaction,
    ) -> Result<TradeTransactionOutcome, Self::Error> {
        let request = normalize_trade_transaction(request)?;
        let trade_id = request.transition.trade_id.clone();
        let operation_id = request.operation_id.clone();
        let expected_version =
            trade_version_to_i64(&trade_id, request.transition.expected_version)?;
        let new_trade_version = request
            .transition
            .expected_version
            .0
            .checked_add(1)
            .map(Revision)
            .ok_or_else(|| StoreError::TradeVersionExhausted {
                trade_id: trade_id.clone(),
            })?;
        let new_trade_version_i64 = trade_version_to_i64(&trade_id, new_trade_version)?;
        let expected_state = request.transition.expected_state.map(state_to_i16);
        let next_state = state_to_i16(request.transition.next_state);
        let updated_at = i64::try_from(request.transition.updated_at_unix_ms)
            .map_err(|_| StorageError::TradeProtocol("trade timestamp is too large".to_string()))?;

        let mut client = self
            .metrics
            .latency
            .measure(super::Stage::PostgresQueue, self.client.lock())
            .await;
        let _postgres_timer = self.metrics.latency.start(super::Stage::PostgresOperation);
        client.ensure_connected().await?;
        let transaction = client.transaction().await?;
        claim_operation(&transaction, &operation_id, "trade").await?;
        let claimed = transaction
            .query_opt(
                r#"
INSERT INTO dbproxy_trade_operations
    (operation_id, trade_id, expected_version, expected_state, next_state, trade_payload,
     result, record_count, ledger_count, outbox_count, updated_at_unix_ms, new_trade_version)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT (operation_id) DO NOTHING
RETURNING operation_id
"#,
                &[
                    &operation_id,
                    &trade_id,
                    &expected_version,
                    &expected_state,
                    &next_state,
                    &request.transition.payload,
                    &request.result,
                    &(request.writes.len() as i64),
                    &(request.ledger_postings.len() as i64),
                    &(request.outbox_events.len() as i64),
                    &updated_at,
                    &new_trade_version_i64,
                ],
            )
            .await?;

        if claimed.is_none() {
            let Some((header, records, ledger, outbox)) =
                load_receipt_parts(&transaction, &operation_id).await?
            else {
                return Err(StorageError::TradeProtocol(
                    "trade operation disappeared after idempotency conflict".to_string(),
                ));
            };
            if !receipt_matches(&header, &records, &ledger, &outbox, &request) {
                return Err(StoreError::OperationIdConflict { operation_id }.into());
            }
            let receipt = receipt_from_parts(
                &request.operation_id,
                &trade_id,
                &header,
                &records,
                &ledger,
                &outbox,
            )?;
            for record in &receipt.records {
                cache_repair::enqueue_in_transaction(
                    &transaction,
                    &record.record,
                    record.new_revision,
                )
                .await?;
            }
            transaction.commit().await?;
            return Ok(TradeTransactionOutcome::Duplicate(receipt));
        }

        let trade_lock_key = advisory_lock_key("trade", &[trade_id.as_str()]);
        transaction
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                &[&trade_lock_key],
            )
            .await?;
        let current_trade = transaction
            .query_opt(
                "SELECT version, state FROM dbproxy_trades WHERE trade_id = $1 FOR UPDATE",
                &[&trade_id],
            )
            .await?;
        let (actual_version, actual_state) = match current_trade {
            Some(row) => (
                trade_version_from_i64(&trade_id, row.get(0))?,
                Some(state_from_i16(row.get(1))?),
            ),
            None => (Revision::ZERO, None),
        };
        if actual_version != request.transition.expected_version {
            return Err(StoreError::TradeVersionConflict {
                trade_id,
                expected: request.transition.expected_version,
                actual: actual_version,
            }
            .into());
        }
        if actual_state != request.transition.expected_state {
            return Err(StoreError::TradeStateConflict {
                trade_id,
                expected: request.transition.expected_state,
                actual: actual_state,
            }
            .into());
        }

        // Serialize transactions that publish to the same logical partition. Without this lock,
        // a later transaction could commit and publish while an earlier uncommitted row is still
        // invisible to the outbox worker. Sorted acquisition prevents multi-partition deadlocks.
        let mut outbox_partitions = request
            .outbox_events
            .iter()
            .map(|event| (event.topic.as_str(), event.partition_key.as_str()))
            .collect::<Vec<_>>();
        outbox_partitions.sort_unstable();
        outbox_partitions.dedup();
        for (topic, partition_key) in outbox_partitions {
            let partition_lock_key = advisory_lock_key("outbox-partition", &[topic, partition_key]);
            transaction
                .query_one(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    &[&partition_lock_key],
                )
                .await?;
        }

        let mut revisions = Vec::with_capacity(request.writes.len());
        for write in &request.writes {
            let lock_key = advisory_lock_key(
                "record",
                &[write.record.namespace.as_str(), write.record.key.as_str()],
            );
            transaction
                .query_one(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    &[&lock_key],
                )
                .await?;
            let current = transaction
                .query_opt(
                    "SELECT revision FROM dbproxy_snapshots WHERE namespace = $1 AND record_key = $2 FOR UPDATE",
                    &[&write.record.namespace, &write.record.key],
                )
                .await?;
            let actual = current
                .map(|row| revision_from_i64(&write.record, row.get(0)))
                .transpose()?
                .unwrap_or(Revision::ZERO);
            if actual != write.expected_revision {
                return Err(StoreError::RevisionConflict {
                    record: write.record.clone(),
                    expected: Some(write.expected_revision),
                    actual,
                }
                .into());
            }
            revisions.push(Revision(actual.0.checked_add(1).ok_or_else(|| {
                StoreError::RevisionExhausted {
                    record: write.record.clone(),
                }
            })?));
        }

        transaction
            .execute(
                r#"
INSERT INTO dbproxy_trades (trade_id, version, state, payload, updated_at_unix_ms)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (trade_id) DO UPDATE
SET version = EXCLUDED.version,
    state = EXCLUDED.state,
    payload = EXCLUDED.payload,
    updated_at_unix_ms = EXCLUDED.updated_at_unix_ms
"#,
                &[
                    &request.transition.trade_id,
                    &new_trade_version_i64,
                    &next_state,
                    &request.transition.payload,
                    &updated_at,
                ],
            )
            .await?;

        for (write, revision) in request.writes.iter().zip(&revisions) {
            let persisted = persist_transactional_snapshot(&transaction, write, *revision).await?;
            transaction
                .execute(
                    r#"
INSERT INTO dbproxy_trade_operation_records
    (operation_id, namespace, record_key, schema_name, schema_version,
     expected_revision, payload, updated_at_unix_ms, new_revision)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
"#,
                    &[
                        &request.operation_id,
                        &write.record.namespace,
                        &write.record.key,
                        &write.schema,
                        &persisted.schema_version,
                        &persisted.expected_revision,
                        &write.payload,
                        &persisted.updated_at_unix_ms,
                        &persisted.new_revision,
                    ],
                )
                .await?;
            cache_repair::enqueue_in_transaction(&transaction, &write.record, *revision).await?;
        }

        for posting in &request.ledger_postings {
            let inserted = transaction
                .query_opt(
                    r#"
INSERT INTO dbproxy_ledger_postings
    (posting_id, operation_id, trade_id, account_id, asset, amount, metadata, created_at_unix_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (posting_id) DO NOTHING
RETURNING posting_id
"#,
                    &[
                        &posting.posting_id,
                        &request.operation_id,
                        &request.transition.trade_id,
                        &posting.account_id,
                        &posting.asset,
                        &posting.amount,
                        &posting.metadata,
                        &updated_at,
                    ],
                )
                .await?;
            if inserted.is_none() {
                return Err(StoreError::LedgerPostingConflict {
                    posting_id: posting.posting_id.clone(),
                }
                .into());
            }
        }

        for event in &request.outbox_events {
            let occurred_at = i64::try_from(event.occurred_at_unix_ms).map_err(|_| {
                StorageError::TradeProtocol("outbox timestamp is too large".to_string())
            })?;
            let inserted = transaction
                .query_opt(
                    r#"
INSERT INTO dbproxy_outbox
    (event_id, operation_id, trade_id, topic, partition_key, payload, occurred_at_unix_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (event_id) DO NOTHING
RETURNING event_id
"#,
                    &[
                        &event.event_id,
                        &request.operation_id,
                        &request.transition.trade_id,
                        &event.topic,
                        &event.partition_key,
                        &event.payload,
                        &occurred_at,
                    ],
                )
                .await?;
            if inserted.is_none() {
                return Err(StoreError::OutboxEventConflict {
                    event_id: event.event_id.clone(),
                }
                .into());
            }
        }

        transaction.commit().await?;
        let records = request
            .writes
            .iter()
            .zip(revisions)
            .map(|(write, new_revision)| TransactionRecordReceipt {
                record: write.record.clone(),
                new_revision,
            })
            .collect::<Vec<_>>();
        Ok(TradeTransactionOutcome::Applied(TradeReceipt {
            operation_id: request.operation_id,
            trade_id: request.transition.trade_id,
            new_trade_version,
            state: request.transition.next_state,
            records,
            ledger_posting_ids: request
                .ledger_postings
                .into_iter()
                .map(|posting| posting.posting_id)
                .collect(),
            outbox_event_ids: request
                .outbox_events
                .into_iter()
                .map(|event| event.event_id)
                .collect(),
            result: request.result,
        }))
    }
}

#[async_trait]
impl AsyncTradeStore for TieredSnapshotStore {
    type Error = StorageError;

    async fn load_trade(&self, trade_id: &str) -> Result<Option<TradeEnvelope>, Self::Error> {
        self.postgres.load_trade(trade_id).await
    }

    async fn load_trade_receipt(
        &self,
        operation_id: &str,
        trade_id: &str,
    ) -> Result<Option<TradeReceipt>, Self::Error> {
        self.postgres
            .load_trade_receipt(operation_id, trade_id)
            .await
    }

    async fn apply_trade(
        &mut self,
        request: TradeTransaction,
    ) -> Result<TradeTransactionOutcome, Self::Error> {
        let committed_writes = request.writes.clone();
        let outcome = self.postgres.apply_trade(request).await?;
        let snapshots = if matches!(&outcome, TradeTransactionOutcome::Duplicate(_)) {
            let records = committed_writes
                .iter()
                .map(|write| write.record.clone())
                .collect::<Vec<_>>();
            self.postgres
                .load_multi(&records)
                .await?
                .into_iter()
                .zip(records)
                .map(|(snapshot, record)| {
                    snapshot.ok_or(StorageError::MissingAfterWrite { record })
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            let revisions = outcome
                .receipt()
                .records
                .iter()
                .map(|record| (record.record.clone(), record.new_revision))
                .collect::<std::collections::HashMap<_, _>>();
            committed_writes
                .iter()
                .map(|write| {
                    let revision = revisions.get(&write.record).copied().ok_or_else(|| {
                        StorageError::PersistenceProtocol(format!(
                            "trade result is missing {:?}",
                            write.record
                        ))
                    })?;
                    Ok(super::snapshot_from_transactional_write(write, revision))
                })
                .collect::<Result<Vec<_>, StorageError>>()?
        };
        self.synchronize_committed_cache_multi(&snapshots).await;
        Ok(outcome)
    }
}
