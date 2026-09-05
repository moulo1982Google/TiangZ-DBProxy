//! DBProxy 的稳定网络协议和有界帧编码。
//! Stable DBProxy wire protocol and bounded frame codec.
//!
//! 协议只描述通用持久化数据，不允许出现 TiangZ 的 Scene、Entity 或玩法类型。
//! The protocol only carries generic persistence data and must not reference TiangZ scenes,
//! entities, or gameplay types.

use std::{collections::HashSet, io};

use prost::Message;
use thiserror::Error;
use tiangz_dbproxy_core::{
    LedgerPosting as CoreLedgerPosting, OutboxEvent as CoreOutboxEvent, RecordKey as CoreRecordKey,
    Revision, SnapshotEnvelope as CoreSnapshotEnvelope, SnapshotWrite, StoreError,
    TradeEnvelope as CoreTradeEnvelope, TradeReceipt as CoreTradeReceipt,
    TradeState as CoreTradeState, TradeTransaction, TradeTransition as CoreTradeTransition,
    TransactionRecordReceipt, TransactionalRecordWrite, TransactionalWrite,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[cfg(test)]
#[path = "../fingerprint.rs"]
mod fingerprint;

pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/tiangz.dbproxy.v1.rs"));
}

include!(concat!(env!("OUT_DIR"), "/protocol_fingerprint.rs"));

/// 第一版公开网络协议。修改不兼容字段时必须提升版本，而不能只改实现。
/// First public wire version. Incompatible schema changes must increment this value.
pub const PROTOCOL_VERSION: u32 = 2;

/// Protocol v2 fingerprint produced before line endings were canonicalized. Servers accept this
/// exact alias during the rolling migration and echo it to legacy clients; other fingerprints
/// remain incompatible.
pub const LEGACY_PROTOCOL_FINGERPRINT_V2: &str =
    "d20f64198cedce3fd673708a08a9700c230d5a7aecc3d2ede47e4701143e4f1f";

/// Exact pre-CommitRecords schema; old clients retain their original RPC semantics.
pub const PRE_COMMIT_PROTOCOL_FINGERPRINT_V2: &str =
    "a5296d1ef9b288fcbd7f43ac9328c3a59187bf5e7ffdb31972d333a289509456";

pub const PRE_RELAY_PROTOCOL_FINGERPRINT_V2: &str =
    "63894bf08f30464ba807fbfe6507bbd74a0aa30de8da1913d7857904f52e3e17";

pub fn is_compatible_protocol_fingerprint(candidate: &str) -> bool {
    candidate == PROTOCOL_FINGERPRINT
        || candidate == LEGACY_PROTOCOL_FINGERPRINT_V2
        || candidate == PRE_COMMIT_PROTOCOL_FINGERPRINT_V2
        || candidate == PRE_RELAY_PROTOCOL_FINGERPRINT_V2
}

impl From<&tiangz_dbproxy_core::AppendRecord> for wire::AppendRecord {
    fn from(value: &tiangz_dbproxy_core::AppendRecord) -> Self {
        Self {
            record: Some((&value.record).into()),
            schema: value.schema.clone(),
            schema_version: value.schema_version,
            payload: value.payload.clone(),
            occurred_at_unix_ms: value.occurred_at_unix_ms,
        }
    }
}

impl TryFrom<wire::AppendRecord> for tiangz_dbproxy_core::AppendRecord {
    type Error = ProtocolError;
    fn try_from(value: wire::AppendRecord) -> Result<Self, Self::Error> {
        if value.schema.trim().is_empty() || value.schema.len() > MAX_SCHEMA_BYTES {
            return Err(ProtocolError::InvalidField("append.schema"));
        }
        Ok(Self {
            record: value
                .record
                .ok_or(ProtocolError::MissingField("append.record"))?
                .try_into()?,
            schema: value.schema,
            schema_version: value.schema_version,
            payload: value.payload,
            occurred_at_unix_ms: value.occurred_at_unix_ms,
        })
    }
}

/// 默认单帧上限；业务快照超过该值应拆分领域记录，而不是无限放大网络缓冲。
/// Default frame limit; larger snapshots should be split by domain instead of growing buffers.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
/// Default application-level limit for one binary payload or transaction result.
/// This is deliberately lower than the frame limit so storage/WAL/cache amplification is bounded.
pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
pub const MAX_AUTH_TOKEN_BYTES: usize = 512;
pub const MAX_CLIENT_NAME_BYTES: usize = 128;
pub const MAX_NAMESPACE_BYTES: usize = 128;
pub const MAX_RECORD_KEY_BYTES: usize = 512;
pub const MAX_SCHEMA_BYTES: usize = 256;
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 256;
pub const MAX_TRANSACTION_RECORDS: usize = 256;
/// One batch is intentionally smaller than a transaction limit so a single read cannot monopolize a connection.
pub const MAX_BATCH_LOAD_RECORDS: usize = 64;
/// Ordinary snapshot batches are bounded independently from atomic multi-record transactions.
pub const MAX_BATCH_SNAPSHOT_WRITES: usize = 64;
pub const MAX_TRADE_ID_BYTES: usize = 256;
pub const MAX_LEDGER_POSTINGS: usize = 512;
pub const MAX_LEDGER_FIELD_BYTES: usize = 256;
pub const MAX_OUTBOX_EVENTS: usize = 64;
pub const MAX_OUTBOX_TOPIC_BYTES: usize = 128;
pub const MAX_OUTBOX_PARTITION_KEY_BYTES: usize = 512;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("frame length {length} is outside 1..={maximum}")]
    InvalidFrameLength { length: usize, maximum: usize },
    #[error("protobuf decode error: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("missing protocol field: {0}")]
    MissingField(&'static str),
    #[error("invalid protocol field: {0}")]
    InvalidField(&'static str),
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// 读取一个大端四字节长度前缀帧；干净关闭返回 `None`，半帧或超限返回错误。
/// Read one big-endian u32 length-prefixed frame; clean EOF returns `None`.
pub async fn read_message<R, M>(reader: &mut R, maximum: usize) -> Result<Option<M>, ProtocolError>
where
    R: AsyncRead + Unpin,
    M: Message + Default,
{
    let mut length_bytes = [0_u8; 4];
    let read = reader.read(&mut length_bytes[..1]).await?;
    if read == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut length_bytes[1..]).await?;
    let length = u32::from_be_bytes(length_bytes) as usize;
    if !(1..=maximum).contains(&length) {
        return Err(ProtocolError::InvalidFrameLength { length, maximum });
    }
    let mut payload = vec![0_u8; length];
    reader.read_exact(&mut payload).await?;
    Ok(Some(M::decode(payload.as_slice())?))
}

/// 写入一个有界长度前缀帧；编码后再校验，禁止在调用侧绕过上限。
/// Write one bounded length-prefixed frame; the encoded payload is always checked here.
pub async fn write_message<W, M>(
    writer: &mut W,
    message: &M,
    maximum: usize,
) -> Result<(), ProtocolError>
where
    W: AsyncWrite + Unpin,
    M: Message,
{
    let payload = message.encode_to_vec();
    if !(1..=maximum).contains(&payload.len()) {
        return Err(ProtocolError::InvalidFrameLength {
            length: payload.len(),
            maximum,
        });
    }
    let length = u32::try_from(payload.len()).map_err(|_| ProtocolError::InvalidFrameLength {
        length: payload.len(),
        maximum,
    })?;
    writer.write_all(&length.to_be_bytes()).await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;
    Ok(())
}

impl From<&CoreRecordKey> for wire::RecordKey {
    fn from(value: &CoreRecordKey) -> Self {
        Self {
            namespace: value.namespace.clone(),
            key: value.key.clone(),
        }
    }
}

impl TryFrom<wire::RecordKey> for CoreRecordKey {
    type Error = ProtocolError;

    fn try_from(value: wire::RecordKey) -> Result<Self, Self::Error> {
        validate_text(&value.namespace, "record.namespace", MAX_NAMESPACE_BYTES)?;
        validate_text(&value.key, "record.key", MAX_RECORD_KEY_BYTES)?;
        Ok(CoreRecordKey::new(value.namespace, value.key)?)
    }
}

impl From<&CoreSnapshotEnvelope> for wire::SnapshotEnvelope {
    fn from(value: &CoreSnapshotEnvelope) -> Self {
        Self {
            record: Some((&value.record).into()),
            schema: value.schema.clone(),
            schema_version: value.schema_version,
            revision: value.revision.0,
            payload: value.payload.clone(),
            updated_at_unix_ms: value.updated_at_unix_ms,
        }
    }
}

impl TryFrom<wire::SnapshotEnvelope> for CoreSnapshotEnvelope {
    type Error = ProtocolError;

    fn try_from(value: wire::SnapshotEnvelope) -> Result<Self, Self::Error> {
        validate_text(&value.schema, "snapshot.schema", MAX_SCHEMA_BYTES)?;
        Ok(Self {
            record: value
                .record
                .ok_or(ProtocolError::MissingField("snapshot.record"))?
                .try_into()?,
            schema: value.schema,
            schema_version: value.schema_version,
            revision: Revision(value.revision),
            payload: value.payload,
            updated_at_unix_ms: value.updated_at_unix_ms,
        })
    }
}

impl From<&SnapshotWrite> for wire::SaveSnapshotRequest {
    fn from(value: &SnapshotWrite) -> Self {
        Self {
            request_id: value.request_id.clone(),
            record: Some((&value.record).into()),
            schema: value.schema.clone(),
            schema_version: value.schema_version,
            payload: value.payload.clone(),
            expected_revision: value.expected_revision.map(|revision| revision.0),
            updated_at_unix_ms: value.updated_at_unix_ms,
        }
    }
}

impl TryFrom<wire::SaveSnapshotRequest> for SnapshotWrite {
    type Error = ProtocolError;

    fn try_from(value: wire::SaveSnapshotRequest) -> Result<Self, Self::Error> {
        validate_text(
            &value.request_id,
            "save_snapshot.request_id",
            MAX_IDEMPOTENCY_KEY_BYTES,
        )?;
        validate_text(&value.schema, "save_snapshot.schema", MAX_SCHEMA_BYTES)?;
        Ok(Self {
            request_id: value.request_id,
            record: value
                .record
                .ok_or(ProtocolError::MissingField("save_snapshot.record"))?
                .try_into()?,
            schema: value.schema,
            schema_version: value.schema_version,
            payload: value.payload,
            expected_revision: value.expected_revision.map(Revision),
            updated_at_unix_ms: value.updated_at_unix_ms,
        })
    }
}

impl From<&TransactionalWrite> for wire::ApplyTransactionRequest {
    fn from(value: &TransactionalWrite) -> Self {
        Self {
            operation_id: value.operation_id.clone(),
            record: Some((&value.record).into()),
            schema: value.schema.clone(),
            schema_version: value.schema_version,
            expected_revision: value.expected_revision.0,
            payload: value.payload.clone(),
            result: value.result.clone(),
            updated_at_unix_ms: value.updated_at_unix_ms,
        }
    }
}

impl TryFrom<wire::ApplyTransactionRequest> for TransactionalWrite {
    type Error = ProtocolError;

    fn try_from(value: wire::ApplyTransactionRequest) -> Result<Self, Self::Error> {
        validate_text(
            &value.operation_id,
            "apply_transaction.operation_id",
            MAX_IDEMPOTENCY_KEY_BYTES,
        )?;
        validate_text(&value.schema, "apply_transaction.schema", MAX_SCHEMA_BYTES)?;
        Ok(Self {
            operation_id: value.operation_id,
            record: value
                .record
                .ok_or(ProtocolError::MissingField("apply_transaction.record"))?
                .try_into()?,
            schema: value.schema,
            schema_version: value.schema_version,
            expected_revision: Revision(value.expected_revision),
            payload: value.payload,
            result: value.result,
            updated_at_unix_ms: value.updated_at_unix_ms,
        })
    }
}

impl From<&TransactionalRecordWrite> for wire::TransactionalRecordWrite {
    fn from(value: &TransactionalRecordWrite) -> Self {
        Self {
            record: Some((&value.record).into()),
            schema: value.schema.clone(),
            schema_version: value.schema_version,
            expected_revision: value.expected_revision.0,
            payload: value.payload.clone(),
            updated_at_unix_ms: value.updated_at_unix_ms,
        }
    }
}

impl TryFrom<wire::TransactionalRecordWrite> for TransactionalRecordWrite {
    type Error = ProtocolError;

    fn try_from(value: wire::TransactionalRecordWrite) -> Result<Self, Self::Error> {
        validate_text(
            &value.schema,
            "transactional_record.schema",
            MAX_SCHEMA_BYTES,
        )?;
        Ok(Self {
            record: value
                .record
                .ok_or(ProtocolError::MissingField("transactional_record.record"))?
                .try_into()?,
            schema: value.schema,
            schema_version: value.schema_version,
            expected_revision: Revision(value.expected_revision),
            payload: value.payload,
            updated_at_unix_ms: value.updated_at_unix_ms,
        })
    }
}

impl From<CoreTradeState> for wire::TradeState {
    fn from(value: CoreTradeState) -> Self {
        match value {
            CoreTradeState::Proposed => Self::Proposed,
            CoreTradeState::Escrowed => Self::Escrowed,
            CoreTradeState::Settled => Self::Settled,
            CoreTradeState::Cancelled => Self::Cancelled,
        }
    }
}

fn core_trade_state(value: i32) -> Result<CoreTradeState, ProtocolError> {
    match wire::TradeState::try_from(value).ok() {
        Some(wire::TradeState::Proposed) => Ok(CoreTradeState::Proposed),
        Some(wire::TradeState::Escrowed) => Ok(CoreTradeState::Escrowed),
        Some(wire::TradeState::Settled) => Ok(CoreTradeState::Settled),
        Some(wire::TradeState::Cancelled) => Ok(CoreTradeState::Cancelled),
        Some(wire::TradeState::Unspecified) | None => {
            Err(ProtocolError::InvalidField("trade.state"))
        }
    }
}

impl From<&CoreTradeEnvelope> for wire::TradeEnvelope {
    fn from(value: &CoreTradeEnvelope) -> Self {
        Self {
            trade_id: value.trade_id.clone(),
            version: value.version.0,
            state: wire::TradeState::from(value.state) as i32,
            payload: value.payload.clone(),
            updated_at_unix_ms: value.updated_at_unix_ms,
        }
    }
}

impl TryFrom<wire::TradeEnvelope> for CoreTradeEnvelope {
    type Error = ProtocolError;

    fn try_from(value: wire::TradeEnvelope) -> Result<Self, Self::Error> {
        validate_text(&value.trade_id, "trade.trade_id", MAX_TRADE_ID_BYTES)?;
        Ok(Self {
            trade_id: value.trade_id,
            version: Revision(value.version),
            state: core_trade_state(value.state)?,
            payload: value.payload,
            updated_at_unix_ms: value.updated_at_unix_ms,
        })
    }
}

impl From<&CoreTradeTransition> for wire::TradeTransition {
    fn from(value: &CoreTradeTransition) -> Self {
        Self {
            trade_id: value.trade_id.clone(),
            expected_version: value.expected_version.0,
            expected_state: value
                .expected_state
                .map(|state| wire::TradeState::from(state) as i32),
            next_state: wire::TradeState::from(value.next_state) as i32,
            payload: value.payload.clone(),
            updated_at_unix_ms: value.updated_at_unix_ms,
        }
    }
}

impl TryFrom<wire::TradeTransition> for CoreTradeTransition {
    type Error = ProtocolError;

    fn try_from(value: wire::TradeTransition) -> Result<Self, Self::Error> {
        validate_text(
            &value.trade_id,
            "trade_transition.trade_id",
            MAX_TRADE_ID_BYTES,
        )?;
        Ok(Self {
            trade_id: value.trade_id,
            expected_version: Revision(value.expected_version),
            expected_state: value.expected_state.map(core_trade_state).transpose()?,
            next_state: core_trade_state(value.next_state)?,
            payload: value.payload,
            updated_at_unix_ms: value.updated_at_unix_ms,
        })
    }
}

impl From<&CoreLedgerPosting> for wire::LedgerPosting {
    fn from(value: &CoreLedgerPosting) -> Self {
        Self {
            posting_id: value.posting_id.clone(),
            account_id: value.account_id.clone(),
            asset: value.asset.clone(),
            amount: value.amount,
            metadata: value.metadata.clone(),
        }
    }
}

impl TryFrom<wire::LedgerPosting> for CoreLedgerPosting {
    type Error = ProtocolError;

    fn try_from(value: wire::LedgerPosting) -> Result<Self, Self::Error> {
        validate_text(
            &value.posting_id,
            "ledger_posting.posting_id",
            MAX_LEDGER_FIELD_BYTES,
        )?;
        validate_text(
            &value.account_id,
            "ledger_posting.account_id",
            MAX_LEDGER_FIELD_BYTES,
        )?;
        validate_text(&value.asset, "ledger_posting.asset", MAX_LEDGER_FIELD_BYTES)?;
        Ok(Self {
            posting_id: value.posting_id,
            account_id: value.account_id,
            asset: value.asset,
            amount: value.amount,
            metadata: value.metadata,
        })
    }
}

impl From<&CoreOutboxEvent> for wire::OutboxEvent {
    fn from(value: &CoreOutboxEvent) -> Self {
        Self {
            event_id: value.event_id.clone(),
            topic: value.topic.clone(),
            partition_key: value.partition_key.clone(),
            payload: value.payload.clone(),
            occurred_at_unix_ms: value.occurred_at_unix_ms,
        }
    }
}

impl TryFrom<wire::OutboxEvent> for CoreOutboxEvent {
    type Error = ProtocolError;

    fn try_from(value: wire::OutboxEvent) -> Result<Self, Self::Error> {
        validate_text(
            &value.event_id,
            "outbox_event.event_id",
            MAX_IDEMPOTENCY_KEY_BYTES,
        )?;
        validate_text(&value.topic, "outbox_event.topic", MAX_OUTBOX_TOPIC_BYTES)?;
        validate_text(
            &value.partition_key,
            "outbox_event.partition_key",
            MAX_OUTBOX_PARTITION_KEY_BYTES,
        )?;
        if !value
            .topic
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(ProtocolError::InvalidField("outbox_event.topic"));
        }
        Ok(Self {
            event_id: value.event_id,
            topic: value.topic,
            partition_key: value.partition_key,
            payload: value.payload,
            occurred_at_unix_ms: value.occurred_at_unix_ms,
        })
    }
}

impl From<&TradeTransaction> for wire::ApplyTradeTransactionRequest {
    fn from(value: &TradeTransaction) -> Self {
        Self {
            operation_id: value.operation_id.clone(),
            transition: Some((&value.transition).into()),
            writes: value.writes.iter().map(Into::into).collect(),
            ledger_postings: value.ledger_postings.iter().map(Into::into).collect(),
            outbox_events: value.outbox_events.iter().map(Into::into).collect(),
            result: value.result.clone(),
        }
    }
}

impl TryFrom<wire::ApplyTradeTransactionRequest> for TradeTransaction {
    type Error = ProtocolError;

    fn try_from(value: wire::ApplyTradeTransactionRequest) -> Result<Self, Self::Error> {
        validate_text(
            &value.operation_id,
            "apply_trade_transaction.operation_id",
            MAX_IDEMPOTENCY_KEY_BYTES,
        )?;
        if value.writes.len() > MAX_TRANSACTION_RECORDS {
            return Err(ProtocolError::InvalidField(
                "apply_trade_transaction.writes",
            ));
        }
        if value.ledger_postings.len() > MAX_LEDGER_POSTINGS {
            return Err(ProtocolError::InvalidField(
                "apply_trade_transaction.ledger_postings",
            ));
        }
        if value.outbox_events.len() > MAX_OUTBOX_EVENTS {
            return Err(ProtocolError::InvalidField(
                "apply_trade_transaction.outbox_events",
            ));
        }
        Ok(Self {
            operation_id: value.operation_id,
            transition: value
                .transition
                .ok_or(ProtocolError::MissingField(
                    "apply_trade_transaction.transition",
                ))?
                .try_into()?,
            writes: value
                .writes
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            ledger_postings: value
                .ledger_postings
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            outbox_events: value
                .outbox_events
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            result: value.result,
        })
    }
}

impl From<&CoreTradeReceipt> for wire::TradeReceipt {
    fn from(value: &CoreTradeReceipt) -> Self {
        Self {
            operation_id: value.operation_id.clone(),
            trade_id: value.trade_id.clone(),
            new_trade_version: value.new_trade_version.0,
            state: wire::TradeState::from(value.state) as i32,
            records: value
                .records
                .iter()
                .map(|record| wire::MultiTransactionRecordReceipt {
                    record: Some((&record.record).into()),
                    new_revision: record.new_revision.0,
                })
                .collect(),
            ledger_posting_ids: value.ledger_posting_ids.clone(),
            outbox_event_ids: value.outbox_event_ids.clone(),
            result: value.result.clone(),
        }
    }
}

impl TryFrom<wire::TradeReceipt> for CoreTradeReceipt {
    type Error = ProtocolError;

    fn try_from(value: wire::TradeReceipt) -> Result<Self, Self::Error> {
        validate_text(
            &value.operation_id,
            "trade_receipt.operation_id",
            MAX_IDEMPOTENCY_KEY_BYTES,
        )?;
        validate_text(
            &value.trade_id,
            "trade_receipt.trade_id",
            MAX_TRADE_ID_BYTES,
        )?;
        if value.new_trade_version == 0 {
            return Err(ProtocolError::InvalidField(
                "trade_receipt.new_trade_version",
            ));
        }
        if value.records.is_empty() || value.records.len() > MAX_TRANSACTION_RECORDS {
            return Err(ProtocolError::InvalidField("trade_receipt.records"));
        }
        if value.ledger_posting_ids.len() > MAX_LEDGER_POSTINGS {
            return Err(ProtocolError::InvalidField(
                "trade_receipt.ledger_posting_ids",
            ));
        }
        if value.outbox_event_ids.len() > MAX_OUTBOX_EVENTS {
            return Err(ProtocolError::InvalidField(
                "trade_receipt.outbox_event_ids",
            ));
        }
        for id in &value.ledger_posting_ids {
            validate_text(
                id,
                "trade_receipt.ledger_posting_id",
                MAX_LEDGER_FIELD_BYTES,
            )?;
        }
        for id in &value.outbox_event_ids {
            validate_text(
                id,
                "trade_receipt.outbox_event_id",
                MAX_IDEMPOTENCY_KEY_BYTES,
            )?;
        }
        if value
            .ledger_posting_ids
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>()
            .len()
            != value.ledger_posting_ids.len()
            || value
                .outbox_event_ids
                .iter()
                .map(String::as_str)
                .collect::<HashSet<_>>()
                .len()
                != value.outbox_event_ids.len()
        {
            return Err(ProtocolError::InvalidField("trade_receipt.identifiers"));
        }
        let records = value
            .records
            .into_iter()
            .map(|record| {
                if record.new_revision == 0 {
                    return Err(ProtocolError::InvalidField(
                        "trade_receipt.record.new_revision",
                    ));
                }
                Ok(TransactionRecordReceipt {
                    record: record
                        .record
                        .ok_or(ProtocolError::MissingField("trade_receipt.record"))?
                        .try_into()?,
                    new_revision: Revision(record.new_revision),
                })
            })
            .collect::<Result<Vec<_>, ProtocolError>>()?;
        if records
            .iter()
            .map(|record| &record.record)
            .collect::<HashSet<_>>()
            .len()
            != records.len()
        {
            return Err(ProtocolError::InvalidField("trade_receipt.records"));
        }
        Ok(Self {
            operation_id: value.operation_id,
            trade_id: value.trade_id,
            new_trade_version: Revision(value.new_trade_version),
            state: core_trade_state(value.state)?,
            records,
            ledger_posting_ids: value.ledger_posting_ids,
            outbox_event_ids: value.outbox_event_ids,
            result: value.result,
        })
    }
}

fn validate_text(value: &str, field: &'static str, maximum: usize) -> Result<(), ProtocolError> {
    if value.trim().is_empty() || value.len() > maximum {
        return Err(ProtocolError::InvalidField(field));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn frame_round_trip_preserves_message() {
        let (mut writer, mut reader) = duplex(4096);
        let expected = wire::ClientFrame {
            body: Some(wire::client_frame::Body::Hello(wire::ClientHello {
                protocol_version: PROTOCOL_VERSION,
                protocol_fingerprint: PROTOCOL_FINGERPRINT.to_string(),
                auth_token: "test-token".to_string(),
                client_name: "protocol-test".to_string(),
            })),
        };
        write_message(&mut writer, &expected, DEFAULT_MAX_FRAME_BYTES)
            .await
            .unwrap();
        let actual = read_message::<_, wire::ClientFrame>(&mut reader, DEFAULT_MAX_FRAME_BYTES)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn oversized_frame_is_rejected_before_allocation() {
        let (mut writer, mut reader) = duplex(16);
        writer.write_all(&1024_u32.to_be_bytes()).await.unwrap();
        let error = read_message::<_, wire::ClientFrame>(&mut reader, 64)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ProtocolError::InvalidFrameLength {
                length: 1024,
                maximum: 64
            }
        ));
    }

    #[test]
    fn protocol_fingerprint_compatibility_is_explicit_and_bounded() {
        assert!(is_compatible_protocol_fingerprint(PROTOCOL_FINGERPRINT));
        assert!(is_compatible_protocol_fingerprint(
            LEGACY_PROTOCOL_FINGERPRINT_V2
        ));
        assert!(!is_compatible_protocol_fingerprint("unknown"));
    }

    #[test]
    fn oversized_record_key_is_rejected_during_conversion() {
        let error = CoreRecordKey::try_from(wire::RecordKey {
            namespace: "player".to_string(),
            key: "x".repeat(MAX_RECORD_KEY_BYTES + 1),
        })
        .unwrap_err();
        assert!(matches!(error, ProtocolError::InvalidField("record.key")));
    }

    #[test]
    fn trade_receipt_rejects_duplicate_untrusted_identifiers() {
        let receipt = wire::TradeReceipt {
            operation_id: "operation-1".to_string(),
            trade_id: "trade-1".to_string(),
            new_trade_version: 1,
            state: wire::TradeState::Escrowed as i32,
            records: vec![wire::MultiTransactionRecordReceipt {
                record: Some(wire::RecordKey {
                    namespace: "wallet".to_string(),
                    key: "buyer".to_string(),
                }),
                new_revision: 1,
            }],
            ledger_posting_ids: vec!["posting-1".to_string(), "posting-1".to_string()],
            outbox_event_ids: Vec::new(),
            result: Vec::new(),
        };
        assert!(matches!(
            CoreTradeReceipt::try_from(receipt),
            Err(ProtocolError::InvalidField("trade_receipt.identifiers"))
        ));
    }
}
