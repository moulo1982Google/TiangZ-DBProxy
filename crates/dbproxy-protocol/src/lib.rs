//! DBProxy 的稳定网络协议和有界帧编码。
//! Stable DBProxy wire protocol and bounded frame codec.
//!
//! 协议只描述通用持久化数据，不允许出现 TiangZ 的 Scene、Entity 或玩法类型。
//! The protocol only carries generic persistence data and must not reference TiangZ scenes,
//! entities, or gameplay types.

use std::io;

use prost::Message;
use thiserror::Error;
use tiangz_dbproxy_core::{
    RecordKey as CoreRecordKey, Revision, SnapshotEnvelope as CoreSnapshotEnvelope, SnapshotWrite,
    StoreError, TransactionalRecordWrite, TransactionalWrite,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/tiangz.dbproxy.v1.rs"));
}

include!(concat!(env!("OUT_DIR"), "/protocol_fingerprint.rs"));

/// 第一版公开网络协议。修改不兼容字段时必须提升版本，而不能只改实现。
/// First public wire version. Incompatible schema changes must increment this value.
pub const PROTOCOL_VERSION: u32 = 1;

/// 默认单帧上限；业务快照超过该值应拆分领域记录，而不是无限放大网络缓冲。
/// Default frame limit; larger snapshots should be split by domain instead of growing buffers.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
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
    fn oversized_record_key_is_rejected_during_conversion() {
        let error = CoreRecordKey::try_from(wire::RecordKey {
            namespace: "player".to_string(),
            key: "x".repeat(MAX_RECORD_KEY_BYTES + 1),
        })
        .unwrap_err();
        assert!(matches!(error, ProtocolError::InvalidField("record.key")));
    }
}
