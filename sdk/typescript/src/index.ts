/**
 * DBProxy的运行时无关TypeScript契约。Transport负责物理I/O，SDK负责稳定类型、
 * 参数校验和防御性复制；本层不得出现任何游戏业务类型。
 *
 * Runtime-neutral TypeScript contract for DBProxy. A Transport owns physical
 * I/O while this SDK owns stable types, validation, and defensive copies. Game
 * domain types must never enter this layer.
 */

export {
  DBPROXY_PROTOCOL_FINGERPRINT,
  DBPROXY_PROTOCOL_VERSION,
} from "./protocol-lock.js";

const UINT64_MAX = 0xffff_ffff_ffff_ffffn;
const MAX_NAMESPACE_BYTES = 128;
const MAX_RECORD_KEY_BYTES = 512;
const MAX_SCHEMA_BYTES = 256;
const MAX_IDEMPOTENCY_KEY_BYTES = 256;

export enum DbProxyErrorCode {
  InvalidRequest = 1001,
  Unauthorized = 1002,
  ProtocolMismatch = 1003,
  RevisionConflict = 2001,
  IdempotencyConflict = 2002,
  OperationConflict = 2003,
  StorageUnavailable = 3001,
  Internal = 9000,
}

export type DbProxyWriteDisposition = "applied" | "duplicate";

export interface DbProxyRecordKey {
  readonly namespace: string;
  readonly key: string;
}

export interface DbProxySnapshotEnvelope {
  readonly record: DbProxyRecordKey;
  readonly schema: string;
  readonly schemaVersion: number;
  readonly revision: bigint;
  readonly payload: Uint8Array;
  readonly updatedAtUnixMs: bigint;
}

export interface DbProxySnapshotWrite {
  readonly requestId: string;
  readonly record: DbProxyRecordKey;
  readonly schema: string;
  readonly schemaVersion: number;
  readonly payload: Uint8Array;
  /** undefined表示无条件写，0n表示只允许首次创建。 / undefined is unconditional; 0n is create-only. */
  readonly expectedRevision?: bigint;
  readonly updatedAtUnixMs: bigint;
}

export interface DbProxySnapshotWriteResult {
  readonly disposition: DbProxyWriteDisposition;
  readonly revision: bigint;
}

export interface DbProxyTransactionalWrite {
  readonly operationId: string;
  readonly record: DbProxyRecordKey;
  readonly schema: string;
  readonly schemaVersion: number;
  readonly expectedRevision: bigint;
  readonly payload: Uint8Array;
  readonly result: Uint8Array;
  readonly updatedAtUnixMs: bigint;
}

export interface DbProxyTransactionalWriteResult {
  readonly disposition: DbProxyWriteDisposition;
  readonly newRevision: bigint;
  readonly result: Uint8Array;
}

export interface DbProxyTransactionReceipt {
  readonly operationId: string;
  readonly record: DbProxyRecordKey;
  readonly newRevision: bigint;
  readonly result: Uint8Array;
}

/**
 * 每个宿主实现一个Transport。实现必须保留DBProxy的ACK语义，不能把Enqueue成功
 * 解释成PostgreSQL已经提交，也不能在超时后复用状态不明的连接。
 *
 * Implemented once per host runtime. It must preserve DBProxy ACK semantics:
 * enqueue success is not a PostgreSQL commit, and a timed-out connection must
 * not be reused when its response boundary is unknown.
 */
export interface DbProxyTransport {
  load(record: DbProxyRecordKey): Promise<DbProxySnapshotEnvelope | undefined>;
  save(write: DbProxySnapshotWrite): Promise<DbProxySnapshotWriteResult>;
  enqueueSnapshot(write: DbProxySnapshotWrite): Promise<void>;
  applyTransaction(
    write: DbProxyTransactionalWrite,
  ): Promise<DbProxyTransactionalWriteResult>;
  loadTransaction(
    operationId: string,
    record: DbProxyRecordKey,
  ): Promise<DbProxyTransactionReceipt | undefined>;
}

export class DbProxyRemoteError extends Error {
  readonly code: DbProxyErrorCode;
  readonly actualRevision: bigint | undefined;

  constructor(code: DbProxyErrorCode, message: string, actualRevision?: bigint) {
    super(message);
    this.name = "DbProxyRemoteError";
    this.code = code;
    this.actualRevision = actualRevision;
  }
}

/**
 * 提供统一校验和所有权边界。业务重试必须复用原requestId/operationId；SDK不会
 * 偷偷生成新幂等键，否则无法判断上一次请求是否已经提交。
 *
 * Provides one validation and ownership boundary. Business retries must reuse
 * the original requestId/operationId; the SDK never invents a new id because
 * the previous request may already have committed.
 */
export class DbProxyClient {
  constructor(private readonly transport: DbProxyTransport) {}

  Load(record: DbProxyRecordKey): Promise<DbProxySnapshotEnvelope | undefined> {
    return this.transport.load(cloneRecordKey(record)).then((snapshot) =>
      snapshot ? cloneSnapshot(snapshot) : undefined
    );
  }

  Save(write: DbProxySnapshotWrite): Promise<DbProxySnapshotWriteResult> {
    return this.transport.save(cloneSnapshotWrite(write));
  }

  EnqueueSnapshot(write: DbProxySnapshotWrite): Promise<void> {
    if (write.expectedRevision !== undefined) {
      throw new TypeError("queued snapshots cannot carry expectedRevision");
    }
    return this.transport.enqueueSnapshot(cloneSnapshotWrite(write));
  }

  ApplyTransaction(
    write: DbProxyTransactionalWrite,
  ): Promise<DbProxyTransactionalWriteResult> {
    return this.transport.applyTransaction(cloneTransactionalWrite(write)).then((result) => ({
      disposition: result.disposition,
      newRevision: requireUint64(result.newRevision, "transaction.newRevision"),
      result: copyBytes(result.result),
    }));
  }

  LoadTransaction(
    operationId: string,
    record: DbProxyRecordKey,
  ): Promise<DbProxyTransactionReceipt | undefined> {
    const stableOperationId = requireText(
      operationId,
      "transaction.operationId",
      MAX_IDEMPOTENCY_KEY_BYTES,
    );
    const stableRecord = cloneRecordKey(record);
    return this.transport.loadTransaction(stableOperationId, stableRecord).then((receipt) =>
      receipt ? cloneTransactionReceipt(receipt) : undefined
    );
  }
}

function cloneRecordKey(record: DbProxyRecordKey): DbProxyRecordKey {
  return {
    namespace: requireText(record.namespace, "record.namespace", MAX_NAMESPACE_BYTES),
    key: requireText(record.key, "record.key", MAX_RECORD_KEY_BYTES),
  };
}

function cloneSnapshot(snapshot: DbProxySnapshotEnvelope): DbProxySnapshotEnvelope {
  return {
    record: cloneRecordKey(snapshot.record),
    schema: requireText(snapshot.schema, "snapshot.schema", MAX_SCHEMA_BYTES),
    schemaVersion: requireUint32(snapshot.schemaVersion, "snapshot.schemaVersion"),
    revision: requireUint64(snapshot.revision, "snapshot.revision"),
    payload: copyBytes(snapshot.payload),
    updatedAtUnixMs: requireUint64(snapshot.updatedAtUnixMs, "snapshot.updatedAtUnixMs"),
  };
}

function cloneSnapshotWrite(write: DbProxySnapshotWrite): DbProxySnapshotWrite {
  return {
    requestId: requireText(
      write.requestId,
      "write.requestId",
      MAX_IDEMPOTENCY_KEY_BYTES,
    ),
    record: cloneRecordKey(write.record),
    schema: requireText(write.schema, "write.schema", MAX_SCHEMA_BYTES),
    schemaVersion: requireUint32(write.schemaVersion, "write.schemaVersion"),
    payload: copyBytes(write.payload),
    expectedRevision: write.expectedRevision === undefined
      ? undefined
      : requireUint64(write.expectedRevision, "write.expectedRevision"),
    updatedAtUnixMs: requireUint64(write.updatedAtUnixMs, "write.updatedAtUnixMs"),
  };
}

function cloneTransactionalWrite(
  write: DbProxyTransactionalWrite,
): DbProxyTransactionalWrite {
  return {
    operationId: requireText(
      write.operationId,
      "transaction.operationId",
      MAX_IDEMPOTENCY_KEY_BYTES,
    ),
    record: cloneRecordKey(write.record),
    schema: requireText(write.schema, "transaction.schema", MAX_SCHEMA_BYTES),
    schemaVersion: requireUint32(write.schemaVersion, "transaction.schemaVersion"),
    expectedRevision: requireUint64(
      write.expectedRevision,
      "transaction.expectedRevision",
    ),
    payload: copyBytes(write.payload),
    result: copyBytes(write.result),
    updatedAtUnixMs: requireUint64(
      write.updatedAtUnixMs,
      "transaction.updatedAtUnixMs",
    ),
  };
}

function cloneTransactionReceipt(
  receipt: DbProxyTransactionReceipt,
): DbProxyTransactionReceipt {
  return {
    operationId: requireText(
      receipt.operationId,
      "transactionReceipt.operationId",
      MAX_IDEMPOTENCY_KEY_BYTES,
    ),
    record: cloneRecordKey(receipt.record),
    newRevision: requireUint64(
      receipt.newRevision,
      "transactionReceipt.newRevision",
    ),
    result: copyBytes(receipt.result),
  };
}

function requireText(value: string, name: string, maximumBytes: number): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new TypeError(`${name} must be a non-empty string`);
  }
  if (utf8ByteLength(value) > maximumBytes) {
    throw new RangeError(`${name} exceeds ${maximumBytes} UTF-8 bytes`);
  }
  return value;
}

/**
 * 在裸V8、Node和浏览器中计算一致的UTF-8字节数，不依赖TextEncoder全局对象。
 * 这里只计算协议限长，不能用它代替真正的字符串编码器。
 *
 * Computes a consistent UTF-8 byte length in bare V8, Node, and browsers
 * without requiring a global TextEncoder. This only validates protocol limits
 * and must not be used as a replacement for an actual string encoder.
 */
function utf8ByteLength(value: string): number {
  let length = 0;
  for (let index = 0; index < value.length; index += 1) {
    const codeUnit = value.charCodeAt(index);
    if (codeUnit <= 0x7f) {
      length += 1;
      continue;
    }
    if (codeUnit <= 0x7ff) {
      length += 2;
      continue;
    }
    if (codeUnit >= 0xd800 && codeUnit <= 0xdbff && index + 1 < value.length) {
      const next = value.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        length += 4;
        index += 1;
        continue;
      }
    }
    length += 3;
  }
  return length;
}

function requireUint32(value: number, name: string): number {
  if (!Number.isInteger(value) || value < 0 || value > 0xffff_ffff) {
    throw new RangeError(`${name} must be uint32`);
  }
  return value;
}

function requireUint64(value: bigint, name: string): bigint {
  if (typeof value !== "bigint" || value < 0n || value > UINT64_MAX) {
    throw new RangeError(`${name} must be uint64 bigint`);
  }
  return value;
}

function copyBytes(value: Uint8Array): Uint8Array {
  if (!(value instanceof Uint8Array)) {
    throw new TypeError("DBProxy payload must be Uint8Array");
  }
  return value.slice();
}
