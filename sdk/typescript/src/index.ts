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
const MAX_TRANSACTION_RECORDS = 256;
const MAX_BATCH_LOAD_RECORDS = 64;
const MAX_BATCH_SNAPSHOT_WRITES = 64;
const MAX_TRADE_ID_BYTES = 256;
const MAX_LEDGER_POSTINGS = 512;
const MAX_LEDGER_FIELD_BYTES = 256;
const MAX_OUTBOX_EVENTS = 64;
const MAX_OUTBOX_TOPIC_BYTES = 128;
const MAX_OUTBOX_PARTITION_KEY_BYTES = 512;
const INT64_MIN = -0x8000_0000_0000_0000n;
const INT64_MAX = 0x7fff_ffff_ffff_ffffn;

export enum DbProxyErrorCode {
  InvalidRequest = 1001,
  Unauthorized = 1002,
  ProtocolMismatch = 1003,
  RevisionConflict = 2001,
  IdempotencyConflict = 2002,
  OperationConflict = 2003,
  TradeConflict = 2004,
  LedgerConflict = 2005,
  OutboxConflict = 2006,
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

export interface DbProxyBatchWriteError {
  readonly code: DbProxyErrorCode;
  readonly message: string;
  readonly actualRevision?: bigint;
}

export type DbProxyBatchSnapshotWriteResult =
  | { readonly ok: true; readonly result: DbProxySnapshotWriteResult }
  | { readonly ok: false; readonly error: DbProxyBatchWriteError };

export type DbProxyBatchSnapshotEnqueueResult =
  | { readonly ok: true }
  | { readonly ok: false; readonly error: DbProxyBatchWriteError };

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

export interface DbProxyTransactionalRecordWrite {
  readonly record: DbProxyRecordKey;
  readonly schema: string;
  readonly schemaVersion: number;
  readonly expectedRevision: bigint;
  readonly payload: Uint8Array;
  readonly updatedAtUnixMs: bigint;
}

export interface DbProxyMultiTransactionalWrite {
  readonly operationId: string;
  readonly writes: readonly DbProxyTransactionalRecordWrite[];
  readonly result: Uint8Array;
}

export interface DbProxyMultiTransactionRecordReceipt {
  readonly record: DbProxyRecordKey;
  readonly newRevision: bigint;
}

export interface DbProxyMultiTransactionalWriteResult {
  readonly disposition: DbProxyWriteDisposition;
  readonly records: readonly DbProxyMultiTransactionRecordReceipt[];
  readonly result: Uint8Array;
}

export interface DbProxyMultiTransactionReceipt {
  readonly operationId: string;
  readonly records: readonly DbProxyMultiTransactionRecordReceipt[];
  readonly result: Uint8Array;
}

export type DbProxyTradeState = "proposed" | "escrowed" | "settled" | "cancelled";

export interface DbProxyTradeEnvelope {
  readonly tradeId: string;
  readonly version: bigint;
  readonly state: DbProxyTradeState;
  readonly payload: Uint8Array;
  readonly updatedAtUnixMs: bigint;
}

export interface DbProxyTradeTransition {
  readonly tradeId: string;
  readonly expectedVersion: bigint;
  readonly expectedState?: DbProxyTradeState;
  readonly nextState: DbProxyTradeState;
  readonly payload: Uint8Array;
  readonly updatedAtUnixMs: bigint;
}

export interface DbProxyLedgerPosting {
  readonly postingId: string;
  readonly accountId: string;
  readonly asset: string;
  readonly amount: bigint;
  readonly metadata: Uint8Array;
}

export interface DbProxyOutboxEvent {
  readonly eventId: string;
  readonly topic: string;
  readonly partitionKey: string;
  readonly payload: Uint8Array;
  readonly occurredAtUnixMs: bigint;
}

export interface DbProxyTradeTransaction {
  readonly operationId: string;
  readonly transition: DbProxyTradeTransition;
  readonly writes: readonly DbProxyTransactionalRecordWrite[];
  readonly ledgerPostings: readonly DbProxyLedgerPosting[];
  readonly outboxEvents: readonly DbProxyOutboxEvent[];
  readonly result: Uint8Array;
}

export interface DbProxyTradeReceipt {
  readonly operationId: string;
  readonly tradeId: string;
  readonly newTradeVersion: bigint;
  readonly state: DbProxyTradeState;
  readonly records: readonly DbProxyMultiTransactionRecordReceipt[];
  readonly ledgerPostingIds: readonly string[];
  readonly outboxEventIds: readonly string[];
  readonly result: Uint8Array;
}

export interface DbProxyTradeTransactionResult {
  readonly disposition: DbProxyWriteDisposition;
  readonly receipt: DbProxyTradeReceipt;
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
  loadMulti(
    records: readonly DbProxyRecordKey[],
  ): Promise<readonly (DbProxySnapshotEnvelope | undefined)[]>;
  save(write: DbProxySnapshotWrite): Promise<DbProxySnapshotWriteResult>;
  saveMulti(
    writes: readonly DbProxySnapshotWrite[],
  ): Promise<readonly DbProxyBatchSnapshotWriteResult[]>;
  enqueueSnapshot(write: DbProxySnapshotWrite): Promise<void>;
  enqueueMultiSnapshot(
    writes: readonly DbProxySnapshotWrite[],
  ): Promise<readonly DbProxyBatchSnapshotEnqueueResult[]>;
  applyTransaction(
    write: DbProxyTransactionalWrite,
  ): Promise<DbProxyTransactionalWriteResult>;
  loadTransaction(
    operationId: string,
    record: DbProxyRecordKey,
  ): Promise<DbProxyTransactionReceipt | undefined>;
  applyMultiTransaction(
    write: DbProxyMultiTransactionalWrite,
  ): Promise<DbProxyMultiTransactionalWriteResult>;
  loadMultiTransaction(
    operationId: string,
    records: readonly DbProxyRecordKey[],
  ): Promise<DbProxyMultiTransactionReceipt | undefined>;
  loadTrade(tradeId: string): Promise<DbProxyTradeEnvelope | undefined>;
  applyTradeTransaction(
    transaction: DbProxyTradeTransaction,
  ): Promise<DbProxyTradeTransactionResult>;
  loadTradeTransaction(
    operationId: string,
    tradeId: string,
  ): Promise<DbProxyTradeReceipt | undefined>;
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

  LoadMulti(
    records: readonly DbProxyRecordKey[],
  ): Promise<readonly (DbProxySnapshotEnvelope | undefined)[]> {
    const stableRecords = cloneBatchLoadRecords(records);
    return this.transport.loadMulti(stableRecords).then((snapshots) => {
      if (!Array.isArray(snapshots) || snapshots.length !== stableRecords.length) {
        throw new TypeError("batch load result count does not match its request");
      }
      return snapshots.map((snapshot, index) => {
        if (!snapshot) return undefined;
        const cloned = cloneSnapshot(snapshot);
        const expected = stableRecords[index];
        if (cloned.record.namespace !== expected.namespace || cloned.record.key !== expected.key) {
          throw new TypeError("batch load snapshot identity does not match its request");
        }
        return cloned;
      });
    });
  }

  Save(write: DbProxySnapshotWrite): Promise<DbProxySnapshotWriteResult> {
    return this.transport.save(cloneSnapshotWrite(write));
  }

  SaveMulti(
    writes: readonly DbProxySnapshotWrite[],
  ): Promise<readonly DbProxyBatchSnapshotWriteResult[]> {
    const stableWrites = cloneBatchSnapshotWrites(writes, false);
    return this.transport.saveMulti(stableWrites).then((results) =>
      cloneBatchWriteResults(results, stableWrites.length)
    );
  }

  EnqueueSnapshot(write: DbProxySnapshotWrite): Promise<void> {
    if (write.expectedRevision !== undefined) {
      throw new TypeError("queued snapshots cannot carry expectedRevision");
    }
    return this.transport.enqueueSnapshot(cloneSnapshotWrite(write));
  }

  EnqueueMultiSnapshot(
    writes: readonly DbProxySnapshotWrite[],
  ): Promise<readonly DbProxyBatchSnapshotEnqueueResult[]> {
    const stableWrites = cloneBatchSnapshotWrites(writes, true);
    return this.transport.enqueueMultiSnapshot(stableWrites).then((results) =>
      cloneBatchEnqueueResults(results, stableWrites.length)
    );
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

  ApplyMultiTransaction(
    write: DbProxyMultiTransactionalWrite,
  ): Promise<DbProxyMultiTransactionalWriteResult> {
    const stable = cloneMultiTransactionalWrite(write);
    return this.transport.applyMultiTransaction(stable).then((result) => ({
      disposition: result.disposition,
      records: result.records.map(cloneMultiTransactionRecordReceipt),
      result: copyBytes(result.result),
    }));
  }

  LoadMultiTransaction(
    operationId: string,
    records: readonly DbProxyRecordKey[],
  ): Promise<DbProxyMultiTransactionReceipt | undefined> {
    const stableOperationId = requireText(
      operationId,
      "multiTransaction.operationId",
      MAX_IDEMPOTENCY_KEY_BYTES,
    );
    const stableRecords = cloneMultiTransactionRecords(records);
    return this.transport
      .loadMultiTransaction(stableOperationId, stableRecords)
      .then((receipt) => receipt ? cloneMultiTransactionReceipt(receipt) : undefined);
  }

  LoadTrade(tradeId: string): Promise<DbProxyTradeEnvelope | undefined> {
    const stableTradeId = requireText(tradeId, "trade.tradeId", MAX_TRADE_ID_BYTES);
    return this.transport.loadTrade(stableTradeId).then((trade) =>
      trade ? cloneTradeEnvelope(trade) : undefined
    );
  }

  ApplyTradeTransaction(
    transaction: DbProxyTradeTransaction,
  ): Promise<DbProxyTradeTransactionResult> {
    const stable = cloneTradeTransaction(transaction);
    return this.transport.applyTradeTransaction(stable).then((result) => {
      if (result.disposition !== "applied" && result.disposition !== "duplicate") {
        throw new TypeError("trade transaction returned an invalid disposition");
      }
      const receipt = cloneTradeReceipt(result.receipt);
      requireTradeReceiptMatches(stable, receipt);
      return { disposition: result.disposition, receipt };
    });
  }

  LoadTradeTransaction(
    operationId: string,
    tradeId: string,
  ): Promise<DbProxyTradeReceipt | undefined> {
    const stableOperationId = requireText(
      operationId,
      "tradeTransaction.operationId",
      MAX_IDEMPOTENCY_KEY_BYTES,
    );
    const stableTradeId = requireText(tradeId, "trade.tradeId", MAX_TRADE_ID_BYTES);
    return this.transport
      .loadTradeTransaction(stableOperationId, stableTradeId)
      .then((receipt) => receipt ? cloneTradeReceipt(receipt) : undefined);
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

function cloneMultiTransactionalWrite(
  write: DbProxyMultiTransactionalWrite,
): DbProxyMultiTransactionalWrite {
  const operationId = requireText(
    write.operationId,
    "multiTransaction.operationId",
    MAX_IDEMPOTENCY_KEY_BYTES,
  );
  if (!Array.isArray(write.writes) || write.writes.length === 0 || write.writes.length > MAX_TRANSACTION_RECORDS) {
    throw new RangeError(`multiTransaction.writes must contain 1..${MAX_TRANSACTION_RECORDS} records`);
  }
  const writes = write.writes.map((item) => cloneMultiTransactionalRecordWrite(item));
  const recordNames = new Set(writes.map((item) => `${item.record.namespace}\u0000${item.record.key}`));
  if (recordNames.size !== writes.length) {
    throw new TypeError("multiTransaction.writes cannot contain duplicate records");
  }
  return {
    operationId,
    writes,
    result: copyBytes(write.result),
  };
}

function cloneMultiTransactionalRecordWrite(
  write: DbProxyTransactionalRecordWrite,
): DbProxyTransactionalRecordWrite {
  return {
    record: cloneRecordKey(write.record),
    schema: requireText(write.schema, "transactionalRecord.schema", MAX_SCHEMA_BYTES),
    schemaVersion: requireUint32(write.schemaVersion, "transactionalRecord.schemaVersion"),
    expectedRevision: requireUint64(
      write.expectedRevision,
      "transactionalRecord.expectedRevision",
    ),
    payload: copyBytes(write.payload),
    updatedAtUnixMs: requireUint64(
      write.updatedAtUnixMs,
      "transactionalRecord.updatedAtUnixMs",
    ),
  };
}

function cloneMultiTransactionRecords(
  records: readonly DbProxyRecordKey[],
): DbProxyRecordKey[] {
  if (!Array.isArray(records) || records.length === 0 || records.length > MAX_TRANSACTION_RECORDS) {
    throw new RangeError(`multiTransaction.records must contain 1..${MAX_TRANSACTION_RECORDS} records`);
  }
  const cloned = records.map(cloneRecordKey);
  const names = new Set(cloned.map((item) => `${item.namespace}\u0000${item.key}`));
  if (names.size !== cloned.length) throw new TypeError("multiTransaction.records contain duplicates");
  return cloned;
}

function cloneBatchLoadRecords(records: readonly DbProxyRecordKey[]): DbProxyRecordKey[] {
  if (!Array.isArray(records) || records.length === 0 || records.length > MAX_BATCH_LOAD_RECORDS) {
    throw new RangeError(`batchLoad.records must contain 1..${MAX_BATCH_LOAD_RECORDS} records`);
  }
  const cloned = records.map(cloneRecordKey);
  const names = new Set(cloned.map((item) => `${item.namespace}\u0000${item.key}`));
  if (names.size !== cloned.length) throw new TypeError("batchLoad.records contain duplicates");
  return cloned;
}

function cloneBatchSnapshotWrites(
  writes: readonly DbProxySnapshotWrite[],
  requireUnconditional: boolean,
): DbProxySnapshotWrite[] {
  if (!Array.isArray(writes) || writes.length === 0 || writes.length > MAX_BATCH_SNAPSHOT_WRITES) {
    throw new RangeError(`batchSnapshot.writes must contain 1..${MAX_BATCH_SNAPSHOT_WRITES} records`);
  }
  const cloned = writes.map(cloneSnapshotWrite);
  const records = new Set(cloned.map((item) => `${item.record.namespace}\u0000${item.record.key}`));
  if (records.size !== cloned.length) {
    throw new TypeError("batchSnapshot.writes cannot contain duplicate records");
  }
  const requestIds = new Set(cloned.map((item) => item.requestId));
  if (requestIds.size !== cloned.length) {
    throw new TypeError("batchSnapshot.writes cannot contain duplicate requestIds");
  }
  if (requireUnconditional && cloned.some((item) => item.expectedRevision !== undefined)) {
    throw new TypeError("queued snapshots cannot carry expectedRevision");
  }
  return cloned;
}

function cloneBatchWriteResults(
  results: readonly DbProxyBatchSnapshotWriteResult[],
  expectedCount: number,
): DbProxyBatchSnapshotWriteResult[] {
  if (!Array.isArray(results) || results.length !== expectedCount) {
    throw new TypeError("batch save result count does not match its request");
  }
  return results.map((entry) => {
    if (entry.ok) {
      return {
        ok: true,
        result: {
          disposition: entry.result.disposition,
          revision: requireUint64(entry.result.revision, "batchSave.result.revision"),
        },
      };
    }
    return { ok: false, error: cloneBatchWriteError(entry.error) };
  });
}

function cloneBatchEnqueueResults(
  results: readonly DbProxyBatchSnapshotEnqueueResult[],
  expectedCount: number,
): DbProxyBatchSnapshotEnqueueResult[] {
  if (!Array.isArray(results) || results.length !== expectedCount) {
    throw new TypeError("batch enqueue result count does not match its request");
  }
  return results.map((entry) => entry.ok
    ? { ok: true }
    : { ok: false, error: cloneBatchWriteError(entry.error) });
}

function cloneBatchWriteError(error: DbProxyBatchWriteError): DbProxyBatchWriteError {
  if (!error || typeof error.message !== "string" || error.message.length === 0) {
    throw new TypeError("batch write error must contain a message");
  }
  if (!Object.values(DbProxyErrorCode).includes(error.code)) {
    throw new TypeError("batch write error contains an invalid code");
  }
  return {
    code: error.code,
    message: error.message,
    actualRevision: error.actualRevision === undefined
      ? undefined
      : requireUint64(error.actualRevision, "batchWrite.error.actualRevision"),
  };
}

function cloneMultiTransactionRecordReceipt(
  receipt: DbProxyMultiTransactionRecordReceipt,
): DbProxyMultiTransactionRecordReceipt {
  return {
    record: cloneRecordKey(receipt.record),
    newRevision: requireUint64(receipt.newRevision, "multiTransactionRecord.newRevision"),
  };
}

function cloneMultiTransactionReceipt(
  receipt: DbProxyMultiTransactionReceipt,
): DbProxyMultiTransactionReceipt {
  const operationId = requireText(
    receipt.operationId,
    "multiTransactionReceipt.operationId",
    MAX_IDEMPOTENCY_KEY_BYTES,
  );
  if (!Array.isArray(receipt.records) || receipt.records.length === 0 || receipt.records.length > MAX_TRANSACTION_RECORDS) {
    throw new RangeError(`multiTransactionReceipt.records must contain 1..${MAX_TRANSACTION_RECORDS} records`);
  }
  return {
    operationId,
    records: receipt.records.map(cloneMultiTransactionRecordReceipt),
    result: copyBytes(receipt.result),
  };
}

function cloneTradeEnvelope(trade: DbProxyTradeEnvelope): DbProxyTradeEnvelope {
  return {
    tradeId: requireText(trade.tradeId, "trade.tradeId", MAX_TRADE_ID_BYTES),
    version: requireUint64(trade.version, "trade.version"),
    state: requireTradeState(trade.state, "trade.state"),
    payload: copyBytes(trade.payload),
    updatedAtUnixMs: requireUint64(trade.updatedAtUnixMs, "trade.updatedAtUnixMs"),
  };
}

function cloneTradeTransaction(
  transaction: DbProxyTradeTransaction,
): DbProxyTradeTransaction {
  const operationId = requireText(
    transaction.operationId,
    "tradeTransaction.operationId",
    MAX_IDEMPOTENCY_KEY_BYTES,
  );
  const transition = cloneTradeTransition(transaction.transition);
  if (!Array.isArray(transaction.writes)
    || transaction.writes.length === 0
    || transaction.writes.length > MAX_TRANSACTION_RECORDS) {
    throw new RangeError(
      `tradeTransaction.writes must contain 1..${MAX_TRANSACTION_RECORDS} records`,
    );
  }
  const writes = transaction.writes.map(cloneMultiTransactionalRecordWrite);
  const records = new Set(
    writes.map((write) => `${write.record.namespace}\u0000${write.record.key}`),
  );
  if (records.size !== writes.length) {
    throw new TypeError("tradeTransaction.writes cannot contain duplicate records");
  }
  if (!Array.isArray(transaction.ledgerPostings)
    || transaction.ledgerPostings.length > MAX_LEDGER_POSTINGS) {
    throw new RangeError(
      `tradeTransaction.ledgerPostings cannot exceed ${MAX_LEDGER_POSTINGS}`,
    );
  }
  const ledgerPostings = transaction.ledgerPostings.map(cloneLedgerPosting);
  const postingIds = new Set(ledgerPostings.map((posting) => posting.postingId));
  if (postingIds.size !== ledgerPostings.length) {
    throw new TypeError("tradeTransaction.ledgerPostings contain duplicate postingIds");
  }
  const balances = new Map<string, bigint>();
  for (const posting of ledgerPostings) {
    balances.set(posting.asset, (balances.get(posting.asset) ?? 0n) + posting.amount);
  }
  for (const [asset, balance] of balances) {
    if (balance !== 0n) {
      throw new RangeError(`tradeTransaction ledger is not balanced for asset ${asset}`);
    }
  }
  if (!Array.isArray(transaction.outboxEvents)
    || transaction.outboxEvents.length > MAX_OUTBOX_EVENTS) {
    throw new RangeError(
      `tradeTransaction.outboxEvents cannot exceed ${MAX_OUTBOX_EVENTS}`,
    );
  }
  const outboxEvents = transaction.outboxEvents.map(cloneOutboxEvent);
  const eventIds = new Set(outboxEvents.map((event) => event.eventId));
  if (eventIds.size !== outboxEvents.length) {
    throw new TypeError("tradeTransaction.outboxEvents contain duplicate eventIds");
  }
  return {
    operationId,
    transition,
    writes,
    ledgerPostings,
    outboxEvents,
    result: copyBytes(transaction.result),
  };
}

function cloneTradeTransition(
  transition: DbProxyTradeTransition,
): DbProxyTradeTransition {
  const expectedVersion = requireUint64(
    transition.expectedVersion,
    "tradeTransition.expectedVersion",
  );
  const expectedState = transition.expectedState === undefined
    ? undefined
    : requireTradeState(transition.expectedState, "tradeTransition.expectedState");
  const nextState = requireTradeState(transition.nextState, "tradeTransition.nextState");
  if ((expectedVersion === 0n) !== (expectedState === undefined)
    || !isValidTradeTransition(expectedState, nextState)) {
    throw new TypeError("tradeTransition contains an illegal state transition");
  }
  return {
    tradeId: requireText(transition.tradeId, "tradeTransition.tradeId", MAX_TRADE_ID_BYTES),
    expectedVersion,
    expectedState,
    nextState,
    payload: copyBytes(transition.payload),
    updatedAtUnixMs: requireUint64(
      transition.updatedAtUnixMs,
      "tradeTransition.updatedAtUnixMs",
    ),
  };
}

function cloneLedgerPosting(posting: DbProxyLedgerPosting): DbProxyLedgerPosting {
  const amount = requireInt64(posting.amount, "ledgerPosting.amount");
  if (amount === 0n) throw new RangeError("ledgerPosting.amount cannot be zero");
  return {
    postingId: requireText(
      posting.postingId,
      "ledgerPosting.postingId",
      MAX_LEDGER_FIELD_BYTES,
    ),
    accountId: requireText(
      posting.accountId,
      "ledgerPosting.accountId",
      MAX_LEDGER_FIELD_BYTES,
    ),
    asset: requireText(posting.asset, "ledgerPosting.asset", MAX_LEDGER_FIELD_BYTES),
    amount,
    metadata: copyBytes(posting.metadata),
  };
}

function cloneOutboxEvent(event: DbProxyOutboxEvent): DbProxyOutboxEvent {
  const topic = requireText(event.topic, "outboxEvent.topic", MAX_OUTBOX_TOPIC_BYTES);
  if (!/^[A-Za-z0-9._-]+$/.test(topic)) {
    throw new TypeError("outboxEvent.topic contains unsupported characters");
  }
  return {
    eventId: requireText(
      event.eventId,
      "outboxEvent.eventId",
      MAX_IDEMPOTENCY_KEY_BYTES,
    ),
    topic,
    partitionKey: requireText(
      event.partitionKey,
      "outboxEvent.partitionKey",
      MAX_OUTBOX_PARTITION_KEY_BYTES,
    ),
    payload: copyBytes(event.payload),
    occurredAtUnixMs: requireUint64(
      event.occurredAtUnixMs,
      "outboxEvent.occurredAtUnixMs",
    ),
  };
}

function cloneTradeReceipt(receipt: DbProxyTradeReceipt): DbProxyTradeReceipt {
  if (!Array.isArray(receipt.records)
    || receipt.records.length === 0
    || receipt.records.length > MAX_TRANSACTION_RECORDS) {
    throw new RangeError(
      `tradeReceipt.records must contain 1..${MAX_TRANSACTION_RECORDS} records`,
    );
  }
  if (!Array.isArray(receipt.ledgerPostingIds)
    || receipt.ledgerPostingIds.length > MAX_LEDGER_POSTINGS
    || !Array.isArray(receipt.outboxEventIds)
    || receipt.outboxEventIds.length > MAX_OUTBOX_EVENTS) {
    throw new RangeError("tradeReceipt identifier counts exceed protocol limits");
  }
  const newTradeVersion = requireUint64(
    receipt.newTradeVersion,
    "tradeReceipt.newTradeVersion",
  );
  if (newTradeVersion === 0n) {
    throw new RangeError("tradeReceipt.newTradeVersion must be greater than zero");
  }
  const records = receipt.records.map(cloneMultiTransactionRecordReceipt);
  if (records.some((record) => record.newRevision === 0n)
    || new Set(records.map((record) =>
      `${record.record.namespace}\u0000${record.record.key}`
    )).size !== records.length) {
    throw new TypeError("tradeReceipt records are invalid or duplicated");
  }
  const ledgerPostingIds = receipt.ledgerPostingIds.map((id) =>
    requireText(id, "tradeReceipt.ledgerPostingId", MAX_LEDGER_FIELD_BYTES)
  );
  const outboxEventIds = receipt.outboxEventIds.map((id) =>
    requireText(id, "tradeReceipt.outboxEventId", MAX_IDEMPOTENCY_KEY_BYTES)
  );
  if (new Set(ledgerPostingIds).size !== ledgerPostingIds.length
    || new Set(outboxEventIds).size !== outboxEventIds.length) {
    throw new TypeError("tradeReceipt identifiers cannot contain duplicates");
  }
  return {
    operationId: requireText(
      receipt.operationId,
      "tradeReceipt.operationId",
      MAX_IDEMPOTENCY_KEY_BYTES,
    ),
    tradeId: requireText(receipt.tradeId, "tradeReceipt.tradeId", MAX_TRADE_ID_BYTES),
    newTradeVersion,
    state: requireTradeState(receipt.state, "tradeReceipt.state"),
    records,
    ledgerPostingIds,
    outboxEventIds,
    result: copyBytes(receipt.result),
  };
}

function requireTradeReceiptMatches(
  transaction: DbProxyTradeTransaction,
  receipt: DbProxyTradeReceipt,
): void {
  if (receipt.operationId !== transaction.operationId
    || receipt.tradeId !== transaction.transition.tradeId
    || receipt.newTradeVersion !== transaction.transition.expectedVersion + 1n
    || receipt.state !== transaction.transition.nextState
    || !bytesEqual(receipt.result, transaction.result)) {
    throw new TypeError("trade transaction receipt does not match the request");
  }

  const expectedRecords = transaction.writes
    .map((write) => ({
      record: write.record,
      newRevision: write.expectedRevision + 1n,
    }))
    .sort(compareRecordReceipts);
  const actualRecords = [...receipt.records].sort(compareRecordReceipts);
  if (expectedRecords.length !== actualRecords.length
    || expectedRecords.some((expected, index) => {
      const actual = actualRecords[index];
      return expected.record.namespace !== actual.record.namespace
        || expected.record.key !== actual.record.key
        || expected.newRevision !== actual.newRevision;
    })) {
    throw new TypeError("trade transaction receipt records do not match the request");
  }

  const expectedPostingIds = transaction.ledgerPostings
    .map((posting) => posting.postingId)
    .sort();
  const actualPostingIds = [...receipt.ledgerPostingIds].sort();
  const expectedEventIds = transaction.outboxEvents.map((event) => event.eventId).sort();
  const actualEventIds = [...receipt.outboxEventIds].sort();
  if (!stringArraysEqual(expectedPostingIds, actualPostingIds)
    || !stringArraysEqual(expectedEventIds, actualEventIds)) {
    throw new TypeError("trade transaction receipt identifiers do not match the request");
  }
}

function compareRecordReceipts(
  left: DbProxyMultiTransactionRecordReceipt,
  right: DbProxyMultiTransactionRecordReceipt,
): number {
  return left.record.namespace.localeCompare(right.record.namespace)
    || left.record.key.localeCompare(right.record.key);
}

function stringArraysEqual(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function requireTradeState(value: string, name: string): DbProxyTradeState {
  if (value !== "proposed"
    && value !== "escrowed"
    && value !== "settled"
    && value !== "cancelled") {
    throw new TypeError(`${name} is not a valid trade state`);
  }
  return value;
}

function isValidTradeTransition(
  from: DbProxyTradeState | undefined,
  to: DbProxyTradeState,
): boolean {
  return (from === undefined && (to === "proposed" || to === "escrowed"))
    || (from === "proposed" && (to === "escrowed" || to === "cancelled"))
    || (from === "escrowed" && (to === "settled" || to === "cancelled"));
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

function requireInt64(value: bigint, name: string): bigint {
  if (typeof value !== "bigint" || value < INT64_MIN || value > INT64_MAX) {
    throw new RangeError(`${name} must be int64 bigint`);
  }
  return value;
}

function copyBytes(value: Uint8Array): Uint8Array {
  if (!(value instanceof Uint8Array)) {
    throw new TypeError("DBProxy payload must be Uint8Array");
  }
  return value.slice();
}
