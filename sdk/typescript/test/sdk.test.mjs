import assert from "node:assert/strict";
import test from "node:test";

import {
  DBPROXY_PROTOCOL_FINGERPRINT,
  DBPROXY_PROTOCOL_VERSION,
  DbProxyClient,
} from "../dist/index.js";

test("protocol lock is generated from the authoritative proto", () => {
  assert.equal(DBPROXY_PROTOCOL_VERSION, 2);
  assert.match(DBPROXY_PROTOCOL_FINGERPRINT, /^[0-9a-f]{64}$/);
});

test("snapshot writes cross the transport boundary as defensive copies", async () => {
  let captured;
  const transport = {
    load: async () => undefined,
    loadMulti: async () => [],
    save: async (write) => {
      captured = write;
      return { disposition: "applied", revision: 1n };
    },
    enqueueSnapshot: async () => undefined,
    applyTransaction: async () => ({
      disposition: "applied",
      newRevision: 1n,
      result: new Uint8Array(),
    }),
    loadTransaction: async () => undefined,
    applyMultiTransaction: async () => ({ disposition: "applied", records: [], result: new Uint8Array() }),
    loadMultiTransaction: async () => undefined,
  };
  const payload = Uint8Array.from([1, 2, 3]);
  const client = new DbProxyClient(transport);

  await client.Save({
    requestId: "save-player-1",
    record: { namespace: "player", key: "1001" },
    schema: "tiangz.player",
    schemaVersion: 1,
    payload,
    expectedRevision: 0n,
    updatedAtUnixMs: 123n,
  });
  payload[0] = 9;

  assert.deepEqual([...captured.payload], [1, 2, 3]);
  assert.equal(captured.expectedRevision, 0n);
});

test("batch load preserves order, missing records, and defensive payload ownership", async () => {
  const payload = Uint8Array.from([1, 2, 3]);
  const records = [
    { namespace: "player", key: "1001:inventory" },
    { namespace: "player", key: "1001:wallet" },
  ];
  const client = new DbProxyClient({
    load: async () => undefined,
    loadMulti: async () => [{
      record: records[0],
      schema: "player.inventory",
      schemaVersion: 1,
      revision: 2n,
      payload,
      updatedAtUnixMs: 10n,
    }, undefined],
    save: async () => ({ disposition: "applied", revision: 1n }),
    enqueueSnapshot: async () => undefined,
    applyTransaction: async () => ({ disposition: "applied", newRevision: 1n, result: new Uint8Array() }),
    loadTransaction: async () => undefined,
    applyMultiTransaction: async () => ({ disposition: "applied", records: [], result: new Uint8Array() }),
    loadMultiTransaction: async () => undefined,
  });
  const snapshots = await client.LoadMulti(records);
  payload[0] = 9;
  assert.equal(snapshots.length, 2);
  assert.deepEqual([...snapshots[0].payload], [1, 2, 3]);
  assert.equal(snapshots[1], undefined);
  assert.throws(() => client.LoadMulti([records[0], records[0]]), /duplicates/);
});

test("batch save preserves per-record outcomes and defensive writes", async () => {
  let captured;
  const payload = Uint8Array.from([1, 2, 3]);
  const client = new DbProxyClient({
    load: async () => undefined,
    loadMulti: async () => [],
    save: async () => ({ disposition: "applied", revision: 1n }),
    saveMulti: async (writes) => {
      captured = writes;
      return [
        { ok: true, result: { disposition: "applied", revision: 2n } },
        { ok: false, error: { code: 2001, message: "revision conflict", actualRevision: 3n } },
      ];
    },
    enqueueSnapshot: async () => undefined,
    enqueueMultiSnapshot: async () => [],
    applyTransaction: async () => ({ disposition: "applied", newRevision: 1n, result: new Uint8Array() }),
    loadTransaction: async () => undefined,
    applyMultiTransaction: async () => ({ disposition: "applied", records: [], result: new Uint8Array() }),
    loadMultiTransaction: async () => undefined,
  });
  const results = await client.SaveMulti([
    {
      requestId: "batch-save-1",
      record: { namespace: "player", key: "1001:wallet" },
      schema: "player.wallet",
      schemaVersion: 1,
      payload,
      expectedRevision: 1n,
      updatedAtUnixMs: 1n,
    },
    {
      requestId: "batch-save-2",
      record: { namespace: "player", key: "1001:items" },
      schema: "player.items",
      schemaVersion: 1,
      payload: new Uint8Array([4]),
      expectedRevision: 2n,
      updatedAtUnixMs: 1n,
    },
  ]);
  payload[0] = 9;
  assert.deepEqual([...captured[0].payload], [1, 2, 3]);
  assert.deepEqual(results[0], { ok: true, result: { disposition: "applied", revision: 2n } });
  assert.deepEqual(results[1], {
    ok: false,
    error: { code: 2001, message: "revision conflict", actualRevision: 3n },
  });
});

test("queued snapshots reject CAS because ACK only means backlog accepted", () => {
  const client = new DbProxyClient({
    load: async () => undefined,
    loadMulti: async () => [],
    save: async () => ({ disposition: "applied", revision: 1n }),
    enqueueSnapshot: async () => undefined,
    applyTransaction: async () => ({
      disposition: "applied",
      newRevision: 1n,
      result: new Uint8Array(),
    }),
    loadTransaction: async () => undefined,
    applyMultiTransaction: async () => ({ disposition: "applied", records: [], result: new Uint8Array() }),
    loadMultiTransaction: async () => undefined,
  });

  assert.throws(() => client.EnqueueSnapshot({
    requestId: "queued-player-1",
    record: { namespace: "player", key: "1001" },
    schema: "tiangz.player",
    schemaVersion: 1,
    payload: new Uint8Array(),
    expectedRevision: 1n,
    updatedAtUnixMs: 123n,
  }), /cannot carry expectedRevision/);
});

test("transaction receipt lookup validates identity and returns defensive bytes", async () => {
  const source = Uint8Array.from([7, 8, 9]);
  const client = new DbProxyClient({
    load: async () => undefined,
    loadMulti: async () => [],
    save: async () => ({ disposition: "applied", revision: 1n }),
    enqueueSnapshot: async () => undefined,
    applyTransaction: async () => ({
      disposition: "applied",
      newRevision: 1n,
      result: new Uint8Array(),
    }),
    loadTransaction: async (operationId, record) => ({
      operationId,
      record,
      newRevision: 3n,
      result: source,
    }),
    applyMultiTransaction: async () => ({ disposition: "applied", records: [], result: new Uint8Array() }),
    loadMultiTransaction: async () => undefined,
  });

  const receipt = await client.LoadTransaction(
    "quest-reward:player-1:5001",
    { namespace: "player", key: "player-1" },
  );
  source[0] = 99;

  assert.equal(receipt?.newRevision, 3n);
  assert.deepEqual([...(receipt?.result ?? [])], [7, 8, 9]);
});

test("multi-record transaction keeps all records and result defensive", async () => {
  let captured;
  const result = Uint8Array.from([4, 5]);
  const client = new DbProxyClient({
    load: async () => undefined,
    loadMulti: async () => [],
    save: async () => ({ disposition: "applied", revision: 1n }),
    enqueueSnapshot: async () => undefined,
    applyTransaction: async () => ({
      disposition: "applied",
      newRevision: 1n,
      result: new Uint8Array(),
    }),
    loadTransaction: async () => undefined,
    applyMultiTransaction: async (write) => {
      captured = write;
      return {
        disposition: "applied",
        records: write.writes.map((item, index) => ({ record: item.record, newRevision: BigInt(index + 1) })),
        result,
      };
    },
    loadMultiTransaction: async () => undefined,
  });
  const payload = Uint8Array.from([9]);
  const returned = await client.ApplyMultiTransaction({
    operationId: "trade-1",
    writes: [
      {
        record: { namespace: "wallet", key: "buyer" },
        schema: "wallet.snapshot",
        schemaVersion: 1,
        expectedRevision: 0n,
        payload,
        updatedAtUnixMs: 1n,
      },
      {
        record: { namespace: "wallet", key: "seller" },
        schema: "wallet.snapshot",
        schemaVersion: 1,
        expectedRevision: 2n,
        payload: new Uint8Array([8]),
        updatedAtUnixMs: 1n,
      },
    ],
    result: new Uint8Array([1]),
  });
  payload[0] = 0;
  result[0] = 0;
  assert.equal(captured.operationId, "trade-1");
  assert.equal(captured.writes.length, 2);
  assert.equal(captured.writes[0].payload[0], 9);
  assert.deepEqual([...returned.result], [4, 5]);
  assert.deepEqual([...returned.records[1].record.key], [..."seller"]);
});

test("trade transaction validates state and ledger while preserving byte ownership", async () => {
  let captured;
  const receiptResult = Uint8Array.from([5]);
  let responseVersion = 1n;
  const client = new DbProxyClient({
    applyTradeTransaction: async (transaction) => {
      captured = transaction;
      return {
        disposition: "applied",
        receipt: {
          operationId: transaction.operationId,
          tradeId: transaction.transition.tradeId,
          newTradeVersion: responseVersion,
          state: "escrowed",
          records: transaction.writes.map((write) => ({
            record: write.record,
            newRevision: 1n,
          })),
          ledgerPostingIds: transaction.ledgerPostings.map((posting) => posting.postingId),
          outboxEventIds: transaction.outboxEvents.map((event) => event.eventId),
          result: receiptResult,
        },
      };
    },
  });
  const transitionPayload = Uint8Array.from([1]);
  const snapshotPayload = Uint8Array.from([2]);
  const ledgerMetadata = Uint8Array.from([3]);
  const eventPayload = Uint8Array.from([4]);
  const resultPayload = Uint8Array.from([5]);
  const trade = {
    operationId: "trade-op-1",
    transition: {
      tradeId: "trade-1",
      expectedVersion: 0n,
      nextState: "escrowed",
      payload: transitionPayload,
      updatedAtUnixMs: 1n,
    },
    writes: [{
      record: { namespace: "inventory", key: "seller" },
      schema: "inventory.snapshot",
      schemaVersion: 1,
      expectedRevision: 0n,
      payload: snapshotPayload,
      updatedAtUnixMs: 1n,
    }],
    ledgerPostings: [
      {
        postingId: "buyer-debit",
        accountId: "buyer",
        asset: "gold",
        amount: -100n,
        metadata: ledgerMetadata,
      },
      {
        postingId: "escrow-credit",
        accountId: "escrow:trade-1",
        asset: "gold",
        amount: 100n,
        metadata: new Uint8Array(),
      },
    ],
    outboxEvents: [{
      eventId: "trade-event-1",
      topic: "trade.escrowed",
      partitionKey: "trade-1",
      payload: eventPayload,
      occurredAtUnixMs: 1n,
    }],
    result: resultPayload,
  };

  const applied = await client.ApplyTradeTransaction(trade);
  transitionPayload[0] = 9;
  snapshotPayload[0] = 9;
  ledgerMetadata[0] = 9;
  eventPayload[0] = 9;
  resultPayload[0] = 9;
  receiptResult[0] = 9;
  assert.equal(captured.transition.payload[0], 1);
  assert.equal(captured.writes[0].payload[0], 2);
  assert.equal(captured.ledgerPostings[0].metadata[0], 3);
  assert.equal(captured.outboxEvents[0].payload[0], 4);
  assert.equal(captured.result[0], 5);
  assert.deepEqual([...applied.receipt.result], [5]);

  responseVersion = 2n;
  await assert.rejects(
    () => client.ApplyTradeTransaction(trade),
    /receipt does not match the request/,
  );

  assert.throws(
    () => client.ApplyTradeTransaction({
      ...trade,
      ledgerPostings: [trade.ledgerPostings[0]],
    }),
    /ledger is not balanced/,
  );
  assert.throws(
    () => client.ApplyTradeTransaction({
      ...trade,
      transition: {
        ...trade.transition,
        expectedVersion: 1n,
        expectedState: "settled",
        nextState: "escrowed",
      },
    }),
    /illegal state transition/,
  );
});

test("SDK validation works in a bare V8 without TextEncoder", async () => {
  const original = globalThis.TextEncoder;
  globalThis.TextEncoder = undefined;
  try {
    const client = new DbProxyClient({
      load: async () => undefined,
      loadMulti: async () => [],
      save: async () => ({ disposition: "applied", revision: 1n }),
      enqueueSnapshot: async () => undefined,
      applyTransaction: async () => ({
        disposition: "applied",
        newRevision: 1n,
        result: new Uint8Array(),
      }),
      loadTransaction: async () => undefined,
      applyMultiTransaction: async () => ({ disposition: "applied", records: [], result: new Uint8Array() }),
      loadMultiTransaction: async () => undefined,
    });
    const result = await client.Save({
      requestId: "裸V8-save-1",
      record: { namespace: "玩家", key: "1001" },
      schema: "tiangz.player",
      schemaVersion: 1,
      payload: new Uint8Array(),
      expectedRevision: 0n,
      updatedAtUnixMs: 123n,
    });
    assert.equal(result.revision, 1n);
  } finally {
    globalThis.TextEncoder = original;
  }
});
