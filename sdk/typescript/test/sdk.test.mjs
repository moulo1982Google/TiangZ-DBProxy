import assert from "node:assert/strict";
import test from "node:test";

import {
  DBPROXY_PROTOCOL_FINGERPRINT,
  DBPROXY_PROTOCOL_VERSION,
  DbProxyClient,
} from "../dist/index.js";

test("protocol lock is generated from the authoritative proto", () => {
  assert.equal(DBPROXY_PROTOCOL_VERSION, 1);
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
