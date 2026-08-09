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

test("queued snapshots reject CAS because ACK only means backlog accepted", () => {
  const client = new DbProxyClient({
    load: async () => undefined,
    save: async () => ({ disposition: "applied", revision: 1n }),
    enqueueSnapshot: async () => undefined,
    applyTransaction: async () => ({
      disposition: "applied",
      newRevision: 1n,
      result: new Uint8Array(),
    }),
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

test("SDK validation works in a bare V8 without TextEncoder", async () => {
  const original = globalThis.TextEncoder;
  globalThis.TextEncoder = undefined;
  try {
    const client = new DbProxyClient({
      load: async () => undefined,
      save: async () => ({ disposition: "applied", revision: 1n }),
      enqueueSnapshot: async () => undefined,
      applyTransaction: async () => ({
        disposition: "applied",
        newRevision: 1n,
        result: new Uint8Array(),
      }),
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
