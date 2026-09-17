import assert from "node:assert/strict";
import test from "node:test";
import { DbProxyClient, DbProxyErrorCode } from "../dist/index.js";

const record = { namespace: "player", key: "one" };
const snapshot = revision => ({ record, schema: "test", schemaVersion: 1, revision, payload: new Uint8Array([1]), updatedAtUnixMs: 1n });

test("default reads never select the cached transport", async () => {
  const client = new DbProxyClient({
    load: async () => snapshot(101n),
    loadMulti: async () => [snapshot(101n)],
    loadCached: async () => { throw new Error("unexpected cache path"); },
    loadCachedMulti: async () => { throw new Error("unexpected cache path"); },
  });
  assert.equal((await client.Load(record)).revision, 101n);
  assert.equal((await client.LoadMulti([record]))[0].revision, 101n);
});

test("cached reads pass fences and reject stale or absent fenced responses", async () => {
  let captured;
  const client = new DbProxyClient({
    loadCached: async (key, minimum) => { captured = [key, minimum]; return snapshot(100n); },
    loadCachedMulti: async () => [undefined],
  });
  assert.equal((await client.LoadCached(record)).revision, 100n);
  await assert.rejects(client.LoadCached(record, 101n), e => e.code === DbProxyErrorCode.StorageUnavailable);
  assert.deepEqual(captured, [record, 101n]);
  assert.notEqual(captured[0], record);
  await assert.rejects(client.LoadCachedMulti([record], [1n]), e => e.code === DbProxyErrorCode.StorageUnavailable);
  assert.deepEqual(await client.LoadCachedMulti([record]), [undefined]);
  assert.throws(() => client.LoadCached(record, -1n));
  assert.throws(() => client.LoadCachedMulti([record], [0n, 1n]));
  assert.throws(() => new DbProxyClient({}).LoadCached(record), /does not support/);
});

test("batch cached reads own fences and validate returned identities", async () => {
  const fences = [1n];
  let finish;
  let captured;
  const client = new DbProxyClient({loadCachedMulti: async (keys, minima) => {
    captured = [keys, minima];
    return new Promise(resolve => { finish = resolve; });
  }});
  const result = client.LoadCachedMulti([record], fences);
  fences[0] = 999n;
  assert.deepEqual(captured[1], [1n]);
  finish([{ ...snapshot(1n), record: { ...record, key: "wrong" } }]);
  await assert.rejects(result, /identity/);
});
