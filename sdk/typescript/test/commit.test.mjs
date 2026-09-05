import assert from "node:assert/strict";
import test from "node:test";
import { DbProxyClient } from "../dist/index.js";

function request() {
  return { operationId: "change-1", writes: [{ record: { namespace: "document", key: "one" }, schema: "document", schemaVersion: 1, expectedRevision: 0n, payload: new Uint8Array([1]), updatedAtUnixMs: 1n }], result: new Uint8Array([2]),
    appends: [{ record: { namespace: "audit", key: "one" }, schema: "fact", schemaVersion: 1, payload: new Uint8Array([3]), occurredAtUnixMs: 1n }],
    outboxEvents: [{ eventId: "event-1", topic: "changed", partitionKey: "one", payload: new Uint8Array([4]), occurredAtUnixMs: 1n }] };
}

test("commit preserves opaque effects and rejects old transports without fallback", async () => {
  let captured;
  const client = new DbProxyClient({ commitRecords: async write => {
    captured = write;
    return { disposition: "applied", records: write.writes.map(w => ({ record: w.record, newRevision: 1n })), result: write.result };
  }});
  const write = request();
  const pending = client.CommitRecords(write);
  write.appends[0].payload[0] = 9;
  write.outboxEvents[0].payload[0] = 9;
  assert.equal((await pending).disposition, "applied");
  assert.equal(captured.appends[0].payload[0], 3);
  assert.equal(captured.outboxEvents[0].payload[0], 4);
  assert.throws(() => new DbProxyClient({ applyMultiTransaction() { assert.fail("must not fall back"); } }).CommitRecords(request()), /does not support/);
});

test("commit rejects duplicate effect IDs and mismatching receipts", async () => {
  const client = new DbProxyClient({ commitRecords: async () => ({ disposition: "applied", records: [], result: new Uint8Array() }) });
  const write = request();
  write.appends.push(write.appends[0]);
  assert.throws(() => client.CommitRecords(write), /duplicate/);
  await assert.rejects(client.CommitRecords(request()), /receipt does not match/);
});
