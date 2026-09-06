import assert from "node:assert/strict";
import test from "node:test";
import { CreateOutboxEvent, DbProxyClient } from "../dist/index.js";

function envelope() {
  return { eventId: "event-1", producer: "game", eventType: "Changed", aggregateType: "document",
    aggregateId: "document-1", partitionKey: "document-1", schemaVersion: 1, routeVersion: 1,
    contentType: "application/octet-stream", payload: new Uint8Array([0, 255]), occurredAtUnixMs: 1n };
}

function request() {
  return { operationId: "commit-1", writes: ["one", "two"].map(key => ({
    record: { namespace: "document", key }, schema: "opaque", schemaVersion: 1,
    expectedRevision: 0n, payload: new Uint8Array([1]), updatedAtUnixMs: 1n,
  })), result: new Uint8Array([2]), appends: [], outboxEvents: [CreateOutboxEvent(envelope())] };
}

test("relay rejects invalid integer versions and non-bigint or overflowing timestamps", () => {
  for (const field of ["schemaVersion", "routeVersion"]) {
    for (const invalid of [0, -1, 1.5, NaN, Infinity, 0x1_0000_0000, "1", 1n]) {
      assert.throws(() => CreateOutboxEvent({ ...envelope(), [field]: invalid }),
        { name: invalid === 0 ? "TypeError" : "RangeError" });
    }
  }
  for (const invalid of [-1n, 1n << 64n, 1, "1", null, undefined]) {
    assert.throws(() => CreateOutboxEvent({ ...envelope(), occurredAtUnixMs: invalid }), /uint64 bigint/);
  }
});

test("relay validates every text field and counts UTF-8 bytes rather than characters", () => {
  for (const field of ["eventId", "producer", "eventType", "aggregateType", "aggregateId", "partitionKey", "contentType"]) {
    for (const invalid of ["", " ", "bad\nvalue", "bad\u0085value", "a".repeat(257), null, 123]) {
      assert.throws(() => CreateOutboxEvent({ ...envelope(), [field]: invalid }), undefined, field);
    }
  }
  assert.doesNotThrow(() => CreateOutboxEvent({ ...envelope(), aggregateId: "玩".repeat(85) }));
  assert.throws(() => CreateOutboxEvent({ ...envelope(), aggregateId: "玩".repeat(86) }), /UTF-8 bytes/);
  assert.doesNotThrow(() => CreateOutboxEvent({ ...envelope(), aggregateId: "😀".repeat(64) }));
  assert.throws(() => CreateOutboxEvent({ ...envelope(), aggregateId: "😀".repeat(65) }), /UTF-8 bytes/);
});

test("relay round-trips byte values and full-width boundary timestamps", () => {
  for (const timestamp of [0n, 1n, (1n << 63n) - 1n, (1n << 64n) - 1n]) {
    const payload = Uint8Array.from({ length: 256 }, (_, i) => i);
    const event = CreateOutboxEvent({ ...envelope(), payload, occurredAtUnixMs: timestamp });
    payload.fill(99);
    const decoded = JSON.parse(Buffer.from(event.payload).toString("utf8"));
    assert.deepEqual(decoded.payload, Array.from({ length: 256 }, (_, i) => i));
    assert.equal(decoded.occurred_at_unix_ms, timestamp.toString());
    assert.equal(event.occurredAtUnixMs, timestamp);
  }
  for (const payload of [[], "bytes", new ArrayBuffer(2)]) {
    assert.throws(() => CreateOutboxEvent({ ...envelope(), payload }), /Uint8Array/);
  }
});

test("unverified relay transports cannot accidentally publish or fall back", () => {
  for (const supportsOutboxRelay of [undefined, false, "true", 1]) {
    const client = new DbProxyClient({ supportsOutboxRelay,
      commitRecords() { assert.fail("must reject before calling transport"); },
      applyMultiTransaction() { assert.fail("must not lose outbox by fallback"); },
    });
    assert.throws(() => client.CommitRecords(request()), /has not verified/);
  }
});

test("commit rejects wrong, duplicate and incorrectly versioned receipt identities", async () => {
  const original = request();
  const correct = original.writes.map(w => ({ record: w.record, newRevision: 1n }));
  const malformed = [
    [], [correct[0]], [correct[0], correct[0]],
    [correct[0], { ...correct[1], record: { namespace: "wrong", key: "two" } }],
    [correct[0], { ...correct[1], newRevision: 2n }],
  ];
  for (const records of malformed) {
    const client = new DbProxyClient({ supportsOutboxRelay: true,
      commitRecords: async () => ({ disposition: "applied", records, result: new Uint8Array() }),
    });
    await assert.rejects(client.CommitRecords(original), /receipt does not match/);
  }
  const invalid = new DbProxyClient({ supportsOutboxRelay: true,
    commitRecords: async () => ({ disposition: "unknown", records: correct, result: new Uint8Array() }),
  });
  await assert.rejects(invalid.CommitRecords(original), /invalid commit disposition/);
});

test("commit accepts reordered duplicate receipts and copies returned bytes and identities", async () => {
  const original = request();
  const response = { disposition: "duplicate", records: original.writes.map(w => ({
    record: { ...w.record }, newRevision: 1n,
  })).reverse(), result: new Uint8Array([5]) };
  const client = new DbProxyClient({ supportsOutboxRelay: true, commitRecords: async () => response });
  const result = await client.CommitRecords(original);
  response.result[0] = 99;
  response.records[0].record.key = "tampered";
  assert.equal(result.disposition, "duplicate");
  assert.equal(result.result[0], 5);
  assert.equal(result.records[0].record.key, "two");
});
