import assert from "node:assert/strict";
import test from "node:test";
import { readFile } from "node:fs/promises";
import { CreateOutboxEvent, DbProxyClient } from "../dist/index.js";

test("generic envelope matches the shared Rust fixture without TextEncoder", async () => {
  const fixture = JSON.parse(await readFile(new URL("./fixtures/relay-event.json", import.meta.url), "utf8"));
  const original = globalThis.TextEncoder;
  globalThis.TextEncoder = undefined;
  try {
    const data = new Uint8Array(fixture.payload);
    const input = { eventId: fixture.event_id, producer: fixture.producer, eventType: fixture.event_type,
      aggregateType: fixture.aggregate_type, aggregateId: fixture.aggregate_id, partitionKey: fixture.partition_key,
      schemaVersion: fixture.schema_version, contentType: fixture.content_type, payload: data,
      occurredAtUnixMs: BigInt(fixture.occurred_at_unix_ms), routeVersion: fixture.route_version };
    const event = CreateOutboxEvent(input);
    data[0] = 99;
    assert.deepEqual(JSON.parse(Buffer.from(event.payload).toString("utf8")), fixture);
    assert.equal(event.topic, "dbproxy.relay.v1.game.1");
    assert.throws(() => CreateOutboxEvent({ ...input, producer: "invalid/source" }), /producer/);
    assert.throws(() => CreateOutboxEvent({ ...input, schemaVersion: 0 }), /positive/);
    const write = { operationId: "commit-event", writes: [{ record: { namespace: "document", key: "1" },
      schema: "document", schemaVersion: 1, expectedRevision: 0n, payload: new Uint8Array(), updatedAtUnixMs: 1n }],
      result: new Uint8Array(), appends: [], outboxEvents: [event] };
    assert.throws(() => new DbProxyClient({ commitRecords() { assert.fail("unverified host must not publish"); } }).CommitRecords(write), /has not verified/);
    const result = await new DbProxyClient({ supportsOutboxRelay: true,
      commitRecords: async request => ({ disposition: "applied", records: [{record: request.writes[0].record,newRevision: 1n}], result: request.result })
    }).CommitRecords(write);
    assert.equal(result.disposition, "applied");
  } finally { globalThis.TextEncoder = original; }
});
