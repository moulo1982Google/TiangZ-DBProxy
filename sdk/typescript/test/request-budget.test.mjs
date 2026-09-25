import assert from "node:assert/strict";
import test from "node:test";
import { DbProxyClient, DbProxyErrorCode } from "../dist/index.js";

const record = { namespace: "budget", key: "one" };
const write = { requestId: "same-operation", record, schema: "budget.v1", schemaVersion: 1,
  payload: Uint8Array.of(1, 2), expectedRevision: 0n, updatedAtUnixMs: 1n };

function fixture() {
  let now = 1000;
  const calls = [];
  const transport = {
    supportsRequestTimeout: true,
    requestTimeoutMs: 5000,
    monotonicNowMs: () => now,
    load: async (key, options) => { calls.push({ key, options }); return undefined; },
    save: async (value, options) => { calls.push({ value, options }); return { disposition: "applied", revision: 1n }; },
  };
  return { calls, transport, client: new DbProxyClient(transport), advance: amount => { now += amount; } };
}

test("scoped calls consume one deadline while concurrent clients stay independent", async () => {
  const f = fixture();
  const first = f.client.WithRequestBudget(100);
  f.advance(30);
  const second = f.client.WithRequestBudget(100);
  await first.Load(record);
  await second.Save(write);
  await f.client.Load(record);
  assert.deepEqual(f.calls.map(call => call.options?.timeoutMs), [70, 100, undefined]);
  assert.equal(first.GetRemainingRequestBudgetMs(), 70);
  assert.throws(() => f.client.GetRemainingRequestBudgetMs(), /no request budget/i);
});

test("nested budgets cannot extend the parent and exhausted work never reaches transport", async () => {
  const f = fixture();
  const parent = f.client.WithRequestBudget(100);
  f.advance(80);
  const child = parent.WithRequestBudget(5000);
  await child.Save(write);
  assert.equal(f.calls[0].options.timeoutMs, 20);
  f.advance(20);
  assert.throws(() => child.Save(write), error => error.code === DbProxyErrorCode.StorageUnavailable && /budget.*exhausted/i.test(error.message));
  assert.equal(f.calls.length, 1);
});

test("payload ownership and the idempotency identity survive retry within a budget", async () => {
  const f = fixture();
  const client = f.client.WithRequestBudget(100);
  await client.Save(write);
  f.advance(25);
  await client.Save(write);
  assert.deepEqual(f.calls.map(call => call.value.requestId), [write.requestId, write.requestId]);
  assert.deepEqual(f.calls[0].value, f.calls[1].value);
  assert.notEqual(f.calls[0].value.payload, write.payload);
  assert.deepEqual(f.calls.map(call => call.options.timeoutMs), [100, 75]);
});

test("old transports keep normal calls but cannot silently claim bounded scopes", async () => {
  const client = new DbProxyClient({ load: async () => undefined });
  assert.equal(await client.Load(record), undefined);
  assert.throws(() => client.WithRequestBudget(), /request timeout/i);
});

test("rejects invalid budgets and non-monotonic clocks before admission", () => {
  const f = fixture();
  for (const invalid of [0, -1, 1.5, Infinity, NaN, 120001, "100"]) {
    assert.throws(() => f.client.WithRequestBudget(invalid), /timeout/i);
  }
  const client = f.client.WithRequestBudget();
  assert.equal(client.GetRemainingRequestBudgetMs(), 5000);
  f.advance(-1);
  assert.throws(() => client.Load(record), /monotonic/i);
  assert.equal(f.calls.length, 0);
});

test("parameter validation time belongs to the shared request deadline", () => {
  const f = fixture();
  const client = f.client.WithRequestBudget(10);
  const slowRecord = { get namespace() { f.advance(10); return "budget"; }, key: "one" };
  assert.throws(() => client.Load(slowRecord), /budget.*exhausted/i);
  assert.equal(f.calls.length, 0);
});

test("every transport family receives the diminishing budget as its final argument", async () => {
  const f = fixture();
  const stopped = new Error("transport reached");
  const transaction = { ...write, operationId: "transaction", result: new Uint8Array() };
  const multi = { operationId: "multi", writes: [write], result: new Uint8Array() };
  const trade = { ...multi, transition: { tradeId: "trade", expectedVersion: 0n, nextState: "escrowed",
    payload: new Uint8Array(), updatedAtUnixMs: 1n }, ledgerPostings: [], outboxEvents: [] };
  const cases = [
    ["Load", "load", [record]], ["LoadMulti", "loadMulti", [[record]]],
    ["LoadCached", "loadCached", [record, 1n]], ["LoadCachedMulti", "loadCachedMulti", [[record], [1n]]],
    ["Save", "save", [write]], ["SaveMulti", "saveMulti", [[write]]],
    ["EnqueueSnapshot", "enqueueSnapshot", [{ ...write, expectedRevision: undefined }]],
    ["EnqueueMultiSnapshot", "enqueueMultiSnapshot", [[{ ...write, expectedRevision: undefined }]]],
    ["ApplyTransaction", "applyTransaction", [transaction]], ["LoadTransaction", "loadTransaction", ["op", record]],
    ["ApplyMultiTransaction", "applyMultiTransaction", [multi]], ["LoadMultiTransaction", "loadMultiTransaction", ["op", [record]]],
    ["CommitRecords", "commitRecords", [{ ...multi, appends: [], outboxEvents: [] }]],
    ["LoadTrade", "loadTrade", ["trade"]], ["ApplyTradeTransaction", "applyTradeTransaction", [trade]],
    ["LoadTradeTransaction", "loadTradeTransaction", ["op", "trade"]],
  ];
  let remaining = 100;
  for (const [, name] of cases) f.transport[name] = async (...args) => {
    assert.deepEqual(args.at(-1), { timeoutMs: remaining }, name);
    assert.ok(Object.isFrozen(args.at(-1)), name);
    throw stopped;
  };
  const client = f.client.WithRequestBudget(100);
  for (const [method, , args] of cases) {
    f.advance(1); remaining -= 1;
    await assert.rejects(client[method](...args), error => error === stopped, method);
  }
});

test("fractional remainders never extend deadlines and invalid clock values fail closed", () => {
  const f = fixture();
  const client = f.client.WithRequestBudget(5);
  f.advance(0.1);
  assert.equal(client.GetRemainingRequestBudgetMs(), 4);
  f.advance(4);
  assert.throws(() => client.Load(record), /budget.*exhausted/i);
  for (const now of [NaN, Infinity, -Infinity]) {
    f.transport.monotonicNowMs = () => now;
    assert.throws(() => f.client.WithRequestBudget(10), /finite monotonic/i);
  }
  assert.equal(f.calls.length, 0);
});
