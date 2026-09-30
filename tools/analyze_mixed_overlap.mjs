// Offline driver-call overlap, not server SQL/lock attribution.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { pathToFileURL } from 'node:url';

export function analyzeOverlap(rows) {
  const manifests = rows.filter(x => x.kind === 'manifest');
  assert.equal(manifests.length, 1);
  const m = manifests[0];
  assert.equal(m.connections, 4);
  assert.equal(m.full_timing, true);
  const intents = new Map();
  for (const x of rows.filter(x => x.kind === 'intent')) {
    assert(!intents.has(x.n));
    assert(Number.isSafeInteger(x.scheduled_us) && x.scheduled_us >= 0);
    intents.set(x.n, x);
  }
  const seen = new Set();
  const calls = rows.filter(x => x.kind === 'response').map(x => {
    const i = intents.get(x.n);
    assert(i && !seen.has(x.n));
    seen.add(x.n);
    assert.equal(x.op, i.op);
    assert.equal(x.sample, i.sample);
    assert.equal(x.outcome.status, 'success');
    for (const k of ['dispatch_us', 'rpc_us', 'end_to_end_us'])
      assert(Number.isSafeInteger(x[k]) && x[k] >= 0);
    const begin = i.scheduled_us + x.dispatch_us;
    return { ...x, begin, end: begin + x.rpc_us, connection: x.n % 4 };
  });
  assert.equal(calls.length, (m.warmup + m.sample) * m.rate);
  assert.equal(intents.size, calls.length);
  assert(!rows.some(x => x.kind === 'not_sent' || x.kind === 'guard_stop'));
  const batches = calls.filter(x => x.op === 'save_multi');
  const transactions = calls.filter(x => x.sample && x.op === 'transaction').map(x => {
    const overlapping = batches.filter(b => b.begin < x.end && x.begin < b.end);
    return { n: x.n, connection: x.connection, rpc_us: x.rpc_us,
      end_to_end_us: x.end_to_end_us,
      overlapping_batches: overlapping.map(b => ({ n: b.n, connection: b.connection,
        overlap_us: Math.min(b.end, x.end) - Math.max(b.begin, x.begin),
        active_at_transaction_begin: b.begin <= x.begin && x.begin < b.end })) };
  });
  const stats = xs => {
    const ys = xs.map(x => x.rpc_us).sort((a, b) => a - b);
    return { count: ys.length, rpc_p99_us: ys.length ? ys[Math.ceil(ys.length * .99) - 1] : null };
  };
  return { run: m.run, scope: 'Intervals start at driver sent timestamp before task spawn and end at its RPC timing read. Includes SDK, network, server and return. Overlap is not causal attribution; no shared server connection or SQL lock inferred.',
    transactions, all: stats(transactions),
    overlapping: stats(transactions.filter(x => x.overlapping_batches.length)),
    non_overlapping: stats(transactions.filter(x => !x.overlapping_batches.length)) };
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  const dir = process.argv[2];
  assert(dir, 'usage: node tools/analyze_mixed_overlap.mjs mixed-paced-directory');
  const rows = fs.readFileSync(path.join(dir, 'requests.jsonl'), 'utf8').trim().split(/\r?\n/).map(JSON.parse);
  const result = analyzeOverlap(rows);
  fs.writeFileSync(path.join(dir, 'overlap-analysis.json'), JSON.stringify(result, null, 2) + '\n');
  console.log(JSON.stringify({ run: result.run, all: result.all, overlapping: result.overlapping, non_overlapping: result.non_overlapping }));
}
