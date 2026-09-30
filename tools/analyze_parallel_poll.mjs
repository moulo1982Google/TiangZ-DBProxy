import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export function analyze(raw) {
  assert(raw.endsWith('\n'), 'journal must be sealed with a final newline');
  const rows = raw.trimEnd().split('\n').map(s => JSON.parse(s));
  let cursor = 0, previousEnd = 0, overlaps = 0;
  const take = kind => { const v = rows[cursor++]; assert.equal(v?.kind, kind); return v; };
  const uint = v => assert(Number.isSafeInteger(v) && v >= 0, 'invalid timing/count');
  const seen = new Set(), next = [[0, 1], [0, 1]];
  const claims = [];
  for (let wave = 0; wave < 14; wave++) {
    const w = take('wave'), publisher = `parallel-${wave % 2 ? 'b' : 'a'}`;
    assert.equal(w.wave, wave); assert.equal(w.publisher, publisher);
    assert.equal(w.sample, wave >= 4); assert.equal(w.scheduled_us, wave * 500000);
    uint(w.dispatch_us);
    assert(w.dispatch_us >= Math.max(w.scheduled_us, previousEnd));
    assert(w.dispatch_us - w.scheduled_us <= 100000, 'load guard');
    function operations(operation, earliest = w.dispatch_us) {
      const started = new Map(), completed = new Map();
      for (let i = 0; i < 4; i++) {
        const v = rows[cursor++];
        assert.equal(v.wave, wave); assert.equal(v.operation, operation);
        assert([0, 1].includes(v.worker));
        if (v.kind === 'started') {
          assert(!started.has(v.worker)); uint(v.at_us);
          assert(v.at_us >= earliest); started.set(v.worker, v);
        } else {
          assert.equal(v.kind, 'operation'); assert(started.has(v.worker));
          assert(!completed.has(v.worker)); assert.equal(v.outcome, 'completed');
          uint(v.begin_us); uint(v.end_us);
          assert(v.begin_us >= started.get(v.worker).at_us && v.end_us >= v.begin_us);
          completed.set(v.worker, v);
        }
      }
      assert.equal(started.size, 2); assert.equal(completed.size, 2);
      return [completed.get(0), completed.get(1)];
    }
    const c = operations('claim');
    const keys = new Set();
    for (let worker = 0; worker < 2; worker++) {
      const l = take('lease'); assert.equal(l.wave, wave); assert.equal(l.worker, worker);
      assert.equal(l.publisher, publisher); assert.equal(l.destination, 'parallel-destination');
      uint(l.token); assert(l.token > 0);
      assert(['key-0', 'key-1'].includes(l.partition)); assert(!keys.has(l.partition)); keys.add(l.partition);
      const key = Number(l.partition.slice(-1));
      assert.equal(l.event, `${publisher}-${next[wave % 2][key]}`);
      next[wave % 2][key] += 2;
      assert(!seen.has(l.event)); seen.add(l.event);
    }
    const a = operations('ack', Math.max(...c.map(v => v.end_us)));
    for (const ack of a) assert(ack.begin_us >= Math.max(...c.map(v => v.end_us)));
    const end = take('wave_completed'); assert.equal(end.wave, wave);
    uint(end.begin_us); uint(end.end_us);
    assert(end.begin_us >= w.dispatch_us && end.begin_us <= Math.min(...c.map(v => v.begin_us)));
    assert(end.end_us >= Math.max(...a.map(v => v.end_us))); previousEnd = end.end_us;
    if (Math.max(...c.map(v => v.begin_us)) < Math.min(...c.map(v => v.end_us))) overlaps++;
    claims.push(...c.map(v => ({wave, worker:v.worker, duration_us:v.end_us-v.begin_us})));
  }
  const final = new Set();
  for (let i = 0; i < 1000; i++) {
    const v = take('final'); assert(!final.has(v.event)); final.add(v.event);
    assert.equal(v.published, seen.has(v.event));
  }
  for (const publisher of ['a','b']) for (let n = 0; n < 500; n++) assert(final.has(`parallel-${publisher}-${n}`));
  const result = take('result');
  assert.equal(result.status, 'SMOKE_ONLY'); assert.equal(result.waves,14);
  assert.equal(result.workers,2); assert.equal(result.publishers,2); assert.equal(result.rows,1000);
  assert.equal(result.claims,28); assert.equal(result.stats,false);
  assert.equal(result.warmup_seconds,2); assert.equal(result.sample_seconds,5);
  assert(overlaps > 0); assert.equal(result.overlap_waves,overlaps); assert.equal(cursor,rows.length);
  return {status:'SMOKE_ONLY', validation:'PARALLEL_CLAIMS_CHECKED', waves:14, claims:28, formal_claims:20, overlap_waves:overlaps, claims_timing:claims};
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const dir = process.argv[2];
  const result = analyze(fs.readFileSync(path.join(dir,'journal.jsonl'),'utf8'));
  fs.writeFileSync(path.join(dir,'analysis.json'),JSON.stringify(result,null,2)+'\n');
  console.log(JSON.stringify(result));
}
