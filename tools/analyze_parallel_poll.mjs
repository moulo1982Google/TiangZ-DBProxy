import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {createHash} from 'node:crypto';

export function validateBudget(warmup, sample, total, mixed) {
  assert([[2,5],[120,300]].some(v=>v[0]===warmup&&v[1]===sample),'unsupported timing');
  assert(Number.isSafeInteger(total)&&total>=1000&&total<=40000&&total%40===0,'invalid row budget');
  const waves=(warmup+sample)*2, calls=waves*2, perPublisher=total/2;
  const ready=total/(mixed?20:2), consumed=calls/2;
  assert(ready>=consumed*2,'eligible reserve below 50%');
  return {warmup,sample,total,waves,calls,perPublisher,ready,consumed,blocked:perPublisher-ready};
}

export function analyze(raw, seal) {
  assert(raw.endsWith('\n'), 'journal must be sealed with a final newline');
  const rows = raw.trimEnd().split('\n').map(s => JSON.parse(s));
  let cursor = 0, previousEnd = 0, overlaps = 0;
  const take = kind => { const v = rows[cursor++]; assert.equal(v?.kind, kind); return v; };
  const uint = v => assert(Number.isSafeInteger(v) && v >= 0, 'invalid timing/count');
  const seen = new Set(), next = [[0, 1], [0, 1]];
  const claims = [];
  const versioned = rows[0]?.kind === 'fixture';
  let mode = 'ready';
  let mixed = false;
  let budget=validateBudget(2,5,1000,false);
  let status='SMOKE_ONLY';
  function distribution(phase) {
    assert.deepEqual(take('distribution'), {kind:'distribution',phase,total:budget.total,future_leased:mode==='none'?budget.total:mode==='leased'?budget.blocked*2:mode==='leased-heads'?4:0,dead:['all-blocked','dead-heads'].includes(mode)?4:0,future_available:mode==='backoff'?budget.blocked*2:mode==='backoff-heads'?4:0,owned:0});
    if(mixed) for(const publisher of ['parallel-a','parallel-b']) assert.deepEqual(take('reserve'),{kind:'reserve',phase,publisher,blocked:budget.blocked,ready_pending:budget.ready-(phase==='before'?0:budget.consumed),blocked_published:0});
    if(mode==='spread-ready') for(const publisher of ['parallel-a','parallel-b']) assert.deepEqual(take('spread'),{kind:'spread',phase,publisher,total:budget.perPublisher,partitions:budget.perPublisher,pending:budget.perPublisher-(phase==='before'?0:budget.consumed)});
  }
  if(versioned) {
    const f=take('fixture'); mode=f.mode;
    assert(['ready','spread-ready','none','all-blocked','leased','backoff','leased-heads','backoff-heads','dead-heads'].includes(mode));
    mixed=['leased','backoff','leased-heads','backoff-heads','dead-heads'].includes(mode);
    budget=validateBudget(f.schema===6?f.warmup_seconds:2,f.schema===6?f.sample_seconds:5,f.schema===6?f.rows:1000,mixed);
    status=budget.sample===5?'SMOKE_ONLY':'BOUNDED_LEDGER_ONLY';
    if(mixed) for(const n of next) {n[0]=budget.blocked;n[1]=budget.blocked+1;}
    if(f.schema===5||f.schema===6) {
      assert.deepEqual(f,{kind:'fixture',schema:f.schema,mode,rows:budget.total,claim_calls:budget.calls,warmup_seconds:budget.warmup,sample_seconds:budget.sample,workers:2,publishers:2,claims_per_second:4,stats:false,seal_required:true});
      assert.deepEqual(seal,{schema:1,file:'journal.jsonl',bytes:Buffer.byteLength(raw),sha256:createHash('sha256').update(raw).digest('hex')},'missing or inconsistent journal seal');
    } else assert.equal(f.schema,mode==='spread-ready'?4:mixed?3:2);
    assert.equal(f.rows,budget.total);assert.equal(f.claim_calls,budget.calls);
    distribution('before');
  }
  for (let wave = 0; wave < budget.waves; wave++) {
    const w = take('wave'), publisher = `parallel-${wave % 2 ? 'b' : 'a'}`;
    assert.equal(w.wave, wave); assert.equal(w.publisher, publisher);
    assert.equal(w.sample, wave >= budget.warmup*2); assert.equal(w.scheduled_us, wave * 500000);
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
    let a = c;
    if(mode==='ready'||mode==='spread-ready'||mixed) {
    const keys = new Set();
    for (let worker = 0; worker < 2; worker++) {
      const l = take('lease'); assert.equal(l.wave, wave); assert.equal(l.worker, worker);
      assert.equal(l.publisher, publisher); assert.equal(l.destination, 'parallel-destination');
      uint(l.token); assert(l.token > 0);
      assert(!keys.has(l.partition)); keys.add(l.partition);
      if(mode==='spread-ready') {
        assert(/^key-(0|[1-9][0-9]*)$/.test(l.partition));
        const key=Number(l.partition.slice(4));assert(Number.isSafeInteger(key)&&key<budget.perPublisher);
        assert.equal(l.event,`${publisher}-${key}`);
      } else {
      assert(['key-0', 'key-1'].includes(l.partition));
      const key = Number(l.partition.slice(-1));
      assert.equal(l.event, `${publisher}-${next[wave % 2][key]}`);
      next[wave % 2][key] += 2;
      }
      assert(!seen.has(l.event)); seen.add(l.event);
    }
    a = operations('ack', Math.max(...c.map(v => v.end_us)));
    for (const ack of a) assert(ack.begin_us >= Math.max(...c.map(v => v.end_us)));
    } else {
      for(const worker of [0,1]) assert.deepEqual(take('empty'),{kind:'empty',wave,worker,publisher});
    }
    const end = take('wave_completed'); assert.equal(end.wave, wave);
    uint(end.begin_us); uint(end.end_us);
    assert(end.begin_us >= w.dispatch_us && end.begin_us <= Math.min(...c.map(v => v.begin_us)));
    assert(end.end_us >= Math.max(...a.map(v => v.end_us))); previousEnd = end.end_us;
    if (Math.max(...c.map(v => v.begin_us)) < Math.min(...c.map(v => v.end_us))) overlaps++;
    claims.push(...c.map(v => ({wave, worker:v.worker, duration_us:v.end_us-v.begin_us})));
  }
  const final = new Set();
  for (let i = 0; i < budget.total; i++) {
    const v = take('final'); assert(!final.has(v.event)); final.add(v.event);
    assert.equal(v.published, seen.has(v.event));
  }
  for (const publisher of ['a','b']) for (let n = 0; n < budget.perPublisher; n++) assert(final.has(`parallel-${publisher}-${n}`));
  if(versioned) distribution('after');
  const result = take('result');
  assert.equal(result.status,status); assert.equal(result.waves,budget.waves);
  assert.equal(result.workers,2); assert.equal(result.publishers,2); assert.equal(result.rows,budget.total);
  assert.equal(result.claims,mode==='ready'||mode==='spread-ready'||mixed?budget.calls:0); assert.equal(result.stats,false);
  assert.equal(result.warmup_seconds,budget.warmup); assert.equal(result.sample_seconds,budget.sample);
  assert(overlaps > 0); assert.equal(result.overlap_waves,overlaps); assert.equal(cursor,rows.length);
  return {status, validation:'PARALLEL_CLAIMS_CHECKED', mode, waves:budget.waves, claim_calls:budget.calls, returned:seen.size, formal_claim_calls:budget.sample*4, overlap_waves:overlaps, claims_timing:claims};
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const dir = process.argv[2];
  const sealPath=path.join(dir,'journal-sealed.json');
  const result = analyze(fs.readFileSync(path.join(dir,'journal.jsonl'),'utf8'),fs.existsSync(sealPath)?JSON.parse(fs.readFileSync(sealPath,'utf8')):undefined);
  fs.writeFileSync(path.join(dir,'analysis.json'),JSON.stringify(result,null,2)+'\n');
  console.log(JSON.stringify(result));
}
