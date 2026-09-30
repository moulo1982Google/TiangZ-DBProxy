import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
const root=process.argv[2];assert(root,'usage: node tools/analyze_mixed_paced.mjs <mixed-paced directory>');
const read=n=>fs.readFileSync(path.join(root,n),'utf8');
const rows=read('requests.jsonl').trim().split(/\r?\n/).map(JSON.parse);
const checks=read('reconciliation.jsonl').trim().split(/\r?\n/).filter(Boolean).map(JSON.parse);
const result=JSON.parse(read('result.json'));
const manifest=rows.filter(r=>r.kind==='manifest');assert.equal(manifest.length,1);const m=manifest[0];
// Historical B2 runs predate the explicit baseline field.
const baseline=m.baseline??'B2';assert(['B1','B2'].includes(baseline));
let repair=null;
if(m.repair_mode && m.repair_mode!=='none') {
  assert(['control','repair'].includes(m.repair_mode));
  repair=JSON.parse(read('repair.json'));
  assert.equal(repair.mode,m.repair_mode);
  assert.equal(repair.targets,(m.warmup+m.sample)*2);
  assert.equal(m.repair_rows,repair.targets);
  assert.equal(repair.observations.length,repair.targets);
  repair.observations.forEach((r,i)=>{assert.equal(r.n,i);assert(r.pending>=0);});
}
assert.equal(m.cleanup,baseline==='B1'?'test-host-disabled':'production-enabled');
if(m.baseline){assert.equal(m.shards,2);assert.equal(m.read_connections,2);assert.equal(m.runtime_workers,4);}
const total=(m.warmup+m.sample)*m.rate;assert.equal(total,result.scheduled);
const stops=rows.filter(r=>r.kind==='guard_stop');assert(stops.length<=1);
assert.deepEqual(result.guard_stop??null,stops[0]??null);
assert.equal(result.full_timing,m.warmup===120 && m.sample===300 && stops.length===0);
const kinds=['load','load_multi','save','save_multi','transaction','commit_records'];
const kind=n=>{const x=n%20;return x<8?0:x<12?1:x<16?2:x<18?3:x===18?4:5;};
const byType=t=>new Map(rows.filter(r=>r.kind===t).map(r=>[r.n,r]));
const intents=byType('intent'),responses=byType('response'),dropped=byType('not_sent');
for(const [type,map] of [['intent',intents],['response',responses],['not_sent',dropped]])assert.equal(map.size,rows.filter(r=>r.kind===type).length,'duplicate ledger slot');
for(let n=0;n<total;n++){
  assert.notEqual(intents.has(n),dropped.has(n));assert.equal(intents.has(n),responses.has(n));
  for(const map of [intents,responses,dropped])if(map.has(n)){
    const row=map.get(n);assert.equal(row.op,kinds[kind(n)]);assert.equal(row.sample,n>=m.warmup*m.rate);
  }
}
assert.equal(intents.size+dropped.size,total);assert.equal(responses.size,result.responses);assert.equal(dropped.size,result.not_sent);
assert.equal(responses.size,intents.size);
if(Object.hasOwn(result,'guard_stop')){
  const policies=rows.filter(r=>r.kind==='guard_policy');assert.equal(policies.length,1);
  assert.deepEqual(policies[0],{kind:'guard_policy',version:1,dispatch_us:100000,in_flight_us:1000000,first_error_or_capacity_miss:true});
}
if(stops.length){
  const stop=stops[0];assert(Number.isInteger(stop.n)&&stop.n>=0&&stop.n<total);
  assert(['response_error','in_flight_limit','dispatch_over_100ms','in_flight_over_1s'].includes(stop.reason));
  assert.equal(dropped.size,total-stop.n);assert.equal(intents.size,stop.n);
  for(let n=stop.n;n<total;n++){assert(!intents.has(n));assert.equal(dropped.get(n)?.reason,'guard_stopped');}
  if(stop.reason==='in_flight_limit')assert(stop.in_flight>=m.concurrency);
  if(stop.reason==='dispatch_over_100ms')assert(stop.dispatch_us>100000);
  if(stop.reason==='in_flight_over_1s')assert(stop.oldest_us>1000000);
  if(stop.reason==='response_error')assert(result.errors>0);
}
assert.equal([...responses.values()].filter(r=>r.outcome.status!=='success').length,result.errors);
const sentWrites=[...intents.keys()].filter(n=>kind(n)>=2);
assert.equal(checks.length,sentWrites.length);assert.deepEqual([...new Set(checks.map(r=>r.n))].sort((a,b)=>a-b),sentWrites.sort((a,b)=>a-b));
assert(checks.filter(r=>!r.valid).length<=result.reconciliation_mismatches);
const p=v=>{if(!v.length)return null;assert(v.every(x=>Number.isFinite(x)&&x>=0));v.sort((a,b)=>a-b);return {count:v.length,p50:v[Math.ceil(v.length*.5)-1],p99:v[Math.ceil(v.length*.99)-1],max:v.at(-1)};};
const perOperation=kinds.map(op=>{const all=[...responses.values()].filter(r=>r.op===op&&r.sample);return {op,end_to_end_us:p(all.map(r=>r.end_to_end_us)),dispatch_us:p(all.map(r=>r.dispatch_us)),rpc_us:p(all.map(r=>r.rpc_us)),errors:all.filter(r=>r.outcome.status!=='success').length,not_sent:[...dropped.values()].filter(r=>r.op===op&&r.sample).length};});
let publication=null;
if(m.outbox_audit){
 publication=JSON.parse(fs.readFileSync(path.join(root,'outbox-publication.json'),'utf8'));
 assert.equal(publication.pg_rows,result.effect_rows/2);
 assert.equal(publication.entries.length,publication.pg_rows);
 assert.equal(new Set(publication.entries.map(e=>e.event_id)).size,publication.pg_rows);
 assert.equal(publication.stream,'dbproxy:outbox:mix-'+m.run);
}
const accepted=result.errors===0&&result.not_sent===0&&result.reconciliation_mismatches===0&&(!repair||(repair.remaining===0&&repair.mismatches===0));
const publicationValid=!publication||(publication.mismatches===0&&publication.redis_rows===publication.pg_rows&&publication.entries.every(e=>e.valid&&e.published&&e.redis_ids.length===1));
const analysis={publication,status:accepted&&publicationValid?(result.full_timing?'COMPLETE_SINGLE_TIMED_ROUND':'SMOKE_ONLY'):'REJECTED_LOAD',manifest:m,result,perOperation,capacity_proven:false};
fs.writeFileSync(path.join(root,'analysis.json'),JSON.stringify(analysis,null,2));console.log(JSON.stringify(analysis,null,2));
if(!accepted||!publicationValid)process.exitCode=1;
