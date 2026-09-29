import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
const root=process.argv[2];assert(root,'usage: node tools/analyze_mixed_paced.mjs <mixed-paced directory>');
const read=n=>fs.readFileSync(path.join(root,n),'utf8');
const rows=read('requests.jsonl').trim().split(/\r?\n/).map(JSON.parse);
const checks=read('reconciliation.jsonl').trim().split(/\r?\n/).filter(Boolean).map(JSON.parse);
const result=JSON.parse(read('result.json'));
const manifest=rows.filter(r=>r.kind==='manifest');assert.equal(manifest.length,1);const m=manifest[0];
const total=(m.warmup+m.sample)*m.rate;assert.equal(total,result.scheduled);
assert.equal(result.full_timing,m.warmup===120 && m.sample===300);
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
assert.equal([...responses.values()].filter(r=>r.outcome.status!=='success').length,result.errors);
const sentWrites=[...intents.keys()].filter(n=>kind(n)>=2);
assert.equal(checks.length,sentWrites.length);assert.deepEqual([...new Set(checks.map(r=>r.n))].sort((a,b)=>a-b),sentWrites.sort((a,b)=>a-b));
assert(checks.filter(r=>!r.valid).length<=result.reconciliation_mismatches);
const p=v=>{if(!v.length)return null;assert(v.every(x=>Number.isFinite(x)&&x>=0));v.sort((a,b)=>a-b);return {count:v.length,p50:v[Math.ceil(v.length*.5)-1],p99:v[Math.ceil(v.length*.99)-1],max:v.at(-1)};};
const perOperation=kinds.map(op=>{const all=[...responses.values()].filter(r=>r.op===op&&r.sample);return {op,end_to_end_us:p(all.map(r=>r.end_to_end_us)),dispatch_us:p(all.map(r=>r.dispatch_us)),rpc_us:p(all.map(r=>r.rpc_us)),errors:all.filter(r=>r.outcome.status!=='success').length,not_sent:[...dropped.values()].filter(r=>r.op===op&&r.sample).length};});
const accepted=result.errors===0&&result.not_sent===0&&result.reconciliation_mismatches===0;
const analysis={status:accepted?(result.full_timing?'COMPLETE_SINGLE_TIMED_ROUND':'SMOKE_ONLY'):'REJECTED_LOAD',manifest:m,result,perOperation,capacity_proven:false};
fs.writeFileSync(path.join(root,'analysis.json'),JSON.stringify(analysis,null,2));console.log(JSON.stringify(analysis,null,2));
if(!accepted)process.exitCode=1;
