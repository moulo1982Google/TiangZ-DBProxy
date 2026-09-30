import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const root=process.argv[2];assert(root,'usage: analyze_repair_pairs.mjs <repair_pairs_RUN>');
const entries=fs.readFileSync(path.join(root,'order.jsonl'),'utf8').trim().split(/\r?\n/).map(JSON.parse);
assert.equal(entries.length,6);assert.equal(new Set(entries.map(x=>x.run)).size,6);
const analyzer=fileURLToPath(new URL('./analyze_mixed_paced.mjs',import.meta.url));
const runs=[];
for(const [i,e] of entries.entries()){
  assert.equal(e.round,Math.floor(i/2));
  assert.equal(e.mode,['control','repair','repair','control','control','repair'][i]);
  assert(/^[a-z][a-z0-9_]+$/.test(e.run));
  const dir=path.join(root,'..',`fault_process_${e.run}`,'mixed-paced');
  const checked=spawnSync(process.execPath,[analyzer,dir],{encoding:'utf8'});
  assert.equal(checked.status,0,`${e.run}: ${checked.stderr}`);
  const a=JSON.parse(checked.stdout),m=a.manifest;
  assert.equal(m.baseline,'B2');assert.equal(m.repair_mode,e.mode);
  assert.equal(m.repair_cache_ttl_ms,1800000,'explicit shared experimental TTL required');
  assert.equal(m.rate,20);assert.equal(m.concurrency,8);assert.equal(m.connections,4);
  assert.deepEqual(m.mix,[40,20,20,10,5,5]);
  if(i)for(const key of ['warmup','sample','batch','payload_bytes','payload_rule','shards','read_connections','runtime_workers','repair_rows'])assert.equal(m[key],runs[0].analysis.manifest[key]);
  const repair=JSON.parse(fs.readFileSync(path.join(dir,'repair.json'),'utf8'));
  if(i){assert.equal(repair.schema_version,runs[0].repair.schema_version);assert.equal(repair.baseline_scope,runs[0].repair.baseline_scope);}
  runs.push({...e,analysis:a,repair});
}
const median=x=>[...x].sort((a,b)=>a-b)[1];
const operations=runs[0].analysis.perOperation.map((op,k)=>{
  const values=mode=>runs.filter(x=>x.mode===mode).map(x=>x.analysis.perOperation[k].end_to_end_us.p99/1000);
  const control=values('control'),repair=values('repair'),before=median(control),after=median(repair);
  return {op:op.op,control_p99_ms:control,repair_p99_ms:repair,median_delta_ms:after-before,median_delta_percent:100*(after/before-1),exceeds_20_percent:after>before*1.2};
});
const result={status:runs.every(x=>x.analysis.result.full_timing)?'COMPLETE_REPAIR_PAIRED_MATRIX':'SMOKE_ONLY',capacity_proven:false,operations,
  repair:runs.map(x=>({run:x.run,mode:x.mode,targets:x.repair.targets,pending_peak:Math.max(...x.repair.observations.map(y=>y.pending)),remaining:x.repair.remaining,mismatches:x.repair.mismatches,seconds_including_verification:x.repair.seconds}))};
fs.writeFileSync(path.join(root,'analysis.json'),JSON.stringify(result,null,2));console.log(JSON.stringify(result,null,2));
