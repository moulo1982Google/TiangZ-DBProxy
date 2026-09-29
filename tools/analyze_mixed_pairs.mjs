import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const root=process.argv[2];
assert(root,'usage: node tools/analyze_mixed_pairs.mjs <mixed_pairs_RUN directory>');
const entries=fs.readFileSync(path.join(root,'order.jsonl'),'utf8').trim().split(/\r?\n/).map(JSON.parse);
assert.equal(entries.length,12,'incomplete paired series');
assert.equal(new Set(entries.map(x=>x.run)).size,12);
assert.notEqual(entries[0].rate,entries[6].rate);
const analyzer=fileURLToPath(new URL('./analyze_mixed_paced.mjs',import.meta.url));
const runs=entries.map((entry,i)=>{
  assert.equal(entry.round,Math.floor((i%6)/2));
  assert.equal(entry.baseline,['B1','B2','B2','B1','B1','B2'][i%6]);
  assert(/^[a-z][a-z0-9_]+$/.test(entry.run));
  const dir=path.join(root,'..',`fault_process_${entry.run}`,'mixed-paced');
  const checked=spawnSync(process.execPath,[analyzer,dir],{encoding:'utf8'});
  assert.equal(checked.status,0,`rejected run ${entry.run}: ${checked.stderr}`);
  const a=JSON.parse(fs.readFileSync(path.join(dir,'analysis.json'),'utf8'));
  assert.equal(a.manifest.baseline,entry.baseline);assert.equal(a.manifest.rate,entry.rate);
  assert.equal(entry.rate,entries[Math.floor(i/6)*6].rate);
  assert.deepEqual(a.manifest.mix,[40,20,20,10,5,5]);
  for(const field of ['warmup','sample','concurrency','connections','batch','payload_bytes','payload_rule','shards','read_connections','runtime_workers']){
    if(i)assert.equal(a.manifest[field],JSON.parse(fs.readFileSync(path.join(root,'..',`fault_process_${entries[0].run}`,'mixed-paced','analysis.json'),'utf8')).manifest[field]);
  }
  return {...entry,analysis:a};
});
const median=x=>x.toSorted((a,b)=>a-b)[1];
const groups=[0,6].map(offset=>{
  const group=runs.slice(offset,offset+6);
  return {rate:group[0].rate,operations:group[0].analysis.perOperation.map((op,k)=>{
    const values=baseline=>group.filter(x=>x.baseline===baseline).map(x=>x.analysis.perOperation[k].end_to_end_us.p99/1000);
    const b1=values('B1'),b2=values('B2'),before=median(b1),after=median(b2);
    return {op:op.op,b1_p99_ms:b1,b2_p99_ms:b2,median_delta_ms:after-before,median_delta_percent:100*(after/before-1),exceeds_20_percent:after>before*1.2};
  })};
});
const full=runs.every(x=>x.analysis.result.full_timing);
const result={status:full?'COMPLETE_PAIRED_TIMED_MATRIX':'SMOKE_ONLY',capacity_proven:false,groups};
fs.writeFileSync(path.join(root,'analysis.json'),JSON.stringify(result,null,2));
console.log(JSON.stringify(result,null,2));
