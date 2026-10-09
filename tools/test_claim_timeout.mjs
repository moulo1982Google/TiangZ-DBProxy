import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {analyzeTimeout} from './analyze_claim_timeout.mjs';
const rows=[{kind:'timeout_fixture',schema:1,run_id:'p7ct_local',rows:0,workers:2,claims:2,timeout_ms:5000,blocker_pid:10,scope:'empty_outbox_relation_lock'}, {kind:'lock_acquired',at_us:0},
  ...[0,1].map(worker=>({kind:'started',wave:0,worker,operation:'claim',at_us:1})),
  {kind:'blocked_peers',at_us:100,peers:[{pid:11,application:'p7ct_worker0'},{pid:12,application:'p7ct_worker1'}]},
  ...[0,1].map(worker=>({kind:'operation',wave:0,worker,operation:'claim',begin_us:2,end_us:5000002,outcome:'unknown'})),
  {kind:'lock_release',at_us:5000010,rollback_confirmed:true},{kind:'timeout_result',at_us:5000020,status:'EXPECTED_CLAIM_TIMEOUT_ONLY',unknown:2,retries:0,acks:0,remaining:0}];
function run(x){const raw=x.map(v=>JSON.stringify(v)).join('\n')+'\n';return analyzeTimeout(raw,{schema:1,file:'journal.jsonl',bytes:Buffer.byteLength(raw),sha256:createHash('sha256').update(raw).digest('hex')});}
assert.equal(run(rows).unknown,2);
const reversed=structuredClone(rows);[reversed[5],reversed[6]]=[reversed[6],reversed[5]];assert.equal(run(reversed).unknown,2);
const mutations=[x=>x.pop(),x=>x.splice(4,1),x=>x.push(x[8]),x=>x[0].run_id='production',x=>x[0].rows=1000,x=>x[0].timeout_ms=1000,x=>x[3].worker=0,x=>x[4].peers[1].pid=11,x=>x[4].peers[0].pid=10,x=>x[4].peers[1].application='other',x=>x[5].end_us=1000,x=>x[5].outcome='completed',x=>x[6].worker=0,x=>x[7].rollback_confirmed=false,x=>x[7].at_us=1,x=>x[8].retries=1,x=>x[8].acks=1,x=>x[8].remaining=1,x=>x[8].at_us=0];
for(const mutate of mutations){const x=structuredClone(rows);mutate(x);assert.throws(()=>run(x));}
console.log(`claim timeout: two legal completion orders + ${mutations.length} strict negative cases (synthetic only)`);
