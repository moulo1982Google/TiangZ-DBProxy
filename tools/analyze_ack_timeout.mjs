import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {reconcileAckTimeout} from './ack_timeout_evidence.mjs';
export function analyzeAckTimeout(raw,seal) {
  assert(raw.endsWith('\n'));
  assert.deepEqual(seal,{schema:1,file:'journal.jsonl',bytes:Buffer.byteLength(raw),sha256:createHash('sha256').update(raw).digest('hex')});
  const rows=raw.trimEnd().split('\n').map(v=>JSON.parse(v));assert.equal(rows.length,10);
  const [f,lock]=rows;assert.equal(f.kind,'ack_fixture');assert.equal(f.schema,1);assert.equal(f.leases.length,2);
  assert.equal(lock.kind,'lock_acquired');
  const started=new Map(),ops=new Map();let blocked;
  for(const r of rows.slice(2,7)) {
    if(r.kind==='blocked_peers') {assert(!blocked&&started.size===2&&ops.size===0);blocked=r;continue;}
    assert.equal(r.wave,0);assert.equal(r.operation,'ack');assert([0,1].includes(r.worker));
    if(r.kind==='started') {assert(!blocked&&!started.has(r.worker));assert(Number.isSafeInteger(r.at_us)&&r.at_us>=lock.at_us);started.set(r.worker,r.at_us);}
    else {assert.equal(r.kind,'operation');assert(blocked&&!ops.has(r.worker));assert.equal(r.outcome,'unknown');assert(r.begin_us>=started.get(r.worker));ops.set(r.worker,r);}
  }
  assert(blocked&&ops.size===2);
  const after=rows[7],release=rows[8],final=rows[9];
  assert.equal(after.kind,'after_timeout');assert.equal(release.kind,'lock_release');assert.equal(final.kind,'final_state');
  for(const list of [blocked.proofs,after.proofs,after.rows,final.rows])assert.equal(list.length,2);
  const evidence={schema:1,run_id:f.run_id,database:f.database,scope:'leased_rows_ack_blocked_then_released',timeout_ms:5000,
    retries:final.retries,extra_claims:final.extra_claims,blocker_pid:lock.blocker_pid,locked_us:lock.at_us,
    blocked_us:blocked.at_us,after_timeout_us:after.at_us,release_us:release.at_us,final_us:final.at_us,
    rollback_confirmed:release.rollback_confirmed,final_active_peers:final.active_peers,total_rows:final.total_rows,
    leases:f.leases.map((l,worker)=>({worker,owner:l.owner,publisher:l.publisher,event_id:l.event_id,partition_key:l.partition_key,
      pid:blocked.proofs[worker].pid,token:l.token,claim_end_us:f.claim_end_us,ack_begin_us:ops.get(worker).begin_us,
      ack_end_us:ops.get(worker).end_us,ack_outcome:ops.get(worker).outcome,blocked:blocked.proofs[worker],
      after_timeout_blocked:after.proofs[worker],claimed_row:l,after_timeout_row:after.rows[worker],final_row:final.rows[worker]}))};
  return {...reconcileAckTimeout(evidence),validation:'SEALED_ACK_UNKNOWN_RECONCILIATION_CHECKED'};
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)) {
  const dir=process.argv[2];const result=analyzeAckTimeout(fs.readFileSync(path.join(dir,'journal.jsonl'),'utf8'),JSON.parse(fs.readFileSync(path.join(dir,'journal-sealed.json'),'utf8')));
  fs.writeFileSync(path.join(dir,'ack-timeout-analysis.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result));
}
