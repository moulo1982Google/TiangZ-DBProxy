import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
export function analyzeTimeout(raw,seal) {
  assert(raw.endsWith('\n'));
  assert.deepEqual(seal,{schema:1,file:'journal.jsonl',bytes:Buffer.byteLength(raw),sha256:createHash('sha256').update(raw).digest('hex')});
  const rows=raw.trimEnd().split('\n').map(v=>JSON.parse(v));assert.equal(rows.length,9);
  const f=rows[0];assert(/^p7ct_[a-z0-9_]+$/.test(f.run_id)&&f.run_id.length<=24);
  const uint=v=>assert(Number.isSafeInteger(v)&&v>=0);
  uint(f.blocker_pid);assert(f.blocker_pid>0);
  assert.deepEqual(f,{kind:'timeout_fixture',schema:1,run_id:f.run_id,rows:0,workers:2,claims:2,timeout_ms:5000,blocker_pid:f.blocker_pid,scope:'empty_outbox_relation_lock'});
  const lock=rows[1];assert.equal(lock.kind,'lock_acquired');uint(lock.at_us);
  const started=new Map(),ended=new Map();let proof;
  for(const v of rows.slice(2,7)) {
    if(v.kind==='blocked_peers') {
      assert(!proof&&started.size===2&&ended.size===0);proof=v;uint(v.at_us);
      assert.equal(v.peers.length,2);assert.deepEqual(v.peers.map(p=>p.application),['p7ct_worker0','p7ct_worker1']);
      const pids=v.peers.map(p=>p.pid);for(const pid of pids){uint(pid);assert(pid>0&&pid!==f.blocker_pid);}assert.equal(new Set(pids).size,2);
      assert(v.at_us>=Math.max(...started.values()));continue;
    }
    assert.equal(v.wave,0);assert.equal(v.operation,'claim');assert([0,1].includes(v.worker));
    if(v.kind==='started'){assert(!started.has(v.worker)&&!proof);uint(v.at_us);assert(v.at_us>=lock.at_us);started.set(v.worker,v.at_us);}
    else {assert.equal(v.kind,'operation');assert(proof&&started.has(v.worker)&&!ended.has(v.worker));assert.equal(v.outcome,'unknown');uint(v.begin_us);uint(v.end_us);assert(v.begin_us>=started.get(v.worker)&&v.end_us>=proof.at_us&&v.end_us-v.begin_us>=5000000);ended.set(v.worker,v.end_us);}
  }
  assert(proof&&ended.size===2);
  const release=rows[7];assert.equal(release.kind,'lock_release');assert.equal(release.rollback_confirmed,true);uint(release.at_us);assert(release.at_us>=Math.max(...ended.values()));
  const result=rows[8];uint(result.at_us);assert(result.at_us>=release.at_us);
  assert.deepEqual(result,{kind:'timeout_result',at_us:result.at_us,status:'EXPECTED_CLAIM_TIMEOUT_ONLY',unknown:2,retries:0,acks:0,remaining:0});
  return {status:result.status,validation:'BLOCKED_CLAIM_UNKNOWN_JOURNAL_CHECKED',run_id:f.run_id,unknown:2,scope:'Empty dedicated DB, two real blocked calls; not a committed operation timeout, capacity result, or full workflow deadline guarantee.'};
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)) {
  const dir=process.argv[2];const result=analyzeTimeout(fs.readFileSync(path.join(dir,'journal.jsonl'),'utf8'),JSON.parse(fs.readFileSync(path.join(dir,'journal-sealed.json'),'utf8')));
  fs.writeFileSync(path.join(dir,'timeout-analysis.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result));
}
