import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {analyzeAckTimeout} from './analyze_ack_timeout.mjs';
import {fixture} from './test_ack_timeout_evidence.mjs';
const f=fixture;
const rows=[{kind:'ack_fixture',schema:1,run_id:f.run_id,database:f.database,leases:f.leases.map(l=>l.claimed_row),claim_end_us:50},
  {kind:'lock_acquired',at_us:f.locked_us,blocker_pid:f.blocker_pid},
  ...[0,1].map(worker=>({kind:'started',worker,wave:0,operation:'ack',at_us:150})),
  {kind:'blocked_peers',at_us:f.blocked_us,proofs:f.leases.map(l=>l.blocked)},
  ...f.leases.map(l=>({kind:'operation',worker:l.worker,wave:0,operation:'ack',begin_us:l.ack_begin_us,end_us:l.ack_end_us,outcome:'unknown'})),
  {kind:'after_timeout',at_us:f.after_timeout_us,proofs:f.leases.map(l=>l.after_timeout_blocked),rows:f.leases.map(l=>l.after_timeout_row)},
  {kind:'lock_release',at_us:f.release_us,rollback_confirmed:true},
  {kind:'final_state',at_us:f.final_us,rows:f.leases.map(l=>l.final_row),total_rows:2,active_peers:0,retries:0,extra_claims:0}];
function run(r,mutateSeal=()=>{}) {const raw=r.map(v=>JSON.stringify(v)).join('\n')+'\n';const seal={schema:1,file:'journal.jsonl',bytes:Buffer.byteLength(raw),sha256:createHash('sha256').update(raw).digest('hex')};mutateSeal(seal);return analyzeAckTimeout(raw,seal);}
assert.equal(run(rows).client_unknown,2);
const reversed=structuredClone(rows);[reversed[5],reversed[6]]=[reversed[6],reversed[5]];assert.equal(run(reversed).final_published,2);
const mutations=[r=>r.pop(),r=>r.push(r[9]),r=>r.splice(4,1),r=>r[3].worker=0,r=>r[5].operation='claim',r=>r[6].worker=0,r=>r[5].outcome='completed',r=>r[5].begin_us=0,r=>r[7].proofs.pop(),r=>r[7].proofs[0].state='idle',r=>r[7].rows[0].published=true,r=>r[8].rollback_confirmed=false,r=>r[9].rows[0].published=false,r=>r[9].retries=1,r=>r[0].database='postgres'];
for(const m of mutations){const r=structuredClone(rows);m(r);assert.throws(()=>run(r));}
assert.throws(()=>run(rows,s=>s.bytes--));assert.throws(()=>run(rows,s=>s.sha256='0'.repeat(64)));
console.log('ack sealed journal: two legal completion orders + 17 strict negatives; synthetic only');
