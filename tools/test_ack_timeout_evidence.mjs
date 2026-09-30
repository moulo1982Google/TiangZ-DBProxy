import assert from 'node:assert/strict';
import {reconcileAckTimeout} from './ack_timeout_evidence.mjs';
const run='p7at_local';
const fixture={schema:1,run_id:run,database:run,scope:'leased_rows_ack_blocked_then_released',
  timeout_ms:5000,retries:0,extra_claims:0,blocker_pid:10,locked_us:100,blocked_us:300,
  after_timeout_us:5000300,release_us:5000400,final_us:5100000,rollback_confirmed:true,
  final_active_peers:0,total_rows:2,leases:[0,1].map(worker=>{
    const event_id=`${run}_event${worker}`,owner=`p7at_worker${worker}`,pid=11+worker;
    const row={event_id,publisher:'p7at_publisher',partition_key:`p7at_partition${worker}`,
      token:1,owner,published:false,lease_present:true,lease_valid:true,attempt_count:0};
    const proof={database:run,pid,application:owner,state:'active',wait_event_type:'Lock',blocking_pids:[10]};
    return {worker,owner,pid,event_id,publisher:row.publisher,partition_key:row.partition_key,token:1,
      claim_end_us:50,ack_begin_us:200,ack_end_us:5000200,ack_outcome:'unknown',blocked:proof,
      after_timeout_blocked:structuredClone(proof),claimed_row:row,after_timeout_row:structuredClone(row),
      final_row:{...row,owner:null,published:true,lease_present:false,lease_valid:false}};
  })};
assert.equal(reconcileAckTimeout(fixture).client_unknown,2);
const tests=[e=>e.database='postgres',e=>e.run_id='p7ct_other',e=>e.scope='committed_response_loss',
  e=>e.timeout_ms=1000,e=>e.retries=1,e=>e.extra_claims=1,e=>e.rollback_confirmed=false,
  e=>e.final_active_peers=1,e=>e.total_rows=3,e=>e.leases.pop(),e=>e.release_us=e.after_timeout_us,
  e=>e.final_us=e.release_us-1,e=>e.final_us=e.release_us+5000001,
  e=>{e.release_us=e.after_timeout_us+2000001;e.final_us=e.release_us;},
  e=>e.blocked_us=2000201,e=>e.leases[1].pid=e.leases[0].pid,
  e=>e.leases[0].pid=e.blocker_pid,e=>e.leases[0].token=0,e=>e.leases[0].token=1.5,
  e=>e.leases[0].ack_end_us=5000199,e=>e.leases[0].ack_outcome='completed',
  e=>e.leases[0].claim_end_us=e.locked_us+1,e=>e.leases[0].ack_begin_us=e.blocked_us+1,
  e=>e.leases[0].after_timeout_blocked.state='idle',e=>e.leases[0].blocked.blocking_pids=[],
  e=>e.leases[0].blocked.database='other',e=>e.leases[0].after_timeout_row.published=true,
  e=>e.leases[0].after_timeout_row.lease_valid=false,e=>e.leases[0].final_row.token=2,
  e=>e.leases[0].final_row.owner='other',e=>e.leases[0].final_row.published=false,
  e=>e.leases[0].final_row.attempt_count=2,e=>e.leases[0].final_row.event_id='other'];
for(const mutate of tests){const e=structuredClone(fixture);mutate(e);assert.throws(()=>reconcileAckTimeout(e));}
console.log(`ack reconciliation contract: one synthetic positive + ${tests.length} strict negatives; no real DB execution`);
