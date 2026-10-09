import {test} from 'node:test';
import assert from 'node:assert/strict';
import {analyzeSdk} from './analyze_mixed_sdk.mjs';
const names=['load','load_multi','save','save_multi','transaction','commit_records'];
const sdk=['load_snapshot','load_multi_snapshot','save_snapshot','save_multi_snapshot','apply_transaction','commit_records'];
const rows=()=>names.map((op,i)=>({kind:'response',op,sample:true,rpc_us:20,sdk_attempts:[{operation:sdk[i],endpoint:0,outcome:'Success',queue_wait_us:2,exchange_us:12}]}));
test('valid callbacks and raw-bound failures',()=>{
  assert.equal(analyzeSdk(rows()).operations[4].queue_wait_us.p99,2);
  for(const alter of [r=>r.sdk_attempts=[],r=>r.sdk_attempts.push(r.sdk_attempts[0]),r=>r.sdk_attempts[0].operation='wrong',r=>r.sdk_attempts[0].queue_wait_us=-1,r=>r.rpc_us=1,r=>r.sdk_attempts[0].outcome='Timeout']){
    const rs=rows();alter(rs[0]);assert.throws(()=>analyzeSdk(rs));
  }
});
