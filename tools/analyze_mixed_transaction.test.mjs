import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {analyzeTransactions} from './analyze_mixed_transaction.mjs';
test('same-request digest, required boundaries and complete trace accounting',()=>{
  const m={run:'probe'},rows=[{kind:'response',n:18,op:'transaction',sample:true,rpc_us:100,outcome:{status:'success'}}];
  const t={schema_version:1,operation_sha256:createHash('sha256').update('probe-18-0').digest('hex'),total_us:90,
    spans:['postgres_connection_wait','postgres_operation','postgres_write_operation','single_transaction_commit','committed_cache_sync'].map(stage=>({stage,begin_us:1,end_us:80}))};
  const log=x=>'prefix ACCEPTANCE_TX_TRACE '+JSON.stringify(x);
  assert.equal(analyzeTransactions(m,rows,log(t)).status,'TRANSACTION_TRACES_CHECKED');
  assert.throws(()=>analyzeTransactions(m,rows,log(t)+'\n'+log(t)));
  assert.throws(()=>analyzeTransactions(m,rows,log({...t,operation_sha256:'wrong'})));
  assert.throws(()=>analyzeTransactions(m,rows,log({...t,total_us:101})));
  assert.throws(()=>analyzeTransactions(m,rows,log({...t,spans:t.spans.slice(1)})));
  assert.throws(()=>analyzeTransactions(m,rows,log({...t,spans:[...t.spans,{stage:'invalid',begin_us:2,end_us:1}]})));
});
test('version two requires unique bounded same-request output timing',()=>{
 const m={run:'probe'},rows=[{kind:'response',n:18,op:'transaction',sample:true,rpc_us:100,outcome:{status:'success'}}];
 const id=createHash('sha256').update('probe-18-0').digest('hex');
 const t={schema_version:2,operation_sha256:id,total_us:90,spans:['postgres_connection_wait','postgres_operation','postgres_write_operation','single_transaction_commit','committed_cache_sync'].map(stage=>({stage,begin_us:1,end_us:80}))};
 const base='ACCEPTANCE_TX_TRACE '+JSON.stringify(t),out=x=>'\nACCEPTANCE_TX_OUTPUT '+JSON.stringify(x),o={schema_version:1,operation_sha256:id,output_us:9};
 assert.equal(analyzeTransactions(m,rows,base+out(o)).requests[0].trace_output_us,9);
 for(const logs of [base,base+out(o)+out(o),base+out({...o,operation_sha256:'wrong'}),base+out({...o,output_us:11}),base+out({...o,output_us:-1}),base+out({...o,output_us:'9'})])assert.throws(()=>analyzeTransactions(m,rows,logs));
});
test('version three validates response association and monotonic boundaries',()=>{
 const m={run:'probe'},rows=[{kind:'response',n:18,op:'transaction',sample:true,rpc_us:120,outcome:{status:'success'}}];
 const id=createHash('sha256').update('probe-18-0').digest('hex');
 const t={schema_version:3,operation_sha256:id,total_us:90,spans:['postgres_connection_wait','postgres_operation','postgres_write_operation','single_transaction_commit','committed_cache_sync'].map(stage=>({stage,begin_us:1,end_us:80}))};
 const base='ACCEPTANCE_TX_TRACE '+JSON.stringify(t)+'\nACCEPTANCE_TX_OUTPUT '+JSON.stringify({schema_version:1,operation_sha256:id,output_us:9});
 const v={schema_version:1,operation_sha256:id,handler_begin_us:2,queued_us:104,write_begin_us:108,write_end_us:125};
 const log=x=>'\nACCEPTANCE_TX_RESPONSE '+JSON.stringify(x);
 assert.equal(analyzeTransactions(m,rows,base+log(v)).requests[0].response_timing.write_end_us,125);
 for(const x of [base,base+log(v)+log(v),base+log({...v,operation_sha256:'wrong'}),base+log({...v,queued_us:98}),base+log({...v,write_begin_us:103}),base+log({...v,write_end_us:-1})])assert.throws(()=>analyzeTransactions(m,rows,x));
});
