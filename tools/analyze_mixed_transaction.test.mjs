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
