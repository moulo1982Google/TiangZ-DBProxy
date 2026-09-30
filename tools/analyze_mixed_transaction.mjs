import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
export function analyzeTransactions(manifest, ledger, logs) {
  const traces = logs.replace(/\x1b\[[0-9;]*m/g,'').split(/\r?\n/).filter(x=>x.includes('ACCEPTANCE_TX_TRACE ')).map(x=>JSON.parse(x.slice(x.indexOf('ACCEPTANCE_TX_TRACE ')+20)));
  const outputMarker='ACCEPTANCE_TX_OUTPUT ';
  const outputs=logs.replace(/\x1b\[[0-9;]*m/g,'').split(/\r?\n/).filter(x=>x.includes(outputMarker)).map(x=>JSON.parse(x.slice(x.indexOf(outputMarker)+outputMarker.length)));
  const outputById=new Map(outputs.map(x=>[x.operation_sha256,x]));
  assert.equal(outputById.size,outputs.length,'duplicate output timing');
  assert.equal(outputs.length,traces.filter(x=>x.schema_version===2).length,'output timing count');
  assert.equal(new Set(traces.map(x=>x.schema_version)).size,1,'mixed trace versions');
  const rows=ledger.filter(x=>x.kind==='response' && x.op==='transaction');
  assert.equal(traces.length,rows.length,'transaction trace count');
  const byId=new Map(traces.map(x=>[x.operation_sha256,x]));assert.equal(byId.size,traces.length);
  const checked=rows.map(r=>{
    assert.equal(r.outcome.status,'success');
    const key=createHash('sha256').update(`${manifest.run}-${r.n}-0`).digest('hex');
    const t=byId.get(key);assert(t,'missing matching transaction trace');assert([1,2].includes(t.schema_version));
    assert(Number.isSafeInteger(t.total_us)&&t.total_us>=0&&t.total_us<=r.rpc_us);
    let trace_output_us=null;
    if(t.schema_version===2){const o=outputById.get(key);assert(o,'missing output timing');assert.equal(o.schema_version,1);assert(Number.isSafeInteger(o.output_us)&&o.output_us>=0);assert(t.total_us+o.output_us<=r.rpc_us,'trace plus output exceeds RPC');trace_output_us=o.output_us;}
    assert(Array.isArray(t.spans)&&t.spans.length<=64);
    for(const s of t.spans){assert(typeof s.stage==='string');assert(Number.isSafeInteger(s.begin_us)&&s.begin_us>=0);assert(Number.isSafeInteger(s.end_us)&&s.end_us>=s.begin_us&&s.end_us<=t.total_us);}
    for(const name of ['postgres_connection_wait','postgres_operation','postgres_write_operation','single_transaction_commit','committed_cache_sync'])
      assert.equal(t.spans.filter(s=>s.stage===name).length,1,`missing or repeated ${name}`);
    return {n:r.n,sample:r.sample,rpc_us:r.rpc_us,sdk_attempts:r.sdk_attempts,...t,trace_output_us};
  });
  const formal=checked.filter(x=>x.sample);assert(formal.length);
  const names=[...new Set(formal.flatMap(x=>x.spans.map(s=>s.stage)))];
  const stages=names.map(stage=>{const values=formal.flatMap(x=>x.spans.filter(s=>s.stage===stage).map(s=>s.end_us-s.begin_us)).sort((a,b)=>a-b);return {stage,count:values.length,mean_us:values.reduce((s,x)=>s+x,0)/values.length,p99_us:values[Math.ceil(values.length*.99)-1],max_us:values.at(-1)};});
  return {status:'TRANSACTION_TRACES_CHECKED',scope:'Same-request storage scopes, nested intervals must not be added; PG operation includes reconnect and SQL. Trace logging overhead affects both groups. No response-send timing.',stages,requests:checked};
}
