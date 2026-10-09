import assert from 'node:assert/strict';
const operations = {load:'load_snapshot',load_multi:'load_multi_snapshot',save:'save_snapshot',save_multi:'save_multi_snapshot',transaction:'apply_transaction',commit_records:'commit_records'};
export function analyzeSdk(rows) {
  const responses = rows.filter(x => x.kind === 'response');
  assert(responses.length, 'missing SDK responses');
  for (const r of responses) {
    assert(Array.isArray(r.sdk_attempts), 'missing SDK callbacks');
    // Reconnect/retry attempts are retained in raw evidence but cannot qualify
    // as an ordinary no-error performance comparison.
    assert.equal(r.sdk_attempts.length, 1, 'missing callback or retry');
    const a = r.sdk_attempts[0];
    assert.equal(a.operation, operations[r.op]);
    assert.equal(a.endpoint, 0);
    assert.equal(a.outcome, 'Success');
    for (const k of ['queue_wait_us','exchange_us']) assert(Number.isSafeInteger(a[k]) && a[k] >= 0);
    assert(a.queue_wait_us + a.exchange_us <= r.rpc_us, 'SDK timing exceeds encompassing driver call');
  }
  const stats = values => {
    values.sort((a,b)=>a-b);
    return {count:values.length,p50:values[Math.ceil(values.length*.5)-1],p99:values[Math.ceil(values.length*.99)-1],max:values.at(-1)};
  };
  return {status:'SDK_CALLBACKS_CHECKED',scope:'One successful attempt per call. Queue includes slot/writer acquisition; exchange includes codec/network/server/return. Independent percentiles are not additive.',
    operations:Object.keys(operations).map(op=>{
      const rs=responses.filter(x=>x.op===op && x.sample);assert(rs.length);
      return {op,queue_wait_us:stats(rs.map(x=>x.sdk_attempts[0].queue_wait_us)),exchange_us:stats(rs.map(x=>x.sdk_attempts[0].exchange_us))};
    })};
}
