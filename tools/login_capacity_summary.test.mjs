import test from 'node:test';
import assert from 'node:assert/strict';
import { capacitySummary } from './login_capacity_summary.mjs';
function fixture() {
  const cases=[];
  for (const domains of [5,30]) for(const mode of ['postgres','cache']) for(const concurrency of [1,2,4,8,12,16]) for(const round of [0,1,2]) {
    cases.push({domains, mode, concurrency, round, operations:100, loadsPerSecond:concurrency<4?concurrency*100:400,
      p95Ms:concurrency, postgresFallbacks:0, cacheStaleHits:0, cacheHits:mode==='cache'?100*domains:0});
  }
  return {status:'completed',profile:{sweep:true},cases};
}
test('吞吐平台优先低并发，不把max_connections当最佳并发',()=>{
  assert.ok(capacitySummary(fixture()).every(x=>x.candidate.concurrency===4));
});
test('不接受未完成或回源污染的报告',()=>{
  const r=fixture(); r.status='running'; assert.throws(()=>capacitySummary(r));
  r.status='completed'; r.cases.find(x=>x.mode==='cache').postgresFallbacks=1;
  assert.throws(()=>capacitySummary(r));
});
test('不接受重复轮次',()=>{
  const r=fixture();r.cases[1].round=0;assert.throws(()=>capacitySummary(r));
});
