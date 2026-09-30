import assert from 'node:assert/strict';
import {analyze} from './analyze_parallel_poll.mjs';
const rows=[], seen=new Set(), next=[[0,1],[0,1]];
for(let wave=0;wave<14;wave++) {
  const t=wave*500000, publisher=`parallel-${wave%2?'b':'a'}`;
  rows.push({kind:'wave',wave,publisher,scheduled_us:t,dispatch_us:t,sample:wave>=4});
  for(const worker of [0,1]) rows.push({kind:'started',wave,worker,operation:'claim',at_us:t+1});
  for(const worker of [0,1]) rows.push({kind:'operation',wave,worker,operation:'claim',begin_us:t+2,end_us:t+100,outcome:'completed'});
  for(const worker of [0,1]) {
    const event=`${publisher}-${next[wave%2][worker]}`;next[wave%2][worker]+=2;seen.add(event);
    rows.push({kind:'lease',wave,worker,publisher,event,partition:`key-${worker}`,token:1,destination:'parallel-destination'});
  }
  for(const worker of [0,1]) rows.push({kind:'started',wave,worker,operation:'ack',at_us:t+101});
  for(const worker of [0,1]) rows.push({kind:'operation',wave,worker,operation:'ack',begin_us:t+102,end_us:t+200,outcome:'completed'});
  rows.push({kind:'wave_completed',wave,begin_us:t,end_us:t+201});
}
for(const p of ['a','b']) for(let n=0;n<500;n++) {const event=`parallel-${p}-${n}`;rows.push({kind:'final',event,published:seen.has(event)});}
rows.push({kind:'result',status:'SMOKE_ONLY',waves:14,workers:2,publishers:2,rows:1000,claims:28,stats:false,warmup_seconds:2,sample_seconds:5,overlap_waves:14});
const encode=x=>x.map(v=>JSON.stringify(v)).join('\n')+'\n';
assert.equal(analyze(encode(rows)).validation,'PARALLEL_CLAIMS_CHECKED');
const mutations=[
  x=>x.splice(1,1),
  x=>x.splice(1,0,x[1]),
  x=>x.find(v=>v.kind==='operation').outcome='unknown',
  x=>x.find(v=>v.kind==='lease').publisher='wrong',
  x=>x.find(v=>v.kind==='lease').event='parallel-a-2',
  x=>x.find(v=>v.kind==='lease').token=0,
  x=>x.find(v=>v.kind==='final').published=false,
  x=>x.find(v=>v.kind==='operation'&&v.operation==='ack').begin_us=1,
  x=>x.find(v=>v.kind==='wave').dispatch_us=100001,
  x=>x.at(-1).overlap_waves=0,
  x=>x.splice(1,0,{kind:'guard',wave:0,reason:'dispatch_lag'}),
  x=>x.find(v=>v.kind==='operation'&&v.operation==='ack').outcome='unknown',
  x=>x.splice(x.findIndex(v=>v.kind==='operation'&&v.operation==='ack'&&v.worker===1),1),
  x=>x.find(v=>v.kind==='started'&&v.operation==='ack').at_us=1,
  x=>x.find(v=>v.kind==='operation').end_us='100',
  x=>{for(const v of x) if(v.kind==='operation'&&v.operation==='claim'&&v.worker===1) v.begin_us=v.end_us; x.at(-1).overlap_waves=0;},
  x=>x.pop(),
];
for(const mutate of mutations){const x=structuredClone(rows);mutate(x);assert.throws(()=>analyze(encode(x)));}
assert.throws(()=>analyze(encode(rows).trimEnd()));
console.log(`parallel ledger: valid fixture + ${mutations.length+1} rejected mutations`);
