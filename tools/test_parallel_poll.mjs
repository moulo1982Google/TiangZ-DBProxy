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
for(const mode of ['none','all-blocked']) {
  const snapshot=phase=>({kind:'distribution',phase,total:1000,future_leased:mode==='none'?1000:0,dead:mode==='all-blocked'?4:0,future_available:0,owned:0});
  const empty=[{kind:'fixture',schema:2,mode,rows:1000,claim_calls:28},snapshot('before')];
  for(const r of structuredClone(rows)) {
    if(r.operation==='ack')continue;
    if(r.kind==='lease'){empty.push({kind:'empty',wave:r.wave,worker:r.worker,publisher:r.publisher});continue;}
    if(r.kind==='final')r.published=false;
    if(r.kind==='result'){empty.push(snapshot('after'));r.claims=0;}
    empty.push(r);
  }
  assert.equal(analyze(encode(empty)).returned,0);
  for(const mutate of [
    x=>x.find(v=>v.kind==='empty').publisher='wrong',
    x=>x.splice(x.findIndex(v=>v.kind==='empty'),1),
    x=>x.find(v=>v.kind==='distribution'&&v.phase==='after').owned=1,
    x=>x.find(v=>v.kind==='distribution'&&v.phase==='before').dead=999,
    x=>x.find(v=>v.kind==='final').published=true,
    x=>x.at(-1).claims=28,
  ]){const x=structuredClone(empty);mutate(x);assert.throws(()=>analyze(encode(x)));}
}
console.log('empty fixtures: two valid modes + 12 rejected mutations');
for(const mode of ['leased','backoff','leased-heads','backoff-heads','dead-heads']) {
  const boundary=phase=>[
    {kind:'distribution',phase,total:1000,future_leased:mode==='leased'?900:mode==='leased-heads'?4:0,dead:mode==='dead-heads'?4:0,future_available:mode==='backoff'?900:mode==='backoff-heads'?4:0,owned:0},
    ...['parallel-a','parallel-b'].map(publisher=>({kind:'reserve',phase,publisher,blocked:450,ready_pending:phase==='before'?50:36,blocked_published:0}))
  ];
  const mixed=[{kind:'fixture',schema:3,mode,rows:1000,claim_calls:28},...boundary('before')];
  for(const r of structuredClone(rows)) {
    if(r.kind==='lease')r.event=r.event.replace(/\d+$/,n=>String(Number(n)+450));
    if(r.kind==='final'){const n=Number(r.event.match(/\d+$/)[0]);r.published=n>=450&&n<464;}
    if(r.kind==='result')mixed.push(...boundary('after'));
    mixed.push(r);
  }
  assert.equal(analyze(encode(mixed)).returned,28);
  for(const mutate of [
    x=>x.find(v=>v.kind==='reserve').ready_pending=0,
    x=>x.find(v=>v.kind==='reserve'&&v.phase==='after').blocked_published=1,
    x=>x.find(v=>v.kind==='distribution').future_available=1,
    x=>x.find(v=>v.kind==='lease').event='parallel-a-0',
    x=>x.splice(x.findIndex(v=>v.kind==='reserve'),1),
  ]) { const x=structuredClone(mixed);mutate(x);assert.throws(()=>analyze(encode(x))); }
}
console.log('mixed fixtures: five valid modes + 25 rejected mutations');
const spreadBoundary=phase=>[
  {kind:'distribution',phase,total:1000,future_leased:0,dead:0,future_available:0,owned:0},
  ...['parallel-a','parallel-b'].map(publisher=>({kind:'spread',phase,publisher,total:500,partitions:500,pending:phase==='before'?500:486}))
];
const spread=[{kind:'fixture',schema:4,mode:'spread-ready',rows:1000,claim_calls:28},...spreadBoundary('before')];
for(const r of structuredClone(rows)){
  if(r.kind==='lease')r.partition='key-'+r.event.match(/\d+$/)[0];
  if(r.kind==='result')spread.push(...spreadBoundary('after'));
  spread.push(r);
}
assert.equal(analyze(encode(spread)).returned,28);
const swapped=structuredClone(spread), leases=swapped.filter(v=>v.kind==='lease');
for(let i=0;i<leases.length;i+=2){
  [leases[i].event,leases[i+1].event]=[leases[i+1].event,leases[i].event];
  [leases[i].partition,leases[i+1].partition]=[leases[i+1].partition,leases[i].partition];
}
assert.equal(analyze(encode(swapped)).returned,28);
for(const mutate of [
  x=>x.find(v=>v.kind==='spread').partitions=2,
  x=>x.find(v=>v.kind==='spread'&&v.phase==='after').pending=0,
  x=>x.find(v=>v.kind==='lease').partition='key-500',
  x=>x.find(v=>v.kind==='lease').event='parallel-a-499',
  x=>x.filter(v=>v.kind==='lease')[2].event='parallel-a-0',
]){const x=structuredClone(spread);mutate(x);assert.throws(()=>analyze(encode(x)));}
console.log('spread: two legal return orders + 5 rejected mutations');
