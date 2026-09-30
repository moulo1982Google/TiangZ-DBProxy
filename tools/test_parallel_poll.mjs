import assert from 'node:assert/strict';
import {analyze,validateBudget} from './analyze_parallel_poll.mjs';
import {createHash} from 'node:crypto';
const versionedFixtures=[];
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
  versionedFixtures.push(empty);
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
  versionedFixtures.push(mixed);
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
versionedFixtures.push(spread);
const dense=[{kind:'fixture',schema:2,mode:'ready',rows:1000,claim_calls:28},spreadBoundary('before')[0],...structuredClone(rows)];
dense.splice(dense.length-1,0,spreadBoundary('after')[0]);
versionedFixtures.push(dense);
const sealOf=raw=>({schema:1,file:'journal.jsonl',bytes:Buffer.byteLength(raw),sha256:createHash('sha256').update(raw).digest('hex')});
for(const fixture of versionedFixtures){
  const current=structuredClone(fixture);
  Object.assign(current[0],{schema:5,warmup_seconds:2,sample_seconds:5,workers:2,publishers:2,claims_per_second:4,stats:false,seal_required:true});
  const raw=encode(current), seal=sealOf(raw);
  assert.equal(analyze(raw,seal).validation,'PARALLEL_CLAIMS_CHECKED');
  for(const invalid of [undefined,{...seal,schema:2},{...seal,file:'other.jsonl'},{...seal,bytes:seal.bytes-1},{...seal,sha256:'0'.repeat(64)}]) assert.throws(()=>analyze(raw,invalid));
  assert.throws(()=>analyze(raw+'\n',seal));
  assert.throws(()=>analyze(raw.trimEnd(),sealOf(raw.trimEnd())));
  for(const [key,value] of [['claim_calls',1680],['warmup_seconds',120],['workers',4],['seal_required',false]]) {
    const changed=structuredClone(current); changed[0][key]=value;
    const bad=encode(changed);assert.throws(()=>analyze(bad,sealOf(bad)));
  }
}
console.log('schema5: nine distributions + 99 seal/configuration rejections');

// Generate independent evidence fixtures with different row counts and durations.
// This is offline analysis coverage, never a PostgreSQL performance result.
function resized(source,total,warmup=2,sample=5) {
  const mode=source[0].mode, mixed=['leased','backoff','leased-heads','backoff-heads','dead-heads'].includes(mode);
  const waves=(warmup+sample)*2,calls=waves*2,per=total/2,blocked=mixed?total*9/20:0;
  const empty=['none','all-blocked'].includes(mode), current=[], published=new Set();
  current.push({...source[0],schema:6,rows:total,claim_calls:calls,warmup_seconds:warmup,sample_seconds:sample,workers:2,publishers:2,claims_per_second:4,stats:false,seal_required:true});
  const boundary=phase=>source.filter(v=>['distribution','reserve','spread'].includes(v.kind)&&v.phase===phase).map(v=>{
    const n={...v};
    if(n.kind==='distribution') {n.total=total;if(n.future_leased>4)n.future_leased=mode==='none'?total:blocked*2;if(n.future_available>4)n.future_available=blocked*2;}
    if(n.kind==='reserve'){n.blocked=blocked;n.ready_pending=per-blocked-(phase==='after'?calls/2:0);}
    if(n.kind==='spread'){n.total=per;n.partitions=per;n.pending=per-(phase==='after'?calls/2:0);}
    return n;
  });
  current.push(...boundary('before'));
  for(let wave=0;wave<waves;wave++) {
    const origin=wave%2,delta=(wave-origin)*500000;
    for(const old of source.filter(v=>v.wave===origin)) {
      const n={...old,wave};
      for(const k of ['scheduled_us','dispatch_us','at_us','begin_us','end_us'])if(k in n)n[k]+=delta;
      if(n.kind==='wave')n.sample=wave>=warmup*2;
      if(n.kind==='lease'){
        const index=blocked+Math.floor(wave/2)*2+n.worker;
        n.event=`${n.publisher}-${index}`;published.add(n.event);
        if(mode==='spread-ready')n.partition=`key-${index}`;
      }
      current.push(n);
    }
  }
  for(const p of ['a','b'])for(let n=0;n<per;n++) {const event=`parallel-${p}-${n}`;current.push({kind:'final',event,published:published.has(event)});}
  current.push(...boundary('after'),{...source.at(-1),status:sample===5?'SMOKE_ONLY':'BOUNDED_LEDGER_ONLY',rows:total,waves,claims:empty?0:calls,overlap_waves:waves,warmup_seconds:warmup,sample_seconds:sample});
  return current;
}
for(const source of versionedFixtures) {
  const current=resized(source,1040), raw=encode(current);
  assert.equal(analyze(raw,sealOf(raw)).claim_calls,28);
  for(const mutate of [
    x=>x[0].rows=1000,
    x=>x[0].rows=100000,
    x=>x[0].warmup_seconds=120,
    x=>x.find(v=>v.kind==='distribution').total=1000,
    x=>x.find(v=>v.kind==='final').event='parallel-a-99999',
    x=>x.splice(x.findIndex(v=>v.kind==='final'),1),
  ]){const changed=structuredClone(current);mutate(changed);const bad=encode(changed);assert.throws(()=>analyze(bad,sealOf(bad)));}
}
const formal=resized(versionedFixtures.find(v=>v[0].mode==='leased-heads'),33600,120,300);
const formalRaw=encode(formal), checked=analyze(formalRaw,sealOf(formalRaw));
assert.equal(checked.status,'BOUNDED_LEDGER_ONLY');assert.equal(checked.claim_calls,1680);assert.equal(checked.formal_claim_calls,1200);
console.log('schema6: nine resized distributions + 54 rejections; synthetic 33600-row/840-wave ledger checked (not database evidence)');
for(const [w,s,n,m] of [[120,300,33560,true],[120,300,1000,false],[120,300,1000,true],[2,5,1001,false],[2,5,100000,true],[Number.MAX_SAFE_INTEGER,300,33600,true],[120,301,33600,true],[2,5,'1000',false]]) assert.throws(()=>validateBudget(w,s,n,m));
for(const source of versionedFixtures)for(const enabled of [false,true]){
  const current=resized(source,1000);current[0].schema=7;current[0].stats=enabled;current.at(-1).stats=enabled;
  for(let wave=0;wave<14;wave+=2){
    const t=wave*500000,empty=['none','all-blocked'].includes(source[0].mode),mode=source[0].mode;
    const dead=['all-blocked','dead-heads'].includes(mode)?4:0,processing=mode==='none'?1000:mode==='leased'?900:mode==='leased-heads'?4:0;
    const records=[{kind:'stats_slot',wave,worker:0,enabled,scheduled_us:t}];
    if(enabled)records.push({kind:'started',wave,worker:0,operation:'stats',at_us:t+1},{kind:'operation',wave,worker:0,operation:'stats',begin_us:t+2,end_us:t+99,outcome:'completed'});
    records.push({kind:'stats_result',wave,worker:0,enabled,at_us:t+100,counts:enabled?{pending:1000-(empty?0:wave*2)-dead-processing,processing,dead_lettered:dead,oldest_age_ms:1}:null});
    current.splice(current.findIndex(v=>v.wave===wave&&v.kind==='wave')+1,0,...records);
  }
  const raw=encode(current);assert.equal(analyze(raw,sealOf(raw)).stats_calls,enabled?7:0);
  for(const phase of [0,1]){
    const newer=structuredClone(current);newer[0].schema=8;newer[0].stats_phase=phase;
    if(phase===1){
      const selected=newer.filter(v=>v.kind.startsWith('stats_')||v.operation==='stats');
      for(const v of selected)newer.splice(newer.indexOf(v),1);
      for(let wave=1;wave<14;wave+=2){
        const group=selected.filter(v=>v.wave===wave-1);
        for(const v of group){v.wave=wave;for(const k of ['scheduled_us','at_us','begin_us','end_us'])if(k in v)v[k]+=500000;if(v.counts&&!['none','all-blocked'].includes(source[0].mode))v.counts.pending-=2;}
        newer.splice(newer.findIndex(v=>v.kind==='wave'&&v.wave===wave)+1,0,...group);
      }
    }
    const newRaw=encode(newer);assert.equal(analyze(newRaw,sealOf(newRaw)).stats_phase,phase);
    for(const mutate of [x=>x[0].stats_phase=2,x=>x[0].stats_phase=1-phase,x=>x.find(v=>v.kind==='stats_slot').scheduled_us+=500000]){const bad=structuredClone(newer);mutate(bad);const text=encode(bad);assert.throws(()=>analyze(text,sealOf(text)));}
  }
  for(const mutate of [
    x=>x.splice(x.findIndex(v=>v.kind==='stats_result'),1),
    x=>x.find(v=>v.kind==='stats_slot').worker=1,
    x=>x.find(v=>v.kind==='stats_result').at_us=0,
    x=>x.find(v=>v.kind==='stats_slot').wave=1,
    x=>x.at(-1).stats=!enabled,
    x=>x.find(v=>v.kind==='stats_result').counts=enabled?{pending:0,processing:0,dead_lettered:0,oldest_age_ms:0}:{},
    x=>x.splice(1,0,{...x.find(v=>v.kind==='stats_slot')}),
  ]){const bad=structuredClone(current);mutate(bad);const rawBad=encode(bad);assert.throws(()=>analyze(rawBad,sealOf(rawBad)));}
  if(enabled){const bad=structuredClone(current);bad.find(v=>v.operation==='stats'&&v.kind==='operation').outcome='unknown';const rawBad=encode(bad);assert.throws(()=>analyze(rawBad,sealOf(rawBad)));}
}
console.log('schema7: nine off/on fixtures, 135 strict stats negative cases');
