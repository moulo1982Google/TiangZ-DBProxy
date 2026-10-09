import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {analyze} from './analyze_parallel_poll.mjs';
import {validateEnvironment} from './parallel_environment.mjs';

const modes=['ready','spread-ready','none','all-blocked','leased','backoff','leased-heads','backoff-heads','dead-heads'];
export const requiredSources=['crates/dbproxy-storage/tests/outbox_parallel_poll.rs','crates/dbproxy-storage/tests/parallel_budget/mod.rs','deploy/remote-test/p07_parallel_preflight.sh','deploy/remote-test/run_p07_parallel.sh','deploy/remote-test/launch_p07_parallel.sh','deploy/remote-test/common.sh','deploy/remote-test/sample_containers.sh','crates/dbproxy-storage/src/outbox.rs','crates/dbproxy-storage/src/outbox_stats.sql','crates/dbproxy-storage/src/outbox_claim.sql'];
const image='sha256:ae3b3d8e17608b277067e3a44ee3e45056a987655b023aad4b025dc0f6811470';
requiredSources.push('deploy/remote-test/capture_p07_environment.sh');
requiredSources.push('crates/dbproxy-storage/tests/parallel_claim_timeout/mod.rs');
requiredSources.push('crates/dbproxy-storage/tests/parallel_ack_timeout/mod.rs');
function sources(raw) {
  assert(raw.endsWith('\n'));
  const found=new Map();
  for(const line of raw.trimEnd().split('\n')) {
    const match=/^([a-f0-9]{64})  \/src\/(\S+)$/.exec(line.trimEnd());
    assert(match,'invalid source digest');assert(!found.has(match[2]),'duplicate source');found.set(match[2],match[1]);
  }
  for(const name of requiredSources)assert(found.has(name),'missing source');
  return Object.fromEntries([...found].sort(([a],[b])=>a.localeCompare(b)));
}
function summarize(rows) {
  const groups=new Map();
  function add(operation,worker,wave,value) {
    const w=rows.find(v=>v.kind==='wave'&&v.wave===wave);
    const key=[w.sample?'sample':'warmup',w.publisher,worker,operation].join('/');
    if(!groups.has(key))groups.set(key,[]);groups.get(key).push(value);
  }
  for(const v of rows) {
    if(v.kind==='operation')add(v.operation,v.worker,v.wave,v.end_us-v.begin_us);
    if(v.kind==='wave_completed')add('wave','coordinator',v.wave,v.end_us-v.begin_us);
  }
  return Object.fromEntries([...groups].sort(([a],[b])=>a.localeCompare(b)).map(([key,values])=>{
    values.sort((a,b)=>a-b);
    const q=p=>values[Math.ceil(values.length*p)-1];
    return [key,{count:values.length,p50_us:q(.5),p95_us:q(.95),p99_us:q(.99),max_us:values.at(-1)}];
  }));
}

// Input metadata only locates immutable raw evidence. Never trust saved analysis.json.
export function analyzePairs(manifest,load) {
  assert.equal(manifest.schema,1);assert([1,3].includes(manifest.rounds));
  assert(Array.isArray(manifest.modes)&&manifest.modes.length>0);
  assert.equal(new Set(manifest.modes).size,manifest.modes.length);
  for(const mode of manifest.modes)assert(modes.includes(mode));
  assert.equal(manifest.runs.length,manifest.modes.length*manifest.rounds*4,'missing phase/arm/round');
  const ids=new Set(),directories=new Set(),prefixes=new Set(),slots=new Map(),intervals=[];
  let commonSources,commonImage,commonServices;
  const configurations=new Map(),runs=[];
  for(const input of manifest.runs) {
    assert(/^[a-z][a-z0-9_]{0,18}$/.test(input.run_id));
    assert(Number.isInteger(input.round)&&input.round>=0&&input.round<manifest.rounds);
    assert([0,1].includes(input.phase));assert.equal(typeof input.stats,'boolean');
    for(const [set,value] of [[ids,input.run_id],[directories,input.directory],[prefixes,input.evidence_prefix]]) {
      assert.equal(typeof value,'string');assert(value.length>0&&!set.has(value),'duplicate run/evidence');set.add(value);
    }
    const evidence=load(input),rows=evidence.raw.trimEnd().split('\n').map(v=>JSON.parse(v)),f=rows[0];
    assert.equal(f.schema,8,'paired evidence requires same phase-aware schema8');
    assert(manifest.modes.includes(f.mode));assert.equal(f.stats,input.stats);assert.equal(f.stats_phase,input.phase);
    const checked=analyze(evidence.raw,evidence.seal);
    const fingerprint=sources(evidence.sources);
    if(commonSources)assert.deepEqual(fingerprint,commonSources,'source version mismatch');else commonSources=fingerprint;
    assert.equal(evidence.images.length,1);const im=evidence.images[0];assert.equal(im.Id,image);
    if(commonImage)assert.equal(im.Id,commonImage);else commonImage=im.Id;
    assert.equal(evidence.containers.length,1);const c=evidence.containers[0];
    assert.equal(c.Image,im.Id);assert.equal(c.Name,`/dbproxy-parallel-${input.run_id}`);
    assert.deepEqual(c.Args,['/src/deploy/remote-test/run_p07_parallel.sh',input.run_id]);
    assert.equal(c.State.Status,'exited');assert.equal(c.State.Running,false);assert.equal(c.State.OOMKilled,false);assert.equal(c.State.ExitCode,0);
    const services=validateEnvironment(evidence.before,evidence.after,c);
    if(commonServices)assert.deepEqual(services,commonServices,'base services differ across paired runs');else commonServices=services;
    for(const [key,value] of Object.entries({CpusetCpus:'20-27,48-55',NanoCpus:4000000000,Memory:17179869184,MemorySwap:17179869184,NetworkMode:'dbproxy-test'}))assert.equal(c.HostConfig[key],value,'workbench resource mismatch');
    const env=new Map();for(const entry of c.Config.Env){const i=entry.indexOf('=');const key=entry.slice(0,i);assert(!env.has(key));env.set(key,entry.slice(i+1));}
    const expected={CARGO_BUILD_JOBS:4,P07_PARALLEL_MODE:f.mode,P07_ROWS:f.rows,P07_WARMUP_SECONDS:f.warmup_seconds,P07_SAMPLE_SECONDS:f.sample_seconds,P07_WORKERS:2,P07_PUBLISHERS:2,P07_CLAIMS_PER_SECOND:4,P07_ROUNDS:1,P07_STATS:Number(f.stats),P07_STATS_PHASE:f.stats_phase};
    for(const [key,value]of Object.entries(expected))assert.equal(env.get(key),String(value),'container/fixture mismatch');
    const begin=Date.parse(c.State.StartedAt),end=Date.parse(c.State.FinishedAt);assert(Number.isFinite(begin)&&end>begin);intervals.push({begin,end});
    const {stats,stats_phase,...config}=f;
    if(configurations.has(f.mode))assert.deepEqual(config,configurations.get(f.mode),'pair budget mismatch');else configurations.set(f.mode,config);
    const key=[f.mode,input.round,input.phase,Number(input.stats)].join('/');assert(!slots.has(key),'duplicate paired slot');
    const run={run_id:input.run_id,mode:f.mode,round:input.round,phase:input.phase,stats:input.stats,status:checked.status,claim_calls:checked.claim_calls,returned:checked.returned,stats_calls:checked.stats_calls,groups:summarize(rows)};
    slots.set(key,run);runs.push(run);
  }
  intervals.sort((a,b)=>a.begin-b.begin);for(let i=1;i<intervals.length;i++)assert(intervals[i].begin>=intervals[i-1].end,'overlapping server runs');
  const pairs=[];
  for(const mode of manifest.modes)for(let round=0;round<manifest.rounds;round++)for(const phase of [0,1]) {
    const off=slots.get(`${mode}/${round}/${phase}/0`),on=slots.get(`${mode}/${round}/${phase}/1`);assert(off&&on,'missing complementary phase or arm');
    const changes={};
    for(const [key,a]of Object.entries(off.groups)) {
      const b=on.groups[key];assert(b);assert.equal(a.count,b.count);
      const absolute=b.p99_us-a.p99_us,percent=a.p99_us===0?null:absolute/a.p99_us*100;
      changes[key]={count:a.count,off_p99_us:a.p99_us,on_p99_us:b.p99_us,delta_us:absolute,delta_percent:percent,over_20_percent:b.p99_us>a.p99_us*1.2};
    }
    assert.deepEqual(Object.keys(on.groups).filter(k=>!k.endsWith('/stats')),Object.keys(off.groups));
    pairs.push({mode,round,phase,off:off.run_id,on:on.run_id,changes});
  }
  return {status:runs.every(v=>v.status==='SMOKE_ONLY')?'SMOKE_PAIRED_LEDGER_ONLY':'BOUNDED_PAIRED_LEDGER_ONLY',validation:'COMPLEMENTARY_PHASE_PAIRS_CHECKED',formal_performance_complete:false,rounds:manifest.rounds,modes:manifest.modes,sources:commonSources,image:commonImage,base_services:commonServices,runs,pairs,scope:'Claim/ack/wave wall time includes shared-connection stats coordination and synchronous journal costs; not production SQL net cost. Boundary snapshots verify recorded base-service limits and mounts; they do not establish unchanged limits between snapshots, distribution stability or absence of external activity.'};
}

if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)) {
  const filename=path.resolve(process.argv[2]),base=path.dirname(filename),manifest=JSON.parse(fs.readFileSync(filename,'utf8'));
  const json=p=>JSON.parse(fs.readFileSync(p,'utf8'));
  const result=analyzePairs(manifest,input=>{
    const dir=path.resolve(base,input.directory),prefix=path.resolve(base,input.evidence_prefix);
    return {before:fs.readFileSync(prefix+'.environment-before.jsonl','utf8'),after:fs.readFileSync(prefix+'.environment-after.jsonl','utf8'),raw:fs.readFileSync(path.join(dir,'journal.jsonl'),'utf8'),seal:json(path.join(dir,'journal-sealed.json')),sources:fs.readFileSync(prefix+'.sources.sha256','utf8'),images:json(prefix+'.image.json'),containers:json(prefix+'.container.json')};
  });
  const output=filename+'.analysis.json';fs.writeFileSync(output,JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify({status:result.status,validation:result.validation,pairs:result.pairs.length,output}));
}
