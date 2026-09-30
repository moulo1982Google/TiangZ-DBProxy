import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {phaseFixtures} from './test_parallel_poll.mjs';
import {analyzePairs,requiredSources} from './analyze_parallel_pairs.mjs';
import {environmentFixture} from './parallel_environment_fixture.mjs';

const image='sha256:ae3b3d8e17608b277067e3a44ee3e45056a987655b023aad4b025dc0f6811470';
const sourceNames=requiredSources;
function seal(e) {e.seal={schema:1,file:'journal.jsonl',bytes:Buffer.byteLength(e.raw),sha256:createHash('sha256').update(e.raw).digest('hex')};}
function fixture(rounds=1) {
  const manifest={schema:1,rounds,modes:[...new Set(phaseFixtures.map(v=>v[0].mode))],runs:[]},evidence={};
  let index=0;
  for(let round=0;round<rounds;round++)for(const rows of phaseFixtures) {
    const f=rows[0],id=`synthetic_${index}`,input={run_id:id,round,phase:f.stats_phase,stats:f.stats,directory:id,evidence_prefix:id};
    const env={CARGO_BUILD_JOBS:4,P07_PARALLEL_MODE:f.mode,P07_ROWS:1000,P07_WARMUP_SECONDS:2,P07_SAMPLE_SECONDS:5,P07_WORKERS:2,P07_PUBLISHERS:2,P07_CLAIMS_PER_SECOND:4,P07_ROUNDS:1,P07_STATS:Number(f.stats),P07_STATS_PHASE:f.stats_phase};
    const c={Name:`/dbproxy-parallel-${id}`,Image:image,Args:['/src/deploy/remote-test/run_p07_parallel.sh',id],State:{Status:'exited',Running:false,OOMKilled:false,ExitCode:0,StartedAt:new Date(index*20000).toISOString(),FinishedAt:new Date(index*20000+10000).toISOString()},HostConfig:{CpusetCpus:'20-27,48-55',NanoCpus:4000000000,Memory:17179869184,MemorySwap:17179869184,NetworkMode:'dbproxy-test'},Config:{Env:Object.entries(env).map(([k,v])=>`${k}=${v}`)}};
    const e={raw:rows.map(v=>JSON.stringify(v)).join('\n')+'\n',sources:sourceNames.map(v=>'a'.repeat(64)+'  /src/'+v).join('\n')+'\n',images:[{Id:image}],containers:[c]};seal(e);
    const environment=environmentFixture(index*20000,index*20000+10000);
    c.Mounts=environment.Mounts;e.before=environment.before;e.after=environment.after;
    manifest.runs.push(input);evidence[id]=e;index++;
  }
  return {manifest,evidence};
}
const run=x=>analyzePairs(x.manifest,input=>x.evidence[input.run_id]);
const base=fixture(),result=run(base);
assert.equal(result.status,'SMOKE_PAIRED_LEDGER_ONLY');assert.equal(result.pairs.length,18);assert.equal(result.formal_performance_complete,false);
assert.equal(result.runs.length,36);
const claim=result.runs[0].groups['sample/parallel-a/0/claim'];
assert.deepEqual(claim,{count:5,p50_us:98,p95_us:98,p99_us:98,max_us:98});
assert.equal(result.runs[0].groups['warmup/parallel-a/0/claim'].count,2);
assert.equal(run(fixture(3)).pairs.length,54);
const reverse=structuredClone(base);reverse.manifest.runs.reverse();assert.equal(run(reverse).pairs.length,18);
// Keep one phase/worker tail regression visible even when other pairs are unchanged.
const changed=structuredClone(base),off=changed.manifest.runs.find(v=>!v.stats),e=changed.evidence[off.run_id];
e.raw=e.raw.trimEnd().split('\n').map(s=>{const v=JSON.parse(s);if(v.kind==='operation'&&v.operation==='claim'&&v.worker===0)v.end_us=v.begin_us+40;return JSON.stringify(v);}).join('\n')+'\n';seal(e);
const review=run(changed);assert(review.pairs.some(p=>Object.values(p.changes).some(v=>v.over_20_percent&&v.delta_percent>100)));
assert(review.pairs.some(p=>Object.values(p.changes).every(v=>!v.over_20_percent)));
const first=x=>x.evidence[x.manifest.runs[0].run_id];
const negatives=[
  x=>x.manifest.runs.pop(),
  x=>x.manifest.runs.splice(0,1),
  x=>x.manifest.runs[1]=structuredClone(x.manifest.runs[0]),
  x=>x.manifest.runs[0].phase=1-x.manifest.runs[0].phase,
  x=>x.manifest.runs[0].stats=!x.manifest.runs[0].stats,
  x=>x.manifest.runs[0].round=1,
  x=>x.manifest.rounds=2,
  x=>x.manifest.modes.push(x.manifest.modes[0]),
  x=>x.manifest.modes[0]='unknown',
  x=>x.manifest.runs[1].directory=x.manifest.runs[0].directory,
  x=>x.manifest.runs[1].evidence_prefix=x.manifest.runs[0].evidence_prefix,
  x=>first(x).sources=first(x).sources.replace('a','b'),
  x=>first(x).sources=first(x).sources.split('\n').slice(1).join('\n'),
  x=>first(x).sources+=first(x).sources.split('\n')[0]+'\n',
  x=>first(x).seal.sha256='0'.repeat(64),
  x=>first(x).raw=first(x).raw.trimEnd(),
  x=>first(x).images[0].Id='sha256:'+'0'.repeat(64),
  x=>first(x).containers[0].Image='wrong',
  x=>first(x).containers[0].State.ExitCode=1,
  x=>first(x).containers[0].State.OOMKilled=true,
  x=>first(x).containers[0].State.Running=true,
  x=>first(x).containers[0].State.FinishedAt='bad',
  x=>first(x).containers[0].State.FinishedAt=new Date(1000000).toISOString(),
  x=>first(x).containers[0].Name='wrong',
  x=>first(x).containers[0].Args[1]='wrong',
  x=>first(x).containers[0].HostConfig.Memory*=2,
  x=>first(x).containers[0].HostConfig.NanoCpus*=2,
  x=>first(x).containers[0].Config.Env.push('P07_STATS=0'),
  x=>first(x).containers[0].Config.Env=first(x).containers[0].Config.Env.filter(v=>!v.startsWith('P07_STATS_PHASE=')),
  x=>{const e=first(x);e.raw=e.raw.replace('"schema":8','"schema":7');seal(e);},
  x=>{const e=first(x);e.raw=e.raw.replace('"rows":1000','"rows":1040');seal(e);},
  x=>{const e=first(x);e.raw=e.raw.replace('"outcome":"completed"','"outcome":"unknown"');seal(e);},
];
for(const mutate of negatives){const bad=structuredClone(base);mutate(bad);assert.throws(()=>run(bad));}
console.log(`paired analyzer: nine modes/two phases/off-on; one and three rounds; reversed order; per-pair regression retained; ${negatives.length} negative cases rejected (synthetic only)`);
