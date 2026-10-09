import assert from 'node:assert/strict';
import {boundary,validateEnvironment} from './parallel_environment.mjs';
import {environmentFixture} from './parallel_environment_fixture.mjs';
const f=environmentFixture(1000,2000),container={Mounts:f.Mounts,State:{StartedAt:new Date(1000).toISOString(),FinishedAt:new Date(2000).toISOString()}};
assert.equal(Object.keys(validateEnvironment(f.before,f.after,container)).length,3);
const rows=raw=>raw.trimEnd().split('\n').map(v=>JSON.parse(v));
const encode=x=>x.map(v=>JSON.stringify(v)).join('\n')+'\n';
const mutations=[x=>x.pop(),x=>x.push(x[1]),x=>x[0].docker_root='/data/docker',x=>x[1].memory*=2,x=>x[1].swap*=2,x=>x[1].nano_cpus=0,x=>x[1].cpuset='0-3',x=>x[2].swap=2147483648,x=>x[3].running=false,x=>x[1].oom=true,x=>x[1].network='host',x=>x[1].id='bad',x=>x[2].image='bad',x=>x[1].started_at='bad',x=>x[1].mounts[0].Source='/sas/pgdata',x=>x[1].mounts[0].RW=false,x=>x[2].mounts[0].Type='volume',x=>x[1].mounts.pop(),x=>x[4].at=new Date(100000).toISOString()];
for(const mutate of mutations){const x=rows(f.before);mutate(x);assert.throws(()=>boundary(encode(x)));}
assert.throws(()=>boundary(f.before.trimEnd()));
const restarted=rows(f.after);restarted[1].id='b'.repeat(64);assert.throws(()=>validateEnvironment(f.before,encode(restarted),container));
assert.throws(()=>validateEnvironment(f.after,f.before,container));
for(const mutate of [c=>c.Mounts[2].RW=true,c=>c.Mounts[2].Source='/wrong',c=>c.Mounts.push(c.Mounts[0]),c=>c.Mounts[0].Type='volume']){const c=structuredClone(container);mutate(c);assert.throws(()=>validateEnvironment(f.before,f.after,c));}
console.log('environment: valid boundary pair + 26 strict negative cases');
