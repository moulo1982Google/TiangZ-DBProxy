import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
const bash=process.env.BASH_EXE||'bash',env={...process.env};
for(const key of Object.keys(env))if(key.startsWith('P07_'))delete env[key];
const scripts=['p07_ack_timeout_preflight.sh','run_p07_ack_timeout.sh','launch_p07_ack_timeout.sh'];
const run=(script,args,extra={})=>{const r=spawnSync(bash,['deploy/remote-test/'+script,...args],{env:{...env,...extra},encoding:'utf8'});assert.ifError(r.error);return r;};
assert.equal(run(scripts[0],['p7at_local']).status,0);
let count=0;
for(const script of scripts){
  for(const args of [[],['postgres'],['p7at_'],['p7at_X'],['p7at_a;b'],['p7at_'+ 'a'.repeat(15)],['p7at_a','extra']]){assert.equal(run(script,args).status,2);count++;}
  for(const key of ['P07_ROWS','P07_STATS','P07_TIMEOUT_RUN_ID','P07_OUTPUT','P07_WARMUP_SECONDS']){assert.equal(run(script,['p7at_local'],{[key]:'1'}).status,2);count++;}
}
console.log(`timeout preflight: valid dedicated RunId + ${count} rejected arguments/overrides before external work`);
