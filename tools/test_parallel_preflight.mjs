import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';

const bash = process.env.BASH_EXE || 'bash';
const base = {...process.env};
for (const key of Object.keys(base)) if (key.startsWith('P07_')) delete base[key];
const valid = {P07_WARMUP_SECONDS:'2', P07_SAMPLE_SECONDS:'5', P07_ROWS:'1000',
  P07_PUBLISHERS:'2', P07_WORKERS:'2', P07_ROUNDS:'1', P07_CLAIMS_PER_SECOND:'4', P07_STATS:'0'};
const run = (script, values) => {
  const result = spawnSync(bash, [script, 'preflight_only'], {env:{...base,...values},encoding:'utf8'});
  assert.ifError(result.error);
  return result;
};
const preflight = 'deploy/remote-test/p07_parallel_preflight.sh';
assert.equal(run(preflight, {}).status, 0);
assert.equal(run(preflight, valid).status, 0);
let rejected = 0;
for (const key of Object.keys(valid)) for (const value of ['', '-1', 'NaN', '18446744073709551616', '999']) {
  for (const script of [preflight, 'deploy/remote-test/launch_p07_parallel.sh', 'deploy/remote-test/run_p07_parallel.sh']) {
    const result = run(script, {[key]:value});
    assert.equal(result.status, 2, result.stderr);
    assert(result.stderr.includes(`Rejected ${key}:`), result.stderr);
    rejected++;
  }
}
assert.equal(run(preflight, {P07_PARALLEL_MODE:'typo'}).status, 2);
console.log(`PARALLEL_PREFLIGHT_CHECKED: default/explicit smoke accepted, ${rejected} parameter rejections before external work, unknown mode rejected`);
