// Validate the fixed-rate component matrix; do not turn it into a capacity claim.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
const root = process.argv[2];
assert(root, 'usage: node tools/analyze_p07.mjs <outbox_run_directory>');
const read = name => fs.readFileSync(path.join(root, name), 'utf8');
const plans = read('distribution.log').split(/\r?\n/).filter(l => l.startsWith('QUERY_PLAN ')).map(line => {
  const match = /^QUERY_PLAN (.+): (.+)$/.exec(line);
  assert(match);
  const [plan] = JSON.parse(match[2]);
  return {label:match[1], execution_ms:plan['Execution Time'],
    rows:plan.Plan['Actual Rows'], hit_blocks:plan.Plan['Shared Hit Blocks'], read_blocks:plan.Plan['Shared Read Blocks']};
});
assert.equal(plans.length, 18);
assert.equal(new Set(plans.map(p => p.label)).size, 18);
for (const name of ['distribution', 'prefix', 'heads']) {
  assert.match(read(`${name}.log`), /test result: ok\. 1 passed/);
}
const csv = name => {
  const [header, ...lines] = read(name).trim().split(/\r?\n/);
  return lines.map(line => Object.fromEntries(header.split(',').map((key, i) => [key, line.split(',')[i]])));
};
const distribution = values => {
  assert(values.length > 0);
  assert(values.every(Number.isFinite));
  values.sort((a, b) => a - b);
  const p = n => values[Math.ceil(values.length*n)-1];
  return {count:values.length, p50:p(.5), p99:p(.99), max:values.at(-1)};
};
const phases = [];
for (const name of fs.readdirSync(root).filter(n => /^r[0-2]_[lb]_(on|off)$/.test(n)).sort()) {
  assert.match(read(`${name}.log`), /test result: ok\. 1 passed/);
  const result = JSON.parse(read(`${name}/result.json`));
  result.publishers ??= 1;
  assert([1,2].includes(result.publishers));
  const [round, mode, stats] = name.split('_');
  assert.equal(result.mode, mode === 'l' ? 'leased' : 'blocked');
  assert.equal(result.stats, stats === 'on');
  assert.equal(result.full_timing, result.warmup_seconds === 120 && result.sample_seconds === 300);
  const rows = csv(`${name}/claims.csv`);
  assert.equal(rows.length, result.total_count);
  assert.equal(rows.length, (result.warmup_seconds + result.sample_seconds)*4);
  rows.forEach((r, i) => {
    assert.equal(Number(r.slot), i);
    assert.equal(r.sample, String(i >= result.warmup_seconds*4));
    assert.equal(r.event_id, mode === 'b' ? `ready-${i}` : '');
    if (result.publishers === 2) assert.equal(r.publisher_filter, i%2===0 ? 'p07-a' : 'p07-b');
  });
  const sampled = rows.filter(r => r.sample === 'true');
  assert.equal(sampled.length, result.sample_count);
  assert.equal(sampled.length, result.sample_seconds*4);
  assert.deepEqual(result.groups, mode === 'b' ? [rows.length/2, rows.length/2] : [0,0]);
  const statRows = csv(`${name}/stats.csv`);
  assert.equal(statRows.length, result.stats_count);
  if (result.stats) assert(statRows.length >= result.warmup_seconds + result.sample_seconds);
  else assert.equal(statRows.length, 0);
  phases.push({name, round:Number(round.slice(1)), ...result,
    claim_ms:distribution(sampled.map(r => Number(r.claim_ms))),
    ack_ms:distribution(sampled.map(r => Number(r.ack_ms))),
    dispatch_ms:distribution(sampled.map(r => Number(r.dispatch_ms))),
    stats_ms:result.stats ? distribution(statRows.filter(r => r.sample === 'true').map(r => Number(r.latency_ms))) : null});
}
assert(phases.length > 0);
assert.equal(new Set(phases.map(p => p.publishers)).size, 1);
const full = phases.length === 12 && phases.every(p => p.full_timing);
if (full) {
  for (let r=0; r<3; r++) for (const m of ['l','b']) for (const s of ['off','on']) {
    assert(phases.some(p => p.name === `r${r}_${m}_${s}`));
  }
}
const result = {status:full ? 'COMPLETE_FIXED_RATE_COMPONENT_MATRIX' : 'PARTIAL_OR_SMOKE',
  scope:'4 claims/s; shared connection statistics at 1/s; not application load or capacity', plans, phases};
fs.writeFileSync(path.join(root, 'analysis.json'), JSON.stringify(result, null, 2));
console.log(JSON.stringify(result, null, 2));
