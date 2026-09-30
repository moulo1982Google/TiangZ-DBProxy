import assert from 'node:assert/strict';
import test from 'node:test';
import {analyzeStageSnapshots, requestBounds, storageBounds, requestOperations, requestStages, storageStages} from './analyze_mixed_stages.mjs';

const manifest = {stage_audit: true, baseline: 'B2', outbox_stats_mode: 'on', warmup: 2, sample: 5, read_connections: 2};
const start = 1000000;
const target = r => r.request_stages.find(s => s.operation === 'apply_transaction' && s.stage === 'handler');
function fixture() {
  return Array.from({length: 9}, (_, i) => ({
    kind: 'stage_snapshot', schema_version: 1, process_id: 42,
    unix_ms: start + i * 1000 + 250, elapsed_us: i * 1000000, capture_us: 25,
    read_pool: {capacity: 2, in_use: i % 3}, storage_bounds_us: storageBounds,
    request_stages: requestOperations.flatMap(operation => requestStages.map(stage => {
      const active = ['load_snapshot', 'load_multi_snapshot', 'save_snapshot', 'save_multi_snapshot', 'apply_transaction', 'commit_records'].includes(operation);
      return {operation, stage, bounds_us: requestBounds,
        buckets: requestBounds.map((_, k) => active && k === 0 ? 100 + i * 10 : 0).concat(active ? 3 : 0),
        sum_us: active ? 9000000 + i * 10000 : 0, max_us: active ? 3000000 : 0};
    })),
    storage_stages: storageStages.map(stage => ({stage,
      buckets: storageBounds.map((_, k) => ['postgres_connection_wait', 'postgres_operation'].includes(stage) && k === 1 ? 100 + i * 10 : 0).concat(0),
      sum_us: ['postgres_connection_wait', 'postgres_operation'].includes(stage) ? 200000 + i * 20000 : 0,
      in_flight: stage === 'postgres_connection_wait' ? i % 2 : 0})),
  }));
}
const analyze = rows => analyzeStageSnapshots(manifest, start, rows);

test('subtract cumulative buckets, exclude warmup and crossing intervals, preserve maximum meaning', () => {
  const a = analyze(fixture());
  assert.equal(a.covered_us, 4000000);
  assert.equal(a.captured_begin_unix_ms, start + 2250);
  assert.equal(a.captured_end_unix_ms, start + 6250);
  const s = a.summaries.find(x => x.key === 'request/apply_transaction/handler');
  assert.equal(s.count, 40);
  assert.equal(s.sum_us, 40000);
  assert.equal(s.mean_us, 1000);
  assert.deepEqual(s.p99_bucket_us, {lower_us: 0, lower_inclusive: true, upper_us: 1000});
  assert.equal(s.cumulative_max_us_at_end, 3000000);
  assert.equal(s.sampled_in_flight_peak, null);
  const idle = a.summaries.find(x => x.key === 'request/load_trade/handler');
  assert.equal(idle.count, 0);
  assert.equal(idle.p99_bucket_us, null);
  const wait = a.summaries.find(x => x.key === 'storage/postgres_connection_wait');
  assert.equal(wait.count, 40);
  assert.deepEqual(wait.p99_bucket_us, {lower_us: 1000, lower_inclusive: false, upper_us: 5000});
  assert.equal(wait.sampled_in_flight_peak, 1);
  assert.equal(a.performance_pass, null);
});

test('series reordering cannot mix operation counters', () => {
  const rows = fixture();
  rows.filter((_, i) => i % 2).forEach(r => {r.request_stages.reverse(); r.storage_stages.reverse();});
  assert.deepEqual(analyze(rows), analyze(fixture()));
});

test('overflow is an unbounded quantile range', () => {
  const rows = fixture();
  for (const r of rows.slice(6)) {
    target(r).buckets[requestBounds.length] += 10;
    target(r).sum_us += 20000000;
  }
  assert.equal(analyze(rows).summaries.find(s => s.key === 'request/apply_transaction/handler').p99_bucket_us.upper_us, null);
});

test('missing stage, changed process, changed buckets and non-finite counters are rejected', () => {
  for (const change of [
    r => r[4].storage_stages.pop(),
    r => {r[4].process_id = 43;},
    r => {r[4].storage_bounds_us = [1];},
    r => {target(r[4]).sum_us = NaN;},
    r => {target(r[4]).buckets[0] = -1;},
    r => r.forEach(row => {target(row).buckets.fill(0); target(row).sum_us = 0;}),
  ]) {
    const rows = fixture(); change(rows);
    assert.throws(() => analyze(rows));
  }
});

test('counter reset, wall-clock jump, missing middle and missing endpoints are rejected', () => {
  const reset = fixture(); target(reset[4]).buckets[0] = 0;
  assert.throws(() => analyze(reset), /reset/);
  const jump = fixture(); jump[4].unix_ms += 500;
  assert.throws(() => analyze(jump), /clock/);
  assert.throws(() => analyze(fixture().filter((_, i) => i !== 3 && i !== 4)), /gap/);
  assert.throws(() => analyze(fixture().slice(4)), /boundaries/);
  assert.throws(() => analyze(fixture().slice(0, 5)), /boundaries/);
});
