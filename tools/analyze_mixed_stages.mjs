// Cumulative counters describe completed scopes, not individual request traces or SQL-only time.
import assert from 'node:assert/strict';

export const requestBounds = [1000, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000];
export const storageBounds = [1000, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000, 2000000, 5000000, 10000000, 15000000, 30000000];
export const requestOperations = ['load_snapshot', 'load_multi_snapshot', 'save_snapshot', 'save_multi_snapshot',
  'enqueue_snapshot', 'enqueue_multi_snapshot', 'apply_transaction', 'load_transaction',
  'apply_multi_transaction', 'load_multi_transaction', 'apply_trade_transaction', 'load_trade',
  'load_trade_transaction', 'commit_records', 'invalid'];
export const requestStages = ['task_schedule', 'record_order_wait', 'handler'];
export const storageStages = ['cache_lookup', 'cache_write', 'fallback_capacity_wait', 'fallback_key_wait',
  'fallback_distributed_lease', 'postgres_connection_wait', 'postgres_read_pool_wait', 'postgres_operation',
  'postgres_read_operation', 'postgres_write_operation', 'committed_cache_sync', 'cache_repair_ack', 'fallback_lease_release'];
const natural = (n, label) => assert(Number.isSafeInteger(n) && n >= 0, `invalid ${label}`);
const requiredKeys = [
  ...requestOperations.flatMap(op => requestStages.map(stage => `request/${op}/${stage}`)),
  ...storageStages.map(stage => `storage/${stage}`),
].sort();

function series(row) {
  assert.deepEqual(row.storage_bounds_us, storageBounds, 'storage bucket schema changed');
  const all = [
    ...row.request_stages.map(s => ({...s, key: `request/${s.operation}/${s.stage}`, domain: 'request'})),
    ...row.storage_stages.map(s => ({...s, key: `storage/${s.stage}`, domain: 'storage', bounds_us: storageBounds})),
  ];
  assert.deepEqual(all.map(s => s.key).sort(), requiredKeys, 'missing, extra or duplicate stage');
  for (const s of all) {
    assert.deepEqual(s.bounds_us, s.domain === 'request' ? requestBounds : storageBounds);
    assert.equal(s.buckets.length, s.bounds_us.length + 1);
    s.buckets.forEach(n => natural(n, 'bucket'));
    natural(s.sum_us, 'sum_us');
    if (s.domain === 'request') natural(s.max_us, 'max_us');
    else natural(s.in_flight, 'in_flight');
  }
  return new Map(all.map(s => [s.key, s]));
}

function quantileBucket(buckets, bounds, quantile) {
  const count = buckets.reduce((a, b) => a + b, 0);
  if (!count) return null;
  const rank = Math.ceil(count * quantile);
  let seen = 0;
  for (let i = 0; i < buckets.length; i++) {
    seen += buckets[i];
    if (seen >= rank) return {lower_us: i ? bounds[i - 1] : 0, lower_inclusive: i === 0, upper_us: bounds[i] ?? null};
  }
  throw new Error('unreachable quantile rank');
}

export function analyzeStageSnapshots(manifest, measurementStart, rows) {
  assert.equal(manifest.stage_audit, true);
  assert.equal(manifest.baseline, 'B2');
  assert(['off', 'on'].includes(manifest.outbox_stats_mode));
  natural(measurementStart, 'measurement start');
  natural(manifest.warmup, 'warmup');
  natural(manifest.sample, 'sample');
  assert(manifest.sample > 0);
  assert(rows.length >= 2, 'missing stage snapshots');
  const begin = measurementStart + manifest.warmup * 1000;
  const end = begin + manifest.sample * 1000;
  const selected = [];
  const snapshots = [];
  let previous;
  for (const row of rows) {
    assert.equal(row.kind, 'stage_snapshot');
    assert.equal(row.schema_version, 1);
    for (const key of ['unix_ms', 'elapsed_us', 'capture_us', 'process_id']) natural(row[key], key);
    assert(row.process_id > 0);
    assert.equal(row.process_id, rows[0].process_id, 'process identity changed');
    assert.equal(row.read_pool?.capacity, manifest.read_connections, 'read pool changed');
    natural(row.read_pool.in_use, 'read pool occupancy');
    assert(row.read_pool.in_use <= row.read_pool.capacity);
    const values = series(row);
    snapshots.push({row, values});
    if (previous) {
      const elapsed = row.elapsed_us - previous.row.elapsed_us;
      const wall = (row.unix_ms - previous.row.unix_ms) * 1000;
      assert(elapsed > 0 && wall > 0 && Math.abs(wall - elapsed) < 250000, 'clock discontinuity');
      const delta = requiredKeys.map(key => {
        const s = values.get(key);
        const before = previous.values.get(s.key);
        const buckets = s.buckets.map((n, i) => n - before.buckets[i]);
        const sum = s.sum_us - before.sum_us;
        assert(buckets.every(n => n >= 0) && sum >= 0, 'stage counter reset');
        if (s.domain === 'request') assert(s.max_us >= before.max_us, 'stage maximum reset');
        return {key: s.key, domain: s.domain, bounds_us: s.bounds_us, buckets, sum_us: sum};
      });
      // Exclude both intervals crossing the warmup/end boundary. Never subtract quantiles or maxima.
      if (previous.row.unix_ms >= begin && row.unix_ms <= end) {
        assert(elapsed <= 2500000, 'stage sampling gap exceeds 2.5 seconds');
        selected.push({from_unix_ms: previous.row.unix_ms, to_unix_ms: row.unix_ms, elapsed_us: elapsed, stages: delta});
      }
    }
    previous = {row, values};
  }
  assert(selected.length > 0, 'no complete formal stage intervals');
  const first = selected[0].from_unix_ms, last = selected.at(-1).to_unix_ms;
  assert(first - begin <= 1500 && end - last <= 1500, 'incomplete stage window boundaries');
  const coverage = selected.reduce((n, i) => n + i.elapsed_us, 0);
  assert(coverage >= Math.max(1, manifest.sample * 1000000 - 2500000), 'incomplete stage coverage');
  const observed = snapshots.filter(s => s.row.unix_ms >= first && s.row.unix_ms <= last);
  const summaries = selected[0].stages.map((s, index) => {
    const buckets = s.buckets.map((_, k) => selected.reduce((n, i) => n + i.stages[index].buckets[k], 0));
    const sum = selected.reduce((n, i) => n + i.stages[index].sum_us, 0);
    const count = buckets.reduce((a, b) => a + b, 0);
    const scopes = observed.map(r => r.values.get(s.key));
    return {key: s.key, count, buckets, sum_us: sum, mean_us: count ? sum / count : null,
      p50_bucket_us: quantileBucket(buckets, s.bounds_us, .5), p99_bucket_us: quantileBucket(buckets, s.bounds_us, .99),
      // A max already observed during warmup is not an interval maximum.
      cumulative_max_us_at_end: s.domain === 'request' ? scopes.at(-1).max_us : null,
      sampled_in_flight_peak: s.domain === 'storage' ? Math.max(...scopes.map(x => x.in_flight)) : null};
  });
  for (const operation of ['load_snapshot', 'load_multi_snapshot', 'save_snapshot', 'save_multi_snapshot', 'apply_transaction', 'commit_records']) {
    for (const stage of requestStages) {
      assert(summaries.find(s => s.key === `request/${operation}/${stage}`).count > 0, 'missing measured request stage activity');
    }
  }
  assert(summaries.find(s => s.key === 'storage/postgres_operation').count > 0, 'missing measured storage activity');
  return {status: 'STAGE_INTERVALS_CHECKED', process_id: rows[0].process_id, formal_begin_unix_ms: begin,
    formal_end_unix_ms: end, captured_begin_unix_ms: first, captured_end_unix_ms: last, covered_us: coverage,
    raw_samples: rows.length, intervals: selected, summaries,
    read_pool_sampled_in_use_peak: Math.max(...observed.map(r => r.row.read_pool.in_use)),
    max_capture_us: Math.max(...observed.map(r => r.row.capture_us)),
    scope: 'Completed scopes within fully contained sample intervals; atomic counter reads are not one coherent request trace. Request handler contains storage stages; do not sum overlapping stages or their quantiles. Storage combines operations. SQL/reconnect/maintenance-lock causes require further evidence. Bucket ranges are not exact P99 values; null upper bound means overflow. Gauges can miss bursts. Process-lifetime maximum includes warmup.',
    performance_pass: null, causality_proven: false};
}
