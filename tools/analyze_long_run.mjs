// Streaming analysis of a long fixed-rate run (P08): per-minute latency/error series, early-vs-late
// trend checks for every resource series, cleanup backlog, checkpoints and slow-write attribution.
// Usage: node tools/analyze_long_run.mjs <run folder> [containers.jsonl] [slow threshold ms = 15]
// Run folder: 1_on/{requests,storage-stages,process-resources}.jsonl, 1_on/{summary.json,postgres.log},
// host-samples.jsonl, wal-samples.txt, pg-periodic.jsonl. Writes <run folder>/long-run.json.
import { createReadStream, existsSync, readFileSync, writeFileSync } from "node:fs";
import { createInterface } from "node:readline";
import { join } from "node:path";

const [runDir, containersArg, thresholdArg] = process.argv.slice(2);
if (!runDir) throw new Error("usage: analyze_long_run.mjs <run folder> [containers.jsonl] [threshold ms]");
// One round per long run; its folder is 1_on or 1_off depending on the cleanup mode.
const round = existsSync(join(runDir, "1_on")) ? join(runDir, "1_on") : join(runDir, "1_off");
const slowUs = Number(thresholdArg ?? 15) * 1000;
const lines = async function* (path) {
  if (!existsSync(path)) return;
  for await (const line of createInterface({ input: createReadStream(path), crlfDelay: Infinity })) if (line) yield line;
};
const pct = (v, q) => (v.length ? v[Math.min(v.length - 1, Math.ceil(v.length * q) - 1)] : null);
const median = (v) => { const s = [...v].sort((a, b) => a - b); return pct(s, 0.5); };
const summary = JSON.parse(readFileSync(join(round, "summary.json"), "utf8"));
const start = summary.start_unix_us, rate = summary.target_rate;
const warmupUs = summary.actual_elapsed_seconds_including_warmup_and_drain ? null : null;
const minuteOf = (us) => Math.floor((us - start) / 60e6);

// --- requests: per-minute series and slow writes ------------------------------------------------
const minutes = new Map();
const bucket = (m) => {
  if (!minutes.has(m)) minutes.set(m, { reads: [], writes: [], errors: 0, not_sent: 0, dispatch_max_us: 0 });
  return minutes.get(m);
};
const slowWrites = [];
let measuredEndUs = start;
const allReads = [], allWrites = [];
for await (const line of lines(join(round, "requests.jsonl"))) {
  if (!line.includes('"response"') && !line.includes('"not_sent"')) continue;
  const r = JSON.parse(line);
  const scheduled = start + Math.round(((r.n - 1) * 1e6) / rate);
  const b = bucket(minuteOf(scheduled));
  if (r.kind === "not_sent") { b.not_sent++; continue; }
  if (r.kind !== "response" || !r.sample) continue;
  const write = r.n % 2 === 0;
  (write ? b.writes : b.reads).push(r.end_to_end_us);
  (write ? allWrites : allReads).push(r.end_to_end_us);
  measuredEndUs = Math.max(measuredEndUs, scheduled + r.end_to_end_us);
  b.dispatch_max_us = Math.max(b.dispatch_max_us, r.dispatch_delay_us);
  if (!(r.outcome && r.outcome.Ok === true)) b.errors++;
  if (write && r.end_to_end_us >= slowUs) slowWrites.push({ n: r.n, sent_us: scheduled + r.dispatch_delay_us, done_us: scheduled + r.end_to_end_us, e2e_us: r.end_to_end_us });
}
const stats = (v) => { const s = v.sort((a, b) => a - b); return { count: s.length, p50_ms: pct(s, 0.5) / 1000, p99_ms: pct(s, 0.99) / 1000, p999_ms: pct(s, 0.999) / 1000, max_ms: (s.at(-1) ?? 0) / 1000 }; };
const series = [...minutes.entries()].sort((a, b) => a[0] - b[0]).map(([m, b]) => ({
  minute: m, reads: b.reads.length, writes: b.writes.length, errors: b.errors, not_sent: b.not_sent,
  read_p99_ms: pct(b.reads.sort((x, y) => x - y), 0.99) / 1000, read_max_ms: (b.reads.at(-1) ?? 0) / 1000,
  write_p99_ms: pct(b.writes.sort((x, y) => x - y), 0.99) / 1000, write_max_ms: (b.writes.at(-1) ?? 0) / 1000,
  slow_writes: b.writes.filter((x) => x >= slowUs).length, dispatch_max_ms: b.dispatch_max_us / 1000,
}));
const measured = series.filter((s) => s.reads + s.writes > 0);

// --- server snapshots: heartbeat and read pool per minute, final stage maxima --------------------
const serverMinute = new Map();
let lastSnapshot = null;
const waitCounts = {};
for await (const line of lines(join(round, "storage-stages.jsonl"))) {
  const s = JSON.parse(line);
  const m = minuteOf(s.unix_ms * 1000);
  const b = serverMinute.get(m) ?? { heartbeat_max_ms: 0, read_pool_in_use_max: 0, pg_waits: {} };
  if (s.stages) {
    b.heartbeat_max_ms = Math.max(b.heartbeat_max_ms, (s.runtime_heartbeat_max_delay_us ?? 0) / 1000);
    b.read_pool_in_use_max = Math.max(b.read_pool_in_use_max, s.read_pool?.in_use ?? 0);
    // Stage maxima at the end of measured traffic; later snapshots include reconciliation.
    if (s.unix_ms * 1000 <= measuredEndUs) lastSnapshot = s;
  } else if (s.kind === "pg_activity") {
    for (const be of s.activity?.backends ?? []) if (be.wait_type && be.wait_type !== "Client" && be.wait_type !== "Activity") {
      const k = `${be.wait_type}/${be.wait}`;
      b.pg_waits[k] = (b.pg_waits[k] ?? 0) + 1;
      waitCounts[k] = (waitCounts[k] ?? 0) + 1;
    }
  }
  serverMinute.set(m, b);
}
const finalStages = lastSnapshot ? {
  request_stage_max_ms: Object.fromEntries(lastSnapshot.request_stages.filter((r) => r.buckets.some((x) => x > 0) && r.max_us > 0).map((r) => [`${r.operation}/${r.stage}`, r.max_us / 1000])),
  storage_stage_over_10ms: Object.fromEntries(lastSnapshot.stages.map((s) => [s.stage, s.buckets.slice(3).reduce((a, b) => a + b, 0)]).filter(([, c]) => c > 0)),
} : null;

// --- PostgreSQL log: checkpoints, autovacuum on DBProxy tables, slow statements ------------------
const pg = { checkpoints: [], autovacuum: 0, slow: [] };
let open = null;
for await (const line of lines(join(round, "postgres.log"))) {
  const m = line.match(/^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}\.\d{3}) UTC \[\d+\] \w+:\s+(.*)$/);
  if (!m) continue;
  const us = Date.parse(`${m[1]}T${m[2]}Z`) * 1000, text = m[3];
  if (/^checkpoint starting/.test(text)) open = { start_us: us, reason: text.replace("checkpoint starting: ", "") };
  else if (/^checkpoint complete/.test(text) && open) {
    const g = (re) => (text.match(re) ?? [])[1];
    pg.checkpoints.push({ ...open, end_us: us, total_s: +g(/total=([\d.]+) s/), wal_added: +g(/(\d+) WAL file\(s\) added/), wal_recycled: +g(/(\d+) recycled/), buffers: +g(/wrote (\d+) buffers/) });
    open = null;
  } else if (/automatic vacuum of table "[^"]*\.public\.dbproxy_/.test(text)) pg.autovacuum++;
  const d = text.match(/^duration: ([\d.]+) ms\s+(statement|execute|bind|parse)[^:]*: ?(.*)$/);
  if (d) {
    // The run's own observers (pg_periodic.sql, end-of-run fixture counts) are not DBProxy traffic.
    const observer = /^SELECT json_build_object\(|^SELECT count\(\*\) FILTER \(WHERE recorded_at/.test(d[3]);
    pg.slow.push({ us, ms: +d[1], observer, what: d[3].startsWith("COMMIT") ? "COMMIT" : d[3].slice(0, 40) });
  }
}
if (open) pg.checkpoints.push({ ...open, end_us: null });
// Recovered logs carry every checkpoint of the day; keep those overlapping this run.
pg.checkpoints = pg.checkpoints.filter((c) => (c.end_us ?? Infinity) >= start && c.start_us <= start + (summary.actual_elapsed_seconds_including_warmup_and_drain + (summary.reconcile_seconds ?? 0) + 60) * 1e6);
pg.slow = pg.slow.filter((s) => s.us >= start);

// --- WAL switches -------------------------------------------------------------------------------
const wal = [];
let prev = null;
for await (const line of lines(join(runDir, "wal-samples.txt"))) {
  const p = line.split("|");
  if (p.length !== 3 || !/^\d+$/.test(p[0])) continue;
  const s = { us: +p[0], file: p[2] };
  if (prev && prev.file !== s.file) wal.push({ from_us: prev.us, to_us: s.us, file: s.file });
  prev = s;
}

// --- slow write attribution ---------------------------------------------------------------------
const attribution = {};
for (const w of slowWrites) {
  const tags = [];
  if (wal.some((x) => x.from_us <= w.done_us + 100_000 && x.to_us >= w.sent_us - 100_000)) tags.push("wal_switch");
  if (pg.checkpoints.some((c) => c.start_us <= w.done_us && (c.end_us ?? Infinity) >= w.sent_us)) tags.push("checkpoint");
  if (pg.slow.some((s) => s.what === "COMMIT" && s.us >= w.sent_us - 20_000 && s.us <= w.done_us + 20_000)) tags.push("slow_commit");
  w.tags = tags;
  const key = tags.length ? tags.join("+") : "none";
  attribution[key] = (attribution[key] ?? 0) + 1;
}
const slowPerHalfHour = {};
for (const w of slowWrites) { const h = Math.floor((w.sent_us - start) / 1800e6); slowPerHalfHour[h] = (slowPerHalfHour[h] ?? 0) + 1; }

// --- resource series (pg periodic, containers, processes, host) -----------------------------------
const pgPeriodic = [];
for await (const line of lines(join(runDir, "pg-periodic.jsonl"))) {
  try { pgPeriodic.push(JSON.parse(line)); } catch { /* partial line at kill time */ }
}
const containers = [];
if (containersArg) for await (const line of lines(containersArg)) { try { containers.push(JSON.parse(line)); } catch { /* ignore */ } }
const processes = [];
for await (const line of lines(join(round, "process-resources.jsonl"))) { try { processes.push(JSON.parse(line)); } catch { /* ignore */ } }
const host = [];
let hostPrev = null;
for await (const line of lines(join(runDir, "host-samples.jsonl"))) {
  let s; try { s = JSON.parse(line); } catch { continue; }
  if (hostPrev && s.md0 && hostPrev.md0) {
    const w = s.md0[2] - hostPrev.md0[2];
    host.push({ us: s.unix_us, writes: w, write_ms: w ? (s.md0[3] - hostPrev.md0[3]) / w : null, io_full_ms: (s.psi_io_full_us - hostPrev.psi_io_full_us) / 1000, cpu_some_ms: (s.psi_cpu_some_us - hostPrev.psi_cpu_some_us) / 1000 });
  }
  hostPrev = s;
}
const endUs = start + (measured.at(-1)?.minute ?? 0) * 60e6 + 60e6;
const inRun = (us) => us >= start && us <= endUs;
const early = (us) => us >= start && us < start + 30 * 60e6;
const late = (us) => us > endUs - 30 * 60e6 && us <= endUs;
const trend = (points) => {
  const e = points.filter((p) => early(p.us)).map((p) => p.v), l = points.filter((p) => late(p.us)).map((p) => p.v);
  const all = points.filter((p) => inRun(p.us)).map((p) => p.v);
  const em = median(e), lm = median(l);
  return { samples: all.length, first: all[0] ?? null, last: all.at(-1) ?? null, min: all.length ? Math.min(...all) : null, max: all.length ? Math.max(...all) : null, early_median: em, late_median: lm, late_vs_early: em ? +(lm / em).toFixed(3) : null };
};
const pgAt = (p) => Date.parse(p.at) * 1000;
const resources = {
  dbproxy_host_rss_mb: trend(processes.map((p) => ({ us: Date.parse(p.utc) * 1000, v: p.hostRssKb / 1024 }))),
  load_tool_rss_mb: trend(processes.map((p) => ({ us: Date.parse(p.utc) * 1000, v: p.loadRssKb / 1024 }))),
  pg_container_anon_mb: trend(containers.filter((c) => c["dbproxy-test-postgres"]).map((c) => ({ us: c.unix_ms * 1000, v: c["dbproxy-test-postgres"].anon / 2 ** 20 }))),
  pg_container_memory_mb: trend(containers.filter((c) => c["dbproxy-test-postgres"]).map((c) => ({ us: c.unix_ms * 1000, v: c["dbproxy-test-postgres"].memory / 2 ** 20 }))),
  redis_used_mb: trend(containers.map((c) => ({ us: c.unix_ms * 1000, v: c.redis?.used_memory / 2 ** 20 }))),
  redis_aof_mb: trend(containers.map((c) => ({ us: c.unix_ms * 1000, v: c.redis?.aof_current_size / 2 ** 20 }))),
  cache_used_mb: trend(containers.map((c) => ({ us: c.unix_ms * 1000, v: c.cache?.used_memory / 2 ** 20 }))),
  cache_keys: trend(containers.map((c) => ({ us: c.unix_ms * 1000, v: c.cache?.keys }))),
  cache_evicted_keys: trend(containers.map((c) => ({ us: c.unix_ms * 1000, v: c.cache?.evicted_keys }))),
  pg_db_mb: trend(pgPeriodic.map((p) => ({ us: pgAt(p), v: p.db_bytes / 2 ** 20 }))),
  idempotency_total_mb: trend(pgPeriodic.map((p) => ({ us: pgAt(p), v: p.idempotency.total_bytes / 2 ** 20 }))),
  idempotency_dead_rows: trend(pgPeriodic.map((p) => ({ us: pgAt(p), v: p.idempotency.dead }))),
  idempotency_live_rows: trend(pgPeriodic.map((p) => ({ us: pgAt(p), v: p.idempotency.live }))),
  snapshots_total_mb: trend(pgPeriodic.map((p) => ({ us: pgAt(p), v: +p.snapshots.total_bytes / 2 ** 20 }))),
  snapshots_dead_rows: trend(pgPeriodic.map((p) => ({ us: pgAt(p), v: +p.snapshots.dead }))),
  cache_repairs_live_rows: trend(pgPeriodic.map((p) => ({ us: pgAt(p), v: p.cache_repairs?.live ?? 0 }))),
  expired_receipts_pending: trend(pgPeriodic.map((p) => ({ us: pgAt(p), v: p.expired_receipts_pending }))),
  wal_segments: trend(pgPeriodic.map((p) => ({ us: pgAt(p), v: p.wal_segments }))),
  pg_connections: trend(pgPeriodic.map((p) => ({ us: pgAt(p), v: Object.values(p.connections ?? {}).reduce((a, b) => a + b, 0) }))),
  runtime_heartbeat_max_ms: trend([...serverMinute.entries()].map(([m, b]) => ({ us: start + m * 60e6 + 30e6, v: b.heartbeat_max_ms }))),
  // diskstats time is whole milliseconds, so average per minute; the 100 ms maximum is kept apart.
  host_disk_write_ms_per_minute: trend((() => {
    const byMinute = new Map();
    for (const h of host) {
      const m = minuteOf(h.us), a = byMinute.get(m) ?? { writes: 0, ms: 0 };
      a.writes += h.writes; a.ms += h.write_ms !== null ? h.write_ms * h.writes : 0;
      byMinute.set(m, a);
    }
    return [...byMinute.entries()].filter(([, a]) => a.writes > 0).map(([m, a]) => ({ us: start + m * 60e6 + 30e6, v: +(a.ms / a.writes).toFixed(3) }));
  })()),
  host_disk_write_ms_100ms_max: trend(host.filter((h) => h.writes > 0).map((h) => ({ us: h.us, v: h.write_ms }))),
};
// Growth that should follow inserts (tables) is reported per inserted row, not as a leak.
const firstPg = pgPeriodic.find((p) => inRun(pgAt(p))), lastPg = [...pgPeriodic].reverse().find((p) => inRun(pgAt(p)));
const perRow = firstPg && lastPg ? {
  snapshot_rows_inserted: +lastPg.snapshots.ins - +firstPg.snapshots.ins,
  snapshot_bytes_per_inserted_row: Math.round((+lastPg.snapshots.total_bytes - +firstPg.snapshots.total_bytes) / Math.max(1, +lastPg.snapshots.ins - +firstPg.snapshots.ins)),
  receipts_inserted: lastPg.idempotency.ins - firstPg.idempotency.ins,
  receipts_deleted: lastPg.idempotency.del - firstPg.idempotency.del,
  idempotency_autovacuums: lastPg.idempotency.autovacuum_count - firstPg.idempotency.autovacuum_count,
  snapshots_autovacuums: +lastPg.snapshots.autovacuum_count - +firstPg.snapshots.autovacuum_count,
  deadlocks: lastPg.xact.deadlocks - firstPg.xact.deadlocks, temp_bytes: lastPg.xact.temp_bytes - firstPg.xact.temp_bytes,
} : null;

const remaining = (f) => (existsSync(join(round, f)) ? readFileSync(join(round, f), "utf8").trim() : null);
const result = {
  run: summary.run, sample_seconds: summary.sample_seconds, rate,
  totals: {
    responses: summary.sample_responses, not_sent: summary.all_not_sent, errors: summary.all_response_errors_or_wrong_data,
    writes_checked: summary.writes_checked, mismatches: summary.mismatches, client_timing_dropped: summary.client_timing_dropped,
    reconcile_seconds: summary.reconcile_seconds, reads: stats(allReads), writes: stats(allWrites),
    fixture_remaining_expired_recent: remaining("remaining.txt"), trickle_remaining_expired_future: remaining("remaining-trickle.txt"),
  },
  minutes: { count: measured.length, with_errors: measured.filter((s) => s.errors).length, with_not_sent: measured.filter((s) => s.not_sent).length, worst_write_p99_ms: Math.max(...measured.map((s) => s.write_p99_ms)), worst_read_p99_ms: Math.max(...measured.map((s) => s.read_p99_ms)) },
  slow_writes: { threshold_ms: slowUs / 1000, total: slowWrites.length, attribution, per_half_hour: slowPerHalfHour, wal_switches: wal.filter((w) => inRun(w.to_us)).length },
  checkpoints: pg.checkpoints.map((c) => ({ start_utc: new Date(c.start_us / 1000).toISOString(), reason: c.reason, total_s: c.total_s, wal_added: c.wal_added, wal_recycled: c.wal_recycled, buffers: c.buffers })),
  autovacuum_on_dbproxy_tables: pg.autovacuum,
  slow_statements: {
    total: pg.slow.filter((s) => !s.observer).length,
    commits: pg.slow.filter((s) => s.what === "COMMIT").length,
    commit_max_ms: Math.max(0, ...pg.slow.filter((s) => s.what === "COMMIT").map((s) => s.ms)),
    max_ms_excluding_observers: Math.max(0, ...pg.slow.filter((s) => !s.observer).map((s) => s.ms)),
    observer_queries: { count: pg.slow.filter((s) => s.observer).length, max_ms: Math.max(0, ...pg.slow.filter((s) => s.observer).map((s) => s.ms)) },
    non_commit_over_20ms: pg.slow.filter((s) => !s.observer && s.what !== "COMMIT" && s.ms >= 20).map((s) => ({ utc: new Date(s.us / 1000).toISOString(), ms: s.ms, what: s.what })).slice(0, 20),
  },
  pg_session_waits: waitCounts,
  growth_per_row: perRow,
  resources,
  final_server_stages: finalStages,
  host_pressure: { io_full_stall_ms: +host.filter((h) => inRun(h.us)).reduce((a, h) => a + h.io_full_ms, 0).toFixed(1), cpu_some_stall_ms: +host.filter((h) => inRun(h.us)).reduce((a, h) => a + h.cpu_some_ms, 0).toFixed(1) },
  per_minute: series.map((s) => ({ ...s, ...(serverMinute.get(s.minute) ?? {}) })),
};
writeFileSync(join(runDir, "long-run.json"), JSON.stringify(result, null, 1) + "\n");
const { per_minute, ...headline } = result;
console.log(JSON.stringify(headline, null, 1));
