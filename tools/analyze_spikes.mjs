// Align every slow request of a fixed-rate probe with what happened at the same instant.
// Usage: node tools/analyze_spikes.mjs <run folder> <round folder> [threshold ms = 15]
// Run folder: host-samples.jsonl (deploy/remote-test/sample_host.sh), pg-stats-before/after.json.
// Round folder: requests.jsonl, summary.json, storage-stages.jsonl, postgres.log.
// Writes <run folder>/spikes.json. Correlation only: a coincidence is not proof of cause.
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const [runDir, roundDir, thresholdArg] = process.argv.slice(2);
if (!runDir || !roundDir) throw new Error("usage: analyze_spikes.mjs <run folder> <round folder> [threshold ms]");
const thresholdUs = Number(thresholdArg ?? 15) * 1000;
const jsonl = (path) =>
  existsSync(path) ? readFileSync(path, "utf8").split("\n").filter(Boolean).map((l) => JSON.parse(l)) : [];
const pct = (sorted, q) => (sorted.length ? sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * q) - 1)] : null);

// --- requests -------------------------------------------------------------------------------
const summary = JSON.parse(readFileSync(join(roundDir, "summary.json"), "utf8"));
const start = summary.start_unix_us;
const rows = jsonl(join(roundDir, "requests.jsonl"));
const intents = new Map(rows.filter((r) => r.kind === "intent").map((r) => [r.n, r]));
const responses = rows.filter((r) => r.kind === "response" && r.sample);
const requests = responses.map((r) => {
  const scheduled = start + intents.get(r.n).scheduled_us;
  return { n: r.n, op: intents.get(r.n).op, sent_us: scheduled + r.dispatch_delay_us, done_us: scheduled + r.end_to_end_us, e2e_us: r.end_to_end_us, dispatch_us: r.dispatch_delay_us };
});
const byOp = (op) => requests.filter((r) => r.op === op).map((r) => r.e2e_us).sort((a, b) => a - b);
const latency = Object.fromEntries(["load", "save"].map((op) => {
  const v = byOp(op);
  return [op, { count: v.length, p50_ms: pct(v, 0.5) / 1000, p99_ms: pct(v, 0.99) / 1000, p999_ms: pct(v, 0.999) / 1000, max_ms: v[v.length - 1] / 1000 }];
}));

// --- PostgreSQL log (multi-line entries; timestamps are statement end times) ------------------
const pgEntries = [];
if (existsSync(join(roundDir, "postgres.log"))) {
  for (const line of readFileSync(join(roundDir, "postgres.log"), "utf8").split("\n")) {
    const m = line.match(/^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}\.\d{3}) UTC \[(\d+)\] (\w+):\s+(.*)$/);
    if (m) pgEntries.push({ us: Date.parse(`${m[1]}T${m[2]}Z`) * 1000, pid: +m[3], level: m[4], text: m[5] });
    else if (pgEntries.length && line.trim()) pgEntries[pgEntries.length - 1].text += " | " + line.trim();
  }
}
const durationOf = (e) => { const m = e.text.match(/^duration: ([\d.]+) ms/); return m ? +m[1] : null; };
const checkpoints = [];
for (const e of pgEntries) {
  if (/^checkpoint starting/.test(e.text)) checkpoints.push({ start_us: e.us, end_us: null, text: e.text });
  else if (/^checkpoint complete/.test(e.text) && checkpoints.length && checkpoints.at(-1).end_us === null) {
    checkpoints.at(-1).end_us = e.us; checkpoints.at(-1).complete = e.text;
  }
}
const autovacuum = pgEntries.filter((e) => /automatic (vacuum|analyze)/.test(e.text));

// --- server snapshots (cumulative histograms every ~100 ms) and PG session samples -------------
const stageLines = jsonl(join(roundDir, "storage-stages.jsonl"));
const snaps = stageLines.filter((l) => l.stages).map((l) => ({ ...l, us: l.unix_ms * 1000 }));
const activity = stageLines.filter((l) => l.kind === "pg_activity").map((l) => ({ ...l, us: l.unix_ms * 1000 }));
const slowBuckets = (a, b, key, startIndex) => {
  const out = {};
  for (const s of b[key]) {
    const prev = a[key].find((p) => p.stage === s.stage && (p.operation ?? null) === (s.operation ?? null));
    const d = s.buckets.slice(startIndex).reduce((sum, x, i) => sum + x - prev.buckets[startIndex + i], 0);
    if (d > 0) out[s.operation ? `${s.operation}/${s.stage}` : s.stage] = d;
  }
  return out;
};

// --- host samples ----------------------------------------------------------------------------
const host = jsonl(join(runDir, "host-samples.jsonl"));
const devices = host.length ? Object.keys(host[0]).filter((k) => Array.isArray(host[0][k])) : [];
const hostDelta = (a, b) => {
  const out = { span_ms: (b.unix_us - a.unix_us) / 1000, load1: b.load1 };
  for (const d of devices) {
    const writes = b[d][2] - a[d][2], wms = b[d][3] - a[d][3], reads = b[d][0] - a[d][0], rms = b[d][1] - a[d][1];
    out[d] = { writes, avg_write_ms: writes ? +(wms / writes).toFixed(2) : 0, reads, avg_read_ms: reads ? +(rms / reads).toFixed(2) : 0, in_flight: b[d][4] };
  }
  for (const k of Object.keys(b).filter((k) => k.startsWith("psi_"))) out[k.replace("_us", "_stall_ms")] = (b[k] - a[k]) / 1000;
  return out;
};
const primary = devices.includes("md0") ? "md0" : devices[0];
const hostIntervals = host.slice(1).map((b, i) => ({ from: host[i].unix_us, to: b.unix_us, ...hostDelta(host[i], b) }));
const inRun = hostIntervals.filter((h) => h.from >= start && h.to <= (requests.at(-1)?.done_us ?? Infinity));
const baselineWrite = inRun.map((h) => h[primary]?.avg_write_ms ?? 0).filter((x) => x > 0).sort((a, b) => a - b);
const baseline = {
  device: primary,
  intervals: inRun.length,
  avg_write_ms_p50: pct(baselineWrite, 0.5), avg_write_ms_p99: pct(baselineWrite, 0.99), avg_write_ms_max: baselineWrite.at(-1) ?? null,
  psi_io_full_stall_ms_total: inRun.reduce((s, h) => s + (h.psi_io_full_stall_ms ?? 0), 0),
  psi_cpu_some_stall_ms_total: inRun.reduce((s, h) => s + (h.psi_cpu_some_stall_ms ?? 0), 0),
};
const diskSlowMs = Math.max(5, 5 * (baseline.avg_write_ms_p50 ?? 1));

// --- WAL segment switches (sampled every ~100 ms: epoch_us|lsn|walfile) -----------------------
const walSamples = existsSync(join(runDir, "wal-samples.txt"))
  ? readFileSync(join(runDir, "wal-samples.txt"), "utf8").split("\n").map((l) => l.split("|")).filter((p) => p.length === 3 && /^\d+$/.test(p[0])).map((p) => ({ us: +p[0], lsn: p[1], file: p[2] }))
  : [];
// A switch happened somewhere between the last sample on the old file and the first on the new one.
const walSwitches = walSamples.slice(1).flatMap((s, i) => (s.file !== walSamples[i].file ? [{ from_us: walSamples[i].us, to_us: s.us, file: s.file }] : []));
const walFileBytes = (lsn) => { const [hi, lo] = lsn.split("/").map((x) => parseInt(x, 16)); return hi * 2 ** 32 + lo; };
const walRate = walSamples.length > 1
  ? (walFileBytes(walSamples.at(-1).lsn) - walFileBytes(walSamples[0].lsn)) / ((walSamples.at(-1).us - walSamples[0].us) / 1e6)
  : null;

// --- per-spike alignment ---------------------------------------------------------------------
const spikes = requests.filter((r) => r.e2e_us >= thresholdUs).sort((a, b) => a.sent_us - b.sent_us).map((r) => {
  const lo = r.sent_us, hi = r.done_us;
  const before = [...snaps].reverse().find((s) => s.us <= lo), after = snaps.find((s) => s.us >= hi);
  const inSnaps = snaps.filter((s) => s.us > lo && s.us <= (after?.us ?? hi));
  const pg = pgEntries.filter((e) => e.us >= lo - 50_000 && e.us <= hi + 50_000).map((e) => ({ at_ms_after_send: (e.us - lo) / 1000, pid: e.pid, duration_ms: durationOf(e), text: e.text.slice(0, 160) }));
  const hosts = hostIntervals.filter((h) => h.to >= lo - 100_000 && h.from <= hi + 100_000);
  const waits = activity.filter((a) => a.us >= lo - 50_000 && a.us <= hi + 50_000).flatMap((a) =>
    (a.activity.backends ?? []).filter((b) => b.state === "active" || b.wait_type).map((b) => ({ at_ms_after_send: (a.us - lo) / 1000, pid: b.pid, state: b.state, wait: b.wait_type ? `${b.wait_type}/${b.wait}` : "cpu" })));
  const maxDiskWrite = Math.max(0, ...hosts.map((h) => h[primary]?.avg_write_ms ?? 0));
  const ioStall = hosts.reduce((s, h) => s + (h.psi_io_full_stall_ms ?? 0), 0);
  const heartbeat = Math.max(0, ...inSnaps.map((s) => s.runtime_heartbeat_max_delay_us ?? 0)) / 1000;
  const checkpoint = checkpoints.find((c) => c.start_us <= hi && (c.end_us ?? Infinity) >= lo);
  const storageSlow = before && after ? slowBuckets(before, after, "stages", 3) : null;
  const requestSlow = before && after ? slowBuckets(before, after, "request_stages", 3) : null;
  const signals = [];
  if (pg.some((e) => e.duration_ms !== null)) signals.push("pg_slow_statement");
  if (checkpoint) signals.push("during_checkpoint");
  if (autovacuum.some((e) => e.us >= lo - 1_000_000 && e.us <= hi + 1_000_000)) signals.push("near_autovacuum");
  if (maxDiskWrite >= diskSlowMs) signals.push("disk_write_slow");
  if (ioStall >= 5) signals.push("host_io_stall");
  if (heartbeat >= 5) signals.push("runtime_stall");
  if (r.dispatch_us >= 5000) signals.push("load_dispatch_late");
  // The switch window [from, to] overlaps the request's life (±100 ms sampling slack).
  const walSwitch = walSwitches.find((w) => w.from_us <= hi + 100_000 && w.to_us >= lo - 100_000);
  if (walSwitch) signals.push("wal_segment_switch");
  return {
    n: r.n, op: r.op, e2e_ms: r.e2e_us / 1000, dispatch_ms: r.dispatch_us / 1000,
    sent_utc: new Date(r.sent_us / 1000).toISOString(), signals,
    storage_stages_over_10ms: storageSlow, request_stages_over_10ms: requestSlow,
    runtime_heartbeat_max_ms: heartbeat, checkpoint: checkpoint ? checkpoint.text : null,
    wal_switch: walSwitch ? { file: walSwitch.file, window_utc: [new Date(walSwitch.from_us / 1000).toISOString(), new Date(walSwitch.to_us / 1000).toISOString()] } : null,
    pg_log: pg, pg_sessions: waits, host: hosts,
  };
});

// --- PostgreSQL statistics delta ---------------------------------------------------------------
const statsDelta = (() => {
  const p = (f) => (existsSync(join(runDir, f)) ? JSON.parse(readFileSync(join(runDir, f), "utf8")) : null);
  const a = p("pg-stats-before.json"), b = p("pg-stats-after.json");
  if (!a || !b) return null;
  const num = (x, y) => Object.fromEntries(Object.keys(y ?? {}).filter((k) => typeof y[k] === "number" && y[k] !== x?.[k]).map((k) => [k, +(y[k] - (x?.[k] ?? 0)).toFixed(3)]));
  const key = (i) => `${i.backend_type}/${i.object}/${i.context}`;
  const io = {};
  for (const row of b.io ?? []) {
    const d = num((a.io ?? []).find((x) => key(x) === key(row)), row);
    if (Object.keys(d).length) io[key(row)] = d;
  }
  return { wal: num(a.wal, b.wal), checkpointer: num(a.checkpointer, b.checkpointer), bgwriter: num(a.bgwriter, b.bgwriter), io };
})();

const signalCounts = {};
for (const s of spikes) for (const k of s.signals.length ? s.signals : ["no_signal"]) signalCounts[k] = (signalCounts[k] ?? 0) + 1;
// Converse check: how many WAL switches inside the measured window had a spike nearby.
const measuredLo = requests.reduce((m, r) => Math.min(m, r.sent_us), Infinity), measuredHi = requests.reduce((m, r) => Math.max(m, r.done_us), 0);
const switchesInWindow = walSwitches.filter((w) => w.from_us >= measuredLo && w.to_us <= measuredHi);
const wal = {
  samples: walSamples.length, bytes_per_second: walRate && Math.round(walRate),
  seconds_per_segment: walRate ? +((16 * 2 ** 20) / walRate).toFixed(1) : null,
  switches_in_measured_window: switchesInWindow.length,
  switches_with_spike: switchesInWindow.filter((w) => spikes.some((s) => s.wal_switch && s.wal_switch.file === w.file)).length,
  switches: switchesInWindow.map((w) => ({ file: w.file, utc: new Date(w.to_us / 1000).toISOString(), spike: spikes.some((s) => s.wal_switch && s.wal_switch.file === w.file) })),
};
const walFiles = ["pg-wal-before.json", "pg-wal-after.json"].map((f) => (existsSync(join(runDir, f)) ? JSON.parse(readFileSync(join(runDir, f), "utf8")) : null));
const result = {
  threshold_ms: thresholdUs / 1000, run: summary.run, latency, baseline, disk_slow_threshold_ms: diskSlowMs,
  wal, wal_directory: { before: walFiles[0], after: walFiles[1] },
  checkpoints: checkpoints.map((c) => ({ start_utc: new Date(c.start_us / 1000).toISOString(), end_utc: c.end_us ? new Date(c.end_us / 1000).toISOString() : null, complete: c.complete ?? null })),
  autovacuum: autovacuum.map((e) => ({ utc: new Date(e.us / 1000).toISOString(), text: e.text.slice(0, 200) })),
  pg_statements_over_5ms: pgEntries.filter((e) => durationOf(e) !== null).length,
  spikes_total: spikes.length, signal_counts: signalCounts, pg_stats_delta: statsDelta, spikes,
};
writeFileSync(join(runDir, "spikes.json"), JSON.stringify(result, null, 2) + "\n");
console.log(JSON.stringify({ run: result.run, latency, baseline, wal, wal_directory: result.wal_directory, pg_stats_delta_io: statsDelta?.io, checkpoints: result.checkpoints.length, autovacuum: result.autovacuum.length, pg_statements_over_5ms: result.pg_statements_over_5ms, spikes_total: spikes.length, signal_counts: signalCounts }, null, 1));
for (const s of spikes) console.log(`${s.sent_utc} n=${s.n} ${s.op} ${s.e2e_ms}ms dispatch=${s.dispatch_ms}ms signals=[${s.signals}] storage>10ms=${JSON.stringify(s.storage_stages_over_10ms)} request>10ms=${JSON.stringify(s.request_stages_over_10ms)} hb=${s.runtime_heartbeat_max_ms}ms`);
