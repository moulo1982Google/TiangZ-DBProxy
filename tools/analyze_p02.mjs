// P02: write cost of the indexes added by migrations 013-015 (kept vs dropped after startup).
// Usage: node tools/analyze_p02.mjs <session containers.jsonl> <run folder>...
// Per run: write/read latency, WAL bytes per write (pg_stat_wal delta), PostgreSQL container CPU
// per write (cgroup usage delta over the run window), receipt and cache-repair index size.
// Groups by payload x mode, takes medians over repetitions and reports kept-minus-dropped.
// Writes p02.json next to the containers file and prints a summary.
import { createReadStream, existsSync, readFileSync, writeFileSync } from "node:fs";
import { createInterface } from "node:readline";
import { dirname, join } from "node:path";

const [containersPath, ...runs] = process.argv.slice(2);
const pct = (v, q) => (v.length ? v[Math.min(v.length - 1, Math.ceil(v.length * q) - 1)] : null);
const median = (v) => { const s = v.filter((x) => x != null).sort((a, b) => a - b); return s.length ? (s.length % 2 ? s[(s.length - 1) / 2] : (s[s.length / 2 - 1] + s[s.length / 2]) / 2) : null; };
const json = (p) => JSON.parse(readFileSync(p, "utf8"));
const containers = existsSync(containersPath)
  ? readFileSync(containersPath, "utf8").split("\n").filter(Boolean).flatMap((l) => { try { return [JSON.parse(l)]; } catch { return []; } })
  : [];

async function run(dir) {
  const round = existsSync(join(dir, "1_off")) ? join(dir, "1_off") : join(dir, "1_on");
  const manifest = json(join(dir, "manifest.json")), summary = json(join(round, "summary.json"));
  const reads = [], writes = [];
  let errors = 0, notSent = 0, allWrites = 0;
  for await (const line of createInterface({ input: createReadStream(join(round, "requests.jsonl")), crlfDelay: Infinity })) {
    if (line.includes('"not_sent"')) { if (line.includes('"sample":true')) notSent++; continue; }
    if (!line.includes('"response"')) continue;
    const r = JSON.parse(line);
    if (r.n % 2 === 0) allWrites++;
    if (!r.sample) continue;
    if (r.outcome?.Ok !== true) errors++;
    (r.n % 2 === 0 ? writes : reads).push(r.end_to_end_us);
  }
  writes.sort((a, b) => a - b); reads.sort((a, b) => a - b);
  // Load window only: fixture seeding and reconciliation happen outside it.
  const t0 = summary.start_unix_us / 1000, t1 = t0 + (manifest.WarmupSeconds + manifest.Seconds) * 1000;
  const windowWrites = ((manifest.WarmupSeconds + manifest.Seconds) * manifest.Rate) / 2;
  const walSamples = readFileSync(join(dir, "wal-samples.txt"), "utf8").split("\n").map((l) => l.split("|"))
    .filter((p) => p.length === 3 && /^\d+$/.test(p[0])).map((p) => ({ ms: +p[0] / 1000, lsn: p[1] }))
    .filter((s) => s.ms >= t0 && s.ms <= t1);
  const lsnBytes = (l) => { const [h, x] = l.split("/").map((v) => parseInt(v, 16)); return h * 2 ** 32 + x; };
  const walBytes = walSamples.length > 1 ? lsnBytes(walSamples.at(-1).lsn) - lsnBytes(walSamples[0].lsn) : null;
  const walSpanMs = walSamples.length > 1 ? walSamples.at(-1).ms - walSamples[0].ms : null;
  const pg = containers.filter((c) => c.unix_ms >= t0 && c.unix_ms <= t1 && c["dbproxy-test-postgres"]);
  const cpuUs = pg.length > 1 ? pg.at(-1)["dbproxy-test-postgres"].cpu_usec - pg[0]["dbproxy-test-postgres"].cpu_usec : null;
  const ioW = pg.length > 1 ? pg.at(-1)["dbproxy-test-postgres"].io_write_bytes - pg[0]["dbproxy-test-postgres"].io_write_bytes : null;
  const periodic = readFileSync(join(dir, "pg-periodic.jsonl"), "utf8").split("\n").filter(Boolean).flatMap((l) => { try { return [JSON.parse(l)]; } catch { return []; } });
  const last = periodic.at(-1);
  // Denominator: writes scheduled inside the load window. Cluster-wide WAL and container CPU
  // also contain background workers and samplers, equally in both modes.
  const spanMs = pg.length > 1 ? pg.at(-1).unix_ms - pg[0].unix_ms : null;
  return {
    run: dir.split(/[\\/]/).pop(), payload: manifest.PayloadBytes, dropped: manifest.DropNewIndexesAfterStartup === 1,
    errors, not_sent: notSent,
    write_p50_ms: pct(writes, 0.5) / 1000, write_p99_ms: pct(writes, 0.99) / 1000, write_max_ms: (writes.at(-1) ?? 0) / 1000,
    read_p99_ms: pct(reads, 0.99) / 1000,
    writes_total: allWrites, window_writes: windowWrites,
    // Samples cover slightly less than the window; scale to per-second rates, then per write.
    wal_bytes_per_write: walBytes != null ? Math.round(((walBytes / walSpanMs) * (t1 - t0)) / windowWrites) : null,
    pg_cpu_us_per_write: cpuUs != null ? Math.round(((cpuUs / spanMs) * (t1 - t0)) / windowWrites) : null,
    pg_io_write_bytes_per_write: ioW != null ? Math.round(((ioW / spanMs) * (t1 - t0)) / windowWrites) : null,
    receipt_index_mb: last ? +(last.idempotency.index_bytes / 2 ** 20).toFixed(1) : null,
    receipt_total_mb: last ? +(last.idempotency.total_bytes / 2 ** 20).toFixed(1) : null,
    dropped_file_present: existsSync(join(round, "dropped-indexes.txt")),
  };
}

const rows = [];
for (const dir of runs) rows.push(await run(dir));
const groups = {};
for (const r of rows) (groups[`${r.payload}`] ??= { kept: [], dropped: [] })[r.dropped ? "dropped" : "kept"].push(r);
const metrics = ["write_p50_ms", "write_p99_ms", "read_p99_ms", "wal_bytes_per_write", "pg_cpu_us_per_write", "pg_io_write_bytes_per_write", "receipt_index_mb", "receipt_total_mb"];
const comparison = Object.fromEntries(Object.entries(groups).map(([payload, g]) => {
  const k = Object.fromEntries(metrics.map((m) => [m, median(g.kept.map((r) => r[m]))]));
  const d = Object.fromEntries(metrics.map((m) => [m, median(g.dropped.map((r) => r[m]))]));
  const delta = Object.fromEntries(metrics.map((m) => [m, k[m] != null && d[m] != null ? { kept_minus_dropped: +(k[m] - d[m]).toFixed(3), pct: d[m] ? +(((k[m] - d[m]) / d[m]) * 100).toFixed(1) : null } : null]));
  return [payload, { kept: k, dropped: d, delta, runs: { kept: g.kept.length, dropped: g.dropped.length } }];
}));
const problems = rows.filter((r) => r.errors || r.not_sent || r.dropped !== r.dropped_file_present).map((r) => r.run);
const out = { rows, comparison, problems };
writeFileSync(join(dirname(containersPath), "p02.json"), JSON.stringify(out, null, 1) + "\n");
for (const r of rows) console.log(JSON.stringify(r));
console.log(JSON.stringify({ comparison, problems }, null, 1));
