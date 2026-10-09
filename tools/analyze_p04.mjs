// P04: drain of a large expired-receipt backlog under fixed-rate load.
// Usage: node tools/analyze_p04.mjs <run folder>
// Uses pg-periodic.jsonl (expired_receipts_pending every PG_SAMPLE_SECONDS) to find when the
// backlog reached zero, then splits request latency into "while draining" and "after drained".
// Requests are placed in time by their fixed-rate schedule (n / rate after the load start).
// Writes p04.json into the run folder and prints it.
import { createReadStream, existsSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { createInterface } from "node:readline";
import { join } from "node:path";

const dir = process.argv[2];
if (!dir) throw new Error("usage: node tools/analyze_p04.mjs <run folder>");
const round = existsSync(join(dir, "1_on")) ? join(dir, "1_on") : join(dir, "1_off");
const json = (p) => JSON.parse(readFileSync(p, "utf8"));
const manifest = json(join(dir, "manifest.json"));
// summary.json is written only when the load tool finishes; a run cut off during reconciliation
// (p04_a) lacks it, so fall back to load.out, which the tool writes at start (about ±1 s).
const hasSummary = existsSync(join(round, "summary.json"));
const startMs = hasSummary
  ? json(join(round, "summary.json")).start_unix_us / 1000
  : statSync(join(round, "load.out")).mtimeMs;
const rate = manifest.Rate;
const periodic = readFileSync(join(dir, "pg-periodic.jsonl"), "utf8").split("\n").filter(Boolean)
  .flatMap((l) => { try { return [JSON.parse(l)]; } catch { return []; } })
  .map((p) => ({ ms: Date.parse(p.at), pending: p.expired_receipts_pending, del: p.idempotency.del, live: p.idempotency.live, dead: p.idempotency.dead, total_mb: p.idempotency.total_bytes / 2 ** 20, autovacuums: p.idempotency.autovacuum_count }));
const first = periodic.find((p) => p.pending > 0);
const drained = periodic.find((p) => first && p.ms > first.ms && p.pending === 0);
// The sample before the first zero bounds the drain end from below.
const beforeDrained = drained ? periodic[periodic.indexOf(drained) - 1] : null;
const drainEndMs = drained?.ms ?? null;
const pct = (v, q) => (v.length ? v[Math.min(v.length - 1, Math.ceil(v.length * q) - 1)] : null);
const buckets = { draining: { reads: [], writes: [], errors: 0, not_sent: 0 }, drained: { reads: [], writes: [], errors: 0, not_sent: 0 } };
for await (const line of createInterface({ input: createReadStream(join(round, "requests.jsonl")), crlfDelay: Infinity })) {
  const isNotSent = line.includes('"not_sent"');
  if (!isNotSent && !line.includes('"response"')) continue;
  const r = JSON.parse(line);
  if (!r.sample) continue;
  const atMs = startMs + ((r.n - 1) * 1000) / rate;
  const b = drainEndMs === null || atMs < drainEndMs ? buckets.draining : buckets.drained;
  if (isNotSent) { b.not_sent++; continue; }
  if (r.outcome?.Ok !== true) b.errors++;
  (r.n % 2 === 0 ? b.writes : b.reads).push(r.end_to_end_us / 1000);
}
const stats = (b) => {
  b.reads.sort((x, y) => x - y); b.writes.sort((x, y) => x - y);
  const s = (v) => ({ count: v.length, p50: pct(v, 0.5), p99: pct(v, 0.99), p999: pct(v, 0.999), max: v.at(-1) ?? null });
  return { reads_ms: s(b.reads), writes_ms: s(b.writes), errors: b.errors, not_sent: b.not_sent };
};
const out = {
  run: manifest.RunId, rate, fixture_expired: manifest.FixtureExpired, fixture_recent: manifest.FixtureRecent,
  load_start: new Date(startMs).toISOString(), load_start_source: hasSummary ? "summary.json" : "load.out mtime (approx.)",
  backlog_first_sample: first ? { at: new Date(first.ms).toISOString(), pending: first.pending } : null,
  drained_by: drained ? new Date(drained.ms).toISOString() : null,
  drain_seconds_upper: drained && first ? (drained.ms - first.ms) / 1000 : null,
  drain_seconds_lower: beforeDrained && first ? (beforeDrained.ms - first.ms) / 1000 : null,
  drain_rate_per_second: drained && first ? Math.round(first.pending / ((drained.ms - first.ms) / 1000)) : null,
  // Cleanup starts with the host, just before the load; the first sample may come up to one
  // sampling interval later, so this bound counts the whole fixture from the load start.
  drain_seconds_from_load_start_upper: drained ? (drained.ms - startMs) / 1000 : null,
  idempotency_start: periodic[0], idempotency_end: periodic.at(-1),
  latency: { while_draining: stats(buckets.draining), after_drained: stats(buckets.drained) },
  pending_curve: periodic.map((p) => ({ s: Math.round((p.ms - startMs) / 1000), pending: p.pending, dead: p.dead, total_mb: +p.total_mb.toFixed(1) })),
};
writeFileSync(join(dir, "p04.json"), JSON.stringify(out, null, 1) + "\n");
console.log(JSON.stringify({ ...out, pending_curve: `${out.pending_curve.length} samples` }, null, 1));
