// P05: receipt cleanup off (B1) vs on (B2) at the same fixed rate, alternating rounds.
// Usage: node tools/analyze_p05.mjs <artifact folder from run_receipt_probe --cleanup both> [...more]
// Per round: successful responses, errors, missed sends, read/write p50/p95/p99/max over the
// sample window, receipts deleted. Per mode: medians across rounds. Verdict against the plan's
// provisional thresholds: throughput drop <= 10%, p99 increase <= 20% (B2 relative to B1).
// Writes <folder>/p05.json for each folder and prints a combined summary.
import { createReadStream, existsSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { createInterface } from "node:readline";
import { join } from "node:path";

const pct = (v, q) => (v.length ? v[Math.min(v.length - 1, Math.ceil(v.length * q) - 1)] : null);
const median = (v) => { const s = [...v].sort((a, b) => a - b); return s.length ? (s.length % 2 ? s[(s.length - 1) / 2] : (s[s.length / 2 - 1] + s[s.length / 2]) / 2) : null; };
const stats = (v) => { v.sort((a, b) => a - b); return { count: v.length, p50_ms: pct(v, 0.5) / 1000, p95_ms: pct(v, 0.95) / 1000, p99_ms: pct(v, 0.99) / 1000, max_ms: (v.at(-1) ?? 0) / 1000 }; };

async function round(dir) {
  const summary = JSON.parse(readFileSync(join(dir, "summary.json"), "utf8"));
  const reads = [], writes = [];
  let errors = 0, notSent = 0, ok = 0;
  for await (const line of createInterface({ input: createReadStream(join(dir, "requests.jsonl")), crlfDelay: Infinity })) {
    if (!line.includes('"sample":true')) continue;
    const r = JSON.parse(line);
    if (r.kind === "not_sent") { notSent++; continue; }
    if (r.kind !== "response") continue;
    if (r.outcome?.Ok === true) ok++; else errors++;
    (r.n % 2 === 0 ? writes : reads).push(r.end_to_end_us);
  }
  const remaining = existsSync(join(dir, "remaining.txt")) ? readFileSync(join(dir, "remaining.txt"), "utf8").trim() : null;
  return {
    successful_per_second: ok / summary.sample_seconds, errors, not_sent: notSent,
    reads: stats(reads), writes: stats(writes), all_p99_ms: summary.p99_us / 1000,
    fixture_remaining_expired_recent: remaining,
  };
}

const combined = [];
for (const folder of process.argv.slice(2)) {
  const manifest = JSON.parse(readFileSync(join(folder, "manifest.json"), "utf8"));
  const rounds = [];
  for (const name of readdirSync(folder).filter((n) => /^\d+_(on|off)$/.test(n)).sort((a, b) => parseInt(a) - parseInt(b) || a.localeCompare(b))) {
    const dir = join(folder, name);
    if (!existsSync(join(dir, "summary.json"))) { rounds.push({ round: name, missing: true }); continue; }
    rounds.push({ round: name, mode: name.endsWith("_on") ? "B2_cleanup_on" : "B1_cleanup_off", ...(await round(dir)) });
  }
  const byMode = (mode) => rounds.filter((r) => r.mode === mode && !r.missing);
  const agg = (list) => ({
    rounds: list.length,
    successful_per_second: median(list.map((r) => r.successful_per_second)),
    errors: list.reduce((a, r) => a + r.errors, 0), not_sent: list.reduce((a, r) => a + r.not_sent, 0),
    read_p99_ms: median(list.map((r) => r.reads.p99_ms)), write_p99_ms: median(list.map((r) => r.writes.p99_ms)),
    all_p99_ms: median(list.map((r) => r.all_p99_ms)),
    read_p50_ms: median(list.map((r) => r.reads.p50_ms)), write_p50_ms: median(list.map((r) => r.writes.p50_ms)),
    read_max_ms: Math.max(...list.map((r) => r.reads.max_ms)), write_max_ms: Math.max(...list.map((r) => r.writes.max_ms)),
  });
  const b1 = agg(byMode("B1_cleanup_off")), b2 = agg(byMode("B2_cleanup_on"));
  const change = (a, b) => (a ? +(((b - a) / a) * 100).toFixed(1) : null);
  const comparison = {
    throughput_change_pct: change(b1.successful_per_second, b2.successful_per_second),
    read_p99_change_pct: change(b1.read_p99_ms, b2.read_p99_ms),
    write_p99_change_pct: change(b1.write_p99_ms, b2.write_p99_ms),
    all_p99_change_pct: change(b1.all_p99_ms, b2.all_p99_ms),
    read_p99_abs_ms: +(b2.read_p99_ms - b1.read_p99_ms).toFixed(3),
    write_p99_abs_ms: +(b2.write_p99_ms - b1.write_p99_ms).toFixed(3),
  };
  const violations = [];
  if (comparison.throughput_change_pct < -10) violations.push("throughput dropped more than 10%");
  for (const k of ["read_p99_change_pct", "write_p99_change_pct", "all_p99_change_pct"]) if (comparison[k] > 20) violations.push(`${k} above +20%`);
  if (b1.errors + b2.errors > 0) violations.push("errors in a fault-free run");
  if (b1.not_sent + b2.not_sent > 0) violations.push("missed sends in a fault-free run");
  const result = {
    folder, fixture_expired: manifest.FixtureExpired, rate: manifest.Rate, sample_seconds: manifest.Seconds,
    warmup_seconds: manifest.WarmupSeconds, background: manifest.BackgroundWorkers, retention_hours: manifest.ReceiptRetentionHours,
    B1_cleanup_off: b1, B2_cleanup_on: b2, comparison, violations, verdict: violations.length ? "FAIL" : "PASS", rounds,
  };
  writeFileSync(join(folder, "p05.json"), JSON.stringify(result, null, 1) + "\n");
  combined.push(result);
}
for (const r of combined) {
  const { rounds, ...head } = r;
  console.log(JSON.stringify(head, null, 1));
  for (const x of rounds) console.log(`  ${x.round}: ok/s=${x.successful_per_second?.toFixed?.(2)} err=${x.errors} not_sent=${x.not_sent} read p50/p99/max=${x.reads?.p50_ms}/${x.reads?.p99_ms}/${x.reads?.max_ms} write p50/p99/max=${x.writes?.p50_ms}/${x.writes?.p99_ms}/${x.writes?.max_ms} remaining=${x.fixture_remaining_expired_recent}`);
}
