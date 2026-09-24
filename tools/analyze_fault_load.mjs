// Phase verdict for a fixed-rate load run with one injected fault.
// Usage: node tools/analyze_fault_load.mjs <artifact folder>
// Reads requests.jsonl, reconciliation.jsonl, summary.json, fault-events.jsonl and fault-plan.json
// (rules written before the run). Writes phase-analysis.json and exits non-zero on any violation.
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const folder = process.argv[2];
if (!folder) throw new Error("usage: node tools/analyze_fault_load.mjs <artifact folder>");
const lines = (name) =>
  readFileSync(join(folder, name), "utf8")
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line));
const summary = JSON.parse(readFileSync(join(folder, "summary.json"), "utf8"));
const plan = JSON.parse(readFileSync(join(folder, "fault-plan.json"), "utf8"));
const events = lines("fault-events.jsonl");
const rows = lines("requests.jsonl");
const checks = lines("reconciliation.jsonl");

const injected = events.find((e) => e.kind === "injected");
const released = events.find((e) => e.kind === "released");
if (!injected || !released) throw new Error("fault-events.jsonl lacks injected/released events");
const graceUs = BigInt(plan.recovery_grace_seconds) * 1_000_000n;
const injectedUs = BigInt(injected.unix_us);
const releasedUs = BigInt(released.unix_us);
const start = BigInt(summary.start_unix_us);

const intents = new Map(rows.filter((r) => r.kind === "intent").map((r) => [r.n, r]));
const scheduledUs = (n) => {
  const intent = intents.get(n);
  if (!intent) throw new Error(`no intent for request ${n}`);
  return start + BigInt(intent.scheduled_us);
};
// A not_sent row has no intent; its scheduled time follows the fixed rate.
const rate = BigInt(summary.target_rate);
const scheduledByRule = (n) => start + (BigInt(n - 1) * 1_000_000n) / rate;

// Rule v2 (2026-09-22): a request belongs to the fault phase when its in-flight interval
// [sent, completed] overlaps the fault window [injected, released + grace]. Requests already
// completed before injection are "before"; requests sent after the window are "after". Rule v1
// classified by send time only and put in-flight victims of an instantaneous fault into "before".
const phaseOf = (sentUs, completedUs) => {
  if (completedUs < injectedUs) return "before";
  if (sentUs <= releasedUs + graceUs) return "during";
  return "after";
};
const phases = {
  before: { responses: 0, ok: 0, wrong_data: 0, errors: 0, allowed_errors: 0, not_sent: 0, max_end_to_end_us: 0 },
  during: { responses: 0, ok: 0, wrong_data: 0, errors: 0, allowed_errors: 0, not_sent: 0, max_end_to_end_us: 0 },
  after: { responses: 0, ok: 0, wrong_data: 0, errors: 0, allowed_errors: 0, not_sent: 0, max_end_to_end_us: 0 },
};
const allowed = plan.allowed_fault_error_patterns.map((p) => new RegExp(p, "i"));
const errorSamples = [];
let lastDisturbanceUs = null;
const disturbance = (sentUs) => {
  if (lastDisturbanceUs === null || sentUs > lastDisturbanceUs) lastDisturbanceUs = sentUs;
};
for (const row of rows) {
  if (row.kind === "response") {
    const sentUs = scheduledUs(row.n) + BigInt(row.dispatch_delay_us);
    const completedUs = scheduledUs(row.n) + BigInt(row.end_to_end_us);
    const phaseName = phaseOf(sentUs, completedUs);
    const phase = phases[phaseName];
    phase.responses += 1;
    phase.max_end_to_end_us = Math.max(phase.max_end_to_end_us, row.end_to_end_us);
    if (row.outcome && row.outcome.Ok === true) {
      phase.ok += 1;
    } else if (row.outcome && row.outcome.Ok === false) {
      phase.wrong_data += 1;
      errorSamples.push({ n: row.n, phase: phaseName, kind: "wrong_data" });
      disturbance(sentUs);
    } else {
      const message = String(row.outcome?.Err ?? row.outcome);
      phase.errors += 1;
      if (allowed.some((p) => p.test(message))) phase.allowed_errors += 1;
      if (errorSamples.length < 20) errorSamples.push({ n: row.n, phase: phaseName, kind: "error", message });
      disturbance(sentUs);
    }
  } else if (row.kind === "not_sent") {
    // A missed send has no completion; classify by its scheduled instant.
    const sentUs = scheduledByRule(row.n);
    phases[phaseOf(sentUs, sentUs)].not_sent += 1;
    disturbance(sentUs);
  }
}
const reconciliation = {
  checked: checks.length,
  mismatches: checks.filter((c) => !c.matches).length,
  committed_without_ok_response: checks.filter((c) => c.matches && c.committed && c.outcome_ok === false).length,
  rolled_back_errors: checks.filter((c) => c.matches && !c.committed && c.outcome_ok === false).length,
};
const violations = [];
if (phases.before.errors + phases.before.wrong_data + phases.before.not_sent > 0) violations.push("normal phase before the fault had errors or missed sends");
if (phases.during.wrong_data > 0) violations.push("fault phase returned wrong data");
if (phases.during.errors !== phases.during.allowed_errors) violations.push("fault phase had errors outside the allowed patterns");
if (phases.after.errors + phases.after.wrong_data + phases.after.not_sent > 0) violations.push("recovery did not complete within the grace period");
if (reconciliation.mismatches > 0) violations.push("write reconciliation found half-written or wrong data");
if (summary.client_timing_dropped > 0) violations.push("client diagnostics were dropped");
if (plan.fault_kind === "blocked_write" && released.target_was_blocked !== true) violations.push("blocked_write fault was never observed at the barrier");
if (plan.fault_kind === "kill_connections" && !(released.killed > 0)) violations.push("kill_connections terminated no backend");
// Relay faults (F06): the host must have been connected through the relay, and held traffic must
// have drained once forwarding resumed.
if (plan.fault_kind?.startsWith("pg_") && !(released.relay_connections_before > 0)) violations.push("the host never connected through the relay");
if (plan.fault_kind?.startsWith("pg_") && released.buffered_bytes_left !== 0) violations.push("relay still held traffic 10 s after release");
const relayStopped = events.find((e) => e.kind === "relay_stopped");
// F10: the fault must actually reach the clients, otherwise the run proves nothing.
if (["read_only", "disk_full"].includes(plan.fault_kind) && phases.during.errors === 0) violations.push(`${plan.fault_kind} produced no client-visible error`);
const recoveryUs = lastDisturbanceUs === null || lastDisturbanceUs < releasedUs ? 0n : lastDisturbanceUs - releasedUs;
const analysis = {
  rule_version: 2,
  phase_rule: "before = completed before injection; during = in-flight interval overlaps [injected, released + grace]; after = sent after the window",
  fault_kind: plan.fault_kind,
  injected_unix_us: injected.unix_us,
  released_unix_us: released.unix_us,
  fault_window_seconds: Number(releasedUs - injectedUs) / 1e6,
  recovery_grace_seconds: plan.recovery_grace_seconds,
  last_disturbance_after_release_seconds: Number(recoveryUs) / 1e6,
  relay: plan.fault_kind?.startsWith("pg_")
    ? {
        connections_before: released.relay_connections_before,
        connections_opened_during_fault: released.relay_connections_opened_during_fault,
        connections_opened_after_release: relayStopped?.relay_connections_after_release ?? null,
        peak_buffered_bytes: released.peak_buffered_bytes,
        buffer_drained_after_ms: released.buffer_drained_after_us / 1000,
      }
    : undefined,
  phases,
  reconciliation,
  error_samples: errorSamples,
  violations,
  verdict: violations.length === 0 ? "PASS" : "FAIL",
};
writeFileSync(join(folder, "phase-analysis.json"), JSON.stringify(analysis, null, 2) + "\n");
console.log(JSON.stringify(analysis, null, 2));
if (violations.length > 0) process.exit(1);
