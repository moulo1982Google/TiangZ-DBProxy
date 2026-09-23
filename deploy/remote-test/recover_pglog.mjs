// Recover the PostgreSQL log window of server runs whose capture came out empty because the
// log file was mode 0600 and unreadable by the workbench user (fixed 2026-09-23).
// For each round folder with summary.json and an empty postgres.log: keep the empty file as
// postgres.log.empty-at-capture, write the lines between start-30 s and end+120 s plus every
// checkpoint line of the whole file (so an in-progress checkpoint is visible), sorted by time.
// Usage: node recover_pglog.mjs /pglog/postgresql.log <round folder>...
import { existsSync, readFileSync, renameSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const [logPath, ...folders] = process.argv.slice(2);
const entries = [];
for (const line of readFileSync(logPath, "utf8").split("\n")) {
  const m = line.match(/^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}\.\d{3}) UTC /);
  if (m) entries.push({ us: Date.parse(`${m[1]}T${m[2]}Z`) * 1000, lines: [line] });
  else if (entries.length && line) entries.at(-1).lines.push(line);
}
for (const folder of folders) {
  const logFile = join(folder, "postgres.log");
  const summaryFile = join(folder, "summary.json");
  if (!existsSync(summaryFile) || !existsSync(logFile) || statSync(logFile).size > 0) {
    console.log(`skip ${folder}`);
    continue;
  }
  const s = JSON.parse(readFileSync(summaryFile, "utf8"));
  const lo = s.start_unix_us - 30e6;
  const hi = s.start_unix_us + (s.actual_elapsed_seconds_including_warmup_and_drain + (s.reconcile_seconds ?? 0) + 120) * 1e6;
  const picked = entries.filter((e) => (e.us >= lo && e.us <= hi) || /checkpoint (starting|complete)/.test(e.lines[0]));
  renameSync(logFile, `${logFile}.empty-at-capture`);
  writeFileSync(logFile, picked.map((e) => e.lines.join("\n")).join("\n") + "\n");
  writeFileSync(join(folder, "postgres.log.recovered.txt"),
    `Recovered on ${new Date().toISOString()} from ${logPath}: window ${new Date(lo / 1000).toISOString()} .. ${new Date(hi / 1000).toISOString()} plus all checkpoint lines. Original capture was empty because the log file was mode 0600.\n`);
  console.log(`recovered ${folder}: ${picked.length} entries`);
}
