import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
const root = path.resolve(import.meta.dirname, '..');
const dir = fs.mkdtempSync(path.join(root, 'target/old-cache-repro-'));
const suffix = path.basename(dir).toLowerCase();
const names = [suffix + '-pg', suffix + '-redis'];
const run = (cmd, args, extra = {}) => {
  const result = spawnSync(cmd, args, { cwd: root, encoding: 'utf8', timeout: 120000, ...extra });
  if (result.error || result.status !== 0) throw Error(`${cmd} failed: ${result.error ?? result.stderr}`);
  return result.stdout.trim();
};
const report = { startedAt: new Date().toISOString(), durationSeconds: 1800, directory: dir, names, rounds: [], status: 'starting' };
const save = () => fs.writeFileSync(path.join(dir, 'report.json'), JSON.stringify(report, null, 2));
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
console.log(dir);
try {
  report.commit = run('git', ['rev-parse', 'HEAD']);
  report.gitStatus = run('git', ['status', '--short']);
  fs.writeFileSync(path.join(dir, 'changes.patch'), run('git', ['diff', 'HEAD']));
  fs.copyFileSync(import.meta.filename, path.join(dir, 'controller.mjs'));
  const build = JSON.parse('[' + run('cargo', ['test', '-p', 'tiangz-dbproxy-storage', '--test', 'postgres_redis', '--no-run', '--message-format=json']).split('\n').filter(x => x.startsWith('{')).join(',') + ']');
  const binary = build.find(x => x.reason === 'compiler-artifact' && x.target.name === 'postgres_redis' && x.executable)?.executable;
  if (!binary) throw Error('missing test binary');
  report.binary = binary; report.binarySha256 = createHash('sha256').update(fs.readFileSync(binary)).digest('hex');
  run('docker', ['run', '-d', '--name', names[0], '--label', 'tiangz.purpose=old-cache-reproduction', '-e', 'POSTGRES_PASSWORD=repro_local_only', '-p', '127.0.0.1::5432', 'postgres:18.4-bookworm']);
  run('docker', ['run', '-d', '--name', names[1], '--label', 'tiangz.purpose=old-cache-reproduction', '-p', '127.0.0.1::6379', 'redis:8.8.1-trixie', 'redis-server', '--save', '', '--appendonly', 'no']);
  report.images = names.map(name => JSON.parse(run('docker', ['inspect', name]))[0].Image);
  const pgPort = run('docker', ['port', names[0], '5432/tcp']).split(':').at(-1);
  const redisPort = run('docker', ['port', names[1], '6379/tcp']).split(':').at(-1);
  let ready = false;
  for (let i = 0; i < 60; i++) {
    const result = spawnSync('docker', ['exec', names[0], 'pg_isready', '-h', '127.0.0.1', '-U', 'postgres'], { encoding: 'utf8' });
    if (result.status === 0) { ready = true; break; } await sleep(1000);
  }
  if (!ready) throw Error('postgres readiness timeout');
  const env = { ...process.env, DBPROXY_POSTGRES_URL: `postgres://postgres:repro_local_only@127.0.0.1:${pgPort}/postgres`, DBPROXY_CACHE_REDIS_URL: `redis://127.0.0.1:${redisPort}/0`, DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION: '1' };
  env.DBPROXY_TEST_POSTGRES_URL = env.DBPROXY_POSTGRES_URL;
  report.status = 'running'; report.workloadStartedAt = new Date().toISOString(); save();
  const deadline = Date.now() + 1800000;
  while (Date.now() < deadline) {
    const at = new Date().toISOString();
    const result = spawnSync(binary, ['acknowledged_write_timeout_must_not_expose_old_cache', '--exact', '--ignored', '--nocapture', '--test-threads=1'], { cwd: root, env, encoding: 'utf8', timeout: 30000 });
    const output = (result.stdout ?? '') + (result.stderr ?? '');
    const index = report.rounds.length + 1;
    fs.writeFileSync(path.join(dir, `round-${index}.log`), output);
    const reproduced = result.status !== 0 && output.includes('successful ACK followed by stale cache read');
    report.rounds.push({ index, at, exitCode: result.status, reproduced, error: result.error?.message }); save();
    console.log(JSON.stringify(report.rounds.at(-1)));
    if (result.status !== 0 && !reproduced) throw Error('unexpected test failure; inspect round log');
    await sleep(Math.min(30000, Math.max(0, deadline - Date.now())));
  }
  report.status = report.rounds.some(x => x.reproduced) ? 'reproduced' : 'not-reproduced';
} catch (error) { report.status = 'error'; report.error = String(error); process.exitCode = 1; }
finally {
  report.cleanup = names.map(name => ({ name, result: spawnSync('docker', ['stop', name], { encoding: 'utf8', timeout: 30000 }).status }));
  report.finishedAt = new Date().toISOString(); save(); console.log(JSON.stringify(report));
}
