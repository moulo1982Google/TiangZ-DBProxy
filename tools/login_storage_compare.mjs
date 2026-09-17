import { spawn, spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
const root = path.resolve(import.meta.dirname, '..');
const sweep = process.argv.includes('--sweep');
const expectedCases = sweep ? 72 : 24;
const dir = fs.mkdtempSync(path.join(root, 'target/login-storage-'));
const id = path.basename(dir).toLowerCase();
const names = [id + '-pg', id + '-redis'];
const report = { scope: 'storage-only; no TCP DBProxy, auth, Actor creation or retained-player reconnect', dir, names, startedAt: new Date().toISOString(), cases: [], status: 'starting' };
report.profile = { sweep, expectedCases, postgresCpu: 2, postgresMemory: '1g', maxConnections: sweep ? 80 : 400,
  note: '并发是同时在途请求数，不是在线玩家数；闭环预热读取，不是生产容量结论' };
const run = (cmd, args) => {
  const r = spawnSync(cmd, args, { cwd: root, encoding: 'utf8', timeout: 60000 });
  if (r.error || r.status !== 0) throw Error(`${cmd}: ${r.error ?? r.stderr}`);
  return r.stdout.trim();
};
const save = () => fs.writeFileSync(path.join(dir, 'report.json'), JSON.stringify(report, null, 2));
let stats;
try {
  report.commit = run('git', ['rev-parse', 'HEAD']);
  report.gitStatus = run('git', ['status', '--short']);
  fs.writeFileSync(path.join(dir, 'changes.patch'), run('git', ['diff', 'HEAD']));
  fs.copyFileSync(import.meta.filename, path.join(dir, 'controller.mjs'));
  fs.copyFileSync(path.join(root, 'src/bin/login_storage_compare.rs'), path.join(dir, 'benchmark.rs'));
  const binary = path.join(root, 'target/release/login_storage_compare' + (process.platform === 'win32' ? '.exe' : ''));
  report.binarySha256 = createHash('sha256').update(fs.readFileSync(binary)).digest('hex');
  report.databaseStorage = 'tmpfs; warm read comparison only, not durability or cold-disk performance';
  run('docker', ['run', '-d', '--name', names[0], '--cpus', '2', '--memory', '1g', '--tmpfs', '/var/lib/postgresql:rw,size=512m', '-e', 'POSTGRES_PASSWORD=compare_local_only', '-p', '127.0.0.1::5432', 'postgres:18.4-bookworm', '-c', `max_connections=${report.profile.maxConnections}`]);
  run('docker', ['run', '-d', '--name', names[1], '--cpus', '2', '--memory', '1g', '-p', '127.0.0.1::6379', 'redis:8.8.1-trixie', 'redis-server', '--save', '', '--appendonly', 'no']);
  report.images = names.map(n => JSON.parse(run('docker', ['inspect', n]))[0].Image);
  const pgPort = run('docker', ['port', names[0], '5432/tcp']).split(':').at(-1);
  const redisPort = run('docker', ['port', names[1], '6379/tcp']).split(':').at(-1);
  let ready = false;
  for (let i = 0; i < 180; i++) {
    if (spawnSync('docker', ['exec', names[0], 'pg_isready', '-h', '127.0.0.1', '-U', 'postgres']).status === 0) { ready = true; break; }
    await new Promise(r => setTimeout(r, 1000));
  }
  if (!ready) throw Error('PG readiness timeout');
  report.pgSettings = run('docker', ['exec', names[0], 'psql', '-U', 'postgres', '-At', '-c', "SELECT name,setting,unit,source FROM pg_settings WHERE name IN ('max_connections','shared_buffers','effective_cache_size','work_mem','max_parallel_workers_per_gather','random_page_cost','effective_io_concurrency','jit','plan_cache_mode') ORDER BY name"]);
  let activeCase = null, statsBuffer = '';
  stats = spawn('docker', ['stats', '--format', '{{json .}}', ...names], { windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
  stats.stdout.on('data', bytes => {
    statsBuffer += bytes;
    let end;
    while ((end = statsBuffer.indexOf('\n')) >= 0) {
      const line = statsBuffer.slice(0, end).replace(/\x1b\[[0-9;]*[A-Za-z]/g, '').trim(); statsBuffer = statsBuffer.slice(end + 1);
      try { fs.appendFileSync(path.join(dir, 'resources.jsonl'), JSON.stringify({ at: new Date().toISOString(), activeCase, sample: JSON.parse(line) }) + '\n'); } catch { /* docker display control lines */ }
    }
  });
  stats.stderr.on('data', bytes => fs.appendFileSync(path.join(dir, 'stats-errors.log'), bytes));
  const child = spawn(binary, [], { cwd: root, windowsHide: true, env: { ...process.env,
    LOGIN_STORAGE_SWEEP: sweep ? '1' : '0',
    DBPROXY_POSTGRES_URL: `postgres://postgres:compare_local_only@127.0.0.1:${pgPort}/postgres`,
    DBPROXY_CACHE_REDIS_URL: `redis://127.0.0.1:${redisPort}/0` }, stdio: ['ignore', 'pipe', 'pipe'] });
  report.status = 'running'; save(); console.log(dir);
  let buffer = '';
  child.stdout.on('data', bytes => {
    fs.appendFileSync(path.join(dir, 'stdout.log'), bytes); buffer += bytes;
    let end;
    while ((end = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, end).trim(); buffer = buffer.slice(end + 1);
      if (line.startsWith('CASE_START ')) activeCase = JSON.parse(line.slice(11));
      if (line.startsWith('CASE_RESULT ')) { report.cases.push(JSON.parse(line.slice(12))); activeCase = null; save(); console.log(line); }
    }
  });
  child.stderr.on('data', bytes => fs.appendFileSync(path.join(dir, 'stderr.log'), bytes));
  const timeout = setTimeout(() => child.kill(), 900000);
  const code = await new Promise((resolve, reject) => { child.once('error', reject); child.once('close', resolve); });
  clearTimeout(timeout);
  if (code !== 0 || report.cases.length !== expectedCases) throw Error(`benchmark incomplete: exit=${code}, cases=${report.cases.length}`);
  report.status = 'completed';
} catch (error) { report.status = 'failed'; report.error = String(error); process.exitCode = 1; }
finally {
  stats?.kill();
  report.cleanup = names.map(name => ({ name, status: spawnSync('docker', ['stop', '-t', '30', name], { encoding: 'utf8', timeout: 40000 }).status }));
  report.finishedAt = new Date().toISOString(); save(); console.log(`REPORT ${path.join(dir, 'report.json')}`);
}
