// Analyze only a completed P01 matrix; smoke evidence remains explicitly non-performance evidence.
// Usage: node tools/analyze_p01_matrix.mjs <fault_process_run/p01> [containers.jsonl]
import { readFileSync, writeFileSync, createReadStream } from 'node:fs';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
const [dir, containers] = process.argv.slice(2);
if (!dir) throw new Error('P01 evidence directory required');
const read = name => JSON.parse(readFileSync(join(dir, name), 'utf8'));
const result = read('result.json');
const phases = read('phases.json');
if (phases.length !== 9 * result.rounds || result.phases !== phases.length) throw new Error('Incomplete phase matrix');
const median = a => [...a].sort((x,y)=>x-y)[Math.floor(a.length/2)];
const sampled = new Map();
let rawRows = 0;
for await (const line of createInterface({input:createReadStream(join(dir,'requests.csv')),crlfDelay:Infinity})) {
  if (line.startsWith('round,')) continue;
  const cells = line.split(',');
  if (cells.length !== 9 || !['true','false'].includes(cells[4])) throw new Error('Malformed raw sample');
  rawRows++;
  if (cells[4] !== 'true') continue;
  const key = `r${cells[0]}-${cells[1]}-${cells[2]}-t${cells[3]}`;
  sampled.set(key,(sampled.get(key)??0)+1);
}
const lsn = value => {const [high,low]=value.split('/');return (BigInt(`0x${high}`)<<32n)+BigInt(`0x${low}`);};
const groups = new Map();
const connections = [];
const wal = [];
for (const phase of phases) {
  if (phase.tenants.length !== 2) throw new Error('Tenant omitted');
  for (let tenant=0;tenant<2;tenant++) {
    const stats=phase.tenants[tenant];
    if (sampled.get(`${phase.phase}-t${tenant}`)!==stats.count) throw new Error('Raw sample count mismatch');
    const key=`${phase.batch}/${phase.mode}/tenant${tenant}`;
    if (!groups.has(key)) groups.set(key,[]);
    groups.get(key).push({...stats,rps:stats.count/phase.sample_seconds,phase:phase.phase});
    const before=read(`${phase.phase}-t${tenant}-before.pg.json`);
    const after=read(`${phase.phase}-t${tenant}-after.pg.json`);
    connections.push(before.client_connections,after.client_connections);
    if (tenant===0) wal.push({phase:phase.phase,bytes:Number(lsn(after.wal_lsn)-lsn(before.wal_lsn))});
  }
}
const matrix=[...groups].map(([scenario,runs])=>({scenario,runs,median_p99_ms:median(runs.map(r=>r.p99_ms)),median_rps:median(runs.map(r=>r.rps))}));
const plans=[];
for (let tenant=0;tenant<2;tenant++) for (const batch of [1,30,64]) {
  const plan=read(`plan-${tenant}-${batch}.json`)[0];
  plans.push({tenant,batch,execution_ms:plan['Execution Time'],rows:plan.Plan['Actual Rows'],shared_hit_blocks:plan.Plan['Shared Hit Blocks'],shared_read_blocks:plan.Plan['Shared Read Blocks']});
}
const resources={};
let minimumHostAvailableKb=null;
let previous=null;
if (containers) for await (const line of createInterface({input:createReadStream(containers),crlfDelay:Infinity})) {
  if (!line) continue;
  const row=JSON.parse(line);
  minimumHostAvailableKb=Math.min(minimumHostAvailableKb??Infinity,row.host_mem_available_kb);
  for (const [name,stat] of Object.entries(row)) {
    if (!name.startsWith('dbproxy-')||typeof stat!=='object') continue;
    const aggregate=resources[name]??={peak_memory_bytes:0,peak_cpu_cores:0};
    aggregate.peak_memory_bytes=Math.max(aggregate.peak_memory_bytes,stat.memory);
    if (previous?.[name] && row.unix_ms>previous.unix_ms) aggregate.peak_cpu_cores=Math.max(aggregate.peak_cpu_cores,(stat.cpu_usec-previous[name].cpu_usec)/((row.unix_ms-previous.unix_ms)*1000));
  }
  previous=row;
}
const output={status:result.full_timing?'COMPLETED_FIXED_RATE_MATRIX':'SMOKE_ONLY',scope:result.scope,result,raw_rows:rawRows,matrix,plans,maximum_client_connections_per_database:Math.max(...connections),wal_per_phase:wal,resources,minimum_host_available_kb:minimumHostAvailableKb};
writeFileSync(join(dir,'analysis.json'),JSON.stringify(output,null,2)+'\n');
console.log(JSON.stringify({status:output.status,phases:phases.length,raw_rows:rawRows,matrix:matrix.map(({runs,...summary})=>summary),plans,maximum_client_connections_per_database:output.maximum_client_connections_per_database,resources,minimumHostAvailableKb},null,2));
