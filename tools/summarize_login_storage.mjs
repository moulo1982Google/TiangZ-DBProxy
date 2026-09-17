import fs from 'node:fs';
import path from 'node:path';
const dir = process.argv[2];
const report = JSON.parse(fs.readFileSync(path.join(dir, 'report.json'), 'utf8'));
const samples = fs.readFileSync(path.join(dir, 'resources.jsonl'), 'utf8').trim().split('\n').map(JSON.parse);
console.log(JSON.stringify({status: report.status, binarySha256:report.binarySha256, cleanup: report.cleanup}));
for (const domains of [5, 30]) for (const concurrency of [...new Set(report.cases.map(x => x.concurrency))].sort((a,b)=>a-b)) for (const mode of ['cache', 'postgres']) {
  const rows = report.cases.filter(x => x.domains === domains && x.concurrency === concurrency && x.mode === mode);
  const selected = samples.filter(x => x.activeCase?.domains === domains && x.activeCase?.concurrency === concurrency && x.activeCase?.mode === mode);
  console.log(JSON.stringify({domains, concurrency, mode, rps:rows.map(x=>Math.round(x.loadsPerSecond)), p95Ms:rows.map(x=>x.p95Ms),
    cpu: ['-pg','-redis'].map(suffix => {
      const cpu = selected.filter(x=>x.sample.Name.endsWith(suffix)).map(x=>parseFloat(x.sample.CPUPerc));
      return {suffix, samples:cpu.length, meanPercent:cpu.reduce((a,b)=>a+b,0)/cpu.length, maxPercent:Math.max(...cpu)};
    })}));
}
