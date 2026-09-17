import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

// 只给当前配置与负载的候选点，不给数据库通用“最佳连接数”。
export function capacitySummary(report) {
  if (report.status !== 'completed' || !report.profile?.sweep || report.cases.length !== 72) {
    throw Error('需要完整72场景阶梯报告');
  }
  const median = values => [...values].sort((a,b)=>a-b)[1];
  const result = [];
  for (const domains of [5,30]) for (const mode of ['postgres','cache']) {
    const rows = [1,2,4,8,12,16].map(concurrency => {
      const cases = report.cases.filter(x=>x.domains===domains && x.mode===mode && x.concurrency===concurrency);
      if (cases.length !== 3 || new Set(cases.map(x=>x.round)).size !== 3 || cases.some(x=>
        !Number.isFinite(x.loadsPerSecond) || x.loadsPerSecond <= 0 || !Number.isFinite(x.p95Ms) || x.p95Ms < 0 ||
        (mode === 'cache' && (x.postgresFallbacks !== 0 || x.cacheStaleHits !== 0 || x.cacheHits !== x.operations * domains)))) {
        throw Error('场景缺失、重复或缓存回源，不能混算');
      }
      return { concurrency, medianLoadsPerSecond: median(cases.map(x=>x.loadsPerSecond)),
        p95MsRange: [Math.min(...cases.map(x=>x.p95Ms)),Math.max(...cases.map(x=>x.p95Ms))] };
    });
    const peak = Math.max(...rows.map(x=>x.medianLoadsPerSecond));
    const candidate = rows.find(x=>x.medianLoadsPerSecond >= peak * .95);
    result.push({domains, mode, rule:'达到已测峰值中位吞吐95%的最小并发；仅候选，未设业务SLA，不是生产推荐',
      candidate, peakAtBoundary: rows.at(-1).medianLoadsPerSecond === peak, rows});
  }
  return result;
}
if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  console.log(JSON.stringify(capacitySummary(JSON.parse(fs.readFileSync(path.join(process.argv[2], 'report.json'),'utf8'))),null,2));
}
