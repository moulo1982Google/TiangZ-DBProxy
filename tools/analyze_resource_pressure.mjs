// Read cumulative cgroup counters without attributing lifetime totals to one run.
import fs from 'node:fs';
import assert from 'node:assert/strict';
const rows=fs.readFileSync(process.argv[2],'utf8').trim().split(/\r?\n/).map(JSON.parse);
assert(rows.length>=2,'need at least two resource samples');
for(let i=1;i<rows.length;i++) assert(rows[i].unix_ms>rows[i-1].unix_ms,'timestamps must increase');
const names=[...new Set(rows.flatMap(r=>Object.keys(r).filter(k=>k.startsWith('dbproxy-'))))];
const result={scope:'observed intervals only; no capacity or acceptance verdict',samples:rows.length,containers:{}};
for(const name of names){
  const samples=rows.filter(r=>r[name]?.cgroup);
  const intervals=[];
  for(let i=1;i<samples.length;i++){
    const prev=samples[i-1],next=samples[i],a=prev[name],b=next[name];
    if(a.cgroup!==b.cgroup) { intervals.push({from:prev.unix_ms,to:next.unix_ms,unavailable:'cgroup changed'});continue; }
    const delta=(group,key)=>{
      const x=a[group]?.[key],y=b[group]?.[key];
      return Number.isFinite(x)&&Number.isFinite(y)&&y>=x?y-x:null;
    };
    const pressure=group=>{
      const values={};
      for(const kind of ['some','full']){
        const x=a[group]?.[kind]?.total,y=b[group]?.[kind]?.total;
        values[kind]=Number.isFinite(x)&&Number.isFinite(y)&&y>=x?100*(y-x)/((next.unix_ms-prev.unix_ms)*1000):null;
      }
      return values;
    };
    intervals.push({from:prev.unix_ms,to:next.unix_ms,
      events:Object.fromEntries(['low','high','max','oom','oom_kill'].map(k=>[k,delta('memory_events',k)])),
      reclaim:Object.fromEntries(['pgscan','pgsteal','workingset_refault_file'].map(k=>[k,delta('memory_stat',k)])),
      memory_stall_percent:pressure('memory_pressure'),io_stall_percent:pressure('io_pressure'),cpu_stall_percent:pressure('cpu_pressure')});
  }
  result.containers[name]={samples:samples.length,missing_samples:rows.length-samples.length,intervals};
}
console.log(JSON.stringify(result,null,2));
