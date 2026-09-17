import { spawn, spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
const root=path.resolve(import.meta.dirname,'..');
const dir=fs.mkdtempSync(path.join(root,'target/authority-read-'));
const id=path.basename(dir).toLowerCase();
const names=[id+'-pg',id+'-redis'];
const report={dir,names,status:'starting',startedAt:new Date().toISOString(),rounds:[]};
const run=(cmd,args)=>{
  const r=spawnSync(cmd,args,{cwd:root,encoding:'utf8',timeout:60000,windowsHide:true});
  if(r.error||r.status!==0)throw Error(`${cmd}: ${r.error??r.stderr}`);return r.stdout.trim();
};
console.log(dir);
try {
  report.commit=run('git',['rev-parse','HEAD']);
  report.gitStatus=run('git',['status','--short']);
  fs.writeFileSync(path.join(dir,'changes.patch'),run('git',['diff','HEAD']));
  fs.copyFileSync(import.meta.filename,path.join(dir,'controller.mjs'));
  fs.copyFileSync(path.join(root,'crates/dbproxy-server/tests/authoritative_reads.rs'),path.join(dir,'test.rs'));
  const build=run('cargo',['test','-p','tiangz-dbproxy-server','--test','authoritative_reads','--no-run','--message-format=json']);
  const artifact=build.split('\n').filter(l=>l.startsWith('{')).map(JSON.parse).find(x=>x.reason==='compiler-artifact'&&x.target.name==='authoritative_reads'&&x.executable);
  if(!artifact)throw Error('test binary missing');
  report.binarySha256=createHash('sha256').update(fs.readFileSync(artifact.executable)).digest('hex');
  run('docker',['run','-d','--name',names[0],'--cpus','2','--memory','1g','--tmpfs','/var/lib/postgresql:rw,size=512m','-e','POSTGRES_PASSWORD=authority_test_only','-p','127.0.0.1::5432','postgres:18.4-bookworm']);
  run('docker',['run','-d','--name',names[1],'-p','127.0.0.1::6379','redis:8.8.1-trixie','redis-server','--save','','--appendonly','no']);
  const pgPort=run('docker',['port',names[0],'5432/tcp']).split(':').at(-1);
  const redisPort=run('docker',['port',names[1],'6379/tcp']).split(':').at(-1);
  let ready=false;
  for(let i=0;i<60;i++){
    if(spawnSync('docker',['exec',names[0],'pg_isready','-h','127.0.0.1','-U','postgres'],{timeout:5000,windowsHide:true}).status===0){ready=true;break;}
    await new Promise(r=>setTimeout(r,1000));
  }
  if(!ready)throw Error('PG readiness timeout');
  for(let round=1;round<=3;round++){
    const child=spawn(artifact.executable,['--ignored','--nocapture','--test-threads=1'],{cwd:root,windowsHide:true,env:{...process.env,
      DBPROXY_POSTGRES_URL:`postgres://postgres:authority_test_only@127.0.0.1:${pgPort}/postgres`,DBPROXY_REDIS_URL:`redis://127.0.0.1:${redisPort}/0`},stdio:['ignore','pipe','pipe']});
    const log=path.join(dir,`round-${round}.log`);
    for(const pipe of [child.stdout,child.stderr])pipe.on('data',bytes=>fs.appendFileSync(log,bytes));
    const timer=setTimeout(()=>child.kill(),55000);
    const code=await new Promise((resolve,reject)=>{child.once('error',reject);child.once('close',resolve);});clearTimeout(timer);
    report.rounds.push({round,code});console.log(JSON.stringify(report.rounds.at(-1)));
    if(code!==0)throw Error(`round ${round} failed: ${log}`);
  }
  const storageBuild=run('cargo',['test','-p','tiangz-dbproxy-storage','--test','postgres_redis','--no-run','--message-format=json']);
  const storageArtifact=storageBuild.split('\n').filter(l=>l.startsWith('{')).map(JSON.parse).find(x=>x.reason==='compiler-artifact'&&x.target.name==='postgres_redis'&&x.executable);
  if(!storageArtifact)throw Error('storage test binary missing');
  report.storageBinarySha256=createHash('sha256').update(fs.readFileSync(storageArtifact.executable)).digest('hex');
  report.storageTests=[];
  for(const name of ['acknowledged_write_timeout_must_not_expose_old_cache','cache_write_timeout_preserves_committed_batch_and_durable_repair','postgres_and_redis_preserve_snapshot_semantics','distributed_fallback_lock_rechecks_cache_before_postgres','cache_lifecycle_renews_serves_stale_and_negative_caches']) {
    const r=spawnSync(storageArtifact.executable,[name,'--exact','--ignored','--nocapture','--test-threads=1'],{cwd:root,encoding:'utf8',timeout:55000,windowsHide:true,env:{...process.env,
      DBPROXY_POSTGRES_URL:`postgres://postgres:authority_test_only@127.0.0.1:${pgPort}/postgres`,DBPROXY_REDIS_URL:`redis://127.0.0.1:${redisPort}/0`,DBPROXY_CACHE_REDIS_URL:`redis://127.0.0.1:${redisPort}/0`}});
    fs.writeFileSync(path.join(dir,`${name}.log`),(r.stdout??'')+(r.stderr??''));
    report.storageTests.push({name,code:r.status,error:r.error?.message});
    if(r.error||r.status!==0)throw Error(`storage regression failed: ${name}`);
  }
  report.status='passed';
}catch(error){report.status='failed';report.error=String(error);process.exitCode=1;}
finally{
  report.cleanup=names.map(name=>({name,status:spawnSync('docker',['stop','-t','15',name],{timeout:25000,windowsHide:true}).status}));
  report.finishedAt=new Date().toISOString();fs.writeFileSync(path.join(dir,'report.json'),JSON.stringify(report,null,2));console.log(JSON.stringify(report));
}
