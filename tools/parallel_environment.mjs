import assert from 'node:assert/strict';
const specifications={
  postgres:{memory:8589934592,swap:8589934592,nano_cpus:4000000000,cpuset:'14-17,42-45',mounts:[['/data/dbproxy-test/pgdata','/var/lib/postgresql'],['/data/dbproxy-test/pglog','/pglog']]},
  redis:{memory:2147483648,swap:4294967296,nano_cpus:0,cpuset:'18,46',mounts:[['/data/dbproxy-test/redis','/data']]},
  cache:{memory:2147483648,swap:4294967296,nano_cpus:0,cpuset:'19,47',mounts:[['/data/dbproxy-test/cache','/data']]},
};
export function boundary(raw) {
  assert(raw.endsWith('\n'),'incomplete boundary');
  const rows=raw.trimEnd().split('\n').map(v=>JSON.parse(v));assert.equal(rows.length,5);
  assert.equal(rows[0].schema,1);assert.equal(rows[0].kind,'boundary');assert.equal(rows[0].docker_root,'/sas/docker');
  assert.equal(rows[4].kind,'boundary_end');
  const begin=Date.parse(rows[0].at),end=Date.parse(rows[4].at);assert(Number.isFinite(begin)&&end>=begin&&end-begin<=30000,'invalid boundary interval');
  const services={};
  for(const [index,[name,spec]]of Object.entries(specifications).entries()) {
    const row=rows[index+1];assert.equal(row.kind,'service');assert.equal(row.name,`/dbproxy-test-${name}`);
    assert(/^[a-f0-9]{64}$/.test(row.id));assert(/^sha256:[a-f0-9]{64}$/.test(row.image));
    assert.equal(row.running,true);assert.equal(row.oom,false);assert.equal(row.network,'dbproxy-test');
    assert(Number.isFinite(Date.parse(row.started_at))&&Date.parse(row.started_at)<=begin);
    for(const key of ['memory','swap','nano_cpus','cpuset'])assert.equal(row[key],spec[key]);
    const actual=row.mounts.map(m=>{assert.equal(m.Type,'bind');assert.equal(m.RW,true);return [m.Source,m.Destination];}).sort();
    assert.deepEqual(actual,[...spec.mounts].sort(),'data mount mismatch');
    services[name]={id:row.id,image:row.image,started_at:row.started_at,memory:row.memory,swap:row.swap,nano_cpus:row.nano_cpus,cpuset:row.cpuset,mounts:actual};
  }
  return {begin,end,services};
}
export function validateEnvironment(before,after,container) {
  const a=boundary(before),b=boundary(after);
  assert.deepEqual(a.services,b.services,'base service changed during run');
  assert(a.end<=Date.parse(container.State.StartedAt)&&b.begin>=Date.parse(container.State.FinishedAt),'boundary does not surround run');
  const expected=[['/data/dbproxy-test/evidence','/evidence',true],['/data/dbproxy-test/pglog','/pglog',false],['/data/dbproxy-test/src/crates/dbproxy-storage/tests','/src/crates/dbproxy-storage/tests',false],['/data/dbproxy-test/src/deploy/remote-test','/src/deploy/remote-test',false]].sort();
  assert.deepEqual(container.Mounts.map(m=>{assert.equal(m.Type,'bind');return [m.Source,m.Destination,m.RW];}).sort(),expected,'workbench mounts mismatch');
  return a.services;
}
