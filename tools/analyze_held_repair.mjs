import assert from 'node:assert/strict';

export function checkHeldRepair(r, text) {
  assert.equal(r.schema_version,3);
  assert.equal(r.baseline_scope,'held_stale_release');
  assert(text.endsWith('\n'),'unterminated repair baseline');
  const rows=text.trim().split(/\r?\n/).map(JSON.parse);
  const state=(held)=>({total:held,held,eligible:0,leased:0,dead:0});
  const revision=r.mode==='repair'?1:2;
  const expected=[];
  for(const phase of ['prepared','started']){
    for(let n=0;n<r.targets;n++)expected.push({kind:'cache_baseline',phase,n,revision:2,cache_revision:revision,matches:true,held:true});
    expected.push({kind:'baseline_ready',phase,targets:r.targets,scope:r.baseline_scope,queue:state(r.targets)});
  }
  assert.deepEqual(rows,expected,'held baseline missing, reordered or changed');
  assert.equal(r.queue_mismatches,0);
  assert.equal(r.remaining,0);
  assert.deepEqual(r.final_queue,state(r.not_injected));
  assert.equal(r.final_items.length,r.targets);
  r.final_items.forEach((x,n)=>assert.deepEqual(x,{n,released:n<r.admitted,queue_valid:true,cache_revision:n<r.admitted?2:revision,authority_revision:2}));
  r.observations.forEach((x,n)=>{
    const q=x.queue;
    for(const key of ['total','held','eligible','leased','dead'])assert(Number.isSafeInteger(q[key])&&q[key]>=0);
    assert.equal(q.total,q.held+q.eligible+q.leased+q.dead);
    assert.equal(q.held,r.targets-n-1);
    assert.equal(q.dead,0);
    assert.equal(x.pending,q.total);
    assert(Number.isSafeInteger(x.release_and_probe_us)&&x.release_and_probe_us>=0);
  });
}
