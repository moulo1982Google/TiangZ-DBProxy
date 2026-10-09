import {checkHeldRepair} from './analyze_held_repair.mjs';
import assert from 'node:assert/strict';
const state=n=>({total:n,held:n,eligible:0,leased:0,dead:0});
function fixture(mode,admitted){const r={schema_version:3,baseline_scope:'held_stale_release',mode,targets:2,admitted,not_injected:2-admitted,queue_mismatches:0,remaining:0,final_queue:state(2-admitted),final_items:[0,1].map(n=>({n,released:n<admitted,queue_valid:true,cache_revision:n<admitted||mode==='control'?2:1,authority_revision:2})),observations:Array.from({length:admitted},(_,n)=>({n,pending:1-n,queue:state(1-n),release_and_probe_us:5}))};const rows=[];for(const phase of ['prepared','started']){for(let n=0;n<2;n++)rows.push({kind:'cache_baseline',phase,n,revision:2,cache_revision:mode==='repair'?1:2,matches:true,held:true});rows.push({kind:'baseline_ready',phase,targets:2,scope:r.baseline_scope,queue:state(2)});}return {r,rows};}
const encode=rows=>rows.map(JSON.stringify).join('\n')+'\n';
for(const mode of ['control','repair'])for(const admitted of [0,1,2]){const {r,rows}=fixture(mode,admitted);checkHeldRepair(r,encode(rows));}
for(const mutate of [x=>x.rows.pop(),x=>x.rows[3].cache_revision=2,x=>x.rows[3].held=false,x=>x.r.final_queue.eligible=1,x=>x.r.final_items[1].queue_valid=false,x=>x.r.final_items[1].cache_revision=2,x=>x.r.observations[0].queue.held=0,x=>x.r.queue_mismatches=1]){const x=fixture('repair',1);mutate(x);assert.throws(()=>checkHeldRepair(x.r,encode(x.rows)));}
const x=fixture('repair',1);assert.throws(()=>checkHeldRepair(x.r,encode(x.rows).trimEnd()));
console.log('HELD_REPAIR_POSITIVE_NEGATIVE_CHECKED');
