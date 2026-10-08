import assert from 'node:assert/strict';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
export const modes=['ready','spread-ready','none','all-blocked','leased','backoff','leased-heads','backoff-heads','dead-heads'];
// Offline arithmetic only. Never creates databases or changes execution preflight.
export function plan(rowsByMode) {
  assert.deepEqual(Object.keys(rowsByMode).sort(),[...modes].sort());
  const distributions=modes.map(mode=>{
    const rows=rowsByMode[mode],mixed=['leased','backoff','leased-heads','backoff-heads','dead-heads'].includes(mode);
    assert(Number.isSafeInteger(rows)&&rows>=1000&&rows<=40000&&rows%40===0);
    const budgetReserve=rows/(mixed?20:2);
    assert(budgetReserve>=1680,'existing 50% reserve budget rejected');
    const empty=['none','all-blocked'].includes(mode),spread=mode==='spread-ready';
    const initial=empty?0:budgetReserve,consumed=empty?0:840,remaining=initial-consumed;
    return {mode,rows_per_database:rows,databases:12,retained_rows:rows*12,
      per_publisher:{total_rows:rows/2,initial_ready_reserve:initial,after_warmup_ready:initial-(empty?0:240),final_ready_reserve:remaining,
        consumed,consumed_fraction:initial?consumed/initial:null,initial_ready_fraction:initial/(rows/2),final_ready_fraction:remaining/(rows/2),
        ready_partitions:empty?0:spread?initial:2,
        partition_model:empty?'blocked: no eligible partition':spread?'840 independent keys consumed completely; other keys untouched':'two FIFO partitions, one claim per partition per publisher wave if both claims succeed',
        fifo_rows_per_ready_partition:empty||spread?null:{initial:initial/2,after_warmup:(initial-240)/2,final:remaining/2}},
      expected_claims_per_run:1680,expected_nonempty_per_run:empty?0:1680,
      drift_status:empty?'blocked fixture unchanged if deadlines remain future':'finite depletion; not stationary',
      execution_allowed:false};
  });
  return {schema:1,status:'OFFLINE_BUDGET_ONLY',execution_allowed:false,warmup_seconds:120,sample_seconds:300,
    claims_per_second:4,phases:2,arms:2,rounds:3,distributions,
    totals:{databases:108,retained_rows:distributions.reduce((n,d)=>n+d.retained_rows,0),
      scheduled_seconds:45360,claim_calls:181440,stats_sql_calls:22680},
    rejection_reasons:['formal execution preflight remains disabled','PG pressure stop boundary remains active',
      'no accepted distribution drift criterion or resource approval','historical environment is not current environment'],
    limits:'Assumes every ready claim succeeds and blocked deadlines stay future; counts are not database evidence, storage bytes, steady state, or capacity.'};
}
export const minimumRows=Object.fromEntries(modes.map(m=>[m,['ready','spread-ready','none','all-blocked'].includes(m)?3360:33600]));
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url))console.log(JSON.stringify(plan(minimumRows),null,2));
