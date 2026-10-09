import assert from 'node:assert/strict';

// Acceptance-only reconciliation contract. This does not issue or retry an ack.
export function reconcileAckTimeout(e) {
  const uint = n => assert(Number.isSafeInteger(n) && n >= 0);
  assert.equal(e.schema, 1);
  assert(/^p7at_[a-z0-9_]{1,14}$/.test(e.run_id));
  assert.equal(e.database, e.run_id);
  assert.equal(e.scope, 'leased_rows_ack_blocked_then_released');
  assert.equal(e.timeout_ms, 5000);
  assert.equal(e.retries, 0);
  assert.equal(e.extra_claims, 0);
  uint(e.blocker_pid); assert(e.blocker_pid > 0);
  for (const key of ['locked_us', 'blocked_us', 'after_timeout_us', 'release_us', 'final_us']) uint(e[key]);
  assert(e.locked_us <= e.blocked_us && e.blocked_us <= e.after_timeout_us);
  assert(e.after_timeout_us < e.release_us && e.release_us <= e.final_us);
  assert(e.release_us - e.after_timeout_us <= 2000000);
  assert(e.final_us - e.release_us <= 5000000);
  assert.equal(e.rollback_confirmed, true);
  assert.equal(e.final_active_peers, 0);
  assert.equal(e.total_rows, 2);
  assert.equal(e.leases.length, 2);
  const events = new Set(), pids = new Set();
  for (const [worker, l] of e.leases.entries()) {
    assert.equal(l.worker, worker);
    assert.equal(l.owner, `p7at_worker${worker}`);
    assert.equal(l.publisher, 'p7at_publisher');
    assert.equal(l.event_id, `${e.run_id}_event${worker}`);
    assert.equal(l.partition_key, `p7at_partition${worker}`);
    assert(!events.has(l.event_id)); events.add(l.event_id);
    uint(l.pid); assert(l.pid > 0 && l.pid !== e.blocker_pid && !pids.has(l.pid)); pids.add(l.pid);
    uint(l.token); assert(l.token > 0);
    for (const key of ['claim_end_us', 'ack_begin_us', 'ack_end_us']) uint(l[key]);
    assert(l.claim_end_us <= e.locked_us && e.locked_us <= l.ack_begin_us);
    assert(l.ack_begin_us <= e.blocked_us && l.ack_end_us - l.ack_begin_us >= 5000000);
    assert(e.blocked_us - l.ack_begin_us <= 2000000);
    assert(l.ack_end_us <= e.after_timeout_us);
    assert.equal(l.ack_outcome, 'unknown');
    // Each proof is from pg_stat_activity/current_database and pg_blocking_pids.
    for (const proof of [l.blocked, l.after_timeout_blocked]) {
      assert.deepEqual(proof, {database:e.run_id, pid:l.pid, application:l.owner,
        state:'active', wait_event_type:'Lock', blocking_pids:[e.blocker_pid]});
    }
    const before = {event_id:l.event_id, publisher:l.publisher, partition_key:l.partition_key,
      token:l.token, owner:l.owner, published:false, lease_present:true, lease_valid:true, attempt_count:0};
    assert.deepEqual(l.claimed_row, before);
    // A row lock allows an MVCC read; require unchanged data while ack is unknown and still blocked.
    assert.deepEqual(l.after_timeout_row, before);
    assert.deepEqual(l.final_row, {...before, owner:null, published:true, lease_present:false, lease_valid:false});
  }
  return {status:'ACK_UNKNOWN_RECONCILED_AS_PUBLISHED', run_id:e.run_id,
    client_unknown:2, final_published:2, retries:0,
    scope:'Row-lock delayed ack continued after client timeout; not a lost response after an already completed commit or a full workflow deadline proof.'};
}
