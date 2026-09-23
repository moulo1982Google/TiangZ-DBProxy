#!/usr/bin/env bash
# Long acceptance round (P08): every sampler of run_spike_round.sh plus PostgreSQL state every
# PG_SAMPLE_SECONDS (default 30). One round, cleanup on, std pacing, read pool, split4.
# Usage: run_long_round.sh <run-id> <seconds> [extra run_receipt_probe.sh arguments...]
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
RUN_ID=$1; SECONDS_=$2; shift 2
require_run_id "$RUN_ID"
HERE=$(dirname "${BASH_SOURCE[0]}")
STAGING="$EVIDENCE_ROOT/.staging_$RUN_ID"
[[ -e $EVIDENCE_ROOT/$RUN_ID || -e $STAGING ]] && { echo "Use a new RunId" >&2; exit 2; }
mkdir -p "$STAGING"
ROUND_DB="${RUN_ID}_1_on"
PG_SAMPLE_SECONDS=${PG_SAMPLE_SECONDS:-30}
RETENTION_HOURS=${RETENTION_HOURS:-24}

pg_stats() {
    pg_sql -At -c "SELECT json_build_object(
        'captured_at', clock_timestamp(),
        'wal', (SELECT row_to_json(w) FROM pg_stat_wal w),
        'checkpointer', (SELECT row_to_json(c) FROM pg_stat_checkpointer c),
        'bgwriter', (SELECT row_to_json(b) FROM pg_stat_bgwriter b),
        'io', (SELECT json_agg(i) FROM pg_stat_io i WHERE writes > 0 OR fsyncs > 0 OR reads > 0))"
}
pg_wal_files() {
    pg_sql -At -c "SELECT json_build_object('captured_at', clock_timestamp(),
        'segments', (SELECT count(*) FROM pg_ls_waldir()), 'bytes', (SELECT sum(size) FROM pg_ls_waldir()),
        'settings', (SELECT json_object_agg(name, setting) FROM pg_settings WHERE name IN
          ('wal_init_zero','wal_recycle','wal_segment_size','max_wal_size','min_wal_size','checkpoint_timeout',
           'wal_sync_method','shared_buffers','max_connections','track_io_timing','track_wal_io_timing',
           'autovacuum_naptime','log_autovacuum_min_duration')))"
}

pg_stats >"$STAGING/pg-stats-before.json"
pg_wal_files >"$STAGING/pg-wal-before.json"
"$HERE/sample_host.sh" "$STAGING/host-samples.jsonl" &
HOST_SAMPLER=$!
( while :; do
      echo "SELECT (extract(epoch FROM clock_timestamp())*1000000)::bigint, pg_current_wal_lsn(), pg_walfile_name(pg_current_wal_lsn());"
      sleep 0.1
  done | psql "$(pg_admin_url)" -At -F '|' -X -q >"$STAGING/wal-samples.txt" 2>"$STAGING/wal-samples.err" ) &
WAL_SAMPLER=$!
# The round database appears after seeding; failures before that are recorded, not fatal.
( while :; do
      if psql "${PG_BASE_URL}/${ROUND_DB}" -At -X -q -v retention_hours="$RETENTION_HOURS" -f "$HERE/pg_periodic.sql" >>"$STAGING/pg-periodic.jsonl" 2>>"$STAGING/pg-periodic.err"; then :; fi
      sleep "$PG_SAMPLE_SECONDS"
  done ) &
PG_SAMPLER=$!
status=0
"$HERE/run_receipt_probe.sh" --run-id "$RUN_ID" --rate 200 --seconds "$SECONDS_" --warmup 10 --rounds 1 \
    --cleanup on --sql-log-ms 5 --read pooled --client split4 --pacing std --retention-hours "$RETENTION_HOURS" "$@" || status=$?
# common.sh enables errexit; a sampler without children makes pkill return 1, which once aborted
# this cleanup before the final statistics were taken (long_p08_a). Never let cleanup fail.
for p in "$HOST_SAMPLER" "$WAL_SAMPLER" "$PG_SAMPLER"; do pkill -P "$p" 2>/dev/null || true; kill "$p" 2>/dev/null || true; done
wait 2>/dev/null || true
pg_stats >"$STAGING/pg-stats-after.json"
pg_wal_files >"$STAGING/pg-wal-after.json"
if [[ -d $EVIDENCE_ROOT/$RUN_ID ]]; then
    mv "$STAGING"/* "$EVIDENCE_ROOT/$RUN_ID/" && rmdir "$STAGING"
fi
echo "LONG_ROUND_DONE run=$RUN_ID status=$status"
exit "$status"
