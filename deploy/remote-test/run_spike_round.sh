#!/usr/bin/env bash
# One spike-localization round: host sampler + PostgreSQL statistics snapshots around a normal
# fixed-rate short test, then tools/analyze_spikes.mjs aligns every slow request with them.
# Usage: run_spike_round.sh <run-id> [extra run_receipt_probe.sh arguments...]
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
RUN_ID=$1; shift
require_run_id "$RUN_ID"
HERE=$(dirname "${BASH_SOURCE[0]}")
STAGING="$EVIDENCE_ROOT/.staging_$RUN_ID"
[[ -e $EVIDENCE_ROOT/$RUN_ID || -e $STAGING ]] && { echo "Use a new RunId" >&2; exit 2; }
mkdir -p "$STAGING"

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
        'segments', (SELECT count(*) FROM pg_ls_waldir()),
        'bytes', (SELECT sum(size) FROM pg_ls_waldir()),
        'settings', (SELECT json_object_agg(name, setting) FROM pg_settings
            WHERE name IN ('wal_init_zero','wal_recycle','wal_segment_size','max_wal_size','min_wal_size','checkpoint_timeout','wal_sync_method')))"
}

pg_stats >"$STAGING/pg-stats-before.json"
pg_wal_files >"$STAGING/pg-wal-before.json"
"$HERE/sample_host.sh" "$STAGING/host-samples.jsonl" &
SAMPLER=$!
# One long-lived session reports the current WAL file every 100 ms (one extra PG connection).
( while :; do
      echo "SELECT (extract(epoch FROM clock_timestamp())*1000000)::bigint, pg_current_wal_lsn(), pg_walfile_name(pg_current_wal_lsn());"
      sleep 0.1
  done | psql "$(pg_admin_url)" -At -F '|' -X -q >"$STAGING/wal-samples.txt" 2>"$STAGING/wal-samples.err" ) &
WAL_SAMPLER=$!
status=0
"$HERE/run_receipt_probe.sh" --run-id "$RUN_ID" --rate 200 --seconds 60 --warmup 10 --rounds 1 \
    --cleanup on --sql-log-ms 5 --read pooled --client split4 --pacing std "$@" || status=$?
kill "$SAMPLER" 2>/dev/null || true; wait "$SAMPLER" 2>/dev/null || true
pkill -P "$WAL_SAMPLER" 2>/dev/null || true; kill "$WAL_SAMPLER" 2>/dev/null || true; wait "$WAL_SAMPLER" 2>/dev/null || true
pg_stats >"$STAGING/pg-stats-after.json"
pg_wal_files >"$STAGING/pg-wal-after.json"
if [[ -d $EVIDENCE_ROOT/$RUN_ID ]]; then
    mv "$STAGING"/* "$EVIDENCE_ROOT/$RUN_ID/" && rmdir "$STAGING"
    node "$SRC/tools/analyze_spikes.mjs" "$EVIDENCE_ROOT/$RUN_ID" "$EVIDENCE_ROOT/$RUN_ID/1_on" \
        >"$EVIDENCE_ROOT/$RUN_ID/spikes.log" 2>&1 || status=1
    cat "$EVIDENCE_ROOT/$RUN_ID/spikes.log"
fi
exit "$status"
