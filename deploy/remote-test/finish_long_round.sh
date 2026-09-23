#!/usr/bin/env bash
# Complete a long round whose wrapper stopped before its final steps (as long_p08_a did):
# take the end-of-run PostgreSQL snapshots now and move staging files into the evidence folder.
# The late capture time is recorded; it is not the same instant as the end of load.
# Usage (inside the workbench): finish_long_round.sh <run-id>
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
RUN_ID=$1
STAGING="$EVIDENCE_ROOT/.staging_$RUN_ID"
TARGET="$EVIDENCE_ROOT/$RUN_ID"
[[ -d $STAGING && -d $TARGET ]] || { echo "nothing to finish for $RUN_ID" >&2; exit 2; }
[[ -e $TARGET/pg-stats-after.json ]] && { echo "already finished" >&2; exit 2; }
pg_sql -At -c "SELECT json_build_object('captured_at', clock_timestamp(), 'late_capture', true,
    'wal', (SELECT row_to_json(w) FROM pg_stat_wal w),
    'checkpointer', (SELECT row_to_json(c) FROM pg_stat_checkpointer c),
    'bgwriter', (SELECT row_to_json(b) FROM pg_stat_bgwriter b),
    'io', (SELECT json_agg(i) FROM pg_stat_io i WHERE writes > 0 OR fsyncs > 0 OR reads > 0))" >"$STAGING/pg-stats-after.json"
pg_sql -At -c "SELECT json_build_object('captured_at', clock_timestamp(), 'late_capture', true,
    'segments', (SELECT count(*) FROM pg_ls_waldir()), 'bytes', (SELECT sum(size) FROM pg_ls_waldir()))" >"$STAGING/pg-wal-after.json"
echo "run_long_round.sh stopped in its cleanup (pkill returned 1 under errexit) after the load had passed. pg-stats-after.json and pg-wal-after.json were captured late at $(date -u +%FT%TZ) by finish_long_round.sh, and the staging files were moved here." >"$STAGING/wrapper-abort-note.txt"
mv "$STAGING"/* "$TARGET/"
rmdir "$STAGING"
echo "finished $RUN_ID"
