#!/usr/bin/env bash
# Multi-record receipt retirement off (B1) vs on (B2) under fixed-rate load, alternating rounds.
# Writes are two-record CommitRecords, so the claim lock added for retirement is on the measured
# path in both modes. The fixture keeps retirement busy through every B2 round (about 500 ops/s).
# Ordinary receipt cleanup has no expired fixture, so it idles in both modes.
# Usage (inside the workbench): run_retire_ab.sh <run-id> [rounds=2] [warmup=60] [seconds=300]
#        [expired ops=250000] [fresh ops=100000]
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
RUN=$1; ROUNDS=${2:-2}; WARM=${3:-60}; SECONDS_=${4:-300}; EXPIRED=${5:-250000}; FRESH=${6:-100000}
require_run_id "${RUN}_1_off"
HERE=$(dirname "${BASH_SOURCE[0]}")
status=0
"$HERE/run_receipt_probe.sh" --run-id "$RUN" --rate 200 --seconds "$SECONDS_" --warmup "$WARM" --rounds "$ROUNDS" \
    --cleanup both --sql-log-ms 20 --read pooled --client split4 --pacing std --background all \
    --fixture-expired 0 --fixture-recent 0 --write-kind commit --retirement 1 \
    --fixture-ops-expired "$EXPIRED" --fixture-ops-recent "$FRESH" --reconcile-budget 600 \
    >"$EVIDENCE_ROOT/$RUN.probe.log" 2>&1 || status=1
grep -E "^PASS|failed|Failed|exceeded" "$EVIDENCE_ROOT/$RUN.probe.log" | head -10
node "$SRC/tools/analyze_p05.mjs" "$EVIDENCE_ROOT/$RUN" >"$EVIDENCE_ROOT/$RUN.ab.log" 2>&1 || status=1
cat "$EVIDENCE_ROOT/$RUN.ab.log"
echo "RETIRE_AB_DONE status=$status"
exit "$status"
