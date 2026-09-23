#!/usr/bin/env bash
# P05: cleanup off (B1) vs on (B2), alternating 3 rounds each, 2 min warmup + 5 min sample,
# all production background workers, once with no expired backlog and once with a backlog large
# enough to keep cleanup deleting through the whole sample window. Then tools/analyze_p05.mjs.
# Usage (inside the workbench): run_p05.sh <run-id-prefix> [rate=200] [large-backlog=300000]
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
PREFIX=$1; RATE=${2:-200}; BACKLOG=${3:-300000}
require_run_id "${PREFIX}_large"
HERE=$(dirname "${BASH_SOURCE[0]}")
status=0
for spec in "none 0" "large $BACKLOG"; do
    set -- $spec
    run="${PREFIX}_$1"
    echo "=== P05 $run: expired backlog $2 ==="
    "$HERE/run_receipt_probe.sh" --run-id "$run" --rate "$RATE" --seconds 300 --warmup 120 --rounds 3 \
        --cleanup both --sql-log-ms 20 --read pooled --client split4 --pacing std \
        --background all --fixture-expired "$2" --reconcile-budget 600 \
        >"$EVIDENCE_ROOT/$run.probe.log" 2>&1 || status=1
    grep -E "^PASS|failed|Failed|exceeded" "$EVIDENCE_ROOT/$run.probe.log" | head -10
done
node "$SRC/tools/analyze_p05.mjs" "$EVIDENCE_ROOT/${PREFIX}_none" "$EVIDENCE_ROOT/${PREFIX}_large" \
    >"$EVIDENCE_ROOT/${PREFIX}.p05.log" 2>&1 || status=1
cat "$EVIDENCE_ROOT/${PREFIX}.p05.log"
echo "P05_DONE status=$status"
exit "$status"
