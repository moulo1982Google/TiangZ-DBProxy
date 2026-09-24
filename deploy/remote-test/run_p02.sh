#!/usr/bin/env bash
# P02: write cost of the indexes added by migrations 013-015. Same fixed-rate load on fresh
# databases, with the indexes kept or dropped right after the host passed its startup check;
# 1 KiB and 16 KiB payloads; two repetitions in alternating order. Receipt cleanup is off so it
# does not confound the measurement (that is P05). All production background workers run.
# Usage (inside the workbench): run_p02.sh <run-id-prefix> [sample seconds=300] [warmup=120]
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
PREFIX=$1; SECONDS_=${2:-300}; WARM=${3:-120}
require_run_id "${PREFIX}_16k_wo_2"
HERE=$(dirname "${BASH_SOURCE[0]}")
status=0
runs=()
for rep in 1 2; do
    for payload in 1024 16384; do
        size=$([[ $payload == 1024 ]] && echo 1k || echo 16k)
        if ((rep == 1)); then order=(with wo); else order=(wo with); fi
        for mode in "${order[@]}"; do
            run="${PREFIX}_${size}_${mode}_${rep}"
            drop=$([[ $mode == wo ]] && echo 1 || echo 0)
            echo "=== P02 $run payload=$payload drop_new_indexes=$drop ==="
            CLEANUP=off WARMUP=$WARM BACKGROUND=all PG_SAMPLE_SECONDS=30 \
                "$HERE/run_long_round.sh" "$run" "$SECONDS_" --payload "$payload" --fixture-expired 0 \
                --fixture-recent 100000 --drop-new-indexes "$drop" --reconcile-budget 600 \
                >"$EVIDENCE_ROOT/$run.round.log" 2>&1 || status=1
            grep -E "^PASS|LONG_ROUND_DONE|failed|Failed|exceeded" "$EVIDENCE_ROOT/$run.round.log" | head -5
            runs+=("$EVIDENCE_ROOT/$run")
        done
    done
done
node "$SRC/tools/analyze_p02.mjs" "$EVIDENCE_ROOT/${PREFIX}.containers.jsonl" "${runs[@]}" \
    >"$EVIDENCE_ROOT/${PREFIX}.p02.log" 2>&1 || status=1
cat "$EVIDENCE_ROOT/${PREFIX}.p02.log"
echo "P02_DONE status=$status"
exit "$status"
