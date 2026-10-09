#!/usr/bin/env bash
# Same production service and bounded application rate; only supplemental shared-connection Outbox stats differs.
set -euo pipefail
RUN_ID=${1:?new run id required}
[[ $RUN_ID =~ ^[a-z][a-z0-9_]{0,14}$ ]] || exit 2
OUT=${EVIDENCE_ROOT:-/evidence}/outbox_pairs_$RUN_ID
[[ ! -e $OUT ]] || { echo 'Use a new RunId'; exit 2; }
mkdir "$OUT"
export MIX_OUTBOX_AUDIT=1 MIX_REPAIR=none
export MIX_BASELINE=B2 MIX_RATE=20 MIX_CONCURRENCY=8
export MIX_WARMUP=${MIX_WARMUP:-120} MIX_SAMPLE=${MIX_SAMPLE:-300}
export FAULT_TESTS=mixed_workload::paced::fixed_rate_six_operations
for round in 0 1 2; do
    order=(off on)
    if [[ $round == 1 ]]; then order=(on off); fi
    for mode in "${order[@]}"; do
        export MIX_OUTBOX_STATS=$mode
        child=${RUN_ID}_r${round}_${mode}
        printf '{"run":"%s","round":%s,"mode":"%s"}\n' "$child" "$round" "$mode" >>"$OUT/order.jsonl"
        bash /src/deploy/remote-test/run_fault_process.sh "$child"
    done
done
echo "OUTBOX_PAIRS_COMPLETED run=$RUN_ID rounds=6"
