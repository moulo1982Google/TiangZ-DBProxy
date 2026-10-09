#!/usr/bin/env bash
# Paired B1/B2 at fractions of a verified reference rate, not a saturation claim.
set -euo pipefail
RUN_ID=${1:?new run id required}
[[ $RUN_ID =~ ^[a-z][a-z0-9_]{0,15}$ ]] || exit 2
OUT=${EVIDENCE_ROOT:-/evidence}/mixed_pairs_$RUN_ID
[[ ! -e $OUT ]] || { echo 'Use a new RunId'; exit 2; }
mkdir "$OUT"
read -r -a rates <<< "${MIX_PAIR_RATES:-64 96}"
[[ ${#rates[@]} == 2 ]] || exit 2
for rate in "${rates[@]}"; do [[ $rate =~ ^[0-9]+$ ]] && ((rate >= 1 && rate <= 128)) || exit 2; done
export MIX_WARMUP=${MIX_WARMUP:-120} MIX_SAMPLE=${MIX_SAMPLE:-300} MIX_CONCURRENCY=${MIX_CONCURRENCY:-8}
export FAULT_TESTS=mixed_workload::paced::fixed_rate_six_operations
for index in 0 1; do
    export MIX_RATE=${rates[$index]}
    for round in 0 1 2; do
        order=(B1 B2)
        if [[ $round == 1 ]]; then order=(B2 B1); fi
        for baseline in "${order[@]}"; do
            export MIX_BASELINE=$baseline
            child=${RUN_ID}_s${index}r${round}_${baseline,,}
            printf '{"run":"%s","rate":%s,"round":%s,"baseline":"%s"}\n' "$child" "$MIX_RATE" "$round" "$baseline" >>"$OUT/order.jsonl"
            bash /src/deploy/remote-test/run_fault_process.sh "$child"
        done
    done
done
echo "MIXED_PAIRS_COMPLETED run=$RUN_ID rounds=12"
