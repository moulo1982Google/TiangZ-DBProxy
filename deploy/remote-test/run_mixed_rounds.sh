#!/usr/bin/env bash
# Container-side fixed-rate six-operation baseline, serial rounds and fresh databases.
set -euo pipefail
RUN_ID=${1:?new run id required}
[[ $RUN_ID =~ ^[a-z][a-z0-9_]{0,15}$ ]] || exit 2
export MIX_RATE=${MIX_RATE:-20} MIX_CONCURRENCY=${MIX_CONCURRENCY:-8}
export MIX_WARMUP=${MIX_WARMUP:-120} MIX_SAMPLE=${MIX_SAMPLE:-300}
export FAULT_TESTS=mixed_workload::paced::fixed_rate_six_operations
for round in 0 1 2; do
    bash /src/deploy/remote-test/run_fault_process.sh "${RUN_ID}_r$round"
done
echo "MIXED_ROUNDS_COMPLETED run=$RUN_ID rounds=3"
