#!/usr/bin/env bash
# Three-round mixed baseline; no capacity claim.
set -euo pipefail
RUN_ID=${1:?new run id required}
[[ $RUN_ID =~ ^[a-z][a-z0-9_]{0,18}$ ]] || exit 2
DATA=${DBPROXY_TEST_DATA:-/data/dbproxy-test}
IMAGE=${DBPROXY_WORKBENCH_IMAGE:-dbproxy-workbench:f06-20260929}
WB=dbproxy-mixed-$RUN_ID
case ${MIX_SUITE:-baseline} in
    baseline) runner=run_mixed_rounds.sh ;;
    paired) runner=run_mixed_pairs.sh ;;
    *) echo 'MIX_SUITE must be baseline or paired'; exit 2 ;;
esac
OUT=$DATA/evidence/fault_process_${RUN_ID}_r0
[[ ! -e $OUT ]] || { echo 'Use a new RunId'; exit 2; }
if docker inspect "$WB" >/dev/null 2>&1; then echo 'Container exists'; exit 2; fi
[[ $(df -Pk "$DATA" | awk 'NR==2 {print $4}') -gt 10485760 ]] || { echo 'Require 10 GiB free'; exit 2; }
[[ $(awk '/MemAvailable:/ {print $2}' /proc/meminfo) -gt 12582912 ]] || { echo 'Require 12 GiB available'; exit 2; }
sampler=''
finish() {
    [[ -z $sampler ]] || kill "$sampler" 2>/dev/null || true
    docker logs "$WB" >"$DATA/evidence/$RUN_ID.workbench.log" 2>&1 || true
    docker inspect "$WB" >"$DATA/evidence/$RUN_ID.container.json" 2>/dev/null || true
    docker stop -t 10 "$WB" >/dev/null 2>&1 || true
}
trap finish EXIT
docker run -d --name "$WB" --network dbproxy-test --cpuset-cpus 20-27,48-55 --cpus 4 \
    --memory 16g --memory-swap 16g -e CARGO_BUILD_JOBS=4 \
    -e MIX_BASELINE="${MIX_BASELINE:-B2}" \
    -e MIX_PAIR_RATES="${MIX_PAIR_RATES:-64 96}" \
    -e MIX_RATE="${MIX_RATE:-20}" -e MIX_CONCURRENCY="${MIX_CONCURRENCY:-8}" \
    -e MIX_WARMUP="${MIX_WARMUP:-120}" \
    -e MIX_SAMPLE="${MIX_SAMPLE:-300}" \
    -v "$DATA/evidence:/evidence" -v "$DATA/pglog:/pglog:ro" \
    -v "$DATA/src/crates/dbproxy-server/tests:/src/crates/dbproxy-server/tests:ro" \
    -v "$DATA/src/crates/dbproxy-server/examples:/src/crates/dbproxy-server/examples:ro" \
    -v "$DATA/src/crates/dbproxy-server/src:/src/crates/dbproxy-server/src:ro" \
    -v "$DATA/src/deploy/remote-test:/src/deploy/remote-test:ro" \
    "$IMAGE" bash "/src/deploy/remote-test/$runner" "$RUN_ID" >/dev/null
docker exec "$WB" sha256sum /src/crates/dbproxy-server/tests/support/mixed_paced.rs \
    /src/crates/dbproxy-server/tests/support/mixed_workload.rs \
    /src/crates/dbproxy-server/src/server_process.rs \
    /src/crates/dbproxy-server/src/main.rs \
    /src/crates/dbproxy-server/examples/acceptance_baseline.rs >"$DATA/evidence/$RUN_ID.sources.sha256"
docker image inspect "$IMAGE" >"$DATA/evidence/$RUN_ID.image.json"
bash "$DATA/src/deploy/remote-test/sample_containers.sh" "$DATA/evidence/$RUN_ID.containers.jsonl" "$WB" 10 >/dev/null 2>&1 &
sampler=$!
low=0
while [[ $(docker inspect -f '{{.State.Running}}' "$WB") == true ]]; do
    available=$(awk '/MemAvailable:/ {print $2}' /proc/meminfo)
    if [[ $available -lt 8388608 ]]; then low=$((low+1)); else low=0; fi
    if [[ $low -ge 3 ]]; then echo 'Stopped own workbench: host available memory below 8 GiB for 30 seconds'; exit 1; fi
    sleep 10
done
status=$(docker inspect -f '{{.State.ExitCode}}' "$WB")
docker logs "$WB"
echo "MIXED_MATRIX run=$RUN_ID status=$status"
exit "$status"
