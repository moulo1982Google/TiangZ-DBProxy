#!/usr/bin/env bash
# Two tenants, 1/30/64 reads, authoritative/hot/cold; defaults are 3 x 2m warmup + 5m sample.
set -euo pipefail
RUN_ID=${1:?new run id required}
[[ $RUN_ID =~ ^[a-z][a-z0-9_]{0,18}$ ]] || exit 2
DATA=${DBPROXY_TEST_DATA:-/data/dbproxy-test}
IMAGE=${DBPROXY_WORKBENCH_IMAGE:-dbproxy-workbench:f06-20260929}
WB=dbproxy-p01-$RUN_ID
OUT=$DATA/evidence/fault_process_$RUN_ID
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
    -e P01_WARMUP_SECONDS="${P01_WARMUP_SECONDS:-120}" \
    -e P01_SAMPLE_SECONDS="${P01_SAMPLE_SECONDS:-300}" -e P01_ROUNDS="${P01_ROUNDS:-3}" \
    -e FAULT_TESTS=batch_read_matrix::p01_two_tenant_batch_read_matrix \
    -v "$DATA/evidence:/evidence" -v "$DATA/pglog:/pglog:ro" \
    -v "$DATA/src/crates/dbproxy-server/tests:/src/crates/dbproxy-server/tests:ro" \
    -v "$DATA/src/deploy/remote-test:/src/deploy/remote-test:ro" \
    "$IMAGE" bash /src/deploy/remote-test/run_fault_process.sh "$RUN_ID" >/dev/null
docker exec "$WB" sha256sum /src/crates/dbproxy-server/tests/support/batch_read_matrix.rs \
    /src/crates/dbproxy-storage/src/snapshot_load_multi.sql >"$DATA/evidence/$RUN_ID.sources.sha256"
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
echo "P01_MATRIX run=$RUN_ID status=$status"
exit "$status"
