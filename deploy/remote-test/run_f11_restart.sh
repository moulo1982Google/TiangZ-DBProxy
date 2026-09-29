#!/usr/bin/env bash
# Host-side. A dedicated AOF Redis only; preserve its volume and all failed evidence.
set -euo pipefail
RUN_ID=${1:?new run id required}
[[ $RUN_ID =~ ^[a-z][a-z0-9_]{0,18}$ ]] || exit 2
DATA=${DBPROXY_TEST_DATA:-/data/dbproxy-test}
IMAGE=${DBPROXY_WORKBENCH_IMAGE:-dbproxy-workbench:f06-20260929}
REDIS=dbproxy-f11-redis-$RUN_ID
WB=dbproxy-f11-$RUN_ID
VOLUME=dbproxy-f11-data-$RUN_ID
OUT=$DATA/evidence/fault_process_$RUN_ID
LOGDIR=$DATA/f11-host-$RUN_ID
[[ ! -e $OUT && ! -e $LOGDIR ]] || { echo 'Use a new RunId'; exit 2; }
for name in "$REDIS" "$WB"; do
    if docker inspect "$name" >/dev/null 2>&1; then echo 'Container already exists'; exit 2; fi
done
if docker volume inspect "$VOLUME" >/dev/null 2>&1; then echo 'Volume already exists'; exit 2; fi
[[ $(df -Pk "$DATA" | awk 'NR==2 {print $4}') -gt 10485760 ]] || { echo 'Require 10 GiB free'; exit 2; }
mkdir -p "$LOGDIR"
docker volume create "$VOLUME" >/dev/null
finish() {
    docker logs "$REDIS" >"$LOGDIR/redis.log" 2>&1 || true
    docker logs "$WB" >"$LOGDIR/workbench.log" 2>&1 || true
    docker stop -t 10 "$WB" "$REDIS" >/dev/null 2>&1 || true
}
trap finish EXIT
docker run -d --name "$REDIS" --network dbproxy-test --cpuset-cpus 18,46 --cpus 1 \
    --memory 1g --memory-swap 1g -v "$VOLUME:/data" redis:8.8.1-trixie \
    redis-server --requirepass tiangz_dev --appendonly yes --appendfsync everysec \
    --maxmemory 256mb --maxmemory-policy noeviction --save '' >/dev/null
ready() {
    for _ in $(seq 1 30); do
        if [[ $(docker exec -e REDISCLI_AUTH=tiangz_dev "$REDIS" redis-cli PING 2>/dev/null) == PONG ]]; then return; fi
        sleep 1
    done
    return 1
}
ready
docker run -d --name "$WB" --network dbproxy-test --cpuset-cpus 20-27,48-55 --cpus 4 \
    --memory 16g --memory-swap 16g -e CARGO_BUILD_JOBS=4 -e FAULT_DEDICATED_REDIS=1 \
    -e FAULT_REDIS_A_URL="redis://:tiangz_dev@$REDIS:6379/4" \
    -e FAULT_TESTS=reliable_redis_restart::f11_redis_restart_preserves_confirmed_and_retries_unknown \
    -v "$DATA/evidence:/evidence" -v "$DATA/pglog:/pglog:ro" \
    -v "$DATA/src/crates/dbproxy-server/tests:/src/crates/dbproxy-server/tests:ro" \
    -v "$DATA/src/deploy/remote-test:/src/deploy/remote-test:ro" \
    "$IMAGE" bash /src/deploy/remote-test/run_fault_process.sh "$RUN_ID" >/dev/null
wait_marker() {
    local marker=$1
    for _ in $(seq 1 2400); do
        [[ -f $marker ]] && return
        [[ $(docker inspect -f '{{.State.Running}}' "$WB") == true ]] || { docker logs "$WB"; return 1; }
        sleep 0.1
    done
    echo "Handshake timeout: $marker"; return 1
}
for mode in stop kill; do
    phase=$OUT/f11/$mode
    inside=/evidence/fault_process_$RUN_ID/f11/$mode
    wait_marker "$phase/fault-go"
    docker exec -e REDISCLI_AUTH=tiangz_dev "$REDIS" redis-cli INFO persistence \
        | docker exec -i "$WB" tee "$inside/redis-before.txt" >/dev/null
    if [[ $mode == stop ]]; then docker stop -t 10 "$REDIS"; else docker kill --signal KILL "$REDIS"; fi
    docker inspect -f '{{json .State}}' "$REDIS" | docker exec -i "$WB" tee "$inside/redis-stopped-state.json" >/dev/null
    date -u +%FT%T.%NZ | docker exec -i "$WB" tee "$inside/fault-offline" >/dev/null
    wait_marker "$phase/offline-checked"
    docker start "$REDIS" >/dev/null
    ready
    docker inspect -f '{{json .Mounts}}' "$REDIS" | docker exec -i "$WB" tee "$inside/redis-restored-mounts.json" >/dev/null
    docker exec -e REDISCLI_AUTH=tiangz_dev "$REDIS" redis-cli INFO persistence \
        | docker exec -i "$WB" tee "$inside/redis-after.txt" >/dev/null
    date -u +%FT%T.%NZ | docker exec -i "$WB" tee "$inside/fault-release" >/dev/null
done
for _ in $(seq 1 120); do
    [[ $(docker inspect -f '{{.State.Running}}' "$WB") == false ]] && break
    sleep 1
done
[[ $(docker inspect -f '{{.State.Running}}' "$WB") == false ]] || { echo 'Test completion timeout'; exit 1; }
status=$(docker inspect -f '{{.State.ExitCode}}' "$WB")
docker logs "$WB"
echo "F11_RESTART run=$RUN_ID status=$status volume=$VOLUME"
exit "$status"
