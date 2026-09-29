#!/usr/bin/env bash
# Host-side, dedicated PG only. Keep the original named data volume and stopped containers.
set -euo pipefail
RUN_ID=${1:?new run id required}
[[ $RUN_ID =~ ^[a-z][a-z0-9_]{0,18}$ ]] || exit 2
DATA=${DBPROXY_TEST_DATA:-/data/dbproxy-test}
IMAGE=${DBPROXY_WORKBENCH_IMAGE:-dbproxy-workbench:f06-20260929}
PG=dbproxy-f05-pg-$RUN_ID
WB=dbproxy-f05-$RUN_ID
VOLUME=dbproxy-f05-data-$RUN_ID
OUT=$DATA/evidence/fault_process_$RUN_ID
LOGDIR=$DATA/pglog-f05-$RUN_ID
[[ ! -e $OUT && ! -e $LOGDIR ]] || { echo 'Use a new RunId'; exit 2; }
for name in "$PG" "$WB"; do
    if docker inspect "$name" >/dev/null 2>&1; then echo 'Container already exists'; exit 2; fi
done
if docker volume inspect "$VOLUME" >/dev/null 2>&1; then echo 'Volume already exists'; exit 2; fi
[[ $(df -Pk "$DATA" | awk 'NR==2 {print $4}') -gt 10485760 ]] || { echo 'Require 10 GiB free'; exit 2; }
mkdir -p "$LOGDIR"
chmod 777 "$LOGDIR"
docker volume create "$VOLUME" >/dev/null
finish() {
    docker logs "$PG" >"$LOGDIR/container.log" 2>&1 || true
    docker logs "$WB" >"$LOGDIR/workbench.log" 2>&1 || true
    docker stop -t 10 "$WB" "$PG" >/dev/null 2>&1 || true
}
trap finish EXIT
docker run -d --name "$PG" --network dbproxy-test --cpuset-cpus 14-17,42-45 --cpus 2 \
    --memory 3g --memory-swap 3g -e POSTGRES_USER=tiangz -e POSTGRES_PASSWORD=tiangz_dev \
    -e POSTGRES_DB=tiangz -e PGDATA=/pgdata/data -v "$VOLUME:/pgdata" -v "$LOGDIR:/pglog" \
    postgres:18.6-bookworm postgres -c shared_buffers=256MB -c max_connections=30 \
    -c max_wal_size=256MB -c min_wal_size=80MB -c logging_collector=on \
    -c log_directory=/pglog -c log_filename=postgresql.log -c log_file_mode=0644 >/dev/null
ready() {
    for _ in $(seq 1 60); do
        # The image's temporary init server accepts Unix sockets before TCP is ready.
        if docker exec "$PG" pg_isready -h 127.0.0.1 -U tiangz -d tiangz >/dev/null 2>&1; then return; fi
        sleep 1
    done
    return 1
}
ready
docker run -d --name "$WB" --network dbproxy-test --cpuset-cpus 20-27,48-55 --cpus 4 \
    --memory 16g --memory-swap 16g -e CARGO_BUILD_JOBS=4 -e FAULT_DEDICATED_PG=1 \
    -e PG_BASE_URL="postgres://tiangz:tiangz_dev@$PG:5432" \
    -e FAULT_TESTS=f05_postgres_restart_keeps_acknowledged_data_and_resumes_cleanup \
    -v "$DATA/evidence:/evidence" -v "$LOGDIR:/pglog:ro" \
    -v "$DATA/src/crates/dbproxy-server/tests/fault_process.rs:/src/crates/dbproxy-server/tests/fault_process.rs:ro" \
    "$IMAGE" bash /src/deploy/remote-test/run_fault_process.sh "$RUN_ID" >/dev/null
for mode in stop kill; do
    phase=$OUT/f05/$mode
    for _ in $(seq 1 2400); do
        [[ -f $phase/fault-go ]] && break
        [[ $(docker inspect -f '{{.State.Running}}' "$WB") == true ]] || { docker logs "$WB"; exit 1; }
        sleep 0.1
    done
    [[ -f $phase/fault-go ]] || { echo 'Fault handshake timeout'; exit 1; }
    # Evidence directories are created by the root workbench; write through that same owner.
    inside=/evidence/fault_process_$RUN_ID/f05/$mode
    date -u +%FT%T.%NZ | docker exec -i "$WB" tee "$inside/host-fault-start.txt" >/dev/null
    if [[ $mode == stop ]]; then docker stop -t 10 "$PG"; else docker kill --signal KILL "$PG"; fi
    docker inspect -f '{{json .State}}' "$PG" | docker exec -i "$WB" tee "$inside/pg-stopped-state.json" >/dev/null
    sleep 3
    docker start "$PG" >/dev/null
    ready
    docker inspect -f '{{json .Mounts}}' "$PG" | docker exec -i "$WB" tee "$inside/pg-restored-mounts.json" >/dev/null
    date -u +%FT%T.%NZ | docker exec -i "$WB" tee "$inside/fault-release" >/dev/null
done
for _ in $(seq 1 180); do
    [[ $(docker inspect -f '{{.State.Running}}' "$WB") == false ]] && break
    sleep 1
done
[[ $(docker inspect -f '{{.State.Running}}' "$WB") == false ]] || { echo 'Test completion timeout'; exit 1; }
status=$(docker inspect -f '{{.State.ExitCode}}' "$WB")
docker logs "$WB"
echo "F05_RESTART run=$RUN_ID status=$status volume=$VOLUME"
exit "$status"
