#!/usr/bin/env bash
# Dedicated PG with bounded tmpfs WAL/log filesystems. No shared-service or real-disk fill.
set -euo pipefail
RUN_ID=${1:?new run id required}
KIND=${2:?wal or log required}
[[ $RUN_ID =~ ^[a-z][a-z0-9_]{0,18}$ && $KIND =~ ^(wal|log)$ ]] || exit 2
DATA=${DBPROXY_TEST_DATA:-/data/dbproxy-test}
IMAGE=${DBPROXY_WORKBENCH_IMAGE:-dbproxy-workbench:f06-20260929}
PG=dbproxy-f10-pg-$RUN_ID
WB=dbproxy-f10-$RUN_ID
KEEP=dbproxy-f10-keep-$RUN_ID
PGVOL=dbproxy-f10-data-$RUN_ID
WALVOL=dbproxy-f10-wal-$RUN_ID
LOGVOL=dbproxy-f10-log-$RUN_ID
OUT=$DATA/evidence/fault_process_$RUN_ID
HOST=$DATA/f10-host-$RUN_ID
[[ ! -e $OUT && ! -e $HOST ]] || { echo 'Use a new RunId'; exit 2; }
for name in "$PG" "$WB" "$KEEP"; do
    if docker inspect "$name" >/dev/null 2>&1; then echo 'Container already exists'; exit 2; fi
done
for volume in "$PGVOL" "$WALVOL" "$LOGVOL"; do
    if docker volume inspect "$volume" >/dev/null 2>&1; then echo 'Volume already exists'; exit 2; fi
done
[[ $(df -Pk "$DATA" | awk 'NR==2 {print $4}') -gt 10485760 ]] || { echo 'Require 10 GiB free'; exit 2; }
mkdir -p "$HOST"
docker volume create "$PGVOL" >/dev/null
docker volume create --driver local --opt type=tmpfs --opt device=tmpfs --opt o=size=128m,mode=1777 "$WALVOL" >/dev/null
docker volume create --driver local --opt type=tmpfs --opt device=tmpfs --opt o=size=8m,mode=1777 "$LOGVOL" >/dev/null
finish() {
    docker logs "$WB" >"$HOST/workbench.log" 2>&1 || true
    docker logs "$PG" >"$HOST/postgres-container.log" 2>&1 || true
    docker stop -t 10 "$WB" "$PG" >/dev/null 2>&1 || true
    # Preserve tmpfs contents before the last mount is stopped, including failed rounds.
    if [[ $(docker inspect -f '{{.State.Running}}' "$KEEP" 2>/dev/null) == true ]]; then
        docker exec "$KEEP" tar -czf /archive/wal-and-log.tgz -C / wal logfs || true
        docker exec "$KEEP" cat /logfs/postgresql.log >"$HOST/postgresql.log" 2>/dev/null || true
        docker stop -t 2 "$KEEP" >/dev/null 2>&1 || true
    fi
}
trap finish EXIT
docker run -d --name "$KEEP" --cpuset-cpus 19,47 --cpus 0.25 --memory 256m --memory-swap 256m \
    -v "$WALVOL:/wal" -v "$LOGVOL:/logfs" -v "$HOST:/archive" \
    postgres:18.6-bookworm sleep infinity >/dev/null
docker run -d --name "$PG" --network dbproxy-test --cpuset-cpus 14-17,42-45 --cpus 2 \
    --memory 3g --memory-swap 3g -e POSTGRES_USER=tiangz -e POSTGRES_PASSWORD=tiangz_dev \
    -e POSTGRES_DB=tiangz -e PGDATA=/pgdata/data -e POSTGRES_INITDB_WALDIR=/wal/data \
    -v "$PGVOL:/pgdata" -v "$WALVOL:/wal" -v "$LOGVOL:/logfs" \
    postgres:18.6-bookworm postgres -c shared_buffers=128MB -c max_connections=30 \
    -c max_wal_size=64MB -c min_wal_size=32MB -c logging_collector=on \
    -c log_directory=/logfs -c log_filename=postgresql.log -c log_file_mode=0644 \
    -c 'log_line_prefix=%m [%p] ' -c log_checkpoints=on >/dev/null
ready() {
    for _ in $(seq 1 60); do
        if docker exec "$PG" pg_isready -h 127.0.0.1 -U tiangz -d tiangz >/dev/null 2>&1; then return; fi
        sleep 1
    done
    return 1
}
ready
docker run -d --name "$WB" --network dbproxy-test --cpuset-cpus 20-27,48-55 --cpus 4 \
    --memory 16g --memory-swap 16g -e CARGO_BUILD_JOBS=4 -e FAULT_DEDICATED_SPACE=1 -e FAULT_SPACE_KIND="$KIND" \
    -e PG_BASE_URL="postgres://tiangz:tiangz_dev@$PG:5432" \
    -e FAULT_TESTS=postgres_space_failure::f10_bounded_wal_or_log_full_recovers_without_false_success \
    -v "$DATA/evidence:/evidence" -v "$LOGVOL:/pglog:ro" \
    -v "$DATA/src/crates/dbproxy-server/tests:/src/crates/dbproxy-server/tests:ro" \
    -v "$DATA/src/deploy/remote-test:/src/deploy/remote-test:ro" \
    "$IMAGE" bash /src/deploy/remote-test/run_fault_process.sh "$RUN_ID" >/dev/null
phase=$OUT/f10-space
inside=/evidence/fault_process_$RUN_ID/f10-space
wait_marker() {
    for _ in $(seq 1 2400); do
        [[ -f $1 ]] && return
        [[ $(docker inspect -f '{{.State.Running}}' "$WB") == true ]] || { docker logs "$WB"; return 1; }
        sleep 0.1
    done
    echo "Handshake timeout: $1"; return 1
}
wait_marker "$phase/fault-go"
if [[ $KIND == wal ]]; then fill=/wal; else fill=/logfs; fi
# Verify the final target is the dedicated tmpfs mount before writing a ballast file.
[[ $(docker exec "$KEEP" stat -f -c %T "$fill") == tmpfs ]] || { echo 'Not tmpfs'; exit 1; }
docker inspect -f '{{json .Mounts}}' "$KEEP" >"$HOST/keeper-mounts.json"
docker volume inspect "$WALVOL" "$LOGVOL" >"$HOST/tmpfs-volumes.json"
if docker exec "$KEEP" dd if=/dev/zero "of=$fill/ballast" bs=1M >"$HOST/fill.log" 2>&1; then
    echo 'Expected ENOSPC did not occur'; exit 1
fi
grep -q 'No space left on device' "$HOST/fill.log"
docker exec "$KEEP" df -Pk "$fill" >"$HOST/full-df.txt"
[[ $(awk 'NR==2 {print $4}' "$HOST/full-df.txt") == 0 ]] || { echo 'Filesystem not full'; exit 1; }
date -u +%FT%T.%NZ | docker exec -i "$WB" tee "$inside/fault-active" >/dev/null
if [[ $KIND == wal ]]; then
    # Emit WAL between switches so each switch advances; stop once allocation failure is visible.
    for n in $(seq 1 20); do
        timeout 5 docker exec "$PG" psql -U tiangz -d postgres -v ON_ERROR_STOP=1 \
            -c 'CREATE TABLE IF NOT EXISTS f10_wal_probe(n int); INSERT INTO f10_wal_probe SELECT generate_series(1,100); SELECT pg_switch_wal();' \
            >>"$HOST/wal-switch.log" 2>&1 || true
        if docker exec "$KEEP" grep -q 'PANIC:.*No space left on device' /logfs/postgresql.log; then break; fi
    done
    docker exec "$KEEP" grep 'PANIC:.*No space left on device' /logfs/postgresql.log >"$HOST/wal-panic.txt"
else
    docker exec "$KEEP" stat -c %s /logfs/postgresql.log >"$HOST/log-size-full-before.txt"
    docker exec "$PG" psql -U tiangz -d postgres -v ON_ERROR_STOP=1 \
        -c "DO \$\$ BEGIN RAISE LOG '%', repeat('x',65536) || 'F10_LOG_FULL_TAIL_$RUN_ID'; END \$\$;" >"$HOST/log-attempt.txt" 2>&1
    sleep 1
    docker exec "$KEEP" stat -c %s /logfs/postgresql.log >"$HOST/log-size-full-after.txt"
    # Zero free blocks does not forbid appending into the current file's final allocated block.
    # Force a 64 KiB message, then prove it could not reach its tail marker.
    before=$(cat "$HOST/log-size-full-before.txt")
    after=$(cat "$HOST/log-size-full-after.txt")
    [[ $((after-before)) -lt 65536 ]] || { echo 'Large log message unexpectedly fit'; exit 1; }
    if docker exec "$KEEP" grep -q "F10_LOG_FULL_TAIL_$RUN_ID" /logfs/postgresql.log; then
        echo 'Logging failure was not established'; exit 1
    fi
fi
wait_marker "$phase/fault-observed"
# Only the exact ballast file created above is removed. No PG files or real filesystem filled.
docker exec "$KEEP" rm -- "$fill/ballast"
if [[ $KIND == wal ]]; then
    docker restart -t 5 "$PG" >/dev/null
fi
ready
docker exec "$PG" psql -U tiangz -d postgres -v ON_ERROR_STOP=1 \
    -c "DO \$\$ BEGIN RAISE LOG 'F10_LOG_RESTORED_$RUN_ID'; END \$\$;" >"$HOST/restored-log-attempt.txt" 2>&1
for _ in $(seq 1 20); do
    docker exec "$KEEP" grep -q "F10_LOG_RESTORED_$RUN_ID" /logfs/postgresql.log && break
    sleep 0.1
done
docker exec "$KEEP" grep "F10_LOG_RESTORED_$RUN_ID" /logfs/postgresql.log >"$HOST/restored-log-proof.txt"
date -u +%FT%T.%NZ | docker exec -i "$WB" tee "$inside/fault-release" >/dev/null
for _ in $(seq 1 180); do
    [[ $(docker inspect -f '{{.State.Running}}' "$WB") == false ]] && break
    sleep 1
done
[[ $(docker inspect -f '{{.State.Running}}' "$WB") == false ]] || { echo 'Test completion timeout'; exit 1; }
status=$(docker inspect -f '{{.State.ExitCode}}' "$WB")
docker logs "$WB"
echo "F10_SPACE run=$RUN_ID kind=$KIND status=$status data=$PGVOL"
exit "$status"
