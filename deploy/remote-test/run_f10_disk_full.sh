#!/usr/bin/env bash
# F10 disk full, run on the host. A throwaway PostgreSQL keeps its whole data directory on a
# small tmpfs volume (RAM, never the real disk); the workbench mounts the same volume and the
# injector fills it with a ballast file during the fault window, then deletes it.
# The throwaway PostgreSQL, its tmpfs volume and nothing else are removed at the end; its log
# directory and all evidence stay under /data/dbproxy-test.
# Usage: run_f10_disk_full.sh <run-id> [tmpfs size, default 1g] [extra run_fault_load.sh arguments...]
set -euo pipefail
RUN_ID=$1; SIZE=${2:-1g}; shift $(( $# >= 2 ? 2 : 1 ))
[[ $RUN_ID =~ ^[a-z][a-z0-9_]{0,24}$ ]] || { echo "bad run id" >&2; exit 2; }
DATA=${DBPROXY_TEST_DATA:-/data/dbproxy-test}
[[ -e $DATA/evidence/$RUN_ID ]] && { echo "Use a new RunId" >&2; exit 2; }
PG=dbproxy-f10-pg-$RUN_ID
VOLUME=dbproxy-f10-fs-$RUN_ID
LOGDIR=$DATA/pglog-f10-$RUN_ID
mkdir -p "$LOGDIR" && chmod 777 "$LOGDIR"
docker volume create --driver local --opt type=tmpfs --opt device=tmpfs --opt "o=size=$SIZE,mode=1777" "$VOLUME" >/dev/null
cleanup() {
    docker logs "$PG" >"$LOGDIR/container.log" 2>&1 || true
    docker rm -f -v "$PG" >/dev/null 2>&1 || true
    docker volume rm "$VOLUME" >/dev/null 2>&1 || true
}
trap cleanup EXIT
# Same NUMA node and CPU set as the main test PostgreSQL, which is idle while this runs.
# WAL is capped so that PostgreSQL itself fits comfortably; the ballast takes the rest.
docker run -d --name "$PG" --network dbproxy-test --network-alias f10-postgres \
    --restart on-failure:10 --cpuset-cpus 14-17,42-45 --cpus 2 --memory 3g --memory-swap 3g \
    -e POSTGRES_USER=tiangz -e POSTGRES_PASSWORD=tiangz_dev -e POSTGRES_DB=tiangz \
    -e PGDATA=/pgfs/data -v "$VOLUME:/pgfs" -v "$LOGDIR:/pglog" \
    postgres:18.6-bookworm postgres \
    -c shared_buffers=256MB -c max_connections=30 -c max_wal_size=256MB -c min_wal_size=80MB \
    -c logging_collector=on -c log_directory=/pglog -c log_filename=postgresql.log \
    -c 'log_line_prefix=%m [%p] ' -c log_checkpoints=on -c log_file_mode=0644 >/dev/null
for _ in $(seq 1 60); do
    docker exec "$PG" pg_isready -U tiangz -d tiangz >/dev/null 2>&1 && break
    sleep 1
done
docker exec "$PG" pg_isready -U tiangz -d tiangz
docker exec -u root "$PG" sh -c 'mkdir -p /pgfs/ballast && chmod 777 /pgfs/ballast && df -h /pgfs'
status=0
docker run --rm --name "dbproxy-f10-$RUN_ID" --network dbproxy-test \
    --user "$(id -u):$(id -g)" --cpuset-cpus 20-27,48-55 --memory 16g \
    -e PG_BASE_URL=postgres://tiangz:tiangz_dev@f10-postgres:5432 \
    -e REDIS_URL=redis://:tiangz_dev@redis:6379/6 -e CACHE_REDIS_URL=redis://:tiangz_dev@cache:6379/6 \
    -e ACCEPT_BALLAST_DIR=/pgfs/ballast \
    -v "$DATA/evidence:/evidence" -v "$LOGDIR:/pglog:ro" -v "$VOLUME:/pgfs" \
    dbproxy-workbench:local bash -lc "deploy/remote-test/run_fault_load.sh --run-id $RUN_ID --fault disk_full $*" \
    || status=$?
docker exec "$PG" pg_isready -U tiangz -d tiangz >"$LOGDIR/ready-after.txt" 2>&1 || true
docker inspect -f '{{.RestartCount}}' "$PG" >"$LOGDIR/restart-count.txt" 2>&1 || true
echo "F10_DISK_FULL run=$RUN_ID status=$status restarts=$(cat "$LOGDIR/restart-count.txt")"
exit "$status"
