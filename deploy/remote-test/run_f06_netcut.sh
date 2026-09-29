#!/usr/bin/env bash
# F06 real network cut, run on the host. A throwaway PostgreSQL (data on a small tmpfs volume)
# lives on its own per-run network with a fixed address. Mid-load it is disconnected, so packets
# to it simply vanish (no RST, no ACK), and after the outage it rejoins with a DIFFERENT fixed
# address under the same name, like a failover. Nothing ever answers at the old address again,
# so connections to it can only end through TCP timeouts. The workbench joins both networks and
# runs run_fault_load.sh --fault external; the injector and this script hand over via files.
# Usage: run_f06_netcut.sh <run-id> <outage seconds> [PG_URL_PARAMS] [extra run_fault_load.sh arguments...]
#   PG_URL_PARAMS e.g. "keepalives=0&tcp_user_timeout=3600" switches DBProxy's network guards
#   off for a before/after comparison; "-" leaves the defaults.
# History: f06_cut_*_a rejoined with the same address; f06_cut_*_b let a placeholder container
# take the old address, whose kernel then answered with RST and ended old connections at once.
set -euo pipefail
RUN_ID=$1; OUTAGE=$2; PARAMS=${3:--}; shift $(( $# >= 3 ? 3 : 2 ))
[[ $PARAMS == - ]] && PARAMS=""
[[ $RUN_ID =~ ^[a-z][a-z0-9_]{0,24}$ ]] || { echo "bad run id" >&2; exit 2; }
[[ $OUTAGE =~ ^[1-9][0-9]*$ && $OUTAGE -le 300 ]] || { echo "outage must be 1..300 seconds" >&2; exit 2; }
DATA=${DBPROXY_TEST_DATA:-/data/dbproxy-test}
IMAGE=${DBPROXY_WORKBENCH_IMAGE:-dbproxy-workbench:local}
EVIDENCE=$DATA/evidence/$RUN_ID
[[ -e $EVIDENCE ]] && { echo "Use a new RunId" >&2; exit 2; }
NET=dbproxy-test
CUTNET=dbproxy-netcut-$RUN_ID
# Fixed, otherwise unused subnet; `docker network create` refuses an overlap, which stops here.
SUBNET=10.231.77.0/24; IP_BEFORE=10.231.77.10; IP_AFTER=10.231.77.20
PG=dbproxy-f06-pg-$RUN_ID
WB=dbproxy-f06-$RUN_ID
VOLUME=dbproxy-f06-fs-$RUN_ID
LOGDIR=$DATA/pglog-f06-$RUN_ID
# Refuse name collisions before installing a cleanup trap: never remove pre-existing resources.
for container in "$PG" "$WB"; do
    if docker container inspect "$container" >/dev/null 2>&1; then
        echo "container already exists: $container" >&2; exit 2
    fi
done
if docker network inspect "$CUTNET" >/dev/null 2>&1 || docker volume inspect "$VOLUME" >/dev/null 2>&1; then
    echo "network or volume already exists; use a new RunId" >&2; exit 2
fi
mkdir -p "$LOGDIR" && chmod 777 "$LOGDIR"
cleanup() {
    docker logs "$PG" >"$LOGDIR/container.log" 2>&1 || true
    docker rm -f -v "$PG" "$WB" >/dev/null 2>&1 || true
    docker volume rm "$VOLUME" >/dev/null 2>&1 || true
    docker network rm "$CUTNET" >/dev/null 2>&1 || true
}
trap cleanup EXIT
docker network create --subnet "$SUBNET" "$CUTNET" >/dev/null
docker volume create --driver local --opt type=tmpfs --opt device=tmpfs --opt "o=size=1g,mode=1777" "$VOLUME" >/dev/null
docker run -d --name "$PG" --network "$CUTNET" --ip "$IP_BEFORE" --network-alias f06-postgres \
    --cpuset-cpus 14-17,42-45 --cpus 2 --memory 3g --memory-swap 3g \
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
ip_of() { docker inspect -f "{{(index .NetworkSettings.Networks \"$CUTNET\").IPAddress}}" "$PG" 2>/dev/null || true; }
# Created first and joined to the cut network before it starts, so its first query can reach PG.
docker create --name "$WB" --network "$NET" \
    --user "$(id -u):$(id -g)" --cpuset-cpus 20-27,48-55 --memory 16g \
    -e PG_BASE_URL=postgres://tiangz:tiangz_dev@f06-postgres:5432 -e PG_URL_PARAMS="$PARAMS" \
    -e REDIS_URL=redis://:tiangz_dev@redis:6379/11 -e CACHE_REDIS_URL=redis://:tiangz_dev@cache:6379/11 \
    -v "$DATA/evidence:/evidence" -v "$LOGDIR:/pglog:ro" \
    -v "$DATA/src/deploy/remote-test:/src/deploy/remote-test:ro" -v "$DATA/src/tools:/src/tools:ro" \
    "$IMAGE" bash /src/deploy/remote-test/run_fault_load.sh --run-id "$RUN_ID" --fault external --fault-duration "$OUTAGE" "$@" >/dev/null
docker network connect "$CUTNET" "$WB"
docker start "$WB" >/dev/null
# Wait for the injector's request, cut the network, hold, rejoin elsewhere, report back.
for _ in $(seq 1 600); do
    [[ -f $EVIDENCE/fault-go ]] && break
    [[ "$(docker inspect -f '{{.State.Running}}' "$WB")" == true ]] || break
    sleep 0.1
done
if [[ -f $EVIDENCE/fault-go ]]; then
    before=$(ip_of)
    # The time is taken before the cut starts; the injector uses it as the injection instant.
    cut_us=$(date +%s%6N)
    docker network disconnect "$CUTNET" "$PG"
    echo "unix_us=$cut_us disconnected $before, done at $(date -u +%FT%T.%3NZ)" >"$EVIDENCE/fault-injected"
    sleep "$OUTAGE"
    docker network connect --ip "$IP_AFTER" --alias f06-postgres "$CUTNET" "$PG"
    after=$(ip_of)
    echo "unix_us=$(date +%s%6N) reconnected $after (was $before)" >"$EVIDENCE/fault-release"
    echo "{\"ip_before\":\"$before\",\"ip_after\":\"$after\",\"address_changed\":$([[ $before != "$after" ]] && echo true || echo false),\"old_address_answers\":false,\"outage_seconds\":$OUTAGE,\"host_url_params\":\"$PARAMS\"}" >"$LOGDIR/netcut.json"
fi
status=$(docker wait "$WB")
docker logs "$WB" >"$LOGDIR/workbench.log" 2>&1 || true
cat "$LOGDIR/netcut.json" 2>/dev/null || echo "no network cut happened"
echo "F06_NETCUT run=$RUN_ID status=$status"
exit "$status"
