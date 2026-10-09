#!/usr/bin/env bash
# Start/stop the dependency containers with plain `docker run`, for hosts without Docker Compose.
# Same settings as docker-compose.yml in this directory; run this on the host, not in a container.
# Usage: deps.sh up | down | status
set -euo pipefail

DATA=${DBPROXY_TEST_DATA:-/data/dbproxy-test}
NET=${DBPROXY_TEST_NET:-dbproxy-test}
PREFIX=dbproxy-test
PG_IMAGE=postgres:18.6-bookworm
REDIS_IMAGE=redis:8.8.1-trixie
# NUMA node 1 on this box is CPU 14-27,42-55; keep the probes on the rest of that node.
PG_CPUSET=${DBPROXY_TEST_PG_CPUSET:-14-17,42-45}
REDIS_CPUSET=${DBPROXY_TEST_REDIS_CPUSET:-18,46}
CACHE_CPUSET=${DBPROXY_TEST_CACHE_CPUSET:-19,47}

case ${1:-status} in
up)
    mkdir -p "$DATA"/{pgdata,pglog,redis,cache,evidence}
    chmod 777 "$DATA"/{pgdata,pglog,redis,cache}
    docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET"
    docker run -d --name "$PREFIX-postgres" --network "$NET" --network-alias postgres \
        --restart no --cpuset-cpus "$PG_CPUSET" --cpus 4 --memory 8g --memory-swap 8g --shm-size 1g \
        -e POSTGRES_USER=tiangz -e POSTGRES_PASSWORD=tiangz_dev -e POSTGRES_DB=tiangz \
        -v "$DATA/pgdata:/var/lib/postgresql" -v "$DATA/pglog:/pglog" \
        --health-cmd 'pg_isready -U tiangz -d tiangz' --health-interval 5s --health-retries 12 \
        "$PG_IMAGE" postgres \
        -c shared_buffers=2GB -c work_mem=2MB -c maintenance_work_mem=64MB \
        -c autovacuum_work_mem=32MB -c max_connections=30 -c wal_buffers=8MB -c jit=off \
        -c logging_collector=on -c log_directory=/pglog -c log_filename=postgresql.log \
        -c 'log_line_prefix=%m [%p] ' -c log_checkpoints=on -c log_file_mode=0644
    docker run -d --name "$PREFIX-redis" --network "$NET" --network-alias redis \
        --restart no --cpuset-cpus "$REDIS_CPUSET" --memory 2g \
        -v "$DATA/redis:/data" \
        --health-cmd 'redis-cli -a tiangz_dev ping | grep PONG' --health-interval 5s --health-retries 12 \
        "$REDIS_IMAGE" redis-server --appendonly yes --requirepass tiangz_dev \
        --maxmemory 1gb --maxmemory-policy noeviction
    docker run -d --name "$PREFIX-cache" --network "$NET" --network-alias cache \
        --restart no --cpuset-cpus "$CACHE_CPUSET" --memory 2g \
        -v "$DATA/cache:/data" \
        --health-cmd 'redis-cli -a tiangz_dev ping | grep PONG' --health-interval 5s --health-retries 12 \
        "$REDIS_IMAGE" redis-server --save '' --appendonly no --requirepass tiangz_dev \
        --maxmemory 1gb --maxmemory-policy noeviction
    for _ in $(seq 1 60); do
        healthy=$(docker inspect -f '{{.State.Health.Status}}' "$PREFIX-postgres" "$PREFIX-redis" "$PREFIX-cache" 2>/dev/null | grep -c healthy || true)
        [[ $healthy -eq 3 ]] && break
        sleep 2
    done
    docker ps --filter "name=$PREFIX" --format '{{.Names}}\t{{.Status}}'
    ;;
down)
    # Only the three containers created above; volumes are bind mounts and are never removed here.
    docker rm -f "$PREFIX-postgres" "$PREFIX-redis" "$PREFIX-cache" 2>/dev/null || true
    docker network rm "$NET" 2>/dev/null || true
    ;;
status)
    docker ps -a --filter "name=$PREFIX" --format '{{.Names}}\t{{.Status}}\t{{.Image}}'
    ;;
*)
    echo "usage: deps.sh up|down|status" >&2
    exit 2
    ;;
esac
