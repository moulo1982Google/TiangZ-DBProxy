#!/usr/bin/env bash
# Host-side launcher for a long round: detached workbench container running run_long_round.sh,
# plus the host container sampler, both independent of the SSH session that started them.
# Usage: launch_long.sh <run-id> <seconds> <redis-db> [extra run_receipt_probe.sh arguments...]
set -euo pipefail
RUN_ID=$1; SECONDS_=$2; REDIS_DB=$3; shift 3
DATA=${DBPROXY_TEST_DATA:-/data/dbproxy-test}
NAME="dbproxy-long-$RUN_ID"
[[ -e $DATA/evidence/$RUN_ID ]] && { echo "Use a new RunId" >&2; exit 2; }
docker run -d --name "$NAME" --network dbproxy-test \
    --user "$(id -u):$(id -g)" --cpuset-cpus 20-27,48-55 --memory 16g \
    -e REDIS_URL="redis://:tiangz_dev@redis:6379/$REDIS_DB" \
    -e CACHE_REDIS_URL="redis://:tiangz_dev@cache:6379/$REDIS_DB" \
    -e PG_SAMPLE_SECONDS="${PG_SAMPLE_SECONDS:-30}" -e RETENTION_HOURS="${RETENTION_HOURS:-24}" \
    -v "$DATA/evidence:/evidence" -v "$DATA/pglog:/pglog:ro" \
    -v "$DATA/src/deploy/remote-test:/src/deploy/remote-test:ro" \
    -v "$DATA/src/tools:/src/tools:ro" \
    dbproxy-workbench:local bash -lc "deploy/remote-test/run_long_round.sh $RUN_ID $SECONDS_ $*" >/dev/null
nohup setsid "$DATA/src/deploy/remote-test/sample_containers.sh" \
    "$DATA/evidence/$RUN_ID.containers.jsonl" "$NAME" "${CONTAINER_SAMPLE_SECONDS:-10}" \
    >/dev/null 2>"$DATA/evidence/$RUN_ID.containers.err" </dev/null &
echo "launched container=$NAME sampler_pid=$!"
