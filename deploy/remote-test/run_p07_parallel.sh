#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
export PATH=/usr/local/cargo/bin:$PATH
RUN_ID=${1:?new run id required}
require_run_id "$RUN_ID"
OUT=$EVIDENCE_ROOT/parallel_$RUN_ID
[[ ! -e $OUT ]] || exit 2
mkdir "$OUT"
require_pglog
offset=$(pglog_offset)
trap 'pglog_since "$offset" >"$OUT/postgresql.log"' EXIT
cd "$SRC"
export DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION=1
cargo test -p tiangz-dbproxy-storage --test outbox_parallel_poll --locked --no-run >"$OUT/build.log" 2>&1
pg_createdb "$RUN_ID"
export DBPROXY_TEST_POSTGRES_URL="$PG_BASE_URL/$RUN_ID"
export P07_OUTPUT="$OUT/poll"
timeout 120 cargo test -p tiangz-dbproxy-storage --test outbox_parallel_poll --locked -- --ignored --exact two_workers_fixed_budget --nocapture >"$OUT/test.log" 2>&1
cat "$OUT/test.log"
grep -q '1 passed' "$OUT/test.log"
echo P07_PARALLEL_SMOKE_COMPLETED
