#!/usr/bin/env bash
# Real-database regression for configurable ordinary receipt retention (2026-09-23).
# Runs inside the workbench as root (cargo writes /src/target); each suite gets a fresh database.
# Usage: run_retention_tests.sh <suffix>   e.g. ret_a  -> databases ret_a_retention, ret_a_faults, ...
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
# `bash -l` resets PATH from /etc/profile; the rust image keeps cargo here.
export PATH="/usr/local/cargo/bin:$PATH"
SUFFIX=$1
require_run_id "$SUFFIX"
OUT="$EVIDENCE_ROOT/retention_$SUFFIX"
[[ -e $OUT ]] && { echo "Use a new suffix" >&2; exit 2; }
mkdir -p "$OUT"
export DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION=1
status=0
run() {
    local name=$1 db=$2; shift 2
    pg_createdb "$db"
    echo "=== $name (db $db) ==="
    if DBPROXY_TEST_POSTGRES_URL="${PG_BASE_URL}/${db}" "$@" >"$OUT/$name.log" 2>&1; then
        grep -E "^test result|RECEIPT|SCALE_PLAN recent=1000000" "$OUT/$name.log" | cut -c1-200
    else
        status=1
        tail -40 "$OUT/$name.log"
    fi
}
cd "$SRC"
run storage_retention "${SUFFIX}_retention" cargo test -p tiangz-dbproxy-storage --test receipt_retention --locked -- --ignored --nocapture --test-threads=1 --skip migration_grants_old_receipts_full_retention
run storage_faults "${SUFFIX}_faults" cargo test -p tiangz-dbproxy-storage --test receipt_cleanup_faults --locked -- --ignored --nocapture --test-threads=1
run storage_scale "${SUFFIX}_scale" cargo test -p tiangz-dbproxy-storage --test receipt_scale --locked -- --ignored --nocapture --test-threads=1
DBPROXY_REDIS_URL="redis://:tiangz_dev@redis:6379/1" run server_worker "${SUFFIX}_worker" cargo test -p tiangz-dbproxy-server --test receipt_cleanup_worker --locked -- --ignored --nocapture --test-threads=1
pg_createdb "${SUFFIX}_proc_a"; pg_createdb "${SUFFIX}_proc_b"
ACCEPT_PG_A="${PG_BASE_URL}/${SUFFIX}_proc_a" ACCEPT_PG_B="${PG_BASE_URL}/${SUFFIX}_proc_b" \
ACCEPT_REDIS_A="redis://:tiangz_dev@redis:6379/2" ACCEPT_REDIS_B="redis://:tiangz_dev@redis:6379/3" \
ACCEPT_CACHE_A="redis://:tiangz_dev@cache:6379/2" ACCEPT_CACHE_B="redis://:tiangz_dev@cache:6379/3" \
DBPROXY_ACCEPTANCE_ARTIFACTS="$OUT/process" DBPROXY_ACCEPTANCE_BINARY="$SRC/target/release/tiangz-dbproxy-server" \
    run server_process "${SUFFIX}_proc_unused" cargo test -p tiangz-dbproxy-server --test acceptance_process --locked -- --ignored --nocapture --test-threads=1
echo "RETENTION_TESTS status=$status"
exit "$status"
