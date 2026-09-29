#!/usr/bin/env bash
# F09 (PostgreSQL connection limit), F15 (fault on tenant A, steady tenant B) and F04 (server
# force-killed during receipt cleanup; databases <run-id>_f04) against the real
# release server process. Superusers ignore CONNECTION LIMIT, so tenant A logs in as a separate
# non-superuser test role; each test creates fresh databases named after the run id.
# Runs inside the workbench as root (cargo writes /src/target).
# Usage: run_fault_process.sh <run-id>   e.g. fp_a -> databases fp_a_f09, fp_a_f15a, fp_a_f15b
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
export PATH="/usr/local/cargo/bin:$PATH"
RUN_ID=$1
require_run_id "${RUN_ID}_f15a"
require_pglog
OUT="$EVIDENCE_ROOT/fault_process_$RUN_ID"
[[ -e $OUT ]] && { echo "Use a new run id" >&2; exit 2; }
mkdir -p "$OUT"
ROLE=dbproxy_fault_limited
# Throwaway login for the isolated test PostgreSQL only; never a service credential.
pg_sql <<SQL
DO \$\$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '$ROLE') THEN
    CREATE ROLE $ROLE LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD 'fault_dev';
  END IF;
END \$\$;
SQL
pg_sql -At -c "SELECT rolname, rolsuper, rolconnlimit FROM pg_roles WHERE rolname = '$ROLE'" >"$OUT/role-before.txt"
cd "$SRC"
# A test file mounted over the image copy can be older than the prebuilt test binary (clock skew
# between machines); refresh its time so cargo always rebuilds from the mounted source. A run
# that silently filtered the test out once reported success (fp_a, F04), hence the check below.
touch crates/dbproxy-server/tests/fault_process.rs 2>/dev/null || true
cargo build --release --locked -p tiangz-dbproxy-server --bin tiangz-dbproxy-server >"$OUT/build.log" 2>&1
json_sha256 "$SRC/target/release/tiangz-dbproxy-server" >"$OUT/binaries.json"
if [[ ${MIX_BASELINE:-B2} == B1 ]]; then
    cargo build --release --locked -p tiangz-dbproxy-server --example acceptance_host >"$OUT/host-build.log" 2>&1
    export DBPROXY_ACCEPTANCE_HOST_BINARY="$SRC/target/release/examples/acceptance_host"
    json_sha256 "$DBPROXY_ACCEPTANCE_HOST_BINARY" >"$OUT/host-binary.json"
fi
status=0
TESTS=${FAULT_TESTS:-f09_postgres_connection_limit_fails_clearly_and_recovers f15_tenant_a_postgres_fault_leaves_tenant_b_serving f04_kill_server_during_cleanup_keeps_invariants}
for test in $TESTS; do
    # Rust module separators are not valid in downloaded Windows filenames.
    log_name=${test//::/__}
    offset=$(pglog_offset)
    echo "=== $test ==="
    if DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION=1 FAULT_RUN_ID="$RUN_ID" FAULT_LIMITED_ROLE="$ROLE" \
        FAULT_PG_ADMIN_BASE="$PG_BASE_URL" FAULT_PG_LIMITED_BASE="postgres://$ROLE:fault_dev@postgres:5432" \
        FAULT_REDIS_A="${FAULT_REDIS_A_URL:-redis://:tiangz_dev@redis:6379/4}" FAULT_REDIS_B="redis://:tiangz_dev@redis:6379/5" \
        FAULT_CACHE_A="redis://:tiangz_dev@cache:6379/4" FAULT_CACHE_B="redis://:tiangz_dev@cache:6379/5" \
        DBPROXY_ACCEPTANCE_ARTIFACTS="$OUT" DBPROXY_ACCEPTANCE_BINARY="$SRC/target/release/tiangz-dbproxy-server" \
        cargo test -p tiangz-dbproxy-server --test fault_process --locked -- --ignored --exact "$test" --nocapture \
        >"$OUT/$log_name.log" 2>&1 && grep -q "^test result: ok. 1 passed" "$OUT/$log_name.log"; then
        grep -E "^test result|_RESULT" "$OUT/$log_name.log" | cut -c1-400
    else
        status=1
        tail -40 "$OUT/$log_name.log"
    fi
    pglog_since "$offset" >"$OUT/$log_name.postgres.log"
    # Every refused (re)connection is one FATAL line: the reconnect rate during the fault.
    echo "refused_connections=$(grep -c 'too many connections for role' "$OUT/$log_name.postgres.log" || true)" \
        | tee "$OUT/$log_name.refused.txt"
done
pg_sql -c "ALTER ROLE $ROLE CONNECTION LIMIT -1" >/dev/null
echo "FAULT_PROCESS status=$status"
exit "$status"
