#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
export PATH="/usr/local/cargo/bin:$PATH"
RUN_ID=${1:?new run id required}
require_run_id "$RUN_ID"
OUT=$EVIDENCE_ROOT/content_$RUN_ID
[[ ! -e $OUT ]] || { echo 'Use a new RunId'; exit 2; }
mkdir -p "$OUT"
pg_createdb "$RUN_ID"
cd "$SRC"
DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION=1 DBPROXY_TEST_POSTGRES_URL="$PG_BASE_URL/$RUN_ID" \
DBPROXY_ACCEPTANCE_ARTIFACTS="$OUT" timeout 180s \
    cargo test -p tiangz-dbproxy-storage --test cleanup_content_ledger --locked -- --ignored --exact cleanup_preserves_full_content_and_concurrent_transactions --nocapture \
    >"$OUT/test.log" 2>&1 || { tail -40 "$OUT/test.log"; exit 1; }
grep -q '^test result: ok. 1 passed' "$OUT/test.log"
grep -E 'CONTENT_LEDGER_RESULT|^test result' "$OUT/test.log"
