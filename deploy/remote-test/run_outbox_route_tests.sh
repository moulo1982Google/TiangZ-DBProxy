#!/usr/bin/env bash
# Container-side A12: each exact test gets a fresh DB; no external broker is contacted.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
export PATH="/usr/local/cargo/bin:$PATH"
RUN_ID=${1:?new run id required}
require_run_id "${RUN_ID}_matrix"
OUT=$EVIDENCE_ROOT/outbox_routes_$RUN_ID
[[ ! -e $OUT ]] || { echo 'Use a new RunId'; exit 2; }
mkdir -p "$OUT"
cd "$SRC"
status=0
for case in matrix prefix heads; do
    case $case in
        matrix) test=publisher_destination_matrix_keeps_same_key_groups_independent;;
        prefix) test=locked_prefix_falls_back_and_continuous_claims_preserve_order;;
        heads) test=blocked_heads_never_allow_followers_to_overtake;;
    esac
    db=${RUN_ID}_$case
    pg_createdb "$db"
    if DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION=1 DBPROXY_TEST_POSTGRES_URL="$PG_BASE_URL/$db" \
        cargo test -p tiangz-dbproxy-storage --test outbox_concurrency --locked -- --ignored --exact "$test" --nocapture \
        >"$OUT/$case.log" 2>&1 && grep -q '^test result: ok. 1 passed' "$OUT/$case.log"; then
        grep -E 'A12_ROUTE|^test result' "$OUT/$case.log"
    else
        status=1
        tail -40 "$OUT/$case.log"
    fi
done
echo "A12_ROUTES status=$status"
exit "$status"
