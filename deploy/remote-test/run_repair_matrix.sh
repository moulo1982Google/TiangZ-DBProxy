#!/usr/bin/env bash
# P06 PG-level distributions and complete-query costs; no business capacity claim.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
export PATH="/usr/local/cargo/bin:$PATH"
RUN_ID=${1:?new run id required}
require_run_id "${RUN_ID}_distribution"
OUT=$EVIDENCE_ROOT/repair_$RUN_ID
[[ ! -e $OUT ]] || { echo 'Use a new RunId'; exit 2; }
mkdir -p "$OUT"
cd "$SRC"
status=0
for phase in distribution order half; do
    case $phase in
        distribution) test=repair_claim_distribution_matrix;;
        order) test=mixed_states_preserve_order_and_skip_locked_heads;;
        half) test=half_locked_claim_and_hot_merge_costs;;
    esac
    db=${RUN_ID}_$phase
    pg_createdb "$db"
    if DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION=1 DBPROXY_TEST_POSTGRES_URL="$PG_BASE_URL/$db" \
        timeout 300s cargo test -p tiangz-dbproxy-storage --test repair_claim_plans --locked -- --ignored --exact "$test" --nocapture \
        >"$OUT/$phase.log" 2>&1 && grep -q '^test result: ok. 1 passed' "$OUT/$phase.log"; then
        grep -E '^test result|P06_HALF_HOT_RESULT' "$OUT/$phase.log"
    else
        status=1
        tail -25 "$OUT/$phase.log"
    fi
done
echo "P06_REPAIR_MATRIX status=$status"
exit "$status"
