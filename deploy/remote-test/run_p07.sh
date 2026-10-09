#!/usr/bin/env bash
# Container-side P07 SQL diagnostics followed by a bounded component timing matrix.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
export PATH="/usr/local/cargo/bin:$PATH"
RUN_ID=${1:?new run id required}
require_run_id "${RUN_ID}_r2_b_on"
OUT=$EVIDENCE_ROOT/outbox_$RUN_ID
[[ ! -e $OUT ]] || { echo 'Use a new RunId'; exit 2; }
mkdir -p "$OUT"
require_pglog
offset=$(pglog_offset)
trap 'pglog_since "$offset" >"$OUT/postgresql.log"' EXIT
cd "$SRC"
export DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION=1
export P07_WARMUP_SECONDS=${P07_WARMUP_SECONDS:-120}
export P07_SAMPLE_SECONDS=${P07_SAMPLE_SECONDS:-300}
rounds=${P07_ROUNDS:-3}
[[ $rounds =~ ^[1-3]$ ]] || exit 2
# Build before any timed phase. Tests and raw logs are retained on failure.
cargo test -p tiangz-dbproxy-storage --test outbox_poll_costs --test outbox_hybrid_plans --test outbox_concurrency --locked --no-run >"$OUT/build.log" 2>&1
sha256sum crates/dbproxy-storage/tests/outbox_poll_costs.rs crates/dbproxy-storage/src/outbox_claim.sql crates/dbproxy-storage/src/outbox_stats.sql >"$OUT/sources.sha256"
for phase in distribution prefix heads; do
    db=${RUN_ID}_$phase
    require_run_id "$db"
    pg_createdb "$db"
    case $phase in
        distribution) suite=outbox_hybrid_plans; test=hybrid_handles_dense_blocked_and_empty_queues;;
        prefix) suite=outbox_concurrency; test=locked_prefix_falls_back_and_continuous_claims_preserve_order;;
        heads) suite=outbox_concurrency; test=blocked_heads_never_allow_followers_to_overtake;;
    esac
    DBPROXY_TEST_POSTGRES_URL="$PG_BASE_URL/$db" timeout 300s cargo test -p tiangz-dbproxy-storage --test "$suite" --locked -- --ignored --exact "$test" --nocapture >"$OUT/$phase.log" 2>&1
    grep '^test result: ok. 1 passed' "$OUT/$phase.log"
done
for ((round=0; round<rounds; round++)); do
    modes=(leased blocked); switches=(off on)
    if ((round%2)); then modes=(blocked leased); switches=(on off); fi
    for mode in "${modes[@]}"; do
        for switch in "${switches[@]}"; do
            phase=r${round}_${mode:0:1}_$switch
            db=${RUN_ID}_$phase
            pg_createdb "$db"
            echo "P07_START $phase $(date -u +%FT%TZ)"
            DBPROXY_TEST_POSTGRES_URL="$PG_BASE_URL/$db" P07_OUTPUT="$OUT/$phase" P07_MODE="$mode" P07_STATS="$switch" \
                timeout 480s cargo test -p tiangz-dbproxy-storage --test outbox_poll_costs --locked -- --ignored --exact sustained_polling_with_shared_stats --nocapture >"$OUT/$phase.log" 2>&1
            grep -E '^test result: ok. 1 passed|P07_POLL_RESULT' "$OUT/$phase.log"
            grep -q '^test result: ok. 1 passed' "$OUT/$phase.log"
        done
    done
done
echo "P07_COMPLETED run=$RUN_ID rounds=$rounds"
