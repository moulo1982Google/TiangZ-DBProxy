#!/usr/bin/env bash
# Source before container creation, compilation or database creation.
# Formal budget arithmetic is tested locally; formal execution is not enabled yet.
p07_parallel_preflight() {
    local pair key expected
    for pair in P07_WARMUP_SECONDS:2 P07_SAMPLE_SECONDS:5 P07_ROWS:1000 \
        P07_PUBLISHERS:2 P07_WORKERS:2 P07_ROUNDS:1 P07_CLAIMS_PER_SECOND:4; do
        key=${pair%%:*}
        expected=${pair#*:}
        if [[ ${!key-$expected} != "$expected" ]]; then
            echo "Rejected $key: this entry supports only fixed-budget 1000-row 2/5 smoke" >&2
            return 2
        fi
    done
    case ${P07_STATS_PHASE-0} in 0|1) ;; *) echo "Rejected P07_STATS_PHASE: require 0 or 1" >&2; return 2 ;; esac
    case ${P07_STATS-0} in 0|1) ;; *) echo "Rejected P07_STATS: require 0 or 1" >&2; return 2 ;; esac
    case ${P07_PARALLEL_MODE-ready} in
        ready|spread-ready|none|all-blocked|leased|backoff|leased-heads|backoff-heads|dead-heads) ;;
        *) echo 'Rejected unknown parallel fixture' >&2; return 2 ;;
    esac
}
p07_parallel_preflight
