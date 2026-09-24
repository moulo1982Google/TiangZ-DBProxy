#!/usr/bin/env bash
# Runs several single-fault load rounds one after another inside one workbench container, so a
# detached launcher can start a whole series. Each argument is one quoted run_fault_load.sh
# argument list; a failed round is recorded and the series continues.
# Usage: run_fault_series.sh <series-log> "<args of round 1>" "<args of round 2>" ...
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
LOG=$1; shift
status=0
for args in "$@"; do
    echo "=== run_fault_load.sh $args ===" >>"$LOG"
    # shellcheck disable=SC2086 # each argument is an intentional word list
    if "$(dirname "${BASH_SOURCE[0]}")/run_fault_load.sh" $args >>"$LOG" 2>&1; then
        echo "ROUND_OK $args" >>"$LOG"
    else
        status=1
        echo "ROUND_FAILED $args" >>"$LOG"
    fi
done
echo "FAULT_SERIES_DONE status=$status" >>"$LOG"
exit "$status"
