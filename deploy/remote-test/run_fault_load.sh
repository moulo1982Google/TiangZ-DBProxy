#!/usr/bin/env bash
# Container-side port of tools/run_fault_load.ps1: one fault during a fixed-rate load, judged per
# phase by tools/analyze_fault_load.mjs against rules written to fault-plan.json before the run.
# Usage: run_fault_load.sh --run-id <id> [--fault blocked_write|kill_connections] [--rate 200]
#        [--seconds 60] [--warmup 10] [--fault-start 30] [--fault-duration 5] [--grace 5]
#        [--payload 1024] [--concurrency 32] [--host-workers 4] [--load-workers 32]
#        [--sql-log-ms 20] [--client split4]
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

RUN_ID=""; FAULT=blocked_write; RATE=200; SECONDS_=60; WARMUP=10; FAULT_START=30; FAULT_DURATION=5; GRACE=5
PAYLOAD=1024; CONCURRENCY=32; HOST_WORKERS=4; LOAD_WORKERS=32; SQL_LOG_MS=20; CLIENT=split4
while [[ $# -gt 0 ]]; do
    case $1 in
        --run-id) RUN_ID=$2;; --fault) FAULT=$2;; --rate) RATE=$2;; --seconds) SECONDS_=$2;; --warmup) WARMUP=$2;;
        --fault-start) FAULT_START=$2;; --fault-duration) FAULT_DURATION=$2;; --grace) GRACE=$2;;
        --payload) PAYLOAD=$2;; --concurrency) CONCURRENCY=$2;; --host-workers) HOST_WORKERS=$2;;
        --load-workers) LOAD_WORKERS=$2;; --sql-log-ms) SQL_LOG_MS=$2;; --client) CLIENT=$2;;
        *) echo "unknown argument $1" >&2; exit 2;;
    esac
    shift 2
done
[[ -n $RUN_ID ]] || { echo "--run-id is required" >&2; exit 2; }
require_run_id "$RUN_ID"
require_pglog
[[ $FAULT =~ ^(blocked_write|kill_connections)$ ]] || { echo "invalid --fault" >&2; exit 2; }
(( FAULT_START < WARMUP + SECONDS_ )) || { echo "--fault-start must fall inside warmup+sample time" >&2; exit 2; }

ARTIFACTS="$EVIDENCE_ROOT/$RUN_ID"
[[ -e $ARTIFACTS ]] && { echo "Use a new RunId; old evidence is never overwritten" >&2; exit 2; }
mkdir -p "$ARTIFACTS"
HOST_EXE="$SRC/target/release/examples/acceptance_host"
LOAD_EXE="$SRC/target/release/examples/acceptance_load"
INJECTOR_EXE="$SRC/target/release/examples/acceptance_fault_injector"
DATABASE="fault_load_$RUN_ID"
export DBPROXY_TEST_POSTGRES_URL="${PG_BASE_URL}/${DATABASE}" DBPROXY_FAULT_DATABASE=$DATABASE
export ACCEPT_LISTEN=127.0.0.1:17981 ACCEPT_RATE=$RATE ACCEPT_SECONDS=$SECONDS_ ACCEPT_WARMUP_SECONDS=$WARMUP
export ACCEPT_PAYLOAD_BYTES=$PAYLOAD ACCEPT_CONCURRENCY=$CONCURRENCY ACCEPT_READ_CONNECTION=pooled
export ACCEPT_CLIENT_CONNECTIONS=$CLIENT ACCEPT_PACING_TIMER=std ACCEPT_CLEANUP=on ACCEPT_FAULT_MODE=1
export ACCEPT_FAULT_KIND=$FAULT ACCEPT_FAULT_START_SECONDS=$FAULT_START ACCEPT_FAULT_DURATION_SECONDS=$FAULT_DURATION
export ACCEPT_RUN=$DATABASE ACCEPT_ARTIFACTS=$ARTIFACTS ACCEPT_STOP_FILE="$ARTIFACTS/stop"

pg_createdb "$DATABASE"
pg_sql -c "ALTER DATABASE \"$DATABASE\" SET log_min_duration_statement = $SQL_LOG_MS" >"$ARTIFACTS/sql-logging.txt"
# Rules are fixed before anything runs; identical to tools/run_fault_load.ps1 (rule version 2).
cat >"$ARTIFACTS/fault-plan.json" <<EOF
{"fault_kind":"$FAULT","fault_start_seconds":$FAULT_START,"fault_duration_seconds":$FAULT_DURATION,"recovery_grace_seconds":$GRACE,
"allowed_fault_error_patterns":["timed out","connection","closed","unavailable","unusable","terminat"],
"phase_rule_version":2,
"phase_assignment":"before = completed before injection; during = in-flight interval overlaps [injection, release + $GRACE s]; after = sent after that window; missed sends by scheduled instant",
"normal_phase":"zero errors, zero wrong data, zero missed sends",
"fault_phase":"zero wrong data; missed sends allowed (in-flight limit); only connection/timeout/unavailable errors allowed",
"recovery_phase":"zero errors and zero missed sends for every request sent later than fault release + $GRACE s",
"reconciliation":"every write either absent (rolled back) or exactly revision 1 with the intended payload; confirmed writes must exist",
"rate":$RATE,"sample_seconds":$SECONDS_,"warmup_seconds":$WARMUP,"payload_bytes":$PAYLOAD,"concurrency":$CONCURRENCY,
"host_workers":$HOST_WORKERS,"load_workers":$LOAD_WORKERS,"read_connection":"pooled","client_connections":"$CLIENT",
"pacing_timer":"std","cleanup":"on","fixtures":"none","full_acceptance":false,
"environment":"container-only workbench on $(hostname), cpuset $(cat /sys/fs/cgroup/cpuset.cpus.effective 2>/dev/null || echo unknown)"}
EOF
json_sha256 "$HOST_EXE" "$LOAD_EXE" "$INJECTOR_EXE" >"$ARTIFACTS/binaries.json"
log_offset=$(pglog_offset)
HOST_PID=$(start_host "$DATABASE" on "$ARTIFACTS" "$HOST_WORKERS")
status=0
TOKIO_WORKER_THREADS="$LOAD_WORKERS" "$LOAD_EXE" >"$ARTIFACTS/load.out" 2>"$ARTIFACTS/load.err" &
LOAD_PID=$!
TOKIO_WORKER_THREADS=2 "$INJECTOR_EXE" >"$ARTIFACTS/injector.out" 2>"$ARTIFACTS/injector.err" &
INJECTOR_PID=$!
deadline=$(( $(date +%s) + WARMUP + SECONDS_ + FAULT_DURATION + 180 ))
while kill -0 "$LOAD_PID" 2>/dev/null; do
    sample_resources "$ARTIFACTS/process-resources.jsonl" "host=$HOST_PID" "load=$LOAD_PID"
    (( $(date +%s) > deadline )) && { echo "Load and reconciliation deadline exceeded" >&2; kill "$LOAD_PID"; status=1; break; }
    sleep 2
done
for _ in $(seq 1 300); do kill -0 "$INJECTOR_PID" 2>/dev/null || break; sleep 0.1; done
if kill -0 "$INJECTOR_PID" 2>/dev/null; then kill "$INJECTOR_PID"; echo "Injector did not finish" >&2; status=1; fi
wait "$INJECTOR_PID" || { echo "Injector failed; evidence preserved: $ARTIFACTS" >&2; status=1; }
wait "$LOAD_PID" || { echo "Load failed on data or diagnostics; evidence preserved: $ARTIFACTS" >&2; status=1; }
stop_host "$HOST_PID" "$ARTIFACTS" || status=1
pglog_since "$log_offset" >"$ARTIFACTS/postgres.log"
cat /sys/fs/cgroup/cpu.stat >"$ARTIFACTS/workbench-cpu.txt" 2>/dev/null || true
[[ $status -eq 0 ]] || exit 1
if node "$SRC/tools/analyze_fault_load.mjs" "$ARTIFACTS" >"$ARTIFACTS/phase-analysis.log" 2>&1; then
    cat "$ARTIFACTS/phase-analysis.log"
    echo "PASS fault=$FAULT run=$RUN_ID"
else
    cat "$ARTIFACTS/phase-analysis.log"
    echo "Fault run did not pass the pre-declared phase rules; evidence preserved: $ARTIFACTS" >&2
    exit 1
fi
