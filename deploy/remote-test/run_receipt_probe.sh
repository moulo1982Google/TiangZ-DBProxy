#!/usr/bin/env bash
# Container-side port of tools/run_receipt_probe.ps1 with the same evidence layout.
# Usage: run_receipt_probe.sh --run-id <id> [--rate 200] [--seconds 30] [--warmup 10] [--rounds 3]
#        [--payload 1024] [--concurrency 32] [--host-workers 4] [--load-workers 32]
#        [--cleanup both|on|off] [--sql-log-ms -1] [--read shared|dedicated|pooled]
#        [--client shared4|shared8|split2|split4] [--pacing tokio|std] [--retention-hours 24]
#        [--reconcile-budget 180] [--trickle N --trickle-span S [--trickle-lead 60]] [--background cleanup|all] [--fixture-expired 100000]
#        [--fixture-recent 100000] [--drop-new-indexes 0|1]
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

RUN_ID=""; RATE=200; SECONDS_=30; WARMUP=10; ROUNDS=3; PAYLOAD=1024; CONCURRENCY=32
HOST_WORKERS=4; LOAD_WORKERS=32; CLEANUP=both; SQL_LOG_MS=-1; READ=shared; CLIENT=shared4; PACING=tokio
RECONCILE_BUDGET=180; TRICKLE=0; TRICKLE_SPAN=0; TRICKLE_LEAD=60; RETENTION_HOURS=24; BACKGROUND=cleanup; FIXTURE_EXPIRED=100000; FIXTURE_RECENT=100000; DROP_NEW_INDEXES=0
while [[ $# -gt 0 ]]; do
    case $1 in
        --run-id) RUN_ID=$2;; --rate) RATE=$2;; --seconds) SECONDS_=$2;; --warmup) WARMUP=$2;;
        --rounds) ROUNDS=$2;; --payload) PAYLOAD=$2;; --concurrency) CONCURRENCY=$2;;
        --host-workers) HOST_WORKERS=$2;; --load-workers) LOAD_WORKERS=$2;; --cleanup) CLEANUP=$2;;
        --sql-log-ms) SQL_LOG_MS=$2;; --read) READ=$2;; --client) CLIENT=$2;; --pacing) PACING=$2;;
        --reconcile-budget) RECONCILE_BUDGET=$2;; --trickle) TRICKLE=$2;; --trickle-span) TRICKLE_SPAN=$2;; --trickle-lead) TRICKLE_LEAD=$2;; --retention-hours) RETENTION_HOURS=$2;; --background) BACKGROUND=$2;; --fixture-expired) FIXTURE_EXPIRED=$2;; --fixture-recent) FIXTURE_RECENT=$2;; --drop-new-indexes) DROP_NEW_INDEXES=$2;;
        *) echo "unknown argument $1" >&2; exit 2;;
    esac
    shift 2
done
[[ -n $RUN_ID ]] || { echo "--run-id is required" >&2; exit 2; }
require_run_id "$RUN_ID"
require_pglog
[[ $CLEANUP =~ ^(both|on|off)$ && $READ =~ ^(shared|dedicated|pooled)$ && $CLIENT =~ ^(shared4|shared8|split2|split4)$ && $PACING =~ ^(tokio|std)$ ]] || { echo "invalid mode" >&2; exit 2; }

ARTIFACTS="$EVIDENCE_ROOT/$RUN_ID"
[[ -e $ARTIFACTS ]] && { echo "Use a new RunId; old evidence is never overwritten" >&2; exit 2; }
mkdir -p "$ARTIFACTS"
HOST_EXE="$SRC/target/release/examples/acceptance_host"
LOAD_EXE="$SRC/target/release/examples/acceptance_load"
export ACCEPT_LISTEN=127.0.0.1:17980 ACCEPT_RATE=$RATE ACCEPT_SECONDS=$SECONDS_ ACCEPT_WARMUP_SECONDS=$WARMUP
export ACCEPT_PAYLOAD_BYTES=$PAYLOAD ACCEPT_CONCURRENCY=$CONCURRENCY ACCEPT_READ_CONNECTION=$READ
export ACCEPT_CLIENT_CONNECTIONS=$CLIENT ACCEPT_PACING_TIMER=$PACING
# The host cleanup worker and every fixture/audit below use the same retention.
export ACCEPT_RECEIPT_RETENTION_HOURS=$RETENTION_HOURS
# cleanup: receipt cleanup only (historical host); all: also backlog, cache repair and outbox workers.
[[ $BACKGROUND =~ ^(cleanup|all)$ ]] || { echo "invalid --background" >&2; exit 2; }
export ACCEPT_BACKGROUND=$BACKGROUND

TEMPLATE="${RUN_ID}_base"
pg_createdb "$TEMPLATE"
BASE="$ARTIFACTS/base"; mkdir -p "$BASE"
HOST_PID=$(start_host "$TEMPLATE" off "$BASE" "$HOST_WORKERS")
stop_host "$HOST_PID" "$BASE"
pg_db_sql "$TEMPLATE" >"$ARTIFACTS/seed.log" <<SQL
INSERT INTO dbproxy_idempotency(request_id,namespace,record_key,schema_name,schema_version,payload,revision,recorded_at)
SELECT 'fixture-'||n,'probe-fixture',n::text,'test',1,'',1,
CASE WHEN n<=$FIXTURE_EXPIRED THEN statement_timestamp()-interval '169 hours' ELSE statement_timestamp() END
FROM generate_series(1,$FIXTURE_EXPIRED+$FIXTURE_RECENT) n;
ANALYZE dbproxy_idempotency;
SQL
# Optional receipts that expire one after another during the run, so cleanup keeps working.
if (( TRICKLE > 0 )); then
    pg_db_sql "$TEMPLATE" >>"$ARTIFACTS/seed.log" <<SQL
INSERT INTO dbproxy_idempotency(request_id,namespace,record_key,schema_name,schema_version,payload,revision,recorded_at)
SELECT 'trickle-'||n,'probe-trickle',n::text,'test',1,'',1,
statement_timestamp()-make_interval(hours => $RETENTION_HOURS)+make_interval(secs => $TRICKLE_LEAD + n::double precision * $TRICKLE_SPAN / $TRICKLE)
FROM generate_series(1,$TRICKLE) n;
ANALYZE dbproxy_idempotency;
SQL
fi
cat >"$ARTIFACTS/manifest.json" <<EOF
{"RunId":"$RUN_ID","Rate":$RATE,"Seconds":$SECONDS_,"WarmupSeconds":$WARMUP,"Rounds":$ROUNDS,"PayloadBytes":$PAYLOAD,"Concurrency":$CONCURRENCY,"HostWorkers":$HOST_WORKERS,"LoadWorkers":$LOAD_WORKERS,"CleanupMode":"$CLEANUP","SqlLogThresholdMs":$SQL_LOG_MS,"ReadConnection":"$READ","ClientConnections":"$CLIENT","PacingTimer":"$PACING","FixtureRecent":$FIXTURE_RECENT,"FixtureExpired":$FIXTURE_EXPIRED,"DropNewIndexesAfterStartup":$DROP_NEW_INDEXES,"FixtureTrickle":$TRICKLE,"FixtureTrickleSpanSeconds":$TRICKLE_SPAN,"FixtureTrickleLeadSeconds":$TRICKLE_LEAD,"ReconcileBudgetSeconds":$RECONCILE_BUDGET,"ReceiptRetentionHours":$RETENTION_HOURS,"BackgroundWorkers":"$BACKGROUND","FullAcceptance":false,"Environment":"container-only workbench on $(hostname), cpuset $(cat /sys/fs/cgroup/cpuset.cpus.effective 2>/dev/null || echo unknown)"}
EOF
json_sha256 "$HOST_EXE" "$LOAD_EXE" >"$ARTIFACTS/binaries.json"

for ((round = 1; round <= ROUNDS; round++)); do
    if [[ $CLEANUP == both ]]; then
        if ((round % 2 == 1)); then modes=(off on); else modes=(on off); fi
    else modes=("$CLEANUP"); fi
    for mode in "${modes[@]}"; do
        name="${RUN_ID}_${round}_${mode}"
        folder="$ARTIFACTS/${round}_${mode}"; mkdir -p "$folder"
        pg_createdb "$name" "TEMPLATE \"$TEMPLATE\""
        pg_sql -c "ALTER DATABASE \"$name\" SET log_min_duration_statement = $SQL_LOG_MS" >"$folder/sql-logging.txt"
        log_offset=$(pglog_offset)
        export ACCEPT_RUN=$name ACCEPT_ARTIFACTS=$folder
        HOST_PID=$(start_host "$name" "$mode" "$folder" "$HOST_WORKERS")
        # P02 control: the host started (and passed the index check) with the full schema; only
        # this isolated round database then loses the indexes added by migrations 013-015, so
        # the same load measures their write cost. Never used against a service database.
        if [[ $DROP_NEW_INDEXES == 1 ]]; then
            pg_db_sql "$name" -c "DROP INDEX dbproxy_idempotency_retention, dbproxy_cache_repairs_unleased_order,
                dbproxy_cache_repairs_expired, dbproxy_cache_repairs_leased_order,
                dbproxy_ledger_postings_operation, dbproxy_outbox_operation" >"$folder/dropped-indexes.txt"
        fi
        status=0
        TOKIO_WORKER_THREADS="$LOAD_WORKERS" "$LOAD_EXE" >"$folder/load.out" 2>"$folder/load.err" &
        LOAD_PID=$!
        deadline=$(( $(date +%s) + WARMUP + SECONDS_ + RECONCILE_BUDGET ))
        while kill -0 "$LOAD_PID" 2>/dev/null; do
            sample_resources "$folder/process-resources.jsonl" "host=$HOST_PID" "load=$LOAD_PID"
            (( $(date +%s) > deadline )) && { echo "Load and reconciliation deadline exceeded" >&2; kill "$LOAD_PID"; status=1; break; }
            sleep 2
        done
        wait "$LOAD_PID" || { echo "Load failed; evidence preserved: $folder" >&2; status=1; }
        if [[ $status -eq 0 ]]; then
            pg_db_sql "$name" -At -c "SELECT count(*) FILTER (WHERE recorded_at < statement_timestamp()-make_interval(hours => $RETENTION_HOURS)),count(*) FILTER (WHERE recorded_at >= statement_timestamp()-make_interval(hours => $RETENTION_HOURS)) FROM dbproxy_idempotency WHERE namespace='probe-fixture'" >"$folder/remaining.txt"
            pg_db_sql "$name" -At -c "SELECT count(*) FILTER (WHERE recorded_at < statement_timestamp()-make_interval(hours => $RETENTION_HOURS)),count(*) FILTER (WHERE recorded_at >= statement_timestamp()-make_interval(hours => $RETENTION_HOURS)) FROM dbproxy_idempotency WHERE namespace='probe-trickle'" >"$folder/remaining-trickle.txt"
            echo "PASS round=$round cleanup=$mode"
            cat "$folder/summary.json"
        fi
        stop_host "$HOST_PID" "$folder" || status=1
        pglog_since "$log_offset" >"$folder/postgres.log"
        cat /sys/fs/cgroup/cpu.stat >"$folder/workbench-cpu.txt" 2>/dev/null || true
        [[ $status -eq 0 ]] || exit 1
    done
done
