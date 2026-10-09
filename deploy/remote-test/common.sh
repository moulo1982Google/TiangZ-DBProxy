# Shared helpers for the container-only probes. Sourced by run_receipt_probe.sh / run_fault_load.sh.
# Runs inside the workbench container; PostgreSQL/Redis are reached over the compose network.
set -euo pipefail

: "${PG_BASE_URL:=postgres://tiangz:tiangz_dev@postgres:5432}"
: "${REDIS_URL:=redis://:tiangz_dev@redis:6379/10}"
: "${CACHE_REDIS_URL:=redis://:tiangz_dev@cache:6379/10}"
: "${EVIDENCE_ROOT:=/evidence}"
: "${PGLOG_FILE:=/pglog/postgresql.log}"
: "${SRC:=/src}"

pg_admin_url() { echo "${PG_BASE_URL}/postgres"; }
# psql/createdb against the admin database; ON_ERROR_STOP keeps failures loud.
pg_sql() { psql "$(pg_admin_url)" -v ON_ERROR_STOP=1 -X -q "$@"; }
pg_db_sql() { local db=$1; shift; psql "${PG_BASE_URL}/${db}" -v ON_ERROR_STOP=1 -X -q "$@"; }
pg_createdb() { local db=$1; shift; psql "$(pg_admin_url)" -v ON_ERROR_STOP=1 -X -q -c "CREATE DATABASE \"$db\" $*"; }

require_run_id() {
    [[ "$1" =~ ^[a-z][a-z0-9_]{0,24}$ ]] || { echo "RunId must be a short lowercase SQL identifier" >&2; exit 2; }
}

# The PostgreSQL log is shared through a bind mount; capture the window by byte offset.
# It must be readable: PostgreSQL creates it 0600 unless log_file_mode is relaxed, and an
# unreadable log once produced silently empty evidence.
require_pglog() {
    [[ -r $PGLOG_FILE ]] || { echo "PostgreSQL log $PGLOG_FILE is not readable by $(id -u); set log_file_mode=0644 and chmod it" >&2; exit 2; }
}
pglog_offset() { stat -c %s "$PGLOG_FILE"; }
pglog_since() { local offset=$1; tail -c +"$((offset + 1))" "$PGLOG_FILE"; }

# Start the acceptance host in the background; prints its PID after READY appears.
# The exit status is written to <folder>/host.exit because the caller may not be its parent shell.
start_host() {
    local database=$1 mode=$2 folder=$3 workers=$4
    rm -f "$folder/host.exit"
    (
        # PG_URL_PARAMS (optional) reaches only the host, e.g. to switch the network guards off.
        TOKIO_WORKER_THREADS="$workers" DBPROXY_TEST_POSTGRES_URL="${PG_BASE_URL}/${database}${PG_URL_PARAMS:+?$PG_URL_PARAMS}" \
        DBPROXY_REDIS_URL="$REDIS_URL" DBPROXY_CACHE_REDIS_URL="$CACHE_REDIS_URL" \
        ACCEPT_CLEANUP="$mode" ACCEPT_STOP_FILE="$folder/stop" \
        "$SRC/target/release/examples/acceptance_host" >"$folder/host.out" 2>"$folder/host.err"
        echo $? >"$folder/host.exit"
    # The subshell must not hold this function's stdout: `HOST_PID=$(start_host ...)` reads until
    # every writer closes the pipe, which would otherwise mean waiting for the host itself.
    ) >/dev/null 2>&1 &
    local wrapper=$!
    local pid=""
    for _ in $(seq 1 200); do
        [[ -z $pid ]] && pid=$(pgrep -P "$wrapper" -x acceptance_host 2>/dev/null | head -1)
        if [[ -f $folder/host.exit ]]; then echo "Host exited; see $folder" >&2; return 1; fi
        if grep -q READY "$folder/host.out" 2>/dev/null && [[ -n $pid ]]; then echo "$pid"; return 0; fi
        sleep 0.1
    done
    [[ -n $pid ]] && kill "$pid" 2>/dev/null
    echo "Host readiness timed out" >&2
    return 1
}

# Graceful stop through the owned stop file; only the PID started above is ever killed.
stop_host() {
    local pid=$1 folder=$2
    touch "$folder/stop"
    for _ in $(seq 1 200); do
        [[ -f $folder/host.exit ]] && break
        sleep 0.1
    done
    if [[ ! -f $folder/host.exit ]]; then
        kill "$pid" 2>/dev/null || true
        echo "Host did not stop gracefully" >&2
        return 1
    fi
    local code; code=$(cat "$folder/host.exit")
    [[ $code -eq 0 ]] || { echo "Host failed during shutdown (exit $code)" >&2; return 1; }
}

# ps-based resource samples for the given PIDs, one JSON line per call.
sample_resources() {
    local out=$1; shift
    local now; now=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
    local line="{\"utc\":\"$now\""
    for entry in "$@"; do
        local name=${entry%%=*} pid=${entry#*=}
        local cpu rss
        read -r cpu rss < <(ps -o cputimes=,rss= -p "$pid" 2>/dev/null || echo "0 0")
        line+=",\"${name}CpuSeconds\":${cpu:-0},\"${name}RssKb\":${rss:-0}"
    done
    echo "$line}" >>"$out"
}

json_sha256() {
    # {"file":"sha"} entries for the given paths.
    local first=1; printf '{'
    for f in "$@"; do
        [[ $first -eq 1 ]] || printf ','
        first=0
        printf '"%s":"%s"' "$(basename "$f")" "$(sha256sum "$f" | cut -d' ' -f1)"
    done
    printf '}\n'
}
