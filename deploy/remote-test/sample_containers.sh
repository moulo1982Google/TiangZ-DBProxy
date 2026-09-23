#!/usr/bin/env bash
# Host-side sampler for long runs: cgroup CPU/memory/IO of each test container and Redis INFO,
# one JSON line every INTERVAL seconds, until the named workbench container exits.
# Run on the host (needs docker access), not inside a container.
# Usage: sample_containers.sh <output.jsonl> <workbench-container> [interval-seconds=10]
set -u
out=$1; workbench=$2; interval=${3:-10}
containers=(dbproxy-test-postgres dbproxy-test-redis dbproxy-test-cache "$workbench")
declare -A cg
resolve() {
    for c in "${containers[@]}"; do
        [[ -n ${cg[$c]:-} && -d ${cg[$c]} ]] && continue
        local id; id=$(docker inspect -f '{{.Id}}' "$c" 2>/dev/null) || continue
        cg[$c]=/sys/fs/cgroup/system.slice/docker-$id.scope
    done
}
stat_field() { awk -v k="$2" '$1 == k { print $2; exit }' "$1" 2>/dev/null; }
io_bytes() { awk '{ for (i = 2; i <= NF; i++) { split($i, kv, "="); if (kv[1] == "rbytes") r += kv[2]; if (kv[1] == "wbytes") w += kv[2] } } END { printf "%d,%d", r, w }' "$1" 2>/dev/null; }
redis_info() {
    docker exec "$1" redis-cli -a tiangz_dev --no-auth-warning INFO 2>/dev/null | tr -d '\r' | awk -F: '
        /^used_memory:/ {m=$2} /^used_memory_rss:/ {rss=$2} /^aof_current_size:/ {aof=$2}
        /^total_commands_processed:/ {cmd=$2} /^evicted_keys:/ {ev=$2} /^expired_keys:/ {ex=$2}
        /^connected_clients:/ {cl=$2} /^rejected_connections:/ {rej=$2}
        /^db[0-9]+:keys=/ { split($2, a, ","); split(a[1], b, "="); keys += b[2] }
        END { printf "{\"used_memory\":%d,\"used_memory_rss\":%d,\"aof_current_size\":%d,\"commands\":%d,\"evicted_keys\":%d,\"expired_keys\":%d,\"clients\":%d,\"rejected_connections\":%d,\"keys\":%d}", m, rss, aof, cmd, ev, ex, cl, rej, keys }'
}
while :; do
    resolve
    line="{\"unix_ms\":$(date +%s%3N)"
    for c in "${containers[@]}"; do
        d=${cg[$c]:-}
        [[ -n $d && -d $d ]] || continue
        mem=$(cat "$d/memory.current" 2>/dev/null || echo 0)
        anon=$(stat_field "$d/memory.stat" anon); file=$(stat_field "$d/memory.stat" file)
        cpu=$(stat_field "$d/cpu.stat" usage_usec); thr=$(stat_field "$d/cpu.stat" throttled_usec)
        io=$(io_bytes "$d/io.stat")
        line+=",\"$c\":{\"memory\":${mem:-0},\"anon\":${anon:-0},\"file\":${file:-0},\"cpu_usec\":${cpu:-0},\"throttled_usec\":${thr:-0},\"io_read_bytes\":${io%,*},\"io_write_bytes\":${io#*,}}"
    done
    line+=",\"redis\":$(redis_info dbproxy-test-redis),\"cache\":$(redis_info dbproxy-test-cache)"
    read -r load1 _ </proc/loadavg
    line+=",\"host_load1\":$load1,\"host_mem_available_kb\":$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)}"
    printf '%s\n' "$line" >>"$out"
    # Stop once the workbench has existed and is no longer running.
    state=$(docker inspect -f '{{.State.Running}}' "$workbench" 2>/dev/null || echo missing)
    [[ $state == false ]] && break
    sleep "$interval"
done
