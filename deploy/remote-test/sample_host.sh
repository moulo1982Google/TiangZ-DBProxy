#!/usr/bin/env bash
# Host-wide disk counters, IO/CPU pressure and load every ~100 ms as JSON lines, until killed.
# /proc/diskstats and /proc/pressure are not namespaced, so this sees the whole host from inside
# a container. Uses only bash builtins in the loop to keep its own overhead negligible.
# Usage: sample_host.sh <output.jsonl>    (devices via SAMPLE_DEVICES, default "md0 nvme0n1 nvme1n1")
set -u
out=$1
devices=" ${SAMPLE_DEVICES:-md0 nvme0n1 nvme1n1} "
exec 3>>"$out"
while :; do
    now=${EPOCHREALTIME/./}
    line="{\"unix_us\":$now"
    # diskstats fields: reads, reads_merged, sectors_read, ms_reading, writes, writes_merged,
    # sectors_written, ms_writing, in_flight, ms_io, weighted_ms_io ...
    while read -r _ _ name rd _ _ rdms wr _ _ wrms inflight iomsec _; do
        [[ $devices == *" $name "* ]] && line+=",\"$name\":[$rd,$rdms,$wr,$wrms,$inflight,$iomsec]"
    done </proc/diskstats
    for kind in io cpu memory; do
        while read -r scope _ _ _ total; do
            line+=",\"psi_${kind}_${scope}_us\":${total#total=}"
        done </proc/pressure/$kind
    done
    read -r load1 _ </proc/loadavg
    line+=",\"load1\":$load1}"
    printf '%s\n' "$line" >&3
    read -rt 0.1 <> <(:) || true
done
