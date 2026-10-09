#!/usr/bin/env bash
# Read-only boundary snapshot. Never change limits, mounts or service state.
set -euo pipefail
OUT=${1:?new evidence file required}
[[ ! -e $OUT && ! -e $OUT.pending ]] || exit 2
[[ $(docker info --format '{{.DockerRootDir}}') == /sas/docker ]] || exit 2
for service in postgres redis cache; do
    case $service in
        postgres) expected='8589934592|8589934592|4000000000|14-17,42-45' ;;
        redis) expected='2147483648|4294967296|0|18,46' ;;
        cache) expected='2147483648|4294967296|0|19,47' ;;
    esac
    actual=$(docker inspect --format '{{.HostConfig.Memory}}|{{.HostConfig.MemorySwap}}|{{.HostConfig.NanoCpus}}|{{.HostConfig.CpusetCpus}}' "dbproxy-test-$service")
    [[ $actual == "$expected" ]] || { echo "Unexpected $service resource limits" >&2; exit 2; }
    [[ $(docker inspect --format '{{.State.Running}}|{{.State.OOMKilled}}|{{.HostConfig.NetworkMode}}' "dbproxy-test-$service") == 'true|false|dbproxy-test' ]] || exit 2
    actual_mounts=$(docker inspect --format '{{range .Mounts}}{{.Type}}|{{.Source}}|{{.Destination}}|{{.RW}}{{println}}{{end}}' "dbproxy-test-$service" | sed '/^$/d' | sort)
    if [[ $service == postgres ]]; then
        expected_mounts=$'bind|/data/dbproxy-test/pgdata|/var/lib/postgresql|true\nbind|/data/dbproxy-test/pglog|/pglog|true'
    else
        expected_mounts="bind|/data/dbproxy-test/$service|/data|true"
    fi
    [[ $actual_mounts == "$expected_mounts" ]] || { echo "Unexpected $service data mounts" >&2; exit 2; }
done
{
    printf '{"schema":1,"kind":"boundary","at":"%s","docker_root":"/sas/docker"}\n' "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    docker inspect --format '{"kind":"service","name":{{json .Name}},"id":{{json .Id}},"image":{{json .Image}},"started_at":{{json .State.StartedAt}},"running":{{json .State.Running}},"oom":{{json .State.OOMKilled}},"memory":{{.HostConfig.Memory}},"swap":{{.HostConfig.MemorySwap}},"nano_cpus":{{.HostConfig.NanoCpus}},"cpuset":{{json .HostConfig.CpusetCpus}},"network":{{json .HostConfig.NetworkMode}},"mounts":{{json .Mounts}}}' dbproxy-test-postgres dbproxy-test-redis dbproxy-test-cache
    printf '{"kind":"boundary_end","at":"%s"}\n' "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
} >"$OUT.pending"
mv -n "$OUT.pending" "$OUT"
