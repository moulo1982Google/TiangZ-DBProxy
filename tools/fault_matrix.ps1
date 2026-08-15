$ErrorActionPreference = "Stop"

$composeFile = "deploy/local/docker-compose.yml"
$envFile = "deploy/local/.env"
$env:DBPROXY_POSTGRES_URL = "postgres://tiangz:tiangz_dev@127.0.0.1:5432/tiangz"
$env:DBPROXY_REDIS_URL = "redis://:tiangz_dev@127.0.0.1:6379/0"
$env:DBPROXY_RUN_DOCKER_FAULTS = "1"

docker compose --env-file $envFile -f $composeFile up -d
try {
    cargo test -p tiangz-dbproxy-storage --test fault_matrix -- --ignored --nocapture --test-threads=1
    if ($LASTEXITCODE -ne 0) {
        throw "DBProxy fault matrix failed with exit code $LASTEXITCODE"
    }
}
finally {
    docker compose --env-file $envFile -f $composeFile up -d
}
