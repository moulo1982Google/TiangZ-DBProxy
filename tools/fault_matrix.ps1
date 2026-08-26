$ErrorActionPreference = "Stop"

$composeFile = "deploy/local/docker-compose.yml"
$laptopComposeFile = "deploy/local/docker-compose.laptop.yml"
$envFile = "deploy/local/.env"
$env:DBPROXY_POSTGRES_URL = "postgres://tiangz:tiangz_dev@127.0.0.1:5432/tiangz"
$env:DBPROXY_REDIS_URL = "redis://:tiangz_dev@127.0.0.1:6379/15"
$env:DBPROXY_RUN_DOCKER_FAULTS = "1"
$env:DBPROXY_REDIS_APPENDONLY = "yes"

docker compose --env-file $envFile -f $composeFile -f $laptopComposeFile up -d postgres redis
try {
    # Database 15 is reserved for this destructive local fault drill. Starting clean makes
    # fixed backlog keys deterministic without touching the normal development database 0.
    docker exec tiangz-dbproxy-redis redis-cli -a tiangz_dev --no-auth-warning -n 15 FLUSHDB | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Could not reset the dedicated Redis fault-matrix database"
    }
    cargo test -p tiangz-dbproxy-storage --test fault_matrix -- --ignored --nocapture --test-threads=1
    if ($LASTEXITCODE -ne 0) {
        throw "DBProxy fault matrix failed with exit code $LASTEXITCODE"
    }
}
finally {
    docker compose --env-file $envFile -f $composeFile -f $laptopComposeFile up -d postgres redis
}
