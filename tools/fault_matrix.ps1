param(
    [Parameter(Mandatory)][string]$Project,
    [Parameter(Mandatory)][string]$PostgresUrl,
    [Parameter(Mandatory)][string]$RedisUrl,
    [Parameter(Mandatory)][string]$CacheRedisUrl
)
$ErrorActionPreference = 'Stop'
# No implicit FLUSHDB, provisioning or fixed global container names.
$env:DBPROXY_RUN_DOCKER_FAULTS='1'
$env:DBPROXY_TEST_COMPOSE_PROJECT=$Project
$env:DBPROXY_TEST_POSTGRES_CONTAINER="$Project-postgres"
$env:DBPROXY_TEST_REDIS_CONTAINER="$Project-redis"
$env:DBPROXY_TEST_CACHE_CONTAINER="$Project-cache"
$env:DBPROXY_POSTGRES_URL=$PostgresUrl
$env:DBPROXY_REDIS_URL=$RedisUrl
$env:DBPROXY_CACHE_REDIS_URL=$CacheRedisUrl
cargo test -p tiangz-dbproxy-storage --test fault_matrix --locked -- --ignored --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw 'Fault matrix failed; preserve its output and test data.' }
