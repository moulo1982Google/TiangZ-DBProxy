$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
    if (-not $env:DBPROXY_POSTGRES_URL) {
        $env:DBPROXY_POSTGRES_URL = "postgres://tiangz:tiangz_dev@127.0.0.1:5432/tiangz"
    }
    if (-not $env:DBPROXY_REDIS_URL) {
        $env:DBPROXY_REDIS_URL = "redis://:tiangz_dev@127.0.0.1:6379/0"
    }
    cargo test -p tiangz-dbproxy-server --test postgres_redis_network --locked -- --ignored --nocapture
    if ($LASTEXITCODE -ne 0) {
        throw "DBProxy network smoke failed with exit code $LASTEXITCODE"
    }
}
finally {
    Pop-Location
}
