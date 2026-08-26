[CmdletBinding()]
param(
    [ValidateSet("up", "down", "status", "pull")]
    [string]$Action = "up",

    [switch]$Aof
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $root "deploy/local/.env"
$composeFile = Join-Path $root "deploy/local/docker-compose.yml"
$laptopComposeFile = Join-Path $root "deploy/local/docker-compose.laptop.yml"

if (-not (Test-Path -LiteralPath $envFile)) {
    throw "Missing $envFile. Copy deploy/local/.env.example to deploy/local/.env first."
}

$env:DBPROXY_REDIS_APPENDONLY = if ($Aof) { "yes" } else { "no" }
$composeArgs = @(
    "compose",
    "--env-file", $envFile,
    "-f", $composeFile,
    "-f", $laptopComposeFile
)

function Invoke-Compose {
    param([string[]]$Arguments)

    & docker @($composeArgs + $Arguments)
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed with exit code $LASTEXITCODE"
    }
}

Push-Location $root
try {
    switch ($Action) {
        "pull" {
            Invoke-Compose @("pull", "postgres", "redis")
        }
        "up" {
            Invoke-Compose @("up", "-d", "--pull", "missing", "postgres", "redis")
            Invoke-Compose @("ps")
        }
        "down" {
            Invoke-Compose @("down")
        }
        "status" {
            Invoke-Compose @("ps")
        }
    }
}
finally {
    Pop-Location
}
