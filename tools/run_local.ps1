param(
    [string]$EnvironmentFile = "deploy/local/.env",
    [string]$ConfigFile = "configs/local.json"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
    $resolved = Resolve-Path $EnvironmentFile
    foreach ($line in Get-Content $resolved) {
        $value = $line.Trim()
        if (-not $value -or $value.StartsWith("#")) {
            continue
        }
        $separator = $value.IndexOf("=")
        if ($separator -le 0) {
            throw "Invalid environment line: $line"
        }
        $name = $value.Substring(0, $separator)
        $content = $value.Substring($separator + 1)
        [Environment]::SetEnvironmentVariable($name, $content, "Process")
    }

    cargo run -p tiangz-dbproxy-server -- --config $ConfigFile
    if ($LASTEXITCODE -ne 0) {
        throw "DBProxy exited with code $LASTEXITCODE"
    }
}
finally {
    Pop-Location
}
