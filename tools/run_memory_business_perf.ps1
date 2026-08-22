param(
    [int[]]$Players = @(100, 300, 500),
    [string[]]$Workloads = @("playerDataBatch", "pickup", "npcShop"),
    [int]$DomainCount = 5,
    [int]$DurationSeconds = 5,
    [int]$Rounds = 3,
    [int]$ClientPoolSize = 64,
    [string]$ConfigFile = "configs/perf-memory-4.json"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$server = Join-Path $root "target/release/tiangz-dbproxy-server.exe"
$load = Join-Path $root "target/release/dbproxy_business_load.exe"
$resultRoot = Join-Path $root "perf/results"
$runId = Get-Date -Format "yyyyMMdd_HHmmss"
$runDirectory = Join-Path $resultRoot "memory_business_$runId"
$results = [System.Collections.Generic.List[object]]::new()

function Wait-TcpPort([string]$HostName, [int]$Port, [int]$TimeoutMs) {
    $deadline = [DateTime]::UtcNow.AddMilliseconds($TimeoutMs)
    while ([DateTime]::UtcNow -lt $deadline) {
        $client = [System.Net.Sockets.TcpClient]::new()
        try {
            $task = $client.ConnectAsync($HostName, $Port)
            if ($task.Wait(250) -and $client.Connected) { return }
        }
        catch {}
        finally { $client.Dispose() }
        Start-Sleep -Milliseconds 50
    }
    throw "Timed out waiting for ${HostName}:${Port}"
}

if ($DurationSeconds -le 0 -or $Rounds -le 0 -or $ClientPoolSize -le 0) {
    throw "DurationSeconds, Rounds, and ClientPoolSize must be positive"
}
if (-not $env:DBPROXY_AUTH_TOKEN) {
    $env:DBPROXY_AUTH_TOKEN = "local-memory-perf-token-1234"
}

Push-Location $root
try {
    New-Item -ItemType Directory -Force -Path $runDirectory | Out-Null
    cargo build --release -p tiangz-dbproxy-server --bin tiangz-dbproxy-server
    if ($LASTEXITCODE -ne 0) { throw "DBProxy server release build failed" }
    cargo build --release --bin dbproxy_business_load
    if ($LASTEXITCODE -ne 0) { throw "DBProxy load generator release build failed" }

    foreach ($playerCount in $Players) {
        foreach ($workload in $Workloads) {
            foreach ($round in 1..$Rounds) {
                $caseName = "${workload}_${playerCount}p_r${round}"
                Write-Host "[memory-business] $caseName"
                $serverStdout = Join-Path $runDirectory "${caseName}_server_stdout.log"
                $serverStderr = Join-Path $runDirectory "${caseName}_server_stderr.log"
                $loadStdout = Join-Path $runDirectory "${caseName}_load_stdout.log"
                $loadStderr = Join-Path $runDirectory "${caseName}_load_stderr.log"
                $serverProcess = $null
                $loadProcess = $null
                try {
                    $serverProcess = Start-Process -FilePath $server -ArgumentList @("--config", $ConfigFile) `
                        -WorkingDirectory $root -WindowStyle Hidden -PassThru `
                        -RedirectStandardOutput $serverStdout -RedirectStandardError $serverStderr
                    Wait-TcpPort "127.0.0.1" 7810 15000

                    $serverProcess.Refresh()
                    $startedAt = [DateTime]::UtcNow
                    $initialCpuSeconds = $serverProcess.TotalProcessorTime.TotalSeconds
                    [long]$peakRssBytes = $serverProcess.WorkingSet64
                    $loadProcess = Start-Process -FilePath $load -ArgumentList @(
                        "--endpoint", "127.0.0.1:7810",
                        "--pool-size", $ClientPoolSize,
                        "--players", $playerCount,
                        "--duration", $DurationSeconds,
                        "--domain-count", $DomainCount,
                        "--workloads", $workload
                    ) -WorkingDirectory $root -WindowStyle Hidden -PassThru `
                        -RedirectStandardOutput $loadStdout -RedirectStandardError $loadStderr

                    while (-not $loadProcess.HasExited) {
                        if ($serverProcess.HasExited) {
                            throw "DBProxy exited during $caseName"
                        }
                        $serverProcess.Refresh()
                        $peakRssBytes = [Math]::Max($peakRssBytes, $serverProcess.WorkingSet64)
                        Start-Sleep -Milliseconds 100
                    }
                    $loadProcess.WaitForExit()
                    $loadExitCode = $loadProcess.ExitCode
                    if ($null -ne $loadExitCode -and $loadExitCode -ne 0) {
                        throw "Load generator failed for $caseName`: $(Get-Content $loadStderr -Raw)"
                    }
                    $serverProcess.Refresh()
                    $elapsedSeconds = ([DateTime]::UtcNow - $startedAt).TotalSeconds
                    $cpuSeconds = $serverProcess.TotalProcessorTime.TotalSeconds - $initialCpuSeconds
                    $resultLine = Get-Content $loadStdout | Where-Object { $_.StartsWith("RESULT_JSON ") } | Select-Object -Last 1
                    if (-not $resultLine) { throw "Load generator returned no RESULT_JSON for $caseName" }
                    $business = $resultLine.Substring("RESULT_JSON ".Length) | ConvertFrom-Json
                    $results.Add([ordered]@{
                        caseName = $caseName
                        workload = $workload
                        players = $playerCount
                        round = $round
                        runtimeWorkerThreads = 4
                        storageBackend = "memory"
                        domainCount = $DomainCount
                        clientPoolSize = $ClientPoolSize
                        processCpuCorePercent = [Math]::Round(($cpuSeconds / $elapsedSeconds) * 100, 2)
                        processCpuMachinePercent = [Math]::Round(($cpuSeconds / $elapsedSeconds) * 100 / [Environment]::ProcessorCount, 2)
                        peakRssBytes = $peakRssBytes
                        business = $business
                    })
                }
                finally {
                    if ($loadProcess -and -not $loadProcess.HasExited) {
                        Stop-Process -Id $loadProcess.Id -Force -ErrorAction SilentlyContinue
                        $loadProcess.WaitForExit()
                    }
                    if ($serverProcess -and -not $serverProcess.HasExited) {
                        Stop-Process -Id $serverProcess.Id -Force -ErrorAction SilentlyContinue
                        $serverProcess.WaitForExit()
                    }
                }
            }
        }
    }

    $report = [ordered]@{
        generatedAt = [DateTime]::UtcNow.ToString("o")
        backend = "memory"
        runtimeWorkerThreads = 4
        storageShards = 16
        parameters = [ordered]@{
            players = $Players
            workloads = $Workloads
            durationSeconds = $DurationSeconds
            domainCount = $DomainCount
            rounds = $Rounds
            clientPoolSize = $ClientPoolSize
        }
        machine = [ordered]@{
            logicalProcessors = [Environment]::ProcessorCount
            os = [Environment]::OSVersion.VersionString
        }
        rounds = $results
    }
    $reportPath = Join-Path $resultRoot "memory_business_$runId.json"
    $latestPath = Join-Path $resultRoot "memory_business_latest.json"
    $report | ConvertTo-Json -Depth 10 | Set-Content -Encoding utf8 $reportPath
    $report | ConvertTo-Json -Depth 10 | Set-Content -Encoding utf8 $latestPath
    Write-Host "[memory-business] report: $reportPath"
}
finally {
    Pop-Location
}
