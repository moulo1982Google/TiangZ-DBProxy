param(
    [ValidateRange(120, 86400)]
    [int]$DurationSeconds = 7200,
    [ValidateRange(1, 10000)]
    [int]$Players = 100,
    [ValidateRange(1, 512)]
    [int]$PoolSize = 32,
    [ValidateRange(100, 60000)]
    [int]$CycleMs = 1000,
    [string]$Endpoint = "127.0.0.1:7800",
    [string]$MetricsEndpoint = "http://127.0.0.1:9090/metrics"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$binary = Join-Path $root "target/release/dbproxy_fault_soak.exe"
$artifactRoot = Join-Path (Split-Path -Parent $root) ".build-tmp/dbproxy-fault-drill"
$runDirectory = Join-Path $artifactRoot ("{0}-{1}s" -f (Get-Date -Format "yyyyMMdd-HHmmss"), $DurationSeconds)
$stdoutPath = Join-Path $runDirectory "workload.stdout.log"
$stderrPath = Join-Path $runDirectory "workload.stderr.log"
$eventPath = Join-Path $runDirectory "events.jsonl"
$samplePath = Join-Path $runDirectory "samples.jsonl"
$postgresContainer = "tiangz-dbproxy-postgres"
$redisContainer = "tiangz-dbproxy-redis"
$loadProcess = $null
$phase = "initializing"
$aofPendingBeforeKill = $null

function Write-DrillEvent {
    param(
        [string]$Name,
        [double]$ElapsedSeconds,
        [hashtable]$Details = @{}
    )
    $event = [ordered]@{
        timestamp = (Get-Date).ToUniversalTime().ToString("o")
        elapsedSeconds = [Math]::Round($ElapsedSeconds, 3)
        phase = $script:phase
        event = $Name
        details = $Details
    }
    $line = $event | ConvertTo-Json -Compress -Depth 6
    Add-Content -LiteralPath $script:eventPath -Value $line -Encoding utf8
    Write-Output ("DRILL_EVENT {0}" -f $line)
}

function Get-ContainerState {
    param([string]$Name)
    $state = docker inspect $Name --format "{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}" 2>$null
    if ($LASTEXITCODE -ne 0) {
        return "missing|none"
    }
    return $state.Trim()
}

function Wait-ContainerHealthy {
    param(
        [string]$Name,
        [int]$TimeoutSeconds = 60
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $state = Get-ContainerState $Name
        if ($state -eq "running|healthy" -or $state -eq "running|none") {
            return
        }
        Start-Sleep -Milliseconds 500
    } while ((Get-Date) -lt $deadline)
    throw "Container $Name did not become healthy; current state: $(Get-ContainerState $Name)"
}

function Start-DrillContainer {
    param([string]$Name)
    $state = Get-ContainerState $Name
    if (-not $state.StartsWith("running|")) {
        docker start $Name | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Could not start container $Name"
        }
    }
    Wait-ContainerHealthy $Name
}

function Stop-DrillContainer {
    param([string]$Name)
    $state = Get-ContainerState $Name
    if ($state.StartsWith("running|")) {
        docker stop --time 10 $Name | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Could not stop container $Name"
        }
    }
}

function Kill-RedisAbruptly {
    $state = Get-ContainerState $script:redisContainer
    if (-not $state.StartsWith("running|")) {
        throw "Redis must be running before the abrupt AOF restart"
    }
    docker kill --signal KILL $script:redisContainer | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Could not kill Redis abruptly"
    }
}

function Get-Metrics {
    try {
        return (Invoke-WebRequest -UseBasicParsing -Uri $script:MetricsEndpoint -TimeoutSec 3).Content
    }
    catch {
        return $null
    }
}

function Get-MetricValue {
    param([string]$Name)
    $metrics = Get-Metrics
    if (-not $metrics) {
        return $null
    }
    $pattern = "(?m)^" + [regex]::Escape($Name) + "\s+([-+0-9.eE]+)\r?$"
    $match = [regex]::Match($metrics, $pattern)
    if (-not $match.Success) {
        return $null
    }
    return [double]::Parse($match.Groups[1].Value, [Globalization.CultureInfo]::InvariantCulture)
}

function Capture-Sample {
    param([double]$ElapsedSeconds)
    $metrics = Get-Metrics
    $selected = @()
    if ($metrics) {
        $selected = $metrics -split "`n" | Where-Object {
            $_ -match '^dbproxy_(live|ready|dependency_up|requests_in_flight|cache_(hits|misses|read_errors|write_errors)|postgres_fallback|rpc_(requests|failures)|backlog_(pending|processing|oldest|polls)|cache_repair_(pending|processing|dead_lettered|oldest|worker)|outbox_(pending|processing|dead_lettered|oldest|worker))'
        } | ForEach-Object { $_.Trim() }
    }
    $dockerStats = docker stats --no-stream --format "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}" $script:postgresContainer $script:redisContainer 2>$null
    $sample = [ordered]@{
        timestamp = (Get-Date).ToUniversalTime().ToString("o")
        elapsedSeconds = [Math]::Round($ElapsedSeconds, 3)
        phase = $script:phase
        postgres = Get-ContainerState $script:postgresContainer
        redis = Get-ContainerState $script:redisContainer
        dockerStats = @($dockerStats)
        metrics = @($selected)
    }
    Add-Content -LiteralPath $script:samplePath -Value ($sample | ConvertTo-Json -Compress -Depth 5) -Encoding utf8
}

function Wait-MetricAtLeast {
    param(
        [string]$Name,
        [double]$Minimum,
        [int]$TimeoutSeconds = 30
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $value = Get-MetricValue $Name
        if ($null -ne $value -and $value -ge $Minimum) {
            return $value
        }
        Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $deadline)
    throw "Metric $Name did not reach $Minimum within $TimeoutSeconds seconds"
}

function Wait-QueuesDrained {
    param([int]$TimeoutSeconds = 180)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $backlog = Get-MetricValue "dbproxy_backlog_pending"
        $backlogProcessing = Get-MetricValue "dbproxy_backlog_processing"
        $repair = Get-MetricValue "dbproxy_cache_repair_pending"
        $repairProcessing = Get-MetricValue "dbproxy_cache_repair_processing"
        $outbox = Get-MetricValue "dbproxy_outbox_pending"
        $outboxProcessing = Get-MetricValue "dbproxy_outbox_processing"
        if ($null -ne $backlog -and $backlog -eq 0 -and
            $backlogProcessing -eq 0 -and $repair -eq 0 -and
            $repairProcessing -eq 0 -and $outbox -eq 0 -and
            $outboxProcessing -eq 0) {
            return
        }
        Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $deadline)
    throw "DBProxy queues did not drain within $TimeoutSeconds seconds"
}

function Invoke-ScheduledAction {
    param(
        [string]$Name,
        [double]$ElapsedSeconds
    )
    Capture-Sample $ElapsedSeconds
    switch ($Name) {
        "redis_stop" {
            Stop-DrillContainer $script:redisContainer
        }
        "redis_start" {
            Start-DrillContainer $script:redisContainer
        }
        "postgres_stop" {
            Stop-DrillContainer $script:postgresContainer
        }
        "postgres_start" {
            Start-DrillContainer $script:postgresContainer
        }
        "postgres_stop_for_aof" {
            Stop-DrillContainer $script:postgresContainer
        }
        "redis_kill_with_backlog" {
            $script:aofPendingBeforeKill = Wait-MetricAtLeast "dbproxy_backlog_pending" 1 30
            Kill-RedisAbruptly
        }
        "redis_start_after_kill" {
            Start-DrillContainer $script:redisContainer
            $afterRestart = Wait-MetricAtLeast "dbproxy_backlog_pending" 1 30
            Write-DrillEvent "aof_backlog_survived_restart" $ElapsedSeconds @{
                pendingBeforeKill = $script:aofPendingBeforeKill
                pendingAfterRestart = $afterRestart
            }
        }
        "postgres_start_aof_drain" {
            Start-DrillContainer $script:postgresContainer
        }
        "both_stop" {
            Stop-DrillContainer $script:postgresContainer
            Stop-DrillContainer $script:redisContainer
        }
        "redis_start_post_joint" {
            Start-DrillContainer $script:redisContainer
        }
        "postgres_start_post_joint" {
            Start-DrillContainer $script:postgresContainer
        }
        default {
            throw "Unknown scheduled action: $Name"
        }
    }
    $script:phase = $Name
    Write-DrillEvent $Name $ElapsedSeconds
    Capture-Sample $ElapsedSeconds
}

if (-not (Test-Path -LiteralPath $binary)) {
    throw "Fault-soak binary is missing: $binary"
}
New-Item -ItemType Directory -Force -Path $runDirectory | Out-Null
Write-Output "DRILL_OUTPUT $runDirectory"

try {
    Start-DrillContainer $redisContainer
    Start-DrillContainer $postgresContainer
    $aofEnabled = docker exec $redisContainer redis-cli -a tiangz_dev --no-auth-warning CONFIG GET appendonly
    if ($LASTEXITCODE -ne 0 -or ($aofEnabled -join "`n") -notmatch "(?m)^yes$") {
        throw "Redis AOF must be enabled for this drill"
    }
    $ready = Invoke-WebRequest -UseBasicParsing -Uri ($MetricsEndpoint -replace "/metrics$", "/ready") -TimeoutSec 3
    if ($ready.StatusCode -ne 200) {
        throw "DBProxy is not ready"
    }

    if (-not $env:DBPROXY_AUTH_TOKEN) {
        $env:DBPROXY_AUTH_TOKEN = "tiangz-dbproxy-local-token-2026"
    }
    $reportInterval = if ($DurationSeconds -le 600) { 5 } else { 60 }
    $tradeIntervalCycles = if ($DurationSeconds -le 600) { 60 } else { 600 }
    $splitPool = $PoolSize -ge 2
    $writePoolSize = if ($splitPool) { [Math]::Max(1, [int][Math]::Floor($PoolSize / 4.0)) } else { $PoolSize }
    $readPoolSize = if ($splitPool) { $PoolSize - $writePoolSize } else { $PoolSize }
    $loadArguments = @(
        "--endpoint", $Endpoint,
        "--pool-size", $PoolSize,
        "--players", $Players,
        "--duration", $DurationSeconds,
        "--cycle-ms", $CycleMs,
        "--trade-interval-cycles", $tradeIntervalCycles,
        "--report-interval", $reportInterval,
        "--validation-timeout", 180
    )
    if ($splitPool) {
        $loadArguments += @(
            "--read-pool-size", $readPoolSize,
            "--write-pool-size", $writePoolSize
        )
    }
    $loadProcess = Start-Process -FilePath $binary -ArgumentList $loadArguments -WorkingDirectory $root -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -WindowStyle Hidden -PassThru

    $readyDeadline = (Get-Date).AddSeconds(180)
    do {
        if ($loadProcess.HasExited) {
            throw "Fault-soak workload exited during setup with code $($loadProcess.ExitCode)"
        }
        if ((Test-Path -LiteralPath $stdoutPath) -and
            (Select-String -LiteralPath $stdoutPath -Pattern '^SOAK_READY ' -Quiet)) {
            break
        }
        Start-Sleep -Milliseconds 500
    } while ((Get-Date) -lt $readyDeadline)
    if (-not (Test-Path -LiteralPath $stdoutPath) -or
        -not (Select-String -LiteralPath $stdoutPath -Pattern '^SOAK_READY ' -Quiet)) {
        throw "Fault-soak workload did not finish seeding within 180 seconds"
    }

    $baseSchedule = @(
        [pscustomobject]@{ At = 600; Name = "redis_stop" },
        [pscustomobject]@{ At = 1200; Name = "redis_start" },
        [pscustomobject]@{ At = 1800; Name = "postgres_stop" },
        [pscustomobject]@{ At = 2400; Name = "postgres_start" },
        [pscustomobject]@{ At = 3300; Name = "postgres_stop_for_aof" },
        [pscustomobject]@{ At = 3600; Name = "redis_kill_with_backlog" },
        [pscustomobject]@{ At = 3720; Name = "redis_start_after_kill" },
        [pscustomobject]@{ At = 4200; Name = "postgres_start_aof_drain" },
        [pscustomobject]@{ At = 5400; Name = "both_stop" },
        [pscustomobject]@{ At = 5700; Name = "redis_start_post_joint" },
        [pscustomobject]@{ At = 6300; Name = "postgres_start_post_joint" }
    )
    $schedule = $baseSchedule | ForEach-Object {
        [pscustomobject]@{
            At = [Math]::Max(1, [int][Math]::Round($_.At * $DurationSeconds / 7200.0))
            Name = $_.Name
        }
    }
    $script:phase = "healthy_baseline"
    $stopwatch = [Diagnostics.Stopwatch]::StartNew()
    Write-DrillEvent "workload_started" 0 @{
        durationSeconds = $DurationSeconds
        players = $Players
        poolSize = $PoolSize
        readPoolSize = $readPoolSize
        writePoolSize = $writePoolSize
        splitPool = $splitPool
        cycleMs = $CycleMs
        tradeIntervalCycles = $tradeIntervalCycles
    }
    Capture-Sample 0
    $actionIndex = 0
    $sampleInterval = if ($DurationSeconds -le 600) { 5 } else { 60 }
    $nextSampleAt = $sampleInterval
    $hardDeadline = (Get-Date).AddSeconds($DurationSeconds + 300)

    while (-not $loadProcess.HasExited) {
        $elapsed = $stopwatch.Elapsed.TotalSeconds
        while ($actionIndex -lt $schedule.Count -and $elapsed -ge $schedule[$actionIndex].At) {
            Invoke-ScheduledAction $schedule[$actionIndex].Name $elapsed
            $actionIndex++
            $elapsed = $stopwatch.Elapsed.TotalSeconds
        }
        if ($elapsed -ge $nextSampleAt) {
            Capture-Sample $elapsed
            $nextSampleAt += $sampleInterval
        }
        if ((Get-Date) -gt $hardDeadline) {
            throw "Fault-soak workload exceeded its duration and validation allowance"
        }
        Start-Sleep -Milliseconds 500
    }
    $loadProcess.WaitForExit()
    $loadExitCode = $loadProcess.ExitCode
    if ($null -ne $loadExitCode -and $loadExitCode -ne 0) {
        throw "Fault-soak workload failed with code $loadExitCode"
    }
    $finalResult = Select-String -LiteralPath $stdoutPath -Pattern '^SOAK_FINAL ' | Select-Object -Last 1
    if (-not $finalResult -or $finalResult.Line -notmatch '"passed":true') {
        throw "Fault-soak workload exited without a passing final reconciliation"
    }

    Start-DrillContainer $redisContainer
    Start-DrillContainer $postgresContainer
    Wait-QueuesDrained 180
    $deadRepair = Get-MetricValue "dbproxy_cache_repair_dead_lettered"
    $deadOutbox = Get-MetricValue "dbproxy_outbox_dead_lettered"
    if ($deadRepair -ne 0 -or $deadOutbox -ne 0) {
        throw "Dead-letter queues are not empty: cacheRepair=$deadRepair outbox=$deadOutbox"
    }
    $script:phase = "completed"
    Capture-Sample $stopwatch.Elapsed.TotalSeconds
    Write-DrillEvent "drill_completed" $stopwatch.Elapsed.TotalSeconds @{
        aofPendingBeforeKill = $aofPendingBeforeKill
        cacheRepairDeadLettered = $deadRepair
        outboxDeadLettered = $deadOutbox
    }
}
catch {
    $failedAt = if ($stopwatch) { $stopwatch.Elapsed.TotalSeconds } else { 0 }
    Write-DrillEvent "drill_failed" $failedAt @{ error = $_.Exception.Message }
    throw
}
finally {
    try { Start-DrillContainer $redisContainer } catch { Write-Warning $_ }
    try { Start-DrillContainer $postgresContainer } catch { Write-Warning $_ }
    if ($loadProcess -and -not $loadProcess.HasExited) {
        Stop-Process -Id $loadProcess.Id -Force -ErrorAction SilentlyContinue
    }
}
