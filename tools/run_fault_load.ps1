param(
    [Parameter(Mandatory)][string]$RunId,
    [Parameter(Mandatory)][string]$PostgresContainer,
    [Parameter(Mandatory)][string]$Project,
    [Parameter(Mandatory)][string]$PostgresBaseUrl,
    [Parameter(Mandatory)][string]$RedisUrl,
    [Parameter(Mandatory)][string]$CacheRedisUrl,
    [ValidateSet('blocked_write','kill_connections')][string]$FaultKind='blocked_write',
    [int]$Rate=200, [int]$Seconds=60, [int]$WarmupSeconds=10,
    [ValidateRange(1,3600)][int]$FaultStartSeconds=30,
    [ValidateRange(0,3600)][int]$FaultDurationSeconds=5,
    [ValidateRange(0,600)][int]$RecoveryGraceSeconds=5,
    [int]$PayloadBytes=1024, [int]$Concurrency=32,
    [ValidateRange(1,64)][int]$HostWorkers=4,
    [ValidateRange(1,64)][int]$LoadWorkers=32,
    [ValidateRange(-1,10000)][int]$SqlLogThresholdMs=20,
    [ValidateSet('shared4','shared8','split2','split4')][string]$ClientConnections='split4'
)
# One fault during a fixed-rate load: normal -> inject -> recover, judged per phase by
# tools/analyze_fault_load.mjs against rules written to fault-plan.json before the run starts.
$ErrorActionPreference='Stop'
if($RunId -notmatch '^[a-z][a-z0-9_]{0,24}$'){throw 'RunId must be a short lowercase SQL identifier'}
if($FaultStartSeconds -ge ($WarmupSeconds+$Seconds)){throw 'FaultStartSeconds must fall inside warmup+sample time'}
# JSON labels avoid embedded double quotes, which Windows PowerShell 5.1 strips from native arguments.
$labels=docker inspect --format '{{json .Config.Labels}}' $PostgresContainer
$owner=if($LASTEXITCODE -eq 0 -and $labels){($labels | ConvertFrom-Json).'com.docker.compose.project'}
if($LASTEXITCODE -ne 0 -or $owner -ne $Project){throw 'Container project mismatch'}
$root=(Resolve-Path .).Path
$artifacts=Join-Path $root "target/$RunId"
if(Test-Path -LiteralPath $artifacts){throw 'Use a new RunId; old evidence is never overwritten'}
New-Item -ItemType Directory -Path $artifacts | Out-Null
$hostExe=(Resolve-Path target/release/examples/acceptance_host.exe).Path
$loadExe=(Resolve-Path target/release/examples/acceptance_load.exe).Path
$injectorExe=(Resolve-Path target/release/examples/acceptance_fault_injector.exe).Path
$database="fault_load_$RunId"
$env:DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION='1'
$env:DBPROXY_REDIS_URL=$RedisUrl
$env:DBPROXY_CACHE_REDIS_URL=$CacheRedisUrl
$env:DBPROXY_TEST_POSTGRES_URL="$PostgresBaseUrl/$database"
$env:DBPROXY_FAULT_DATABASE=$database
$env:ACCEPT_LISTEN='127.0.0.1:17981'
$env:ACCEPT_RATE="$Rate"; $env:ACCEPT_SECONDS="$Seconds"; $env:ACCEPT_WARMUP_SECONDS="$WarmupSeconds"
$env:ACCEPT_PAYLOAD_BYTES="$PayloadBytes"; $env:ACCEPT_CONCURRENCY="$Concurrency"
$env:ACCEPT_READ_CONNECTION='pooled'
$env:ACCEPT_CLIENT_CONNECTIONS=$ClientConnections
$env:ACCEPT_PACING_TIMER='std'
$env:ACCEPT_CLEANUP='on'
$env:ACCEPT_FAULT_MODE='1'
$env:ACCEPT_FAULT_KIND=$FaultKind
$env:ACCEPT_FAULT_START_SECONDS="$FaultStartSeconds"
$env:ACCEPT_FAULT_DURATION_SECONDS="$FaultDurationSeconds"
$env:ACCEPT_RUN=$database
$env:ACCEPT_ARTIFACTS=$artifacts
$env:ACCEPT_STOP_FILE=Join-Path $artifacts 'stop'
docker exec $PostgresContainer createdb -U tiangz $database
if($LASTEXITCODE -ne 0){throw 'Fresh database creation failed'}
docker exec $PostgresContainer psql -U tiangz -d postgres -v ON_ERROR_STOP=1 -c "ALTER DATABASE $database SET log_min_duration_statement = $SqlLogThresholdMs" > (Join-Path $artifacts 'sql-logging.txt')
if($LASTEXITCODE -ne 0){throw 'Test database logging setup failed'}
# Rules are fixed before anything runs. Normal and recovery phases keep the zero-error bar of the
# ordinary short test; only the fault window may show missed sends and connection-class errors.
@{
    fault_kind=$FaultKind; fault_start_seconds=$FaultStartSeconds; fault_duration_seconds=$FaultDurationSeconds
    recovery_grace_seconds=$RecoveryGraceSeconds
    allowed_fault_error_patterns=@('timed out','connection','closed','unavailable','unusable','terminat')
    phase_rule_version=2
    phase_assignment="before = completed before injection; during = in-flight interval overlaps [injection, release + $RecoveryGraceSeconds s]; after = sent after that window; missed sends by scheduled instant"
    normal_phase='zero errors, zero wrong data, zero missed sends'
    fault_phase='zero wrong data; missed sends allowed (in-flight limit); only connection/timeout/unavailable errors allowed'
    recovery_phase="zero errors and zero missed sends for every request sent later than fault release + $RecoveryGraceSeconds s"
    reconciliation='every write either absent (rolled back) or exactly revision 1 with the intended payload; confirmed writes must exist'
    rate=$Rate; sample_seconds=$Seconds; warmup_seconds=$WarmupSeconds; payload_bytes=$PayloadBytes; concurrency=$Concurrency
    host_workers=$HostWorkers; load_workers=$LoadWorkers; read_connection='pooled'; client_connections=$ClientConnections
    pacing_timer='std'; cleanup='on'; fixtures='none'; full_acceptance=$false
} | ConvertTo-Json | Set-Content (Join-Path $artifacts 'fault-plan.json')
Get-FileHash $hostExe,$loadExe,$injectorExe | ConvertTo-Json | Set-Content (Join-Path $artifacts 'binaries.json')
$logStart=(Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
$env:TOKIO_WORKER_THREADS="$HostWorkers"
$hostProcess=Start-Process -FilePath $hostExe -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $artifacts 'host.out') -RedirectStandardError (Join-Path $artifacts 'host.err')
# Windows PowerShell 5.1 leaves ExitCode null unless the handle was read before the process exits.
$null=$hostProcess.Handle
for($i=0;$i -lt 200;$i++){
    if($hostProcess.HasExited){throw "Host exited; see $artifacts"}
    if((Get-Content (Join-Path $artifacts 'host.out') -Raw) -match 'READY'){break}
    Start-Sleep -Milliseconds 100
}
if(-not ((Get-Content (Join-Path $artifacts 'host.out') -Raw) -match 'READY')){Stop-Process -Id $hostProcess.Id; throw 'Host readiness timed out'}
$loadProcess=$null; $injectorProcess=$null
try {
    $env:TOKIO_WORKER_THREADS="$LoadWorkers"
    $loadProcess=Start-Process -FilePath $loadExe -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $artifacts 'load.out') -RedirectStandardError (Join-Path $artifacts 'load.err')
    $null=$loadProcess.Handle
    $env:TOKIO_WORKER_THREADS='2'
    $injectorProcess=Start-Process -FilePath $injectorExe -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $artifacts 'injector.out') -RedirectStandardError (Join-Path $artifacts 'injector.err')
    $null=$injectorProcess.Handle
    $deadline=(Get-Date).AddSeconds($WarmupSeconds+$Seconds+$FaultDurationSeconds+180)
    do {
        @{utc=(Get-Date).ToUniversalTime().ToString('o');hostCpuSeconds=$hostProcess.TotalProcessorTime.TotalSeconds;hostWorkingSet=$hostProcess.WorkingSet64;loadCpuSeconds=$loadProcess.TotalProcessorTime.TotalSeconds;loadWorkingSet=$loadProcess.WorkingSet64} | ConvertTo-Json -Compress | Add-Content (Join-Path $artifacts 'process-resources.jsonl')
        $hostProcess.Refresh(); $loadProcess.Refresh()
        if((Get-Date) -gt $deadline){throw 'Load and reconciliation deadline exceeded'}
    }while(!$loadProcess.WaitForExit(2000))
    if(!$injectorProcess.WaitForExit(30000)){Stop-Process -Id $injectorProcess.Id; throw 'Injector did not finish'}
    if($injectorProcess.ExitCode -ne 0){throw "Injector failed; evidence preserved: $artifacts"}
    if($loadProcess.ExitCode -ne 0){throw "Load failed on data or diagnostics; evidence preserved: $artifacts"}
} finally {
    if($null -ne $loadProcess -and !$loadProcess.HasExited){Stop-Process -Id $loadProcess.Id}
    if($null -ne $injectorProcess -and !$injectorProcess.HasExited){Stop-Process -Id $injectorProcess.Id}
    New-Item -ItemType File -Path (Join-Path $artifacts 'stop') -Force | Out-Null
    if(!$hostProcess.WaitForExit(20000)){Stop-Process -Id $hostProcess.Id; Write-Warning 'Host did not stop gracefully'}
    # PostgreSQL logs arrive on stderr; let cmd.exe redirect so PowerShell 5.1 does not raise NativeCommandError.
    cmd /c "docker logs --since $logStart --timestamps $PostgresContainer > `"$(Join-Path $artifacts 'postgres.log')`" 2>&1"
    docker exec $PostgresContainer cat /sys/fs/cgroup/cpu.stat > (Join-Path $artifacts 'postgres-cpu.txt')
}
if($hostProcess.ExitCode -ne 0){throw 'Host failed during shutdown'}
node (Join-Path $root 'tools/analyze_fault_load.mjs') $artifacts > (Join-Path $artifacts 'phase-analysis.log') 2>&1
$verdict=$LASTEXITCODE
Get-Content (Join-Path $artifacts 'phase-analysis.log') -Raw
if($verdict -ne 0){throw "Fault run did not pass the pre-declared phase rules; evidence preserved: $artifacts"}
Write-Output "PASS fault=$FaultKind run=$RunId"
