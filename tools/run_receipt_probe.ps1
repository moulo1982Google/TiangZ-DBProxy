param(
    [Parameter(Mandatory)][string]$RunId,
    [Parameter(Mandatory)][string]$PostgresContainer,
    [Parameter(Mandatory)][string]$Project,
    [Parameter(Mandatory)][string]$PostgresBaseUrl,
    [Parameter(Mandatory)][string]$RedisUrl,
    [Parameter(Mandatory)][string]$CacheRedisUrl,
    [int]$Rate=200, [int]$Seconds=30, [int]$WarmupSeconds=10, [int]$Rounds=3,
    [int]$PayloadBytes=1024, [int]$Concurrency=32,
    [ValidateRange(1,64)][int]$HostWorkers=4,
    [ValidateRange(1,64)][int]$LoadWorkers=32,
    [ValidateSet('both','on','off')][string]$CleanupMode='both',
    [ValidateRange(-1,10000)][int]$SqlLogThresholdMs=-1,
    [ValidateSet('shared','dedicated','pooled')][string]$ReadConnection='shared',
    [ValidateSet('shared4','shared8','split2','split4')][string]$ClientConnections='shared4',
    [ValidateSet('tokio','std')][string]$PacingTimer='tokio',
    [ValidateRange(1,8760)][int]$ReceiptRetentionHours=24
)
$ErrorActionPreference='Stop'
if($RunId -notmatch '^[a-z][a-z0-9_]{0,24}$'){throw 'RunId must be a short lowercase SQL identifier'}
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
$env:DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION='1'
$env:DBPROXY_REDIS_URL=$RedisUrl
$env:DBPROXY_CACHE_REDIS_URL=$CacheRedisUrl
$env:ACCEPT_LISTEN='127.0.0.1:17980'
$env:ACCEPT_RATE="$Rate"; $env:ACCEPT_SECONDS="$Seconds"; $env:ACCEPT_WARMUP_SECONDS="$WarmupSeconds"
$env:ACCEPT_PAYLOAD_BYTES="$PayloadBytes"; $env:ACCEPT_CONCURRENCY="$Concurrency"
$env:ACCEPT_READ_CONNECTION=$ReadConnection
$env:ACCEPT_CLIENT_CONNECTIONS=$ClientConnections
$env:ACCEPT_PACING_TIMER=$PacingTimer
$env:ACCEPT_RECEIPT_RETENTION_HOURS="$ReceiptRetentionHours"
function Start-ProbeHost([string]$database,[string]$mode,[string]$folder) {
    $env:TOKIO_WORKER_THREADS="$HostWorkers"
    $env:DBPROXY_TEST_POSTGRES_URL="$PostgresBaseUrl/$database"
    $env:ACCEPT_CLEANUP=$mode
    $env:ACCEPT_STOP_FILE=Join-Path $folder 'stop'
    $process=Start-Process -FilePath $hostExe -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $folder 'host.out') -RedirectStandardError (Join-Path $folder 'host.err')
    # Windows PowerShell 5.1 leaves ExitCode null unless the handle was read before the process exits.
    $null=$process.Handle
    for($i=0;$i -lt 200;$i++){
        if($process.HasExited){throw "Host exited; see $folder"}
        if((Get-Content (Join-Path $folder 'host.out') -Raw) -match 'READY'){return $process}
        Start-Sleep -Milliseconds 100
    }
    # Only the process created above can be terminated here.
    Stop-Process -Id $process.Id
    throw 'Host readiness timed out'
}
function Stop-ProbeHost($process,[string]$folder) {
    New-Item -ItemType File -Path (Join-Path $folder 'stop') -Force | Out-Null
    if(!$process.WaitForExit(20000)){Stop-Process -Id $process.Id; throw 'Host did not stop gracefully'}
    if($process.ExitCode -ne 0){throw 'Host failed during shutdown'}
}
$template="${RunId}_base"
docker exec $PostgresContainer createdb -U tiangz $template
if($LASTEXITCODE -ne 0){throw 'Fresh database creation failed'}
$baseFolder=New-Item -ItemType Directory -Path (Join-Path $artifacts 'base')
$hostProcess=Start-ProbeHost $template 'off' $baseFolder.FullName
Stop-ProbeHost $hostProcess $baseFolder.FullName
$seed=@"
INSERT INTO dbproxy_idempotency(request_id,namespace,record_key,schema_name,schema_version,payload,revision,recorded_at)
SELECT 'fixture-'||n,'probe-fixture',n::text,'test',1,'',1,
CASE WHEN n<=100000 THEN statement_timestamp()-interval '169 hours' ELSE statement_timestamp() END
FROM generate_series(1,200000) n;
ANALYZE dbproxy_idempotency;
"@
$seed | docker exec -i $PostgresContainer psql -U tiangz -d $template -v ON_ERROR_STOP=1 > (Join-Path $artifacts 'seed.log')
if($LASTEXITCODE -ne 0){throw 'Fixture failed'}
@{RunId=$RunId;Rate=$Rate;Seconds=$Seconds;WarmupSeconds=$WarmupSeconds;Rounds=$Rounds;PayloadBytes=$PayloadBytes;Concurrency=$Concurrency;HostWorkers=$HostWorkers;LoadWorkers=$LoadWorkers;CleanupMode=$CleanupMode;SqlLogThresholdMs=$SqlLogThresholdMs;ReadConnection=$ReadConnection;ClientConnections=$ClientConnections;PacingTimer=$PacingTimer;ReceiptRetentionHours=$ReceiptRetentionHours;FixtureRecent=100000;FixtureExpired=100000;FullAcceptance=$false} | ConvertTo-Json | Set-Content (Join-Path $artifacts 'manifest.json')
Get-FileHash $hostExe,$loadExe | ConvertTo-Json | Set-Content (Join-Path $artifacts 'binaries.json')
for($round=1;$round -le $Rounds;$round++){
    $modes=if($round % 2 -eq 1){@('off','on')}else{@('on','off')}
    if($CleanupMode -ne 'both'){$modes=@($CleanupMode)}
    foreach($mode in $modes){
        $name="${RunId}_${round}_${mode}"
        $folder=(New-Item -ItemType Directory -Path (Join-Path $artifacts "${round}_${mode}")).FullName
        docker exec $PostgresContainer createdb -U tiangz -T $template $name
        if($LASTEXITCODE -ne 0){throw 'Database clone failed'}
        docker exec $PostgresContainer psql -U tiangz -d postgres -v ON_ERROR_STOP=1 -c "ALTER DATABASE $name SET log_min_duration_statement = $SqlLogThresholdMs" > (Join-Path $folder 'sql-logging.txt')
        if($LASTEXITCODE -ne 0){throw 'Test database logging setup failed'}
        $logStart=(Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
        $env:ACCEPT_RUN=$name; $env:ACCEPT_ARTIFACTS=$folder
        $hostProcess=Start-ProbeHost $name $mode $folder
        $loadProcess=$null
        try {
            $env:TOKIO_WORKER_THREADS="$LoadWorkers"
            $loadProcess=Start-Process -FilePath $loadExe -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $folder 'load.out') -RedirectStandardError (Join-Path $folder 'load.err')
            $null=$loadProcess.Handle
            $deadline=(Get-Date).AddSeconds($WarmupSeconds+$Seconds+180)
            do {
                @{utc=(Get-Date).ToUniversalTime().ToString('o');hostCpuSeconds=$hostProcess.TotalProcessorTime.TotalSeconds;hostWorkingSet=$hostProcess.WorkingSet64;loadCpuSeconds=$loadProcess.TotalProcessorTime.TotalSeconds;loadWorkingSet=$loadProcess.WorkingSet64} | ConvertTo-Json -Compress | Add-Content (Join-Path $folder 'process-resources.jsonl')
                $hostProcess.Refresh(); $loadProcess.Refresh()
                if((Get-Date) -gt $deadline){throw 'Load and reconciliation deadline exceeded'}
            }while(!$loadProcess.WaitForExit(2000))
            if($loadProcess.ExitCode -ne 0){throw "Load failed; evidence preserved: $folder"}
            docker exec $PostgresContainer psql -U tiangz -d $name -At -c "SELECT count(*) FILTER (WHERE recorded_at < statement_timestamp()-make_interval(hours => $ReceiptRetentionHours)),count(*) FILTER (WHERE recorded_at >= statement_timestamp()-make_interval(hours => $ReceiptRetentionHours)) FROM dbproxy_idempotency WHERE namespace='probe-fixture'" > (Join-Path $folder 'remaining.txt')
            if($LASTEXITCODE -ne 0){throw 'Post-run receipt audit failed'}
            Write-Output "PASS round=$round cleanup=$mode"
            Get-Content (Join-Path $folder 'summary.json') -Raw
        } finally {
            if($null -ne $loadProcess -and !$loadProcess.HasExited){Stop-Process -Id $loadProcess.Id}
            Stop-ProbeHost $hostProcess $folder
            # PostgreSQL logs arrive on stderr; let cmd.exe redirect so PowerShell 5.1 does not raise NativeCommandError.
            cmd /c "docker logs --since $logStart --timestamps $PostgresContainer > `"$(Join-Path $folder 'postgres.log')`" 2>&1"
            docker exec $PostgresContainer cat /sys/fs/cgroup/cpu.stat > (Join-Path $folder 'postgres-cpu.txt')
        }
    }
}
