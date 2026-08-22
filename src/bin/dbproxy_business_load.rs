use std::{
    env,
    error::Error,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde_json::json;
use tiangz_dbproxy_client::{ClientConfig, DbProxyClientPool};
use tiangz_dbproxy_core::{
    MultiRecordTransactionalWrite, RecordKey, Revision, SnapshotWrite, TransactionalRecordWrite,
};

const BASE_DOMAINS: [&str; 5] = ["inventory", "progression", "quest", "runtime", "wallet"];
const BASE_PAYLOAD_BYTES: [usize; 5] = [2_048, 1_024, 1_024, 2_048, 256];
type DynError = Box<dyn Error + Send + Sync>;

#[derive(Clone)]
struct Options {
    endpoint: String,
    auth_token: String,
    pool_size: usize,
    players: usize,
    duration: Duration,
    domain_count: usize,
    workloads: Vec<Workload>,
}

#[derive(Clone, Copy)]
enum Workload {
    PlayerLoadSingle,
    PlayerLoadBatch,
    Pickup,
    NpcShop,
}

impl Workload {
    const fn name(self) -> &'static str {
        match self {
            Self::PlayerLoadSingle => "playerDataSingle",
            Self::PlayerLoadBatch => "playerDataBatch",
            Self::Pickup => "pickup",
            Self::NpcShop => "npcShop",
        }
    }
}

#[derive(Clone)]
struct PlayerState {
    player_id: u64,
    domains: Vec<String>,
    records: Vec<RecordKey>,
    revisions: Vec<Revision>,
    operation_sequence: u64,
}

struct WorkerResult {
    state: PlayerState,
    latencies_us: Vec<u64>,
    errors: Vec<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), DynError> {
    let options = parse_options()?;
    let mut config = ClientConfig::new(
        &options.endpoint,
        &options.auth_token,
        "dbproxy-business-load",
    );
    config.request_timeout = Duration::from_secs(10);
    let pool = DbProxyClientPool::connect(config, options.pool_size).await?;
    let run_id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let setup_started = Instant::now();
    let mut states = seed_players(&pool, options.players, options.domain_count, run_id).await?;
    eprintln!(
        "seeded {} players across {} persistence domains in {:.3}s",
        states.len(),
        options.domain_count,
        setup_started.elapsed().as_secs_f64()
    );

    for workload in &options.workloads {
        let (next_states, mut latencies, errors) =
            run_workload(pool.clone(), states, *workload, options.duration).await;
        states = next_states;
        latencies.sort_unstable();
        let succeeded = latencies.len();
        let seconds = options.duration.as_secs_f64();
        let result = json!({
            "workload": workload.name(),
            "endpoint": options.endpoint,
            "clientPoolSize": options.pool_size,
            "players": options.players,
            "domainCount": options.domain_count,
            "durationSeconds": seconds,
            "succeeded": succeeded,
            "failed": errors.len(),
            "operationsPerSecond": succeeded as f64 / seconds,
            "latencyMs": {
                "p50": percentile_ms(&latencies, 50),
                "p95": percentile_ms(&latencies, 95),
                "p99": percentile_ms(&latencies, 99),
                "max": latencies.last().copied().unwrap_or(0) as f64 / 1_000.0,
            },
            "payloadBytes": match workload {
                Workload::PlayerLoadSingle | Workload::PlayerLoadBatch =>
                    (0..options.domain_count).map(domain_payload_bytes).sum::<usize>(),
                Workload::Pickup => BASE_PAYLOAD_BYTES[0] + BASE_PAYLOAD_BYTES[2] + BASE_PAYLOAD_BYTES[4] + 512,
                Workload::NpcShop => BASE_PAYLOAD_BYTES[0] + BASE_PAYLOAD_BYTES[4] + 256,
            },
            "firstErrors": errors.into_iter().take(5).collect::<Vec<_>>(),
        });
        println!("RESULT_JSON {result}");
    }
    Ok(())
}

async fn seed_players(
    pool: &DbProxyClientPool,
    players: usize,
    domain_count: usize,
    run_id: u128,
) -> Result<Vec<PlayerState>, DynError> {
    let mut tasks = tokio::task::JoinSet::new();
    for player_index in 0..players {
        let pool = pool.clone();
        tasks.spawn(async move {
            let player_id = run_id
                .checked_mul(100_000)
                .and_then(|value| value.checked_add(player_index as u128 + 1))
                .ok_or("benchmark player id exhausted")? as u64;
            let domains = (0..domain_count).map(domain_name).collect::<Vec<_>>();
            let records = domains
                .iter()
                .map(|domain| {
                    RecordKey::new("player", format!("{player_id}:{domain}"))
                        .expect("static benchmark record key is valid")
                })
                .collect::<Vec<_>>();
            for index in 0..domains.len() {
                pool.save(SnapshotWrite {
                    request_id: format!("perf-seed:{run_id}:{player_index}:{}", domains[index]),
                    record: records[index].clone(),
                    schema: format!("tiangz.demo.player.{}", domains[index]),
                    schema_version: 1,
                    payload: payload(&domains[index], player_id, domain_payload_bytes(index)),
                    expected_revision: Some(Revision::ZERO),
                    updated_at_unix_ms: run_id as u64,
                })
                .await?;
            }
            Ok::<_, DynError>(PlayerState {
                player_id,
                domains,
                records,
                revisions: vec![Revision(1); domain_count],
                operation_sequence: 0,
            })
        });
    }
    let mut states = Vec::with_capacity(players);
    while let Some(result) = tasks.join_next().await {
        states.push(result??);
    }
    Ok(states)
}

async fn run_workload(
    pool: DbProxyClientPool,
    states: Vec<PlayerState>,
    workload: Workload,
    duration: Duration,
) -> (Vec<PlayerState>, Vec<u64>, Vec<String>) {
    let deadline = Instant::now() + duration;
    let mut tasks = tokio::task::JoinSet::new();
    for state in states {
        let pool = pool.clone();
        tasks.spawn(async move {
            let mut state = state;
            let mut latencies_us = Vec::new();
            let mut errors = Vec::new();
            while Instant::now() < deadline {
                let started = Instant::now();
                let result = execute(&pool, &mut state, workload).await;
                let elapsed = started.elapsed().as_micros().min(u64::MAX as u128) as u64;
                match result {
                    Ok(()) => latencies_us.push(elapsed),
                    Err(error) => {
                        errors.push(error.to_string());
                        break;
                    }
                }
            }
            WorkerResult {
                state,
                latencies_us,
                errors,
            }
        });
    }
    let mut next_states = Vec::new();
    let mut latencies = Vec::new();
    let mut errors = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(result) => {
                next_states.push(result.state);
                latencies.extend(result.latencies_us);
                errors.extend(result.errors);
            }
            Err(error) => errors.push(format!("load worker failed: {error}")),
        }
    }
    (next_states, latencies, errors)
}

async fn execute(
    pool: &DbProxyClientPool,
    state: &mut PlayerState,
    workload: Workload,
) -> Result<(), DynError> {
    state.operation_sequence += 1;
    match workload {
        Workload::PlayerLoadSingle => {
            let mut loads = tokio::task::JoinSet::new();
            for record in &state.records {
                let pool = pool.clone();
                let record = record.clone();
                loads.spawn(async move { pool.load(&record).await });
            }
            while let Some(loaded) = loads.join_next().await {
                if loaded??.is_none() {
                    return Err("player data load missed a seeded domain".into());
                }
            }
        }
        Workload::PlayerLoadBatch => {
            if pool
                .load_multi(&state.records)
                .await?
                .iter()
                .any(Option::is_none)
            {
                return Err("player batch load missed a seeded domain".into());
            }
        }
        Workload::Pickup => {
            apply_multi(pool, state, &[0, 2, 4], "pickup", 512).await?;
        }
        Workload::NpcShop => {
            apply_multi(pool, state, &[0, 4], "npc-shop", 256).await?;
        }
    }
    Ok(())
}

async fn apply_multi(
    pool: &DbProxyClientPool,
    state: &mut PlayerState,
    domain_indices: &[usize],
    operation: &str,
    result_bytes: usize,
) -> Result<(), DynError> {
    let operation_id = format!(
        "perf:{operation}:{}:{}",
        state.player_id, state.operation_sequence
    );
    let writes = domain_indices
        .iter()
        .map(|&index| TransactionalRecordWrite {
            record: state.records[index].clone(),
            schema: format!("tiangz.demo.player.{}", state.domains[index]),
            schema_version: 1,
            expected_revision: state.revisions[index],
            payload: payload(
                &state.domains[index],
                state.player_id,
                domain_payload_bytes(index),
            ),
            updated_at_unix_ms: state.operation_sequence,
        })
        .collect();
    let outcome = pool
        .apply_multi_transaction(MultiRecordTransactionalWrite {
            operation_id,
            writes,
            result: vec![b'r'; result_bytes],
        })
        .await?;
    let records = match outcome {
        tiangz_dbproxy_core::MultiRecordTransactionalWriteOutcome::Applied { records, .. }
        | tiangz_dbproxy_core::MultiRecordTransactionalWriteOutcome::Duplicate {
            records, ..
        } => records,
    };
    for receipt in records {
        let index = state
            .records
            .iter()
            .position(|record| record == &receipt.record)
            .ok_or("DBProxy returned an unknown player record")?;
        state.revisions[index] = receipt.new_revision;
    }
    Ok(())
}

fn payload(domain: &str, player_id: u64, bytes: usize) -> Vec<u8> {
    let prefix = format!(
        r#"{{"version":1,"data":{{"characterId":"{player_id}","domain":"{domain}","padding":""#
    );
    let suffix = b"\"}}";
    let padding = bytes.saturating_sub(prefix.len() + suffix.len());
    let mut result = Vec::with_capacity(prefix.len() + padding + suffix.len());
    result.extend_from_slice(prefix.as_bytes());
    result.resize(result.len() + padding, b'x');
    result.extend_from_slice(suffix);
    result
}

fn domain_name(index: usize) -> String {
    BASE_DOMAINS
        .get(index)
        .map(|domain| (*domain).to_string())
        .unwrap_or_else(|| format!("extension-{index}"))
}

fn domain_payload_bytes(index: usize) -> usize {
    BASE_PAYLOAD_BYTES.get(index).copied().unwrap_or(1_024)
}

fn percentile_ms(sorted_us: &[u64], percentile: usize) -> f64 {
    if sorted_us.is_empty() {
        return 0.0;
    }
    let index = ((sorted_us.len() - 1) * percentile) / 100;
    sorted_us[index] as f64 / 1_000.0
}

fn parse_options() -> Result<Options, DynError> {
    let mut endpoint = "127.0.0.1:7810".to_string();
    let mut pool_size = 32;
    let mut players = 100;
    let mut duration_seconds = 10;
    let mut domain_count = 5;
    let mut workloads = vec![
        Workload::PlayerLoadSingle,
        Workload::PlayerLoadBatch,
        Workload::Pickup,
        Workload::NpcShop,
    ];
    let args = env::args().skip(1).collect::<Vec<_>>();
    let mut index = 0;
    while index < args.len() {
        let value = args
            .get(index + 1)
            .ok_or("benchmark option requires a value")?;
        match args[index].as_str() {
            "--endpoint" => endpoint = value.clone(),
            "--pool-size" => pool_size = value.parse()?,
            "--players" => players = value.parse()?,
            "--duration" => duration_seconds = value.parse()?,
            "--domain-count" => domain_count = value.parse()?,
            "--workloads" => {
                workloads = value
                    .split(',')
                    .map(|name| match name {
                        "playerData" | "playerDataSingle" => Ok(Workload::PlayerLoadSingle),
                        "playerDataBatch" => Ok(Workload::PlayerLoadBatch),
                        "pickup" => Ok(Workload::Pickup),
                        "npcShop" => Ok(Workload::NpcShop),
                        _ => Err(format!("unknown workload: {name}")),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
            }
            option => return Err(format!("unknown benchmark option: {option}").into()),
        }
        index += 2;
    }
    if pool_size == 0
        || players == 0
        || duration_seconds == 0
        || domain_count == 0
        || domain_count > tiangz_dbproxy_protocol::MAX_BATCH_LOAD_RECORDS
        || workloads.is_empty()
    {
        return Err("pool size, players, duration, domains, and workloads must be within their non-zero limits".into());
    }
    let auth_token = env::var("DBPROXY_AUTH_TOKEN")?;
    Ok(Options {
        endpoint,
        auth_token,
        pool_size,
        players,
        duration: Duration::from_secs(duration_seconds),
        domain_count,
        workloads,
    })
}
