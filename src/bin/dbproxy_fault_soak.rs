use std::{
    env,
    error::Error,
    io::{self, Write},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_json::json;
use tiangz_dbproxy_client::{ClientConfig, DbProxyClientPool};
use tiangz_dbproxy_core::{
    LedgerPosting, OutboxEvent, RecordKey, Revision, SnapshotWrite, TradeState, TradeTransaction,
    TradeTransactionOutcome, TradeTransition, TransactionalRecordWrite, TransactionalWrite,
    TransactionalWriteOutcome,
};
use tiangz_dbproxy_protocol::MAX_BATCH_SNAPSHOT_WRITES;
use tokio::{task::JoinSet, time::Instant};

type DynError = Box<dyn Error + Send + Sync>;

#[derive(Clone)]
struct Options {
    endpoint: String,
    failover_endpoint: Option<String>,
    auth_token: String,
    pool_size: usize,
    read_pool_size: Option<usize>,
    write_pool_size: Option<usize>,
    players: usize,
    duration: Duration,
    cycle: Duration,
    trade_interval_cycles: u64,
    report_interval: Duration,
    validation_timeout: Duration,
}

struct PlayerState {
    index: usize,
    direct_record: RecordKey,
    queued_record: RecordKey,
    direct_revision: Revision,
    transaction_sequence: u64,
    trade_sequence: u64,
    pending_transaction: Option<TransactionalWrite>,
    pending_trade: Option<TradeTransaction>,
}

#[derive(Default)]
struct Counters {
    load_ok: AtomicU64,
    load_errors: AtomicU64,
    missing_snapshots: AtomicU64,
    reads_behind_acknowledged_revision: AtomicU64,
    reads_ahead_of_local_revision: AtomicU64,
    enqueue_ok: AtomicU64,
    enqueue_errors: AtomicU64,
    transaction_applied: AtomicU64,
    transaction_duplicate: AtomicU64,
    transaction_errors: AtomicU64,
    trade_applied: AtomicU64,
    trade_duplicate: AtomicU64,
    trade_errors: AtomicU64,
    invariant_errors: AtomicU64,
}

#[derive(Clone, Copy, Default)]
struct CounterSnapshot {
    load_ok: u64,
    load_errors: u64,
    missing_snapshots: u64,
    reads_behind_acknowledged_revision: u64,
    reads_ahead_of_local_revision: u64,
    enqueue_ok: u64,
    enqueue_errors: u64,
    transaction_applied: u64,
    transaction_duplicate: u64,
    transaction_errors: u64,
    trade_applied: u64,
    trade_duplicate: u64,
    trade_errors: u64,
    invariant_errors: u64,
}

impl Counters {
    fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            load_ok: self.load_ok.load(Ordering::Relaxed),
            load_errors: self.load_errors.load(Ordering::Relaxed),
            missing_snapshots: self.missing_snapshots.load(Ordering::Relaxed),
            reads_behind_acknowledged_revision: self
                .reads_behind_acknowledged_revision
                .load(Ordering::Relaxed),
            reads_ahead_of_local_revision: self
                .reads_ahead_of_local_revision
                .load(Ordering::Relaxed),
            enqueue_ok: self.enqueue_ok.load(Ordering::Relaxed),
            enqueue_errors: self.enqueue_errors.load(Ordering::Relaxed),
            transaction_applied: self.transaction_applied.load(Ordering::Relaxed),
            transaction_duplicate: self.transaction_duplicate.load(Ordering::Relaxed),
            transaction_errors: self.transaction_errors.load(Ordering::Relaxed),
            trade_applied: self.trade_applied.load(Ordering::Relaxed),
            trade_duplicate: self.trade_duplicate.load(Ordering::Relaxed),
            trade_errors: self.trade_errors.load(Ordering::Relaxed),
            invariant_errors: self.invariant_errors.load(Ordering::Relaxed),
        }
    }
}

impl CounterSnapshot {
    fn saturating_sub(self, earlier: Self) -> Self {
        Self {
            load_ok: self.load_ok.saturating_sub(earlier.load_ok),
            load_errors: self.load_errors.saturating_sub(earlier.load_errors),
            missing_snapshots: self
                .missing_snapshots
                .saturating_sub(earlier.missing_snapshots),
            reads_behind_acknowledged_revision: self
                .reads_behind_acknowledged_revision
                .saturating_sub(earlier.reads_behind_acknowledged_revision),
            reads_ahead_of_local_revision: self
                .reads_ahead_of_local_revision
                .saturating_sub(earlier.reads_ahead_of_local_revision),
            enqueue_ok: self.enqueue_ok.saturating_sub(earlier.enqueue_ok),
            enqueue_errors: self.enqueue_errors.saturating_sub(earlier.enqueue_errors),
            transaction_applied: self
                .transaction_applied
                .saturating_sub(earlier.transaction_applied),
            transaction_duplicate: self
                .transaction_duplicate
                .saturating_sub(earlier.transaction_duplicate),
            transaction_errors: self
                .transaction_errors
                .saturating_sub(earlier.transaction_errors),
            trade_applied: self.trade_applied.saturating_sub(earlier.trade_applied),
            trade_duplicate: self.trade_duplicate.saturating_sub(earlier.trade_duplicate),
            trade_errors: self.trade_errors.saturating_sub(earlier.trade_errors),
            invariant_errors: self
                .invariant_errors
                .saturating_sub(earlier.invariant_errors),
        }
    }

    fn json(self) -> serde_json::Value {
        json!({
            "loadOk": self.load_ok,
            "loadErrors": self.load_errors,
            "missingSnapshots": self.missing_snapshots,
            "readsBehindAcknowledgedRevision": self.reads_behind_acknowledged_revision,
            "readsAheadOfLocalRevision": self.reads_ahead_of_local_revision,
            "enqueueOk": self.enqueue_ok,
            "enqueueErrors": self.enqueue_errors,
            "transactionApplied": self.transaction_applied,
            "transactionDuplicate": self.transaction_duplicate,
            "transactionErrors": self.transaction_errors,
            "tradeApplied": self.trade_applied,
            "tradeDuplicate": self.trade_duplicate,
            "tradeErrors": self.trade_errors,
            "invariantErrors": self.invariant_errors,
        })
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), DynError> {
    let options = parse_options()?;
    let mut config =
        ClientConfig::new(&options.endpoint, &options.auth_token, "dbproxy-fault-soak");
    if let Some(endpoint) = &options.failover_endpoint {
        config = config.with_endpoints([endpoint.clone()]);
    }
    config.connect_timeout = Duration::from_secs(2);
    config.request_timeout = Duration::from_secs(5);
    let pool = match (options.read_pool_size, options.write_pool_size) {
        (Some(read_size), Some(write_size)) => {
            DbProxyClientPool::connect_split(config, read_size, write_size).await?
        }
        (None, None) => DbProxyClientPool::connect(config, options.pool_size).await?,
        _ => return Err("read and write pool sizes must be supplied together".into()),
    };
    let run_id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let states = seed_and_warm_players(&pool, options.players, run_id).await?;
    let counters = Arc::new(Counters::default());
    let started = Instant::now();
    let deadline = started + options.duration;
    let queued_records = states
        .iter()
        .map(|state| state.queued_record.clone())
        .collect::<Vec<_>>();

    emit(
        "SOAK_READY",
        json!({
            "runId": run_id.to_string(),
            "players": options.players,
            "failoverEndpointConfigured": options.failover_endpoint.is_some(),
            "poolSize": pool.len(),
            "readPoolSize": pool.read_len(),
            "writePoolSize": pool.write_len(),
            "splitPool": pool.is_split(),
            "durationSeconds": options.duration.as_secs(),
            "cycleMs": options.cycle.as_millis(),
            "tradeIntervalCycles": options.trade_interval_cycles,
        }),
    );

    let reporter = tokio::spawn(report_progress(
        counters.clone(),
        started,
        deadline,
        options.report_interval,
    ));
    let enqueue_worker = tokio::spawn(run_enqueue_batches(
        pool.clone(),
        queued_records,
        counters.clone(),
        run_id,
        started,
        deadline,
    ));
    let mut workers = JoinSet::new();
    for state in states {
        let pool = pool.clone();
        let counters = counters.clone();
        let cycle = options.cycle;
        let trade_interval_cycles = options.trade_interval_cycles;
        let stagger = Duration::from_millis(
            (cycle.as_millis() as u64).saturating_mul(state.index as u64) / options.players as u64,
        );
        workers.spawn(async move {
            tokio::time::sleep(stagger).await;
            run_player(
                pool,
                state,
                counters,
                run_id,
                deadline,
                cycle,
                trade_interval_cycles,
            )
            .await
        });
    }

    let mut final_states = Vec::with_capacity(options.players);
    while let Some(joined) = workers.join_next().await {
        final_states.push(joined?);
    }
    let acknowledged_enqueues = enqueue_worker.await?;
    reporter.await?;

    let validation = validate_final_state(
        &pool,
        &final_states,
        &acknowledged_enqueues,
        run_id,
        options.validation_timeout,
    )
    .await;
    let total = counters.snapshot();
    emit(
        "SOAK_FINAL",
        json!({
            "runId": run_id.to_string(),
            "elapsedSeconds": started.elapsed().as_secs_f64(),
            "totals": total.json(),
            "validation": {
                "passed": validation.is_ok(),
                "message": validation.as_ref().err().map(ToString::to_string),
            },
        }),
    );
    validation
}

async fn seed_and_warm_players(
    pool: &DbProxyClientPool,
    players: usize,
    run_id: u128,
) -> Result<Vec<PlayerState>, DynError> {
    let mut tasks = JoinSet::new();
    for index in 0..players {
        let pool = pool.clone();
        tasks.spawn(async move {
            let direct_record =
                RecordKey::new("fault-soak-player", format!("{run_id}:{index}:direct"))?;
            let queued_record =
                RecordKey::new("fault-soak-player", format!("{run_id}:{index}:queued"))?;
            let direct_revision = seed_snapshot(
                &pool,
                format!("soak:seed:{run_id}:{index}:direct"),
                direct_record.clone(),
                payload(index, 0, "direct"),
                run_id as u64,
            )
            .await?;
            seed_snapshot(
                &pool,
                format!("soak:seed:{run_id}:{index}:queued"),
                queued_record.clone(),
                payload(index, 0, "queued"),
                run_id as u64,
            )
            .await?;
            if pool.load(&direct_record).await?.is_none()
                || pool.load(&queued_record).await?.is_none()
            {
                return Err::<_, DynError>(
                    "seeded snapshot disappeared during cache warmup".into(),
                );
            }
            Ok::<_, DynError>(PlayerState {
                index,
                direct_record,
                queued_record,
                direct_revision,
                transaction_sequence: 0,
                trade_sequence: 0,
                pending_transaction: None,
                pending_trade: None,
            })
        });
    }
    let mut states = Vec::with_capacity(players);
    while let Some(joined) = tasks.join_next().await {
        states.push(joined??);
    }
    states.sort_unstable_by_key(|state| state.index);
    Ok(states)
}

async fn seed_snapshot(
    pool: &DbProxyClientPool,
    request_id: String,
    record: RecordKey,
    payload: Vec<u8>,
    updated_at_unix_ms: u64,
) -> Result<Revision, DynError> {
    let outcome = pool
        .save(SnapshotWrite {
            request_id,
            record,
            schema: "tiangz.fault-soak.player".to_string(),
            schema_version: 1,
            payload,
            expected_revision: Some(Revision::ZERO),
            updated_at_unix_ms,
        })
        .await?;
    Ok(match outcome {
        tiangz_dbproxy_core::SnapshotWriteOutcome::Applied { revision }
        | tiangz_dbproxy_core::SnapshotWriteOutcome::Duplicate { revision } => revision,
    })
}

async fn run_player(
    pool: DbProxyClientPool,
    mut state: PlayerState,
    counters: Arc<Counters>,
    run_id: u128,
    deadline: Instant,
    cycle: Duration,
    trade_interval_cycles: u64,
) -> PlayerState {
    let mut cycle_sequence = 0_u64;
    while Instant::now() < deadline {
        let cycle_started = Instant::now();
        cycle_sequence += 1;
        observe_direct_snapshot(&pool, &state, &counters).await;

        if cycle_sequence.is_multiple_of(5) {
            observe_queued_snapshot(&pool, &state, &counters).await;
        }
        if cycle_sequence.is_multiple_of(10) {
            apply_direct_transaction(&pool, &mut state, &counters, run_id).await;
        }
        if cycle_sequence.is_multiple_of(trade_interval_cycles) {
            apply_trade(&pool, &mut state, &counters, run_id).await;
        }

        let next_cycle = cycle_started + cycle;
        if next_cycle < deadline {
            tokio::time::sleep_until(next_cycle).await;
        }
    }
    state
}

async fn observe_direct_snapshot(
    pool: &DbProxyClientPool,
    state: &PlayerState,
    counters: &Counters,
) {
    match pool.load(&state.direct_record).await {
        Ok(Some(snapshot)) => {
            counters.load_ok.fetch_add(1, Ordering::Relaxed);
            if snapshot.revision < state.direct_revision {
                counters
                    .reads_behind_acknowledged_revision
                    .fetch_add(1, Ordering::Relaxed);
            } else if snapshot.revision > state.direct_revision {
                counters
                    .reads_ahead_of_local_revision
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(None) => {
            counters.missing_snapshots.fetch_add(1, Ordering::Relaxed);
        }
        Err(_) => {
            counters.load_errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn observe_queued_snapshot(
    pool: &DbProxyClientPool,
    state: &PlayerState,
    counters: &Counters,
) {
    match pool.load(&state.queued_record).await {
        Ok(Some(_)) => {
            counters.load_ok.fetch_add(1, Ordering::Relaxed);
        }
        Ok(None) => {
            counters.missing_snapshots.fetch_add(1, Ordering::Relaxed);
        }
        Err(_) => {
            counters.load_errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn run_enqueue_batches(
    pool: DbProxyClientPool,
    records: Vec<RecordKey>,
    counters: Arc<Counters>,
    run_id: u128,
    started: Instant,
    deadline: Instant,
) -> Vec<u64> {
    let batch_interval = Duration::from_secs(5);
    let mut next_batch = started + batch_interval;
    let mut sequence = 0_u64;
    let mut acknowledged = vec![0_u64; records.len()];
    while next_batch < deadline {
        tokio::time::sleep_until(next_batch).await;
        sequence += 1;
        let writes = records
            .iter()
            .enumerate()
            .map(|(player, record)| SnapshotWrite {
                request_id: format!("soak:enqueue:{run_id}:{player}:{sequence}"),
                record: record.clone(),
                schema: "tiangz.fault-soak.queued".to_string(),
                schema_version: 1,
                payload: payload(player, sequence, "queued"),
                expected_revision: None,
                updated_at_unix_ms: sequence,
            })
            .collect::<Vec<_>>();
        for (chunk_index, chunk) in writes.chunks(MAX_BATCH_SNAPSHOT_WRITES).enumerate() {
            let player_offset = chunk_index * MAX_BATCH_SNAPSHOT_WRITES;
            match pool.enqueue_multi_snapshot(chunk).await {
                Ok(outcomes) if outcomes.len() == chunk.len() => {
                    for (index, outcome) in outcomes.into_iter().enumerate() {
                        let player = player_offset + index;
                        match outcome {
                            Ok(()) => {
                                acknowledged[player] = sequence;
                                counters.enqueue_ok.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => {
                                counters.enqueue_errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
                Ok(_) => {
                    counters.invariant_errors.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    counters
                        .enqueue_errors
                        .fetch_add(chunk.len() as u64, Ordering::Relaxed);
                }
            }
        }
        let now = Instant::now();
        next_batch = next_periodic_deadline(next_batch, batch_interval, now);
    }
    acknowledged
}

fn next_periodic_deadline(previous: Instant, interval: Duration, now: Instant) -> Instant {
    let scheduled = previous + interval;
    if scheduled <= now {
        now + interval
    } else {
        scheduled
    }
}

async fn apply_direct_transaction(
    pool: &DbProxyClientPool,
    state: &mut PlayerState,
    counters: &Counters,
    run_id: u128,
) {
    let sequence = state.transaction_sequence + 1;
    let request = state
        .pending_transaction
        .get_or_insert_with(|| TransactionalWrite {
            operation_id: format!("soak:transaction:{run_id}:{}:{sequence}", state.index),
            record: state.direct_record.clone(),
            schema: "tiangz.fault-soak.transaction".to_string(),
            schema_version: 1,
            expected_revision: state.direct_revision,
            payload: payload(state.index, sequence, "direct"),
            result: format!("committed:{sequence}").into_bytes(),
            updated_at_unix_ms: sequence,
        });
    match pool.apply_transaction(request.clone()).await {
        Ok(TransactionalWriteOutcome::Applied { new_revision, .. }) => {
            complete_transaction(state, counters, sequence, new_revision);
            counters.transaction_applied.fetch_add(1, Ordering::Relaxed);
        }
        Ok(TransactionalWriteOutcome::Duplicate { new_revision, .. }) => {
            complete_transaction(state, counters, sequence, new_revision);
            counters
                .transaction_duplicate
                .fetch_add(1, Ordering::Relaxed);
        }
        Err(_) => {
            counters.transaction_errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn complete_transaction(
    state: &mut PlayerState,
    counters: &Counters,
    sequence: u64,
    new_revision: Revision,
) {
    let expected = Revision(state.direct_revision.0 + 1);
    if new_revision != expected {
        counters.invariant_errors.fetch_add(1, Ordering::Relaxed);
    }
    state.direct_revision = new_revision;
    state.transaction_sequence = sequence;
    state.pending_transaction = None;
}

async fn apply_trade(
    pool: &DbProxyClientPool,
    state: &mut PlayerState,
    counters: &Counters,
    run_id: u128,
) {
    let sequence = state.trade_sequence + 1;
    let request = state
        .pending_trade
        .get_or_insert_with(|| trade(run_id, state.index, sequence));
    match pool.apply_trade_transaction(request.clone()).await {
        Ok(TradeTransactionOutcome::Applied(receipt)) => {
            if receipt.new_trade_version != Revision(1) {
                counters.invariant_errors.fetch_add(1, Ordering::Relaxed);
            }
            state.trade_sequence = sequence;
            state.pending_trade = None;
            counters.trade_applied.fetch_add(1, Ordering::Relaxed);
        }
        Ok(TradeTransactionOutcome::Duplicate(receipt)) => {
            if receipt.new_trade_version != Revision(1) {
                counters.invariant_errors.fetch_add(1, Ordering::Relaxed);
            }
            state.trade_sequence = sequence;
            state.pending_trade = None;
            counters.trade_duplicate.fetch_add(1, Ordering::Relaxed);
        }
        Err(_) => {
            counters.trade_errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn trade(run_id: u128, player: usize, sequence: u64) -> TradeTransaction {
    let suffix = format!("{run_id}:{player}:{sequence}");
    let trade_id = format!("soak-trade:{suffix}");
    let debit = RecordKey::new("fault-soak-trade", format!("{suffix}:debit"))
        .expect("generated trade record is valid");
    let credit = RecordKey::new("fault-soak-trade", format!("{suffix}:credit"))
        .expect("generated trade record is valid");
    TradeTransaction {
        operation_id: format!("soak:trade-operation:{suffix}"),
        transition: TradeTransition {
            trade_id: trade_id.clone(),
            expected_version: Revision::ZERO,
            expected_state: None,
            next_state: TradeState::Escrowed,
            payload: b"fault-soak-escrow".to_vec(),
            updated_at_unix_ms: sequence,
        },
        writes: vec![
            TransactionalRecordWrite {
                record: debit.clone(),
                schema: "tiangz.fault-soak.wallet".to_string(),
                schema_version: 1,
                expected_revision: Revision::ZERO,
                payload: b"gold=-1".to_vec(),
                updated_at_unix_ms: sequence,
            },
            TransactionalRecordWrite {
                record: credit.clone(),
                schema: "tiangz.fault-soak.escrow".to_string(),
                schema_version: 1,
                expected_revision: Revision::ZERO,
                payload: b"gold=1".to_vec(),
                updated_at_unix_ms: sequence,
            },
        ],
        ledger_postings: vec![
            LedgerPosting {
                posting_id: format!("soak:ledger:{suffix}:debit"),
                account_id: debit.key,
                asset: "gold".to_string(),
                amount: -1,
                metadata: Vec::new(),
            },
            LedgerPosting {
                posting_id: format!("soak:ledger:{suffix}:credit"),
                account_id: credit.key,
                asset: "gold".to_string(),
                amount: 1,
                metadata: Vec::new(),
            },
        ],
        outbox_events: vec![OutboxEvent {
            event_id: format!("soak:event:{suffix}"),
            topic: "fault-soak.trade".to_string(),
            partition_key: trade_id,
            payload: b"escrowed".to_vec(),
            occurred_at_unix_ms: sequence,
        }],
        result: b"escrowed".to_vec(),
    }
}

async fn report_progress(
    counters: Arc<Counters>,
    started: Instant,
    deadline: Instant,
    interval: Duration,
) {
    let mut previous = CounterSnapshot::default();
    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        tokio::time::sleep(interval.min(deadline.saturating_duration_since(now))).await;
        let current = counters.snapshot();
        emit(
            "SOAK_INTERVAL",
            json!({
                "elapsedSeconds": started.elapsed().as_secs_f64(),
                "delta": current.saturating_sub(previous).json(),
                "total": current.json(),
            }),
        );
        previous = current;
    }
}

async fn validate_final_state(
    pool: &DbProxyClientPool,
    states: &[PlayerState],
    acknowledged_enqueues: &[u64],
    run_id: u128,
    timeout: Duration,
) -> Result<(), DynError> {
    let deadline = Instant::now() + timeout;
    loop {
        let error = match validate_once(pool, states, acknowledged_enqueues, run_id).await {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        if Instant::now() >= deadline {
            return Err(format!("final state did not converge within {timeout:?}: {error}").into());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn validate_once(
    pool: &DbProxyClientPool,
    states: &[PlayerState],
    acknowledged_enqueues: &[u64],
    run_id: u128,
) -> Result<(), DynError> {
    for state in states {
        let direct = pool
            .load(&state.direct_record)
            .await?
            .ok_or("direct snapshot is missing")?;
        if direct.revision != state.direct_revision {
            return Err(format!(
                "player {} direct revision mismatch: expected {:?}, got {:?}",
                state.index, state.direct_revision, direct.revision
            )
            .into());
        }
        let queued = pool
            .load(&state.queued_record)
            .await?
            .ok_or("queued snapshot is missing")?;
        let queued_sequence = payload_sequence(&queued.payload)?;
        let acknowledged = acknowledged_enqueues[state.index];
        if queued_sequence < acknowledged {
            return Err(format!(
                "player {} acknowledged enqueue {} but PostgreSQL/Redis exposes {}",
                state.index, acknowledged, queued_sequence
            )
            .into());
        }
        if state.trade_sequence > 0 {
            let trade_id = format!(
                "soak-trade:{run_id}:{}:{}",
                state.index, state.trade_sequence
            );
            let trade = pool
                .load_trade(&trade_id)
                .await?
                .ok_or("completed trade is missing")?;
            if trade.version != Revision(1) || trade.state != TradeState::Escrowed {
                return Err(format!("player {} trade state is inconsistent", state.index).into());
            }
        }
    }
    Ok(())
}

fn payload(player: usize, sequence: u64, kind: &str) -> Vec<u8> {
    format!("kind={kind};player={player};sequence={sequence}").into_bytes()
}

fn payload_sequence(payload: &[u8]) -> Result<u64, DynError> {
    let text = std::str::from_utf8(payload)?;
    let value = text
        .split(';')
        .find_map(|field| field.strip_prefix("sequence="))
        .ok_or("snapshot payload has no sequence")?;
    Ok(value.parse()?)
}

fn emit(prefix: &str, value: serde_json::Value) {
    println!("{prefix} {value}");
    let _ = io::stdout().flush();
}

fn parse_options() -> Result<Options, DynError> {
    let mut endpoint = "127.0.0.1:7800".to_string();
    let mut failover_endpoint = None;
    let mut pool_size = 32_usize;
    let mut read_pool_size = None;
    let mut write_pool_size = None;
    let mut players = 100_usize;
    let mut duration_seconds = 7_200_u64;
    let mut cycle_ms = 1_000_u64;
    let mut trade_interval_cycles = 600_u64;
    let mut report_seconds = 60_u64;
    let mut validation_timeout_seconds = 180_u64;
    let args = env::args().skip(1).collect::<Vec<_>>();
    let mut index = 0;
    while index < args.len() {
        let value = args
            .get(index + 1)
            .ok_or("fault-soak option requires a value")?;
        match args[index].as_str() {
            "--endpoint" => endpoint = value.clone(),
            "--failover-endpoint" => failover_endpoint = Some(value.clone()),
            "--pool-size" => pool_size = value.parse()?,
            "--read-pool-size" => read_pool_size = Some(value.parse()?),
            "--write-pool-size" => write_pool_size = Some(value.parse()?),
            "--players" => players = value.parse()?,
            "--duration" => duration_seconds = value.parse()?,
            "--cycle-ms" => cycle_ms = value.parse()?,
            "--trade-interval-cycles" => trade_interval_cycles = value.parse()?,
            "--report-interval" => report_seconds = value.parse()?,
            "--validation-timeout" => validation_timeout_seconds = value.parse()?,
            option => return Err(format!("unknown fault-soak option: {option}").into()),
        }
        index += 2;
    }
    if pool_size == 0
        || read_pool_size == Some(0)
        || write_pool_size == Some(0)
        || players == 0
        || duration_seconds == 0
        || cycle_ms == 0
        || trade_interval_cycles == 0
        || report_seconds == 0
        || validation_timeout_seconds == 0
    {
        return Err("fault-soak numeric options must be non-zero".into());
    }
    if read_pool_size.is_some() != write_pool_size.is_some() {
        return Err("read and write pool sizes must be supplied together".into());
    }
    Ok(Options {
        endpoint,
        failover_endpoint,
        auth_token: env::var("DBPROXY_AUTH_TOKEN")?,
        pool_size,
        read_pool_size,
        write_pool_size,
        players,
        duration: Duration::from_secs(duration_seconds),
        cycle: Duration::from_millis(cycle_ms),
        trade_interval_cycles,
        report_interval: Duration::from_secs(report_seconds),
        validation_timeout: Duration::from_secs(validation_timeout_seconds),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn periodic_deadline_preserves_an_on_time_schedule() {
        let origin = Instant::now();
        let interval = Duration::from_secs(5);

        assert_eq!(
            next_periodic_deadline(origin, interval, origin + Duration::from_secs(1)),
            origin + interval
        );
    }

    #[test]
    fn periodic_deadline_skips_missed_intervals() {
        let origin = Instant::now();
        let interval = Duration::from_secs(5);
        let now = origin + Duration::from_secs(30);

        assert_eq!(
            next_periodic_deadline(origin, interval, now),
            now + interval
        );
    }
}
