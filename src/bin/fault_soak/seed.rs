//! 仅初始化阶段允许有界重试，完成后才启动正式计时与故障注入。
//! Bounded setup retries finish before measured load and fault injection begin.
use super::{DynError, PlayerState, emit, payload, retryable_operation_error};
use std::{future::Future, time::Duration};
use tiangz_dbproxy_client::{ClientError, DbProxyClientPool};
use tiangz_dbproxy_core::{RecordKey, Revision, SnapshotWrite};
use tokio::{
    task::JoinSet,
    time::{Instant, sleep_until, timeout_at},
};

/// 所有玩家的预置与预热共享期限；暂时不可用重试同一请求，永久错误立即失败。
/// Setup shares one deadline across players; transient retries retain the request and permanent errors fail immediately.
async fn retry_seed<T, F, Fut>(deadline: Instant, mut operation: F) -> Result<T, DynError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ClientError>>,
{
    let mut attempts = 0;
    loop {
        if Instant::now() >= deadline {
            return Err("seed/warmup deadline expired before request".into());
        }
        attempts += 1;
        match timeout_at(deadline, operation()).await {
            Ok(Ok(value)) => {
                if attempts > 1 {
                    emit(
                        "SOAK_SEED_RECOVERED",
                        serde_json::json!({"attempts": attempts}),
                    );
                }
                return Ok(value);
            }
            Ok(Err(error)) if retryable_operation_error(&error) => {
                if attempts <= 3 {
                    emit(
                        "SOAK_SEED_RETRY",
                        serde_json::json!({"attempt": attempts,
                        "error": error.to_string().chars().take(512).collect::<String>()}),
                    );
                }
                sleep_until(deadline.min(Instant::now() + Duration::from_millis(100))).await;
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => return Err("seed/warmup operation exceeded shared deadline".into()),
        }
    }
}

pub(super) async fn seed_and_warm_players(
    pool: &DbProxyClientPool,
    players: usize,
    run_id: u128,
) -> Result<Vec<PlayerState>, DynError> {
    let deadline = Instant::now() + Duration::from_secs(90);
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
                deadline,
            )
            .await?;
            seed_snapshot(
                &pool,
                format!("soak:seed:{run_id}:{index}:queued"),
                queued_record.clone(),
                payload(index, 0, "queued"),
                run_id as u64,
                deadline,
            )
            .await?;
            if retry_seed(deadline, || pool.load(&direct_record))
                .await?
                .is_none()
                || retry_seed(deadline, || pool.load(&queued_record))
                    .await?
                    .is_none()
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
    deadline: Instant,
) -> Result<Revision, DynError> {
    let request = SnapshotWrite {
        request_id,
        record,
        schema: "tiangz.fault-soak.player".to_string(),
        schema_version: 1,
        payload,
        expected_revision: Some(Revision::ZERO),
        updated_at_unix_ms,
    };
    let outcome = retry_seed(deadline, || pool.save(request.clone())).await?;
    Ok(match outcome {
        tiangz_dbproxy_core::SnapshotWriteOutcome::Applied { revision }
        | tiangz_dbproxy_core::SnapshotWriteOutcome::Duplicate { revision } => revision,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[tokio::test]
    async fn transient_setup_errors_recover_without_hiding_permanent_errors() {
        let calls = AtomicUsize::new(0);
        let value = retry_seed(Instant::now() + Duration::from_secs(2), || async {
            match calls.fetch_add(1, Ordering::SeqCst) {
                0 => Err(ClientError::RequestNotSentTimeout),
                1 => Err(ClientError::RequestTimeout),
                _ => Ok(42),
            }
        })
        .await
        .unwrap();
        assert_eq!(value, 42);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        let calls = AtomicUsize::new(0);
        let error = retry_seed(Instant::now() + Duration::from_secs(2), || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(ClientError::UnexpectedResponse("permanent"))
        })
        .await
        .unwrap_err();
        assert!(error.to_string().contains("permanent"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn expired_setup_never_starts_a_new_attempt() {
        let calls = AtomicUsize::new(0);
        let error = retry_seed(Instant::now(), || {
            calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok::<(), ClientError>(()))
        })
        .await
        .unwrap_err();
        assert!(error.to_string().contains("before request"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn setup_deadline_cancels_pending_work_and_does_not_reset_on_retry() {
        struct Dropped<'a>(&'a AtomicBool);
        impl Drop for Dropped<'_> {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = AtomicBool::new(false);
        let calls = AtomicUsize::new(0);
        let error = retry_seed(Instant::now() + Duration::from_millis(30), || async {
            calls.fetch_add(1, Ordering::SeqCst);
            let _guard = Dropped(&dropped);
            std::future::pending::<Result<(), ClientError>>().await
        })
        .await
        .unwrap_err();
        assert!(error.to_string().contains("shared deadline"));
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let calls = AtomicUsize::new(0);
        let error = retry_seed(Instant::now() + Duration::from_millis(30), || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(ClientError::RequestNotSentTimeout)
        })
        .await
        .unwrap_err();
        assert!(error.to_string().contains("before request"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
