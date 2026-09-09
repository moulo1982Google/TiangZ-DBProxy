//! 在真实服务上持续验证通用原子提交、幂等回执和共享目标Outbox；不包含游戏规则。
//! Exercises generic commits, idempotent receipts and fan-in Outbox against a real server.
use std::{env, error::Error, time::Duration};
use tiangz_dbproxy_client::{ClientConfig, ClientError, DbProxyClientPool};
use tiangz_dbproxy_core::{
    AppendRecord, CommitEffects, EventEnvelope, MultiRecordTransactionalWrite,
    MultiRecordTransactionalWriteOutcome, RecordKey, Revision, TransactionalRecordWrite,
};
use tiangz_dbproxy_protocol::{ProtocolError, wire};
use tokio::time::Instant;

type Failure = Box<dyn Error + Send + Sync>;

#[test]
fn retry_policy_does_not_hide_permanent_contract_failures() {
    assert!(retryable(&ClientError::RequestTimeout));
    assert!(retryable(&ClientError::Protocol(ProtocolError::Io(
        std::io::Error::from(std::io::ErrorKind::ConnectionReset),
    ))));
    assert!(!retryable(&ClientError::UnexpectedResponse("wrong reply")));
    for code in [
        wire::ErrorCode::StorageUnavailable,
        wire::ErrorCode::Internal,
    ] {
        let error = ClientError::Remote(tiangz_dbproxy_client::RemoteError {
            code,
            message: "fixture".into(),
            actual_revision: None,
        });
        assert_eq!(
            retryable(&error),
            code == wire::ErrorCode::StorageUnavailable
        );
    }
}

fn retryable(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::ConnectTimeout
            | ClientError::RequestTimeout
            | ClientError::ConnectionUnusable
            | ClientError::ConnectionClosed
            | ClientError::Protocol(ProtocolError::Io(_))
    ) || matches!(error, ClientError::Remote(remote) if remote.code == wire::ErrorCode::StorageUnavailable)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Failure> {
    let seconds: u64 = env::var("DBPROXY_RELAY_SOAK_SECONDS")?.parse()?;
    if !(30..=14400).contains(&seconds) {
        return Err("duration must be 30..14400 seconds".into());
    }
    let prefix = env::var("DBPROXY_RELAY_SOAK_PREFIX")?;
    let interval_ms: u64 = env::var("DBPROXY_RELAY_SOAK_INTERVAL_MS")
        .unwrap_or_else(|_| "2000".into())
        .parse()?;
    if !(250..=60_000).contains(&interval_ms) {
        return Err("interval must be 250..60000 milliseconds".into());
    }
    let config = ClientConfig::new(
        "127.0.0.1:7800",
        env::var("DBPROXY_AUTH_TOKEN")?,
        "relay-soak",
    )
    .with_endpoints(["127.0.0.1:7801".to_string()]);
    let pool = DbProxyClientPool::connect(config, 2).await?;
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut committed = 0u64;
    let mut retries = 0u64;
    println!(
        "RELAY_READY {}",
        serde_json::json!({"prefix":prefix,"seconds":seconds,"intervalMs":interval_ms})
    );
    while Instant::now() < deadline {
        let sequence = committed + 1;
        let id = format!("{prefix}:{sequence}");
        let payload = serde_json::to_vec(&serde_json::json!({"sequence":sequence}))?;
        let request = MultiRecordTransactionalWrite {
            operation_id: id.clone(),
            writes: ["a", "b"]
                .into_iter()
                .map(|side| {
                    Ok(TransactionalRecordWrite {
                        record: RecordKey::new("relay_soak", format!("{id}:{side}"))?,
                        schema: "document".into(),
                        schema_version: 1,
                        expected_revision: Revision::ZERO,
                        payload: payload.clone(),
                        updated_at_unix_ms: sequence,
                    })
                })
                .collect::<Result<Vec<_>, tiangz_dbproxy_core::StoreError>>()?,
            result: payload.clone(),
        };
        let effects = CommitEffects {
            appends: vec![AppendRecord {
                record: RecordKey::new("relay_soak_audit", &id)?,
                schema: "fact".into(),
                schema_version: 1,
                payload: payload.clone(),
                occurred_at_unix_ms: sequence,
            }],
            outbox_events: vec![
                EventEnvelope {
                    event_id: id.clone(),
                    producer: if sequence.is_multiple_of(2) {
                        "achievement"
                    } else {
                        "game"
                    }
                    .into(),
                    event_type: "DocumentChanged".into(),
                    aggregate_type: "document".into(),
                    aggregate_id: prefix.clone(),
                    partition_key: prefix.clone(),
                    schema_version: 1,
                    content_type: "application/json".into(),
                    payload: payload.clone(),
                    occurred_at_unix_ms: sequence,
                    route_version: 1,
                }
                .into_outbox()?,
            ],
        };
        let retry_deadline = Instant::now() + Duration::from_secs(180);
        loop {
            match pool.commit_records(request.clone(), effects.clone()).await {
                Ok(_) => break,
                Err(error) => {
                    retries += 1;
                    if !retryable(&error) || Instant::now() >= retry_deadline {
                        return Err(error.into());
                    }
                    if retries == 1 || retries.is_multiple_of(30) {
                        println!(
                            "RELAY_RETRY {}",
                            serde_json::json!({"sequence":sequence,"retries":retries})
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
        // 响应丢失后仍以同一请求重试；不允许把结果未知当成新的操作。
        // Keep identical identity and effects across ambiguous responses.
        loop {
            match pool.commit_records(request.clone(), effects.clone()).await {
                Ok(MultiRecordTransactionalWriteOutcome::Duplicate { .. }) => break,
                Ok(_) => return Err("acknowledged commit applied twice".into()),
                Err(error) if !retryable(&error) || Instant::now() >= retry_deadline => {
                    return Err(error.into());
                }
                Err(_) => {
                    retries += 1;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
        for write in &request.writes {
            loop {
                match pool.load(&write.record).await {
                    Ok(Some(snapshot))
                        if snapshot.revision == Revision(1) && snapshot.payload == payload =>
                    {
                        break;
                    }
                    Ok(_) => {
                        return Err(
                            "acknowledged generic commit snapshot is missing or incorrect".into(),
                        );
                    }
                    Err(error) if !retryable(&error) || Instant::now() >= retry_deadline => {
                        return Err(error.into());
                    }
                    Err(_) => {
                        retries += 1;
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }
        }
        committed = sequence;
        if committed == 1 || committed.is_multiple_of(30) {
            println!(
                "RELAY_INTERVAL {}",
                serde_json::json!({"committed":committed,"retries":retries})
            );
        }
        tokio::time::sleep(Duration::from_millis(interval_ms)).await;
    }
    println!(
        "RELAY_FINAL {}",
        serde_json::json!({"passed":true,"prefix":prefix,"committed":committed,"retries":retries})
    );
    Ok(())
}
