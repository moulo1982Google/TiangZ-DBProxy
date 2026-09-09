//! 通过真实 RPC 确认专用快照入队，故障恢复后直接核对 PostgreSQL；不以指标采样代替持久性证据。
//! Verifies acknowledged RPC enqueues against authoritative PostgreSQL after an AOF fault.
use std::{env, error::Error, time::Duration};
use tiangz_dbproxy_client::{ClientConfig, DbProxyClientPool};
use tiangz_dbproxy_core::{AsyncSnapshotStore, RecordKey, Revision, SnapshotWrite};
use tiangz_dbproxy_storage::{PostgresSnapshotStore, RedisSnapshotBacklog};

type Failure = Box<dyn Error + Send + Sync>;
const COUNT: usize = 64;

fn requests(prefix: &str) -> Result<Vec<SnapshotWrite>, Failure> {
    if prefix.is_empty()
        || prefix.len() > 80
        || !prefix
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-')
    {
        return Err("invalid isolated AOF probe prefix".into());
    }
    (0..COUNT)
        .map(|i| {
            Ok(SnapshotWrite {
                request_id: format!("{prefix}:{i}"),
                record: RecordKey::new("local_aof_probe", format!("{prefix}:{i}"))?,
                schema: "aof-probe".into(),
                schema_version: 1,
                payload: format!("acknowledged:{prefix}:{i}").into_bytes(),
                expected_revision: None,
                updated_at_unix_ms: 1,
            })
        })
        .collect()
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Failure> {
    // Bound connection/retry/verification time even when a dependency never recovers.
    tokio::time::timeout(Duration::from_secs(150), run()).await?
}

async fn run() -> Result<(), Failure> {
    let args: Vec<_> = env::args().collect();
    let mode = args.get(1).ok_or("missing mode")?;
    let writes = requests(args.get(2).ok_or("missing prefix")?)?;
    match mode.as_str() {
        "enqueue" => {
            let pool = DbProxyClientPool::connect(
                ClientConfig::new(
                    "127.0.0.1:7800",
                    env::var("DBPROXY_AUTH_TOKEN")?,
                    "aof-probe",
                ),
                1,
            )
            .await?;
            // Only a successful real response constitutes an acknowledged write.
            // On an ambiguous response fail the drill; do not fabricate an ACK from Redis depth.
            let outcomes = pool.enqueue_multi_snapshot(&writes).await?;
            if outcomes.len() != COUNT {
                return Err("incomplete enqueue receipt".into());
            }
            for outcome in outcomes {
                outcome.map_err(tiangz_dbproxy_client::ClientError::Remote)?;
            }
            println!(
                "AOF_ACK {}",
                serde_json::json!({"count":COUNT,"requests":writes.iter().map(|w| &w.request_id).collect::<Vec<_>>() })
            );
        }
        "stats" => {
            let backlog = RedisSnapshotBacklog::connect(&env::var("DBPROXY_REDIS_URL")?).await?;
            let stats = backlog.stats().await?;
            // Leased items remain durable backlog, even when the pending gauge is zero.
            if stats.pending + stats.processing < COUNT as u64 {
                return Err("acknowledged probe backlog is incomplete".into());
            }
            println!(
                "AOF_BACKLOG {}",
                serde_json::json!({"pending":stats.pending,"processing":stats.processing})
            );
        }
        "verify" => {
            let postgres =
                PostgresSnapshotStore::connect(&env::var("DBPROXY_POSTGRES_URL")?).await?;
            loop {
                let mut complete = true;
                for write in &writes {
                    match postgres.load(&write.record).await? {
                        None => {
                            complete = false;
                            break;
                        }
                        Some(s)
                            if s.revision == Revision(1)
                                && s.payload == write.payload
                                && s.schema == write.schema
                                && s.schema_version == write.schema_version => {}
                        Some(_) => {
                            return Err(format!(
                                "AOF payload or revision mismatch: {}",
                                write.request_id
                            )
                            .into());
                        }
                    }
                }
                if complete {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            println!(
                "AOF_VERIFIED {}",
                serde_json::json!({"count":COUNT,"passed":true})
            );
        }
        _ => return Err("expected enqueue, stats or verify".into()),
    }
    Ok(())
}

#[test]
fn probe_requests_are_unique_stable_and_unconditional() {
    let writes = requests("run-1").unwrap();
    assert_eq!(writes, requests("run-1").unwrap());
    assert_eq!(writes.len(), COUNT);
    assert_eq!(
        writes
            .iter()
            .map(|w| &w.record)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        COUNT
    );
    assert!(writes.iter().all(|w| w.expected_revision.is_none()));
    assert!(requests("../invalid").is_err());
    let mut queue = tiangz_dbproxy_core::SnapshotFlushQueue::new();
    for write in writes {
        queue.enqueue(write).unwrap();
    }
    assert_eq!(queue.len(), COUNT);
}
