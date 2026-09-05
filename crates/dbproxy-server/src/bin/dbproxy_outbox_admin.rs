//! 本机/运维侧 CLI，权限由独立 PostgreSQL 账号控制；不监听管理 HTTP。
//! Operator CLI secured by a dedicated PostgreSQL role, without an admin HTTP listener.
use std::{env, error::Error};
use tiangz_dbproxy_storage::PostgresSnapshotStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.len() < 3 || args[0] != "--postgres-url-env" {
        return Err("usage: --postgres-url-env NAME list | inspect EVENT_ID | retry EVENT_ID OPERATOR REASON".into());
    }
    let valid = matches!(args[2].as_str(), "list") && args.len() == 3
        || args[2] == "inspect" && args.len() == 4
        || args[2] == "retry" && args.len() == 6;
    if !valid {
        return Err("invalid outbox admin arguments; bulk replay is not supported".into());
    }
    let url = env::var(&args[1]).map_err(|_| "PostgreSQL environment variable is missing")?;
    let queue = PostgresSnapshotStore::connect_existing(&url)
        .await?
        .outbox_queue();
    match args[2].as_str() {
        "list" => println!(
            "{}",
            serde_json::to_string(&queue.dead_letter_ids().await?)?
        ),
        "inspect" => println!(
            "{}",
            serde_json::to_string_pretty(&queue.inspect(&args[3]).await?)?
        ),
        "retry" => {
            let retried = queue
                .retry_dead_letter(&args[3], &args[4], &args[5])
                .await?;
            println!(
                "{}",
                serde_json::json!({"event_id":args[3],"retried":retried})
            );
            if !retried {
                return Err(
                    "event is absent, already published, not dead-lettered, or actively leased"
                        .into(),
                );
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}
