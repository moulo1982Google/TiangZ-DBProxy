use std::{env, error::Error, net::SocketAddr, sync::Arc, time::Duration};

use tiangz_dbproxy_server::{
    DbProxyBackend, DbProxyServer, ServerConfig, StorageBackend, run_backlog_worker,
};
use tokio::{sync::watch, task::JoinSet};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let listen_addr: SocketAddr = env_value("DBPROXY_LISTEN_ADDR", "127.0.0.1:7800").parse()?;
    let auth_token = required_env("DBPROXY_AUTH_TOKEN")?;
    let postgres_url = required_env("DBPROXY_POSTGRES_URL")?;
    let redis_url = required_env("DBPROXY_REDIS_URL")?;
    let shard_count = parse_env("DBPROXY_STORAGE_SHARDS", 4_usize)?;
    let worker_count = parse_env("DBPROXY_BACKLOG_WORKERS", 1_usize)?;
    let lease_ms = parse_env("DBPROXY_BACKLOG_LEASE_MS", 30_000_u64)?;
    let max_frame_bytes = parse_env(
        "DBPROXY_MAX_FRAME_BYTES",
        tiangz_dbproxy_protocol::DEFAULT_MAX_FRAME_BYTES,
    )?;
    let shutdown_grace_ms = parse_env("DBPROXY_SHUTDOWN_GRACE_MS", 5_000_u64)?;
    if worker_count == 0 {
        return Err("DBPROXY_BACKLOG_WORKERS must be greater than zero".into());
    }
    if lease_ms == 0 {
        return Err("DBPROXY_BACKLOG_LEASE_MS must be greater than zero".into());
    }
    if shutdown_grace_ms == 0 {
        return Err("DBPROXY_SHUTDOWN_GRACE_MS must be greater than zero".into());
    }

    let backend = Arc::new(StorageBackend::connect(&postgres_url, &redis_url, shard_count).await?);
    let mut server_config = ServerConfig::new(listen_addr, auth_token);
    server_config.max_frame_bytes = max_frame_bytes;
    server_config.shutdown_grace = Duration::from_millis(shutdown_grace_ms);
    let server_backend: Arc<dyn DbProxyBackend> = backend.clone();
    let server = DbProxyServer::bind(server_config, server_backend).await?;
    let actual_addr = server.local_addr()?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let signal_tx = shutdown_tx.clone();
    tokio::spawn(async move {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to install Ctrl+C handler");
        }
        let _ = signal_tx.send(true);
    });

    let mut workers = JoinSet::new();
    for _ in 0..worker_count {
        workers.spawn(run_backlog_worker(
            Arc::clone(&backend),
            lease_ms,
            Duration::from_millis(20),
            Duration::from_secs(1),
            shutdown_rx.clone(),
        ));
    }
    tracing::info!(%actual_addr, shard_count, worker_count, "TiangZ DBProxy started");
    let serve_result = server.serve(shutdown_rx.clone()).await;
    let _ = shutdown_tx.send(true);
    if tokio::time::timeout(Duration::from_millis(shutdown_grace_ms), async {
        while let Some(joined) = workers.join_next().await {
            if let Err(error) = joined {
                tracing::error!(%error, "DBProxy backlog worker stopped unexpectedly");
            }
        }
    })
    .await
    .is_err()
    {
        workers.abort_all();
        tracing::warn!(
            shutdown_grace_ms,
            "DBProxy backlog worker shutdown grace expired; Redis leases will recover unfinished work"
        );
    }
    serve_result?;
    tracing::info!("TiangZ DBProxy stopped");
    Ok(())
}

fn required_env(name: &'static str) -> Result<String, Box<dyn Error>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(value),
        _ => Err(format!("{name} is required").into()),
    }
}

fn env_value(name: &'static str, default: &'static str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

fn parse_env<T>(name: &'static str, default: T) -> Result<T, Box<dyn Error>>
where
    T: std::str::FromStr,
    T::Err: Error + 'static,
{
    match env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(_) => Ok(default),
    }
}
