use std::{env, error::Error, sync::Arc};

use tiangz_dbproxy_server::{
    DbProxyBackend, DbProxyServer, ServerConfig, StorageBackend,
    config::{config_path_from_args, load_config},
    run_backlog_worker,
};
use tokio::{sync::watch, task::JoinSet};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config_path = config_path_from_args(env::args())?;
    let config = load_config(config_path)?;
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(&config.log_filter)?)
        .init();

    let backend = Arc::new(
        StorageBackend::connect(
            &config.postgres_url,
            &config.redis_url,
            config.storage_shards,
        )
        .await?,
    );
    let mut server_config = ServerConfig::new(config.listen_addr, config.auth_token.clone());
    server_config.max_frame_bytes = config.max_frame_bytes;
    server_config.handshake_timeout = config.handshake_timeout;
    server_config.shutdown_grace = config.shutdown_grace;
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
    for _ in 0..config.backlog_workers {
        workers.spawn(run_backlog_worker(
            Arc::clone(&backend),
            config.backlog_lease_ms,
            config.backlog_idle_delay,
            config.backlog_failure_delay,
            shutdown_rx.clone(),
        ));
    }
    tracing::info!(
        %actual_addr,
        config = %config.source.display(),
        shard_count = config.storage_shards,
        worker_count = config.backlog_workers,
        "TiangZ DBProxy started"
    );
    let serve_result = server.serve(shutdown_rx.clone()).await;
    let _ = shutdown_tx.send(true);
    if tokio::time::timeout(config.shutdown_grace, async {
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
            shutdown_grace_ms = config.shutdown_grace.as_millis(),
            "DBProxy backlog worker shutdown grace expired; Redis leases will recover unfinished work"
        );
    }
    serve_result?;
    tracing::info!("TiangZ DBProxy stopped");
    Ok(())
}
