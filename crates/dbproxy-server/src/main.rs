use std::{env, error::Error, sync::Arc};

use tiangz_dbproxy_server::{
    DbProxyBackend, DbProxyMetrics, DbProxyServer, MemoryBackend, ObservabilityServer,
    ServerConfig, StorageBackend,
    config::{ResolvedDbProxyConfig, ResolvedStorage, config_path_from_args, load_config},
    run_backlog_worker_observed,
};
use tokio::{sync::watch, task::JoinSet};
use tracing_subscriber::EnvFilter;

fn main() -> Result<(), Box<dyn Error>> {
    let config_path = config_path_from_args(env::args())?;
    let config = load_config(config_path)?;
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(&config.log_filter)?)
        .init();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.runtime_worker_threads)
        .enable_all()
        .build()?;
    runtime.block_on(run(config))
}

async fn run(config: ResolvedDbProxyConfig) -> Result<(), Box<dyn Error>> {
    match config.storage.clone() {
        ResolvedStorage::PostgresRedis {
            postgres_url,
            redis_url,
            shards,
        } => {
            let backend =
                Arc::new(StorageBackend::connect(&postgres_url, &redis_url, shards).await?);
            let server_backend: Arc<dyn DbProxyBackend> = backend.clone();
            run_server(config, server_backend, Some(backend)).await
        }
        ResolvedStorage::Memory { shards } => {
            let backend: Arc<dyn DbProxyBackend> = Arc::new(MemoryBackend::new(shards)?);
            run_server(config, backend, None).await
        }
    }
}

async fn run_server(
    config: ResolvedDbProxyConfig,
    server_backend: Arc<dyn DbProxyBackend>,
    durable_backend: Option<Arc<StorageBackend>>,
) -> Result<(), Box<dyn Error>> {
    let mut server_config = ServerConfig::new(config.listen_addr, config.auth_token.clone());
    server_config.max_frame_bytes = config.max_frame_bytes;
    server_config.handshake_timeout = config.handshake_timeout;
    server_config.shutdown_grace = config.shutdown_grace;
    let metrics = Arc::new(DbProxyMetrics::default());
    server_config.metrics = Arc::clone(&metrics);
    let server = DbProxyServer::bind(server_config, server_backend).await?;
    let actual_addr = server.local_addr()?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let observability = match config.observability_listen_addr {
        Some(address) => Some(
            ObservabilityServer::start(
                address,
                Arc::clone(&metrics),
                config.storage.name(),
                shutdown_rx.clone(),
            )
            .await?,
        ),
        None => None,
    };

    let signal_tx = shutdown_tx.clone();
    tokio::spawn(async move {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to install Ctrl+C handler");
        }
        let _ = signal_tx.send(true);
    });

    let mut workers = JoinSet::new();
    if let Some(backend) = durable_backend {
        for _ in 0..config.backlog_workers {
            workers.spawn(run_backlog_worker_observed(
                Arc::clone(&backend),
                config.backlog_lease_ms,
                config.backlog_idle_delay,
                config.backlog_failure_delay,
                shutdown_rx.clone(),
                Some(Arc::clone(&metrics)),
            ));
        }
    }
    metrics.mark_ready();
    tracing::info!(
        %actual_addr,
        config = %config.source.display(),
        storage_backend = config.storage.name(),
        shard_count = config.storage.shards(),
        runtime_worker_threads = config.runtime_worker_threads,
        backlog_worker_count = if matches!(config.storage, ResolvedStorage::PostgresRedis { .. }) {
            config.backlog_workers
        } else {
            0
        },
        observability_addr = observability.as_ref().map(ObservabilityServer::local_addr).map(|value| value.to_string()),
        "TiangZ DBProxy started"
    );
    let serve_result = server.serve(shutdown_rx.clone()).await;
    metrics.mark_stopping();
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
    if let Some(observability) = observability {
        observability.stop().await;
    }
    serve_result?;
    metrics.mark_stopped();
    tracing::info!("TiangZ DBProxy stopped");
    Ok(())
}
