use std::{
    env,
    error::Error,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tiangz_dbproxy_server::{
    DbProxyBackend, DbProxyMetrics, DbProxyServer, MemoryBackend, ObservabilityServer,
    RetryWorkerPolicy, ServerConfig, StorageBackend, StorageBackendConfig,
    config::{ResolvedDbProxyConfig, ResolvedStorage, config_path_from_args, load_config},
    run_backlog_worker_observed, run_cache_repair_worker_observed, run_outbox_worker_observed,
    run_storage_metrics_poller,
};
use tokio::{sync::watch, task::JoinSet};
use tracing_subscriber::EnvFilter;

fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<_>>();
    if args.get(1).is_some_and(|a| a == "--check-config") {
        if args.len() != 3 {
            return Err("usage: --check-config PATH".into());
        }
        tiangz_dbproxy_server::config::check_config(&args[2])?;
        println!(
            "Configuration structure and capabilities verified; no environment secrets read, no connections opened. Secret availability is checked at startup."
        );
        return Ok(());
    }
    if args
        .get(1)
        .is_some_and(|a| a == "--check-tenants" || a == "--tenants")
    {
        if args.len() != 3 {
            return Err("usage: --tenants PATH or --check-tenants PATH".into());
        }
        let path = std::path::Path::new(&args[2]);
        if args[1] == "--check-tenants" {
            tiangz_dbproxy_server::tenant_config::check_deployment(path)?;
            println!(
                "Tenant structure checked offline; credential and storage isolation checks run at startup."
            );
            return Ok(());
        }
        let (deployment, configs) = tiangz_dbproxy_server::tenant_config::load_deployment(path)?;
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::try_new(&configs[0].log_filter)?)
            .with_ansi(false)
            .init();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(configs[0].runtime_worker_threads)
            .enable_all()
            .build()?;
        return runtime.block_on(async move {
            let mut tenants = Vec::new();
            for (declaration, config) in deployment.tenants.into_iter().zip(configs) {
                let (backend, durable) = prepare_backend(&config).await?;
                tenants.push((Some(declaration.id), config, backend, durable));
            }
            run_servers(
                tenants,
                Some((deployment.listen_addr, deployment.max_connections)),
            )
            .await
        });
    }
    let config_path = config_path_from_args(env::args())?;
    let config = load_config(config_path)?;
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(&config.log_filter)?)
        .with_ansi(false)
        .init();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.runtime_worker_threads)
        .enable_all()
        .build()?;
    runtime.block_on(run(config))
}

type BackendPair = (Arc<dyn DbProxyBackend>, Option<Arc<StorageBackend>>);
type TenantRuntime = (
    Option<String>,
    ResolvedDbProxyConfig,
    Arc<dyn DbProxyBackend>,
    Option<Arc<StorageBackend>>,
);

async fn run(config: ResolvedDbProxyConfig) -> Result<(), Box<dyn Error>> {
    let (backend, durable) = prepare_backend(&config).await?;
    run_servers(vec![(None, config, backend, durable)], None).await
}

async fn prepare_backend(config: &ResolvedDbProxyConfig) -> Result<BackendPair, Box<dyn Error>> {
    match config.storage.clone() {
        ResolvedStorage::PostgresRedis {
            postgres_url,
            redis_url,
            cache_redis_url,
            authoritative_read_namespaces,
            shards,
            cache_fallback_concurrency,
            cache_fallback_timeout_ms,
            cache_operation_timeout_ms,
            postgres_connection_wait_timeout_ms,
            postgres_reconnect_cooldown_ms,
            cache_fallback_circuit_failure_threshold,
            cache_fallback_circuit_cooldown_ms,
            cache_fallback_lock_lease_ms,
            cache_fallback_lock_wait_ms,
            cache_fallback_lock_poll_ms,
            cache_ttl_ms,
            cache_ttl_jitter_ms,
            cache_negative_ttl_ms,
            cache_stale_while_revalidate_ms,
        } => {
            let backend = Arc::new(
                StorageBackend::connect_with_outbox(
                    &postgres_url,
                    &redis_url,
                    &cache_redis_url,
                    StorageBackendConfig {
                        shard_count: shards,
                        tiered: tiangz_dbproxy_storage::TieredSnapshotStoreConfig {
                            postgres: tiangz_dbproxy_storage::PostgresRequestConfig {
                                connection_wait_timeout: Duration::from_millis(
                                    postgres_connection_wait_timeout_ms,
                                ),
                                reconnect_cooldown: Duration::from_millis(
                                    postgres_reconnect_cooldown_ms,
                                ),
                            },
                            cache_operation_timeout: Duration::from_millis(
                                cache_operation_timeout_ms,
                            ),
                            fallback: tiangz_dbproxy_storage::CacheFallbackConfig {
                                max_concurrent: cache_fallback_concurrency,
                                timeout: Duration::from_millis(cache_fallback_timeout_ms),
                            },
                            circuit: tiangz_dbproxy_storage::CacheFallbackCircuitConfig {
                                failure_threshold: cache_fallback_circuit_failure_threshold,
                                cooldown: Duration::from_millis(cache_fallback_circuit_cooldown_ms),
                            },
                            lock: tiangz_dbproxy_storage::CacheFallbackLockConfig {
                                lease: Duration::from_millis(cache_fallback_lock_lease_ms),
                                wait: Duration::from_millis(cache_fallback_lock_wait_ms),
                                poll_interval: Duration::from_millis(cache_fallback_lock_poll_ms),
                            },
                            cache: tiangz_dbproxy_storage::SnapshotCacheConfig {
                                ttl: Duration::from_millis(cache_ttl_ms),
                                ttl_jitter: Duration::from_millis(cache_ttl_jitter_ms),
                                negative_ttl: Duration::from_millis(cache_negative_ttl_ms),
                                stale_while_revalidate: Duration::from_millis(
                                    cache_stale_while_revalidate_ms,
                                ),
                            },
                        },
                        enqueue: tiangz_dbproxy_storage::EnqueueBatchConfig {
                            ack: config.backlog_enqueue_ack,
                            ..Default::default()
                        },
                    },
                    &config.outbox_relay,
                )
                .await?
                .with_authoritative_read_namespaces(authoritative_read_namespaces.into_vec())?,
            );
            let server_backend: Arc<dyn DbProxyBackend> = backend.clone();
            Ok((server_backend, Some(backend)))
        }
        ResolvedStorage::Memory { shards } => {
            let backend: Arc<dyn DbProxyBackend> = Arc::new(MemoryBackend::new(shards)?);
            Ok((backend, None))
        }
    }
}

async fn run_servers(
    tenants: Vec<TenantRuntime>,
    shared: Option<(std::net::SocketAddr, usize)>,
) -> Result<(), Box<dyn Error>> {
    let config = &tenants[0].1;
    let grace = config.shutdown_grace;
    let mut server_config = ServerConfig::new(config.listen_addr, config.auth_token.clone());
    server_config.max_frame_bytes = config.max_frame_bytes;
    server_config.max_payload_bytes = config.max_payload_bytes;
    server_config.max_connections = shared.map_or(config.max_connections, |value| value.1);
    server_config.listen_addr = shared.map_or(config.listen_addr, |value| value.0);
    server_config.max_in_flight_per_connection = config.max_in_flight_per_connection;
    server_config.handshake_timeout = config.handshake_timeout;
    server_config.shutdown_grace = grace;
    let admission_metrics = Arc::new(DbProxyMetrics::default());
    server_config.metrics = Arc::clone(&admission_metrics);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut workers = JoinSet::new();
    let mut monitors = Vec::new();
    let mut routes = Vec::new();
    let mut tenant_metrics = Vec::new();
    for (id, config, backend, durable_backend) in &tenants {
        let metrics = if shared.is_some() {
            Arc::new(DbProxyMetrics::default())
        } else {
            Arc::clone(&admission_metrics)
        };
        if durable_backend.is_some() {
            metrics.require_healthy_dependencies();
        }
        if let Some(durable) = durable_backend {
            *metrics
                .outbox_relay
                .lock()
                .unwrap_or_else(|error| error.into_inner()) =
                Some(Arc::clone(&durable.outbox_relay_metrics));
        }
        if let Some(id) = id {
            routes.push(tiangz_dbproxy_server::TenantBackend::new(
                id,
                &config.auth_token,
                Arc::clone(backend),
                config.max_connections,
                Arc::clone(&metrics),
            )?);
        }
        tenant_metrics.push(metrics);
    }
    let signal_listener = shutdown_listener(
        shutdown_tx.clone(),
        std::iter::once(Arc::clone(&admission_metrics))
            .chain(tenant_metrics.iter().cloned())
            .collect(),
    )?;
    // 先绑定并校验所有监听端口；失败时尚未启动后台任务。
    // Bind every listener before spawning background work.
    let server = if shared.is_some() {
        DbProxyServer::bind_tenants(server_config, routes).await?
    } else {
        DbProxyServer::bind(server_config, Arc::clone(&tenants[0].2)).await?
    };
    for ((_, config, _, _), metrics) in tenants.iter().zip(&tenant_metrics) {
        if let Some(address) = config.observability_listen_addr {
            match ObservabilityServer::start_with_binding_policy(
                address,
                config.observability_allow_non_loopback,
                Arc::clone(metrics),
                config.storage.name(),
                shutdown_rx.clone(),
            )
            .await
            {
                Ok(monitor) => monitors.push(monitor),
                Err(error) => {
                    let _ = shutdown_tx.send(true);
                    for monitor in monitors {
                        monitor.stop().await;
                    }
                    return Err(error.into());
                }
            }
        }
    }
    for ((id, config, _, durable_backend), metrics) in tenants.iter().zip(&tenant_metrics) {
        let durable_backend = durable_backend.clone();
        if let Some(backend) = durable_backend {
            workers.spawn(run_storage_metrics_poller(
                Arc::clone(&backend),
                Arc::clone(metrics),
                Duration::from_secs(5),
                shutdown_rx.clone(),
            ));
            for _ in 0..config.backlog_workers {
                workers.spawn(run_backlog_worker_observed(
                    Arc::clone(&backend),
                    config.backlog_lease_ms,
                    config.backlog_idle_delay,
                    config.backlog_failure_delay,
                    shutdown_rx.clone(),
                    Some(Arc::clone(metrics)),
                ));
            }
            let instance = worker_instance_id();
            let cache_repair_policy = RetryWorkerPolicy {
                lease_ms: config.cache_repair.lease_ms,
                base_retry_delay_ms: config.cache_repair.base_retry_delay_ms,
                max_retry_delay_ms: config.cache_repair.max_retry_delay_ms,
                max_attempts: config.cache_repair.max_attempts,
            };
            for index in 0..config.cache_repair.workers {
                workers.spawn(run_cache_repair_worker_observed(
                    Arc::clone(&backend),
                    format!("cache-repair-{instance}-{index}"),
                    cache_repair_policy,
                    config.cache_repair.idle_delay,
                    shutdown_rx.clone(),
                    Some(Arc::clone(metrics)),
                ));
            }
            let outbox_policy = RetryWorkerPolicy {
                lease_ms: config.outbox.lease_ms,
                base_retry_delay_ms: config.outbox.base_retry_delay_ms,
                max_retry_delay_ms: config.outbox.max_retry_delay_ms,
                max_attempts: config.outbox.max_attempts,
            };
            for index in 0..config.outbox.workers {
                workers.spawn(run_outbox_worker_observed(
                    Arc::clone(&backend),
                    format!("outbox-{instance}-{index}"),
                    outbox_policy,
                    config.outbox.idle_delay,
                    shutdown_rx.clone(),
                    Some(Arc::clone(metrics)),
                ));
            }
        }

        metrics.mark_ready();
        tracing::info!(
            tenant = id.as_deref().unwrap_or("legacy"),
            storage_backend = config.storage.name(),
            "tenant backend ready"
        );
    }
    admission_metrics.mark_ready();
    let signal_task = tokio::spawn(signal_listener);
    tracing::info!(actual_addr = %server.local_addr()?, tenant_count = tenants.len(), "TiangZ DBProxy started");
    let serve_result = server.serve(shutdown_rx.clone()).await;
    signal_task.abort();
    admission_metrics.mark_stopping();
    for metrics in &tenant_metrics {
        metrics.mark_stopping();
    }
    let _ = shutdown_tx.send(true);
    if tokio::time::timeout(grace, async {
        while let Some(joined) = workers.join_next().await {
            if let Err(error) = joined {
                tracing::error!(%error, "DBProxy worker stopped unexpectedly");
            }
        }
    })
    .await
    .is_err()
    {
        workers.abort_all();
        while workers.join_next().await.is_some() {}
        tracing::warn!("DBProxy worker shutdown grace expired; durable leases remain recoverable");
    }
    for monitor in monitors {
        monitor.stop().await;
    }
    serve_result?;
    for metrics in &tenant_metrics {
        metrics.mark_stopped();
    }
    admission_metrics.mark_stopped();
    Ok(())
}

/// 两种 Unix 停止信号共用关闭通道；Windows 保留 Ctrl+C 行为。
/// Route both Unix stop signals through the same shutdown channel; retain Ctrl+C on Windows.
fn shutdown_listener(
    shutdown_tx: watch::Sender<bool>,
    metrics: Vec<Arc<DbProxyMetrics>>,
) -> std::io::Result<impl std::future::Future<Output = ()> + Send> {
    #[cfg(unix)]
    let (mut interrupt, mut terminate) = (
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?,
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
    );
    Ok(async move {
        #[cfg(unix)]
        let signal = tokio::select! {
            _ = interrupt.recv() => "SIGINT",
            _ = terminate.recv() => "SIGTERM",
        };
        #[cfg(not(unix))]
        let signal = {
            if let Err(error) = tokio::signal::ctrl_c().await {
                tracing::error!(%error, "failed to install Ctrl+C handler; stopping server");
            }
            "Ctrl+C"
        };
        for tenant in metrics {
            tenant.mark_stopping();
        }
        tracing::info!(signal, "DBProxy shutdown requested");
        let _ = shutdown_tx.send(true);
    })
}

fn worker_instance_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("{}-{nanos}", std::process::id())
}
