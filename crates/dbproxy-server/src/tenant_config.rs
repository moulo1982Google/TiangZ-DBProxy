//! 静态租户部署：凭据绑定独立后端，任何歧义在打开连接之前拒绝。
//! Static tenant deployment rejects ambiguous credentials/storage before opening connections.
use crate::config::{
    ConfigError, ResolvedDbProxyConfig, ResolvedStorage, check_config, load_config,
};
use serde::Deserialize;
use std::{
    collections::HashSet,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TenantDeployment {
    pub config_version: u32,
    pub listen_addr: SocketAddr,
    pub max_connections: usize,
    pub tenants: Vec<TenantConfigRef>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TenantConfigRef {
    pub id: String,
    pub config: PathBuf,
}

pub fn read_deployment(path: &Path) -> Result<TenantDeployment, ConfigError> {
    let text = fs::read_to_string(path)
        .map_err(|_| ConfigError("cannot read tenant deployment".into()))?;
    let mut value: TenantDeployment = serde_json::from_str(&text)
        .map_err(|_| ConfigError("invalid tenant deployment JSON".into()))?;
    if value.config_version != 1
        || value.tenants.is_empty()
        || value.tenants.len() > 64
        || !(1..=tokio::sync::Semaphore::MAX_PERMITS).contains(&value.max_connections)
    {
        return Err(ConfigError(
            "invalid tenant deployment version/count/connection limit".into(),
        ));
    }
    let mut ids = HashSet::new();
    for tenant in &mut value.tenants {
        if !crate::tenancy::valid_tenant_id(&tenant.id)
            || !ids.insert(tenant.id.clone())
            || tenant.config.as_os_str().is_empty()
        {
            return Err(ConfigError(
                "invalid or duplicate tenant declaration".into(),
            ));
        }
        tenant.config = path.parent().unwrap_or(Path::new(".")).join(&tenant.config);
    }
    Ok(value)
}

pub fn check_deployment(path: &Path) -> Result<(), ConfigError> {
    for tenant in read_deployment(path)?.tenants {
        check_config(tenant.config)?;
    }
    Ok(())
}

pub fn load_deployment(
    path: &Path,
) -> Result<(TenantDeployment, Vec<ResolvedDbProxyConfig>), ConfigError> {
    let deployment = read_deployment(path)?;
    let configs = deployment
        .tenants
        .iter()
        .map(|tenant| load_config(&tenant.config))
        .collect::<Result<Vec<_>, _>>()?;
    validate_isolation(&configs)?;
    Ok((deployment, configs))
}

/// 第一版保守拒绝跨租户重复数据库名/Redis DB 编号，包括端点别名。
/// V1 conservatively rejects reused database names/Redis DB numbers, even across endpoint aliases.
pub fn validate_isolation(configs: &[ResolvedDbProxyConfig]) -> Result<(), ConfigError> {
    if configs.is_empty() {
        return Err(ConfigError("no tenant configuration".into()));
    }
    let first = &configs[0];
    let mut tokens = HashSet::new();
    let mut databases = HashSet::new();
    let mut redis_databases = HashSet::new();
    let mut monitoring = HashSet::new();
    for config in configs {
        if !(16..=tiangz_dbproxy_protocol::MAX_AUTH_TOKEN_BYTES).contains(&config.auth_token.len())
        {
            return Err(ConfigError("invalid tenant token length".into()));
        }
        if !tokens.insert(&config.auth_token) {
            return Err(ConfigError("tenant credentials must differ".into()));
        }
        if config.max_frame_bytes != first.max_frame_bytes
            || config.max_payload_bytes != first.max_payload_bytes
            || config.handshake_timeout != first.handshake_timeout
            || config.shutdown_grace != first.shutdown_grace
            || config.runtime_worker_threads != first.runtime_worker_threads
            || config.log_filter != first.log_filter
        {
            return Err(ConfigError(
                "shared listener/runtime settings must agree across tenants".into(),
            ));
        }
        if config
            .observability_listen_addr
            .is_some_and(|address| !monitoring.insert(address))
        {
            return Err(ConfigError(
                "tenant observability endpoints must differ".into(),
            ));
        }
        if let ResolvedStorage::PostgresRedis {
            postgres_url,
            redis_url,
            cache_redis_url,
            ..
        } = &config.storage
        {
            let pg: tokio_postgres::Config = postgres_url
                .parse()
                .map_err(|_| ConfigError("invalid tenant PostgreSQL connection".into()))?;
            let name = pg
                .get_dbname()
                .filter(|name| !name.is_empty())
                .ok_or_else(|| ConfigError("explicit PostgreSQL database required".into()))?;
            if !databases.insert(name.to_owned()) {
                return Err(ConfigError(
                    "PostgreSQL database names must be unique per tenant".into(),
                ));
            }
            let mut own_databases = HashSet::new();
            for url in std::iter::once(redis_url)
                .chain(std::iter::once(cache_redis_url))
                .chain(
                    config
                        .outbox_relay
                        .publishers
                        .iter()
                        .map(|publisher| &publisher.url),
                )
            {
                let client = redis::Client::open(url.as_str())
                    .map_err(|_| ConfigError("invalid tenant Redis connection".into()))?;
                let db = client.get_connection_info().redis_settings().db();
                if db < 0 {
                    return Err(ConfigError("invalid tenant Redis database".into()));
                }
                own_databases.insert(db);
            }
            if own_databases.iter().any(|db| redis_databases.contains(db)) {
                return Err(ConfigError(
                    "Redis cache/backlog/outbox database numbers must not overlap tenants".into(),
                ));
            }
            redis_databases.extend(own_databases);
        }
    }
    Ok(())
}
