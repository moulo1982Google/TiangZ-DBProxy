//! 认证选择后端，连接生命周期内不可切换租户。
//! Authentication selects a backend immutable for the connection lifetime.
use crate::{DbProxyBackend, DbProxyMetrics, ServerError};
use std::{fmt, sync::Arc};
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct TenantBackend {
    pub(crate) id: String,
    pub(crate) token: String,
    pub(crate) backend: Arc<dyn DbProxyBackend>,
    pub(crate) slots: Arc<Semaphore>,
    pub(crate) metrics: Arc<DbProxyMetrics>,
}

impl TenantBackend {
    pub fn new(
        id: impl Into<String>,
        token: impl Into<String>,
        backend: Arc<dyn DbProxyBackend>,
        max_connections: usize,
        metrics: Arc<DbProxyMetrics>,
    ) -> Result<Self, ServerError> {
        let id = id.into();
        let token = token.into();
        if !valid_tenant_id(&id) {
            return Err(ServerError::InvalidConfig("invalid tenant id"));
        }
        if !(16..=tiangz_dbproxy_protocol::MAX_AUTH_TOKEN_BYTES).contains(&token.len()) {
            return Err(ServerError::InvalidConfig("invalid tenant token length"));
        }
        if !(1..=Semaphore::MAX_PERMITS).contains(&max_connections) {
            return Err(ServerError::InvalidConfig(
                "invalid tenant connection limit",
            ));
        }
        metrics.connection_limit_updated(max_connections);
        Ok(Self {
            id,
            token,
            backend,
            slots: Arc::new(Semaphore::new(max_connections)),
            metrics,
        })
    }
}

impl fmt::Debug for TenantBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TenantBackend")
            .field("id", &self.id)
            .field("token", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

pub fn valid_tenant_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id.as_bytes()[0].is_ascii_lowercase()
        && id
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-' || c == b'.')
}

pub(crate) fn validate_tenants(tenants: &[TenantBackend]) -> Result<(), ServerError> {
    if tenants.is_empty() || tenants.len() > 64 {
        return Err(ServerError::InvalidConfig("expected 1..=64 tenants"));
    }
    for (index, tenant) in tenants.iter().enumerate() {
        for other in &tenants[..index] {
            if tenant.id == other.id
                || crate::constant_time_token_eq(tenant.token.as_bytes(), other.token.as_bytes())
            {
                return Err(ServerError::InvalidConfig(
                    "duplicate tenant identity or credential",
                ));
            }
            if Arc::ptr_eq(&tenant.backend, &other.backend) {
                return Err(ServerError::InvalidConfig(
                    "tenants cannot share a backend object",
                ));
            }
        }
    }
    Ok(())
}
