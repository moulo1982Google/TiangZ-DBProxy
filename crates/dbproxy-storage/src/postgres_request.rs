//! 请求连接排队与重连失败冷却；不对已发送的 SQL 设置执行期限。
//! Request queue deadlines and reconnect failure cooldown, never SQL execution deadlines.

use std::time::{Duration, Instant};
use tokio::{sync::MutexGuard, time::timeout};

use crate::{ReconnectingPostgresClient, SharedPostgresClient, StorageError, duration_millis};

pub const DEFAULT_POSTGRES_CONNECTION_WAIT_TIMEOUT_MS: u64 = 2_000;
pub const DEFAULT_POSTGRES_RECONNECT_COOLDOWN_MS: u64 = 500;

/// 仅供请求分片使用；独立维护连接沿用原有等待与重连策略。
/// Request-shard policy; dedicated maintenance connections retain their existing behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PostgresRequestConfig {
    /// 等待连接锁的单次预算，不含重连或 SQL。
    /// Per-acquisition mutex budget, excluding reconnect and SQL.
    pub connection_wait_timeout: Duration,
    /// 同一连接的重连失败或取消后的冷却时间。
    /// Shared cooldown after reconnect failure or cancellation.
    pub reconnect_cooldown: Duration,
}

impl Default for PostgresRequestConfig {
    fn default() -> Self {
        Self {
            connection_wait_timeout: Duration::from_millis(
                DEFAULT_POSTGRES_CONNECTION_WAIT_TIMEOUT_MS,
            ),
            reconnect_cooldown: Duration::from_millis(DEFAULT_POSTGRES_RECONNECT_COOLDOWN_MS),
        }
    }
}

impl PostgresRequestConfig {
    pub(crate) fn validate(self) -> Result<Self, StorageError> {
        if self.connection_wait_timeout < Duration::from_millis(1)
            || Instant::now()
                .checked_add(self.connection_wait_timeout)
                .is_none()
        {
            return Err(StorageError::InvalidPostgresConnectionWaitTimeout);
        }
        if self.reconnect_cooldown < Duration::from_millis(1)
            || Instant::now()
                .checked_add(self.reconnect_cooldown)
                .is_none()
        {
            return Err(StorageError::InvalidPostgresReconnectCooldown);
        }
        Ok(self)
    }
}

/// 超时仅丢弃锁等待者；拿到锁后由调用者执行重连与 SQL。
/// Expiry drops only the mutex waiter; reconnect and SQL run after acquisition.
pub(crate) async fn lock_client(
    client: &SharedPostgresClient,
    wait: Option<Duration>,
) -> Result<MutexGuard<'_, ReconnectingPostgresClient>, StorageError> {
    match wait {
        Some(wait) => timeout(wait, client.lock()).await.map_err(|_| {
            StorageError::PostgresConnectionWaitTimeout {
                timeout_ms: duration_millis(wait),
            }
        }),
        None => Ok(client.lock().await),
    }
}

/// 重连失败或被取消时启动冷却；成功才清除冷却，避免取消造成串行重连风暴。
/// Failure or cancellation starts cooldown; only success clears it.
pub(crate) struct ReconnectAttempt<'a> {
    pub retry_at: &'a mut Option<Instant>,
    pub cooldown: Duration,
    pub succeeded: bool,
}

impl Drop for ReconnectAttempt<'_> {
    fn drop(&mut self) {
        *self.retry_at = if self.succeeded || self.cooldown.is_zero() {
            None
        } else {
            Instant::now().checked_add(self.cooldown)
        };
    }
}
