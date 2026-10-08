//! 可靠 Redis 的确认与 I/O 预算；不修改 Redis 配置或降低确认等级。
//! Reliable Redis acknowledgement and I/O budgets; never changes persistence or ACK strength.
use crate::{StorageError, StorageMetrics, latency::Stage};
use redis::aio::MultiplexedConnection;
use std::time::Duration;
use tokio::time::Instant;

pub const DEFAULT_REDIS_AOF_ACK_TIMEOUT_MS: u64 = 2_000;
pub const DEFAULT_REDIS_RESPONSE_TIMEOUT_MS: u64 = 3_000;

pub(crate) fn redis_result<T>(
    result: Result<T, redis::RedisError>,
    metrics: &StorageMetrics,
    stage: Stage,
) -> Result<T, StorageError> {
    result.map_err(|error| {
        if error.is_timeout() {
            metrics.latency.timed_out(stage);
        }
        error.into()
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedisDurabilityConfig {
    pub aof_ack_timeout: Duration,
    pub response_timeout: Duration,
}

impl Default for RedisDurabilityConfig {
    fn default() -> Self {
        Self {
            aof_ack_timeout: Duration::from_millis(DEFAULT_REDIS_AOF_ACK_TIMEOUT_MS),
            response_timeout: Duration::from_millis(DEFAULT_REDIS_RESPONSE_TIMEOUT_MS),
        }
    }
}

impl RedisDurabilityConfig {
    /// 联网前拒绝无界等待与倒置的预算；WAITAOF 的零值意味着无限等待。
    /// Reject unbounded or inverted budgets before networking; WAITAOF zero means forever.
    pub fn validate(self) -> Result<(), StorageError> {
        if self.aof_ack_timeout < Duration::from_millis(1)
            || self.response_timeout > Duration::from_secs(60)
            || self.response_timeout <= self.aof_ack_timeout
        {
            return Err(StorageError::InvalidRedisDurabilityBudget(
                "require 1ms <= AOF timeout < Redis response timeout <= 60000ms",
            ));
        }
        Ok(())
    }

    pub(crate) fn connection_config(self) -> redis::AsyncConnectionConfig {
        redis::AsyncConnectionConfig::new()
            .set_connection_timeout(Some(self.response_timeout))
            .set_response_timeout(Some(self.response_timeout))
    }

    /// 只确认当前连接写入，并消耗原期限的剩余时间；超时不表示写入被撤销。
    /// Confirm this connection's writes within the original deadline; timeout is not rollback.
    pub(crate) async fn wait_for_local_aof(
        self,
        connection: &mut MultiplexedConnection,
        deadline: Instant,
        metrics: &StorageMetrics,
        stage: Stage,
    ) -> Result<(), StorageError> {
        let _timer = metrics.latency.start(stage);
        let remaining = deadline.saturating_duration_since(Instant::now());
        let timeout_ms = self.aof_ack_timeout.min(remaining).as_millis() as u64;
        if timeout_ms == 0 {
            metrics.latency.timed_out(stage);
            return Err(StorageError::RedisDurabilityDeadlineExceeded);
        }
        let reply: Result<(i64, i64), redis::RedisError> = redis::cmd("WAITAOF")
            .arg(1)
            .arg(0)
            .arg(timeout_ms)
            .query_async(connection)
            .await;
        match reply {
            Ok((local, _)) if local >= 1 => Ok(()),
            Ok(_) => {
                metrics.latency.timed_out(stage);
                Err(StorageError::RedisAofNotDurable { timeout_ms })
            }
            Err(error) => {
                if error.is_timeout() {
                    metrics.latency.timed_out(stage);
                }
                Err(error.into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_budgets_cannot_become_infinite_or_expire_before_aof() {
        for (aof, io) in [
            (0, 3000),
            (2000, 2000),
            (3000, 2000),
            (2000, 60001),
            (u64::MAX, u64::MAX),
        ] {
            assert!(
                RedisDurabilityConfig {
                    aof_ack_timeout: Duration::from_millis(aof),
                    response_timeout: Duration::from_millis(io),
                }
                .validate()
                .is_err()
            );
        }
        for aof in [2000, 3000, 5000] {
            RedisDurabilityConfig {
                aof_ack_timeout: Duration::from_millis(aof),
                response_timeout: Duration::from_millis(aof + 1000),
            }
            .validate()
            .unwrap();
        }
    }
}
