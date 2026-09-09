//! 路由注册与发布契约；租约、持久重试和死信不属于 MQ 驱动。
//! Route registration and publication contract, independent of durable retry policy.
use crate::{PostgresOutboxQueue, StorageError};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tiangz_dbproxy_core::OutboxEvent;

/// 仅固定地址、协议和数据库号，不持久化密码或因凭据轮换误报路由变化。
/// Pins address, protocol and database without persisting credentials.
pub fn redis_endpoint_fingerprint(url: &str) -> Result<String, StorageError> {
    use redis::IntoConnectionInfo;
    use sha2::{Digest, Sha256};
    let info = url
        .into_connection_info()
        .map_err(|_| StorageError::QueueProtocol("invalid Redis URL".into()))?;
    Ok(format!(
        "{:x}",
        Sha256::digest(format!("{:?}/{}", info.addr(), info.redis_settings().db()).as_bytes())
    ))
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OutboxRoute {
    pub producer: String,
    pub version: u32,
    pub publisher: String,
    pub destination: String,
}

impl OutboxRoute {
    pub fn key(&self) -> String {
        format!(
            "{}{}.{}",
            tiangz_dbproxy_core::RELAY_TOPIC_PREFIX,
            self.producer,
            self.version
        )
    }
}

pub struct PublishMessage<'a> {
    pub event: &'a OutboxEvent,
    pub destination: &'a str,
    pub operation_id: &'a str,
    pub trade_id: &'a str,
}

#[derive(Clone, Debug)]
pub struct PublishReceipt {
    pub message_id: String,
}

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("transient publisher failure: {0}")]
    Transient(&'static str),
    #[error("invalid publication: {0}")]
    Permanent(&'static str),
}

#[async_trait]
pub trait Publisher: Send + Sync {
    /// 仅在约定的 MQ 持久确认后成功；不能代表业务消费者已处理。
    /// Success means broker durability acknowledgement, not consumer completion.
    async fn publish(&self, message: PublishMessage<'_>) -> Result<PublishReceipt, PublishError>;
}

impl PostgresOutboxQueue {
    /// 禁用声明只用于未来路由，不把已有持久路由的停用伪装成配置开关。
    /// Disabled declarations cannot silently disable an already registered route.
    pub async fn ensure_unregistered_routes(&self, keys: &[String]) -> Result<(), StorageError> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut client = self.client.lock().await;
        client.ensure_connected().await?;
        if client
            .query_opt(
                "SELECT route_key FROM dbproxy_outbox_routes WHERE route_key=ANY($1) LIMIT 1",
                &[&keys],
            )
            .await?
            .is_some()
        {
            return Err(StorageError::QueueProtocol("cannot disable an existing route through a future declaration; route retirement requires an explicit migration".into()));
        }
        Ok(())
    }
    /// 固定 Publisher 身份；凭据可轮换，但相同 ID 不能偷偷改到另一目标。
    /// Pins endpoint identity while allowing credential rotation.
    pub async fn register_publisher(
        &self,
        id: &str,
        fingerprint: &str,
    ) -> Result<(), StorageError> {
        let mut client = self.client.lock().await;
        client.ensure_connected().await?;
        let tx = client.transaction().await?;
        tx.execute("INSERT INTO dbproxy_outbox_publishers VALUES($1,'redisStream',$2) ON CONFLICT DO NOTHING", &[&id,&fingerprint]).await?;
        let row = tx.query_one("SELECT backend,endpoint_fingerprint FROM dbproxy_outbox_publishers WHERE publisher_id=$1", &[&id]).await?;
        if row.get::<_, String>(0) != "redisStream" || row.get::<_, String>(1) != fingerprint {
            return Err(StorageError::QueueProtocol(
                "publisher endpoint changed; use a new publisher ID".into(),
            ));
        }
        tx.commit().await?;
        Ok(())
    }

    /// 路由版本不可原地修改；旧积压继续使用已保存的目标。
    /// Route versions are immutable; pending events retain their original destination.
    pub async fn register_route(&self, route: &OutboxRoute) -> Result<(), StorageError> {
        let valid_id = |id: &str| {
            !id.is_empty()
                && id.len() <= 64
                && id != "legacy"
                && id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
        };
        if !valid_id(&route.producer)
            || !valid_id(&route.publisher)
            || route.version == 0
            || route.destination.trim().is_empty()
            || route.destination.len() > 256
            || route.destination.chars().any(char::is_control)
            || route.destination.starts_with("dbproxy:outbox:")
        {
            return Err(StorageError::QueueProtocol(
                "invalid or reserved outbox route".into(),
            ));
        }
        let mut client = self.client.lock().await;
        client.ensure_connected().await?;
        let tx = client.transaction().await?;
        let key = route.key();
        tx.execute("INSERT INTO dbproxy_outbox_routes VALUES($1,$2,$3,$4,'redisStream',$5) ON CONFLICT DO NOTHING",
            &[&key,&route.producer,&i64::from(route.version),&route.publisher,&route.destination]).await?;
        let row = tx
            .query_one(
                "SELECT publisher_id,destination FROM dbproxy_outbox_routes WHERE route_key=$1",
                &[&key],
            )
            .await?;
        if row.get::<_, String>(0) != route.publisher
            || row.get::<_, String>(1) != route.destination
        {
            return Err(StorageError::QueueProtocol(
                "outbox route changed; increment its version".into(),
            ));
        }
        tx.commit().await?;
        Ok(())
    }

    /// 启动前检查所有未发布事件的 Publisher，不能丢弃缺失配置的旧积压。
    /// Refuses startup when pending events require an unavailable publisher.
    pub async fn required_publishers(&self) -> Result<Vec<String>, StorageError> {
        let mut client = self.client.lock().await;
        client.ensure_connected().await?;
        Ok(client.query("SELECT publisher_id FROM dbproxy_outbox_routes UNION SELECT publisher_id FROM dbproxy_outbox WHERE published_at IS NULL", &[]).await?
            .into_iter().map(|r|r.get(0)).collect())
    }
}
