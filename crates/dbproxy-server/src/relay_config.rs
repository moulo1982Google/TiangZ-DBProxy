//! Redis 路由配置与显式禁用的未来 MQ 声明。
//! Redis routing configuration with disabled declarations for future MQ backends.
use crate::config::ConfigError;
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    fmt,
};
use tiangz_dbproxy_storage::OutboxRoute;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OutboxRelaySection {
    #[serde(default = "default_timeout")]
    pub publish_timeout_ms: u64,
    #[serde(default)]
    pub default_publisher: Option<String>,
    #[serde(default)]
    pub publishers: Vec<PublisherSection>,
    #[serde(default)]
    pub sources: Vec<SourceSection>,
}
impl Default for OutboxRelaySection {
    fn default() -> Self {
        Self {
            publish_timeout_ms: default_timeout(),
            default_publisher: None,
            publishers: vec![],
            sources: vec![],
        }
    }
}
fn default_timeout() -> u64 {
    5_000
}
fn enabled_default() -> bool {
    true
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum PublisherBackend {
    RedisStream,
    Kafka,
    RabbitMq,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PublisherSection {
    pub id: String,
    pub backend: PublisherBackend,
    pub connection_env: String,
    #[serde(default = "enabled_default")]
    pub enabled: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceSection {
    #[serde(default = "enabled_default")]
    pub enabled: bool,
    pub producer: String,
    pub version: u32,
    #[serde(default)]
    pub publisher: Option<String>,
    pub destination: String,
}

#[derive(Clone)]
pub struct ResolvedOutboxRelay {
    pub publish_timeout_ms: u64,
    pub publishers: Vec<ResolvedPublisher>,
    pub routes: Vec<OutboxRoute>,
    pub disabled_routes: Vec<String>,
}
impl Default for ResolvedOutboxRelay {
    fn default() -> Self {
        Self {
            publish_timeout_ms: default_timeout(),
            publishers: vec![],
            routes: vec![],
            disabled_routes: vec![],
        }
    }
}
#[derive(Clone)]
pub struct ResolvedPublisher {
    pub id: String,
    pub url: String,
}
impl fmt::Debug for ResolvedOutboxRelay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedOutboxRelay")
            .field("publish_timeout_ms", &self.publish_timeout_ms)
            .field(
                "publisher_ids",
                &self.publishers.iter().map(|p| &p.id).collect::<Vec<_>>(),
            )
            .field("routes", &self.routes)
            .finish()
    }
}

impl OutboxRelaySection {
    /// 联网前验证引用与能力；禁用的未来 MQ 不读取密钥、更不启动驱动。
    /// Checks references before networking; disabled backends need no secrets or drivers.
    pub(crate) fn resolve(
        self,
        lease_ms: u64,
        environment: &impl Fn(&str) -> Option<String>,
    ) -> Result<ResolvedOutboxRelay, ConfigError> {
        if !(3_000..=60_000).contains(&self.publish_timeout_ms)
            || lease_ms < self.publish_timeout_ms + 1_000
        {
            return Err(ConfigError("outboxRelay.publishTimeoutMs must be 3000..60000 and leave at least 1000ms before lease expiry".into()));
        }
        if self.publishers.len() > 32 || self.sources.len() > 64 {
            return Err(ConfigError(
                "outboxRelay exceeds publisher/source limits (32/64)".into(),
            ));
        }
        let mut by_id = HashMap::new();
        for publisher in &self.publishers {
            identifier(&publisher.id)?;
            if publisher.id == "legacy" || by_id.insert(publisher.id.clone(), publisher).is_some() {
                return Err(ConfigError("duplicate or reserved publisher ID".into()));
            }
            super::config::validate_environment_name(&publisher.connection_env)?;
            if publisher.enabled && publisher.backend != PublisherBackend::RedisStream {
                return Err(ConfigError(
                    "Kafka/RabbitMQ publisher is not implemented; set enabled=false".into(),
                ));
            }
        }
        if let Some(id) = &self.default_publisher {
            require_active(&by_id, id)?;
        }
        let mut route_keys = HashSet::new();
        let mut routes = Vec::new();
        let mut disabled_routes = Vec::new();
        for source in &self.sources {
            identifier(&source.producer)?;
            if source.producer == "legacy"
                || source.version == 0
                || !route_keys.insert((&source.producer, source.version))
            {
                return Err(ConfigError(
                    "duplicate/reserved source or zero route version".into(),
                ));
            }
            let id = source
                .publisher
                .as_ref()
                .or(self.default_publisher.as_ref())
                .ok_or_else(|| {
                    ConfigError("source requires publisher or defaultPublisher".into())
                })?;
            if !by_id.contains_key(id) {
                return Err(ConfigError("source references a missing publisher".into()));
            }
            if source.enabled {
                require_active(&by_id, id)?;
            }
            if source.destination.trim().is_empty()
                || source.destination.len() > 256
                || source.destination.chars().any(char::is_control)
                || source.destination.starts_with("dbproxy:outbox:")
            {
                return Err(ConfigError(
                    "invalid destination or reserved legacy stream prefix".into(),
                ));
            }
            if !source.enabled {
                disabled_routes.push(format!(
                    "{}{}.{}",
                    tiangz_dbproxy_core::RELAY_TOPIC_PREFIX,
                    source.producer,
                    source.version
                ));
                continue;
            }
            routes.push(OutboxRoute {
                producer: source.producer.clone(),
                version: source.version,
                publisher: id.clone(),
                destination: source.destination.clone(),
            });
        }
        let mut publishers = Vec::new();
        for publisher in &self.publishers {
            if publisher.enabled {
                let url =
                    super::config::required_environment(environment, &publisher.connection_env)?;
                tiangz_dbproxy_storage::redis_endpoint_fingerprint(&url)
                    .map_err(|_| ConfigError("invalid Redis publisher connection URL".into()))?;
                publishers.push(ResolvedPublisher {
                    id: publisher.id.clone(),
                    url,
                });
            }
        }
        Ok(ResolvedOutboxRelay {
            publish_timeout_ms: self.publish_timeout_ms,
            publishers,
            routes,
            disabled_routes,
        })
    }
}

fn identifier(value: &str) -> Result<(), ConfigError> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
    {
        return Err(ConfigError(
            "publisher/producer identifiers must be 1..64 ASCII letters, digits, '_' or '-'".into(),
        ));
    }
    Ok(())
}
fn require_active(map: &HashMap<String, &PublisherSection>, id: &str) -> Result<(), ConfigError> {
    if !map.get(id).is_some_and(|p| p.enabled) {
        return Err(ConfigError(
            "source/default references a missing or disabled publisher".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    fn section() -> Value {
        json!({"defaultPublisher":"events", "publishers":[
        {"id":"events","backend":"redisStream","connectionEnv":"EVENT_REDIS"},
        {"id":"future","backend":"kafka","connectionEnv":"UNSET_KAFKA","enabled":false}],
        "sources":[{"producer":"game","version":1,"destination":"game.events"}]})
    }
    fn resolve(value: Value) -> Result<ResolvedOutboxRelay, ConfigError> {
        serde_json::from_value::<OutboxRelaySection>(value)
            .unwrap()
            .resolve(30_000, &|name| {
                assert_ne!(
                    name, "UNSET_KAFKA",
                    "disabled publishers must not resolve credentials"
                );
                Some("redis://user:secret@localhost:6379/1".into())
            })
    }
    #[test]
    fn executable_redis_and_disabled_future_declarations() {
        let config = resolve(section()).unwrap();
        assert_eq!(config.publishers.len(), 1);
        assert_eq!(config.routes[0].publisher, "events");
        assert!(!format!("{config:?}").contains("secret"));
        assert_eq!(config.routes[0].key(), "dbproxy.relay.v1.game.1");
        let mut value = section();
        value["sources"].as_array_mut().unwrap().push(json!({"producer":"future","version":1,"publisher":"future","destination":"future.events","enabled":false}));
        assert_eq!(resolve(value).unwrap().routes.len(), 1);
    }
    #[test]
    fn rejects_unsupported_missing_duplicate_or_unsafe_routes() {
        let mut value = section();
        value["publishers"][1]["enabled"] = json!(true);
        assert!(
            resolve(value)
                .unwrap_err()
                .to_string()
                .contains("not implemented")
        );
        for id in ["missing", "future"] {
            let mut value = section();
            value["defaultPublisher"] = json!(id);
            assert!(resolve(value).is_err());
        }
        let mut value = section();
        value["publishers"][1]["id"] = json!("events");
        assert!(resolve(value).is_err());
        let mut value = section();
        let source = value["sources"][0].clone();
        value["sources"].as_array_mut().unwrap().push(source);
        assert!(resolve(value).is_err());
        for destination in ["", "dbproxy:outbox:legacy", "bad\nlabel"] {
            let mut value = section();
            value["sources"][0]["destination"] = json!(destination);
            assert!(resolve(value).is_err());
        }
        let mut value = section();
        value["publishTimeoutMs"] = json!(30000);
        assert!(resolve(value).is_err());
    }
    #[test]
    fn rejects_unknown_fields_and_checks_repository_example_offline() {
        let mut value = section();
        value["sources"][0]["table"] = json!("game_outbox");
        assert!(serde_json::from_value::<OutboxRelaySection>(value).is_err());
        crate::config::check_config(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../configs/outbox-relay.example.json"
        ))
        .unwrap();
    }
}
