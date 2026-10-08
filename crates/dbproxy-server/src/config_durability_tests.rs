use super::*;
use serde_json::json;

fn resolve(
    backlog: serde_json::Value,
    outbox: serde_json::Value,
) -> Result<ResolvedDbProxyConfig, ConfigError> {
    let config: DbProxyConfig = serde_json::from_value(json!({
        "configVersion":1,
        "server":{"listenAddr":"127.0.0.1:7800","authTokenEnv":"AUTH"},
        "storage":{"backend":"memory"},
        "backlog":backlog,
        "outboxRelay":outbox
    }))
    .unwrap();
    config.resolve(Path::new("fixture.json"), |name| {
        (name == "AUTH").then(|| "fixture-token".to_string())
    })
}

#[test]
fn old_configuration_keeps_two_second_aof_and_separate_local_deadlines() {
    let config = resolve(json!({}), json!({})).unwrap();
    assert_eq!(
        config.backlog_enqueue_config,
        tiangz_dbproxy_storage::EnqueueBatchConfig::default()
    );
    assert_eq!(
        config.outbox_relay.durability,
        tiangz_dbproxy_storage::RedisDurabilityConfig::default()
    );
    assert_eq!(config.outbox_relay.publish_timeout_ms, 5000);
}

#[test]
fn enqueue_and_background_publishing_have_independent_profiles() {
    let config = resolve(
        json!({"aofAckTimeoutMs":3000,"redisResponseTimeoutMs":4000,"enqueueTimeoutMs":6000}),
        json!({"aofAckTimeoutMs":5000,"redisResponseTimeoutMs":6000,"publishTimeoutMs":8500}),
    )
    .unwrap();
    assert_eq!(
        config.backlog_enqueue_config.durability.aof_ack_timeout,
        Duration::from_secs(3)
    );
    assert_eq!(
        config.backlog_enqueue_config.total_timeout,
        Duration::from_secs(6)
    );
    assert_eq!(
        config.outbox_relay.durability.aof_ack_timeout,
        Duration::from_secs(5)
    );
    assert_eq!(config.outbox_relay.publish_timeout_ms, 8500);
}

#[test]
fn invalid_or_incomplete_profiles_fail_before_reading_secrets() {
    for backlog in [
        json!({"aofAckTimeoutMs":0}),
        json!({"aofAckTimeoutMs":5000}),
        json!({"enqueueQueueWaitTimeoutMs":0}),
        json!({"enqueueTimeoutMs":4000}),
        json!({"redisResponseTimeoutMs":2000}),
        json!({"enqueueTimeoutMs":u64::MAX}),
    ] {
        assert!(resolve(backlog, json!({})).is_err());
    }
    for outbox in [
        json!({"aofAckTimeoutMs":0}),
        json!({"aofAckTimeoutMs":5000}),
        json!({"redisResponseTimeoutMs":5000}),
        json!({"publishTimeoutMs":30000}),
        json!({"redisResponseTimeoutMs":u64::MAX}),
    ] {
        assert!(resolve(json!({}), outbox).is_err());
    }
    let config: DbProxyConfig = serde_json::from_value(json!({"configVersion":1,
        "server":{"listenAddr":"127.0.0.1:7800","authTokenEnv":"AUTH"},
        "storage":{"backend":"memory"},"backlog":{"aofAckTimeoutMs":0}}))
    .unwrap();
    assert!(
        config
            .resolve(Path::new("fixture.json"), |_| panic!(
                "invalid budget must fail before environment access"
            ))
            .is_err()
    );
}
