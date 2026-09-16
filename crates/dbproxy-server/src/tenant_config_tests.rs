use crate::config::{ResolvedDbProxyConfig, ResolvedStorage, load_config_with};
use crate::tenant_config::{check_deployment, validate_isolation};
use std::path::Path;
fn configs() -> Vec<ResolvedDbProxyConfig> {
    ["slg", "mmorpg"]
        .into_iter()
        .enumerate()
        .map(|(index, name)| {
            load_config_with(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join(format!("../../configs/tenants/{name}.example.json")),
                |key| {
                    if key.ends_with("TOKEN") {
                        Some(format!("test-only-{name}-token-123456"))
                    } else if key.ends_with("POSTGRES_URL") {
                        Some(format!("postgres://user:secret@localhost/{name}"))
                    } else if key.ends_with("REDIS_URL") {
                        Some(format!("redis://:secret@localhost/{}", index + 3))
                    } else {
                        None
                    }
                },
            )
            .unwrap()
        })
        .collect()
}
#[test]
fn shared_servers_with_separate_databases_are_valid() {
    validate_isolation(&configs()).unwrap();
}
#[test]
fn offline_example_checks_without_secrets_or_network() {
    check_deployment(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs/tenants.example.json"),
    )
    .unwrap();
}
#[test]
fn reject_same_pg_database_despite_different_users_hosts_or_url_spelling() {
    let mut configs = configs();
    if let ResolvedStorage::PostgresRedis { postgres_url, .. } = &mut configs[1].storage {
        *postgres_url = "postgres://other:other@127.0.0.1/slg".into();
    }
    assert!(validate_isolation(&configs).is_err());
}
#[test]
fn reject_cache_overlap_with_another_tenants_reliable_queue() {
    let mut configs = configs();
    if let ResolvedStorage::PostgresRedis {
        cache_redis_url, ..
    } = &mut configs[1].storage
    {
        *cache_redis_url = "redis://another-host/3".into();
    }
    assert!(validate_isolation(&configs).is_err());
}
#[test]
fn reject_external_publisher_overlap() {
    let mut configs = configs();
    configs[1]
        .outbox_relay
        .publishers
        .push(crate::relay_config::ResolvedPublisher {
            id: "external".into(),
            url: "redis://alias/3".into(),
        });
    assert!(validate_isolation(&configs).is_err());
}
#[test]
fn reject_credentials_monitoring_and_runtime_ambiguity() {
    for change in 0..3 {
        let mut configs = configs();
        match change {
            0 => configs[1].auth_token = configs[0].auth_token.clone(),
            1 => configs[1].observability_listen_addr = configs[0].observability_listen_addr,
            _ => configs[1].max_payload_bytes /= 2,
        }
        assert!(validate_isolation(&configs).is_err());
    }
}
#[test]
fn invalid_connection_strings_do_not_leak_credentials() {
    let mut configs = configs();
    if let ResolvedStorage::PostgresRedis { postgres_url, .. } = &mut configs[0].storage {
        *postgres_url = "supersecret invalid".into();
    }
    assert!(
        !validate_isolation(&configs)
            .unwrap_err()
            .to_string()
            .contains("supersecret")
    );
}

#[test]
fn invalid_token_lengths_are_rejected_before_backend_creation() {
    for length in [0, 15, tiangz_dbproxy_protocol::MAX_AUTH_TOKEN_BYTES + 1] {
        let mut configs = configs();
        configs[0].auth_token = "x".repeat(length);
        assert_eq!(
            validate_isolation(&configs).unwrap_err().to_string(),
            "invalid tenant token length"
        );
    }
}
