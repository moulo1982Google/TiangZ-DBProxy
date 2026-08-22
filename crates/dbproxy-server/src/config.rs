//! DBProxy 启动配置。普通参数来自严格 JSON，密钥只通过环境变量名间接引用。
//! DBProxy startup configuration. Regular settings use strict JSON while secrets are referenced by environment-variable name.

use std::{
    env,
    error::Error,
    fmt, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::Deserialize;
use tiangz_dbproxy_protocol::DEFAULT_MAX_FRAME_BYTES;

const DEFAULT_CONFIG_PATH: &str = "configs/local.json";

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DbProxyConfig {
    #[serde(rename = "$schema", default)]
    pub schema_document: Option<String>,
    pub config_version: u32,
    pub server: ServerSection,
    #[serde(default)]
    pub runtime: RuntimeSection,
    pub storage: StorageSection,
    #[serde(default)]
    pub backlog: BacklogSection,
    #[serde(default)]
    pub logging: LoggingSection,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimeSection {
    #[serde(default = "default_runtime_worker_threads")]
    pub worker_threads: usize,
}

impl Default for RuntimeSection {
    fn default() -> Self {
        Self {
            worker_threads: default_runtime_worker_threads(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServerSection {
    pub listen_addr: SocketAddr,
    pub auth_token_env: String,
    #[serde(default = "default_max_frame_bytes")]
    pub max_frame_bytes: usize,
    #[serde(default = "default_handshake_timeout_ms")]
    pub handshake_timeout_ms: u64,
    #[serde(default = "default_shutdown_grace_ms")]
    pub shutdown_grace_ms: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "backend", rename_all = "camelCase", deny_unknown_fields)]
pub enum StorageSection {
    PostgresRedis {
        #[serde(rename = "postgresUrlEnv")]
        postgres_url_env: String,
        #[serde(rename = "redisUrlEnv")]
        redis_url_env: String,
        #[serde(default = "default_storage_shards")]
        shards: usize,
    },
    Memory {
        #[serde(default = "default_storage_shards")]
        shards: usize,
    },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BacklogSection {
    #[serde(default = "default_backlog_workers")]
    pub workers: usize,
    #[serde(default = "default_backlog_lease_ms")]
    pub lease_ms: u64,
    #[serde(default = "default_backlog_idle_delay_ms")]
    pub idle_delay_ms: u64,
    #[serde(default = "default_backlog_failure_delay_ms")]
    pub failure_delay_ms: u64,
}

impl Default for BacklogSection {
    fn default() -> Self {
        Self {
            workers: default_backlog_workers(),
            lease_ms: default_backlog_lease_ms(),
            idle_delay_ms: default_backlog_idle_delay_ms(),
            failure_delay_ms: default_backlog_failure_delay_ms(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LoggingSection {
    #[serde(default = "default_log_filter_env")]
    pub filter_env: String,
    #[serde(default = "default_log_filter")]
    pub default_filter: String,
}

impl Default for LoggingSection {
    fn default() -> Self {
        Self {
            filter_env: default_log_filter_env(),
            default_filter: default_log_filter(),
        }
    }
}

/// 已解析密钥的运行时配置。Debug 实现刻意隐藏连接串和认证令牌。
/// Runtime settings with resolved secrets. Debug intentionally redacts credentials and tokens.
#[derive(Clone)]
pub struct ResolvedDbProxyConfig {
    pub source: PathBuf,
    pub listen_addr: SocketAddr,
    pub auth_token: String,
    pub max_frame_bytes: usize,
    pub handshake_timeout: Duration,
    pub shutdown_grace: Duration,
    pub runtime_worker_threads: usize,
    pub storage: ResolvedStorage,
    pub backlog_workers: usize,
    pub backlog_lease_ms: u64,
    pub backlog_idle_delay: Duration,
    pub backlog_failure_delay: Duration,
    pub log_filter: String,
}

#[derive(Clone)]
pub enum ResolvedStorage {
    PostgresRedis {
        postgres_url: String,
        redis_url: String,
        shards: usize,
    },
    Memory {
        shards: usize,
    },
}

impl ResolvedStorage {
    pub const fn shards(&self) -> usize {
        match self {
            Self::PostgresRedis { shards, .. } | Self::Memory { shards } => *shards,
        }
    }

    pub const fn name(&self) -> &'static str {
        match self {
            Self::PostgresRedis { .. } => "postgresRedis",
            Self::Memory { .. } => "memory",
        }
    }
}

impl fmt::Debug for ResolvedDbProxyConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedDbProxyConfig")
            .field("source", &self.source)
            .field("listen_addr", &self.listen_addr)
            .field("auth_token", &"[REDACTED]")
            .field("max_frame_bytes", &self.max_frame_bytes)
            .field("runtime_worker_threads", &self.runtime_worker_threads)
            .field("storage_backend", &self.storage.name())
            .field("storage_shards", &self.storage.shards())
            .field("backlog_workers", &self.backlog_workers)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct ConfigError(String);

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for ConfigError {}

/// 解析唯一支持的启动参数 `--config <path>`；多 Endpoint 属于客户端故障切换，不进入本配置。
/// Parse the sole startup option `--config <path>`; multi-endpoint failover belongs to clients, not this file.
pub fn config_path_from_args(
    args: impl IntoIterator<Item = String>,
) -> Result<PathBuf, ConfigError> {
    let mut values = args.into_iter();
    let _program = values.next();
    let Some(option) = values.next() else {
        return Ok(PathBuf::from(DEFAULT_CONFIG_PATH));
    };
    if option != "--config" {
        return Err(ConfigError(format!("unknown DBProxy argument: {option}")));
    }
    let path = values
        .next()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ConfigError("--config requires a path".to_string()))?;
    if let Some(extra) = values.next() {
        return Err(ConfigError(format!("unexpected DBProxy argument: {extra}")));
    }
    Ok(PathBuf::from(path))
}

/// 加载严格 JSON 并解析密钥引用。未知字段、空环境变量和危险的零值都会在联网前失败。
/// Load strict JSON and resolve secret references. Unknown fields, empty variables, and unsafe zeroes fail before networking.
pub fn load_config(path: impl AsRef<Path>) -> Result<ResolvedDbProxyConfig, ConfigError> {
    load_config_with(path, |name| env::var(name).ok())
}

fn load_config_with(
    path: impl AsRef<Path>,
    environment: impl Fn(&str) -> Option<String>,
) -> Result<ResolvedDbProxyConfig, ConfigError> {
    let path = path.as_ref();
    let content = fs::read_to_string(path)
        .map_err(|error| ConfigError(format!("failed to read {}: {error}", path.display())))?;
    let config: DbProxyConfig = serde_json::from_str(&content).map_err(|error| {
        ConfigError(format!(
            "invalid DBProxy config {}: {error}",
            path.display()
        ))
    })?;
    config.resolve(path, environment)
}

impl DbProxyConfig {
    fn resolve(
        self,
        source: &Path,
        environment: impl Fn(&str) -> Option<String>,
    ) -> Result<ResolvedDbProxyConfig, ConfigError> {
        if self.config_version != 1 {
            return Err(ConfigError(format!(
                "unsupported DBProxy configVersion: {}",
                self.config_version
            )));
        }
        require_positive("server.maxFrameBytes", self.server.max_frame_bytes)?;
        require_positive(
            "server.handshakeTimeoutMs",
            self.server.handshake_timeout_ms,
        )?;
        require_positive("server.shutdownGraceMs", self.server.shutdown_grace_ms)?;
        require_positive("runtime.workerThreads", self.runtime.worker_threads)?;
        require_positive("backlog.workers", self.backlog.workers)?;
        require_positive("backlog.leaseMs", self.backlog.lease_ms)?;
        require_positive("backlog.idleDelayMs", self.backlog.idle_delay_ms)?;
        require_positive("backlog.failureDelayMs", self.backlog.failure_delay_ms)?;

        let auth_token = required_environment(&environment, &self.server.auth_token_env)?;
        let storage = match self.storage {
            StorageSection::PostgresRedis {
                postgres_url_env,
                redis_url_env,
                shards,
            } => {
                require_positive("storage.shards", shards)?;
                ResolvedStorage::PostgresRedis {
                    postgres_url: required_environment(&environment, &postgres_url_env)?,
                    redis_url: required_environment(&environment, &redis_url_env)?,
                    shards,
                }
            }
            StorageSection::Memory { shards } => {
                require_positive("storage.shards", shards)?;
                ResolvedStorage::Memory { shards }
            }
        };
        validate_environment_name(&self.logging.filter_env)?;
        let log_filter = environment(&self.logging.filter_env)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(self.logging.default_filter);
        if log_filter.trim().is_empty() {
            return Err(ConfigError(
                "logging.defaultFilter cannot be empty".to_string(),
            ));
        }

        Ok(ResolvedDbProxyConfig {
            source: source.to_path_buf(),
            listen_addr: self.server.listen_addr,
            auth_token,
            max_frame_bytes: self.server.max_frame_bytes,
            handshake_timeout: Duration::from_millis(self.server.handshake_timeout_ms),
            shutdown_grace: Duration::from_millis(self.server.shutdown_grace_ms),
            runtime_worker_threads: self.runtime.worker_threads,
            storage,
            backlog_workers: self.backlog.workers,
            backlog_lease_ms: self.backlog.lease_ms,
            backlog_idle_delay: Duration::from_millis(self.backlog.idle_delay_ms),
            backlog_failure_delay: Duration::from_millis(self.backlog.failure_delay_ms),
            log_filter,
        })
    }
}

fn required_environment(
    environment: &impl Fn(&str) -> Option<String>,
    name: &str,
) -> Result<String, ConfigError> {
    validate_environment_name(name)?;
    environment(name)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ConfigError(format!(
                "required environment variable {name} is missing or empty"
            ))
        })
}

fn validate_environment_name(name: &str) -> Result<(), ConfigError> {
    let mut characters = name.chars();
    let valid_first = characters
        .next()
        .is_some_and(|value| value == '_' || value.is_ascii_alphabetic());
    if !valid_first || !characters.all(|value| value == '_' || value.is_ascii_alphanumeric()) {
        return Err(ConfigError(format!(
            "invalid environment variable name: {name}"
        )));
    }
    Ok(())
}

fn require_positive<T>(name: &str, value: T) -> Result<(), ConfigError>
where
    T: PartialEq + From<u8>,
{
    if value == T::from(0) {
        Err(ConfigError(format!("{name} must be greater than zero")))
    } else {
        Ok(())
    }
}

const fn default_max_frame_bytes() -> usize {
    DEFAULT_MAX_FRAME_BYTES
}
const fn default_handshake_timeout_ms() -> u64 {
    5_000
}
const fn default_shutdown_grace_ms() -> u64 {
    5_000
}
const fn default_storage_shards() -> usize {
    4
}
fn default_runtime_worker_threads() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
}
const fn default_backlog_workers() -> usize {
    1
}
const fn default_backlog_lease_ms() -> u64 {
    30_000
}
const fn default_backlog_idle_delay_ms() -> u64 {
    20
}
const fn default_backlog_failure_delay_ms() -> u64 {
    1_000
}
fn default_log_filter_env() -> String {
    "RUST_LOG".to_string()
}
fn default_log_filter() -> String {
    "info".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::HashMap,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn write_config(content: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = env::temp_dir().join(format!("tiangz-dbproxy-config-{suffix}.json"));
        fs::write(&path, content).unwrap();
        path
    }

    #[test]
    fn loads_strict_config_and_resolves_secret_references() {
        let path = write_config(
            r#"{
          "configVersion": 1,
          "server": { "listenAddr": "127.0.0.1:7800", "authTokenEnv": "AUTH" },
          "runtime": { "workerThreads": 4 },
          "storage": { "backend": "postgresRedis", "postgresUrlEnv": "PG", "redisUrlEnv": "REDIS" }
        }"#,
        );
        let values = HashMap::from([
            ("AUTH", "0123456789abcdef"),
            ("PG", "postgres://secret"),
            ("REDIS", "redis://secret"),
        ]);
        let config =
            load_config_with(&path, |name| values.get(name).map(ToString::to_string)).unwrap();
        fs::remove_file(path).unwrap();
        assert_eq!(config.storage.shards(), 4);
        assert_eq!(config.runtime_worker_threads, 4);
        assert_eq!(config.backlog_workers, 1);
        assert_eq!(config.log_filter, "info");
        let debug = format!("{config:?}");
        assert!(!debug.contains("postgres://secret"));
        assert!(!debug.contains("0123456789abcdef"));
    }

    #[test]
    fn rejects_unknown_fields_and_missing_secrets() {
        let path = write_config(
            r#"{
          "configVersion": 1,
          "server": { "listenAddr": "127.0.0.1:7800", "authTokenEnv": "AUTH", "typo": 1 },
          "storage": { "backend": "postgresRedis", "postgresUrlEnv": "PG", "redisUrlEnv": "REDIS" }
        }"#,
        );
        let error = load_config_with(&path, |_| None).unwrap_err();
        fs::remove_file(path).unwrap();
        assert!(error.to_string().contains("unknown field"));

        let path = write_config(
            r#"{
          "configVersion": 1,
          "server": { "listenAddr": "127.0.0.1:7800", "authTokenEnv": "AUTH" },
          "storage": { "backend": "postgresRedis", "postgresUrlEnv": "PG", "redisUrlEnv": "REDIS" }
        }"#,
        );
        let error = load_config_with(&path, |_| None).unwrap_err();
        fs::remove_file(path).unwrap();
        assert!(error.to_string().contains("AUTH"));
    }

    #[test]
    fn rejects_an_unknown_config_version_before_resolving_secrets() {
        let path = write_config(
            r#"{
          "configVersion": 2,
          "server": { "listenAddr": "127.0.0.1:7800", "authTokenEnv": "AUTH" },
          "storage": { "backend": "postgresRedis", "postgresUrlEnv": "PG", "redisUrlEnv": "REDIS" }
        }"#,
        );
        let error = load_config_with(&path, |_| None).unwrap_err();
        fs::remove_file(path).unwrap();
        assert!(error.to_string().contains("configVersion: 2"));
    }

    #[test]
    fn memory_backend_does_not_require_database_secrets() {
        let path = write_config(
            r#"{
          "configVersion": 1,
          "server": { "listenAddr": "127.0.0.1:7800", "authTokenEnv": "AUTH" },
          "runtime": { "workerThreads": 4 },
          "storage": { "backend": "memory", "shards": 8 }
        }"#,
        );
        let config = load_config_with(&path, |name| {
            (name == "AUTH").then(|| "memory-test-token".to_string())
        })
        .unwrap();
        fs::remove_file(path).unwrap();
        assert_eq!(config.runtime_worker_threads, 4);
        assert_eq!(config.storage.name(), "memory");
        assert_eq!(config.storage.shards(), 8);
    }
}
