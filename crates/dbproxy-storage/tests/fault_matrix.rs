use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tiangz_dbproxy_core::{
    AsyncSnapshotStore, AsyncTransactionalStore, RecordKey, Revision, TransactionalWrite,
    TransactionalWriteOutcome,
};
use tiangz_dbproxy_storage::{StorageError, TieredSnapshotStore};

const POSTGRES_CONTAINER: &str = "tiangz-dbproxy-postgres";
const REDIS_CONTAINER: &str = "tiangz-dbproxy-redis";

fn test_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after Unix epoch")
        .as_nanos();
    format!("{}-{}", std::process::id(), nanos)
}

fn env_urls() -> (String, String) {
    (
        std::env::var("DBPROXY_POSTGRES_URL")
            .expect("DBPROXY_POSTGRES_URL must be set for the fault matrix"),
        std::env::var("DBPROXY_REDIS_URL")
            .expect("DBPROXY_REDIS_URL must be set for the fault matrix"),
    )
}

fn docker(args: &[&str]) {
    let status = Command::new("docker")
        .args(args)
        .status()
        .expect("docker must be available for the fault matrix");
    assert!(status.success(), "docker {:?} failed with {status}", args);
}

fn wait_healthy(container: &str) {
    for _ in 0..40 {
        let output = Command::new("docker")
            .args(["inspect", "--format", "{{.State.Health.Status}}", container])
            .output()
            .expect("docker inspect must be available");
        let status = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if status == "healthy" {
            return;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    panic!("container {container} did not become healthy");
}

struct RestartGuard {
    container: &'static str,
    active: bool,
}

impl RestartGuard {
    fn stop(container: &'static str) -> Self {
        docker(&["stop", container]);
        Self {
            container,
            active: true,
        }
    }

    fn restart(&mut self) {
        if self.active {
            docker(&["start", self.container]);
            wait_healthy(self.container);
            self.active = false;
        }
    }
}

impl Drop for RestartGuard {
    fn drop(&mut self) {
        if self.active {
            let _ = Command::new("docker")
                .args(["start", self.container])
                .status();
            wait_healthy(self.container);
            self.active = false;
        }
    }
}

fn require_opt_in() -> bool {
    if std::env::var("DBPROXY_RUN_DOCKER_FAULTS").as_deref() != Ok("1") {
        eprintln!("fault matrix skipped: set DBPROXY_RUN_DOCKER_FAULTS=1 explicitly");
        return false;
    }
    true
}

fn transaction(
    operation_id: &str,
    record: RecordKey,
    expected_revision: Revision,
    payload: &[u8],
    result: &[u8],
) -> TransactionalWrite {
    TransactionalWrite {
        operation_id: operation_id.to_string(),
        record,
        schema: "fault-matrix.player".to_string(),
        schema_version: 1,
        expected_revision,
        payload: payload.to_vec(),
        result: result.to_vec(),
        updated_at_unix_ms: 1,
    }
}

#[tokio::test]
#[ignore = "会停止并恢复本机 Redis；设置 DBPROXY_RUN_DOCKER_FAULTS=1 后显式运行"]
async fn redis_outage_falls_back_and_retry_repairs_cache() {
    if !require_opt_in() {
        return;
    }
    let (postgres_url, redis_url) = env_urls();
    let mut store = TieredSnapshotStore::connect(&postgres_url, &redis_url)
        .await
        .expect("PostgreSQL and Redis must be available");
    let key = RecordKey::new("fault-matrix", test_suffix()).unwrap();
    let first = transaction(
        &format!("first-{}", test_suffix()),
        key.clone(),
        Revision::ZERO,
        b"v1",
        b"committed-v1",
    );
    assert!(matches!(
        store.apply(first).await.unwrap(),
        TransactionalWriteOutcome::Applied {
            new_revision: Revision(1),
            ..
        }
    ));

    let mut redis = RestartGuard::stop(REDIS_CONTAINER);
    let fallback = tokio::time::timeout(Duration::from_secs(5), store.load(&key))
        .await
        .expect("PostgreSQL fallback must not hang when Redis is down")
        .unwrap()
        .expect("durable snapshot must remain readable");
    assert_eq!(fallback.revision, Revision(1));

    let second = transaction(
        &format!("second-{}", test_suffix()),
        key.clone(),
        Revision(1),
        b"v2",
        b"committed-v2",
    );
    let error = tokio::time::timeout(Duration::from_secs(5), store.apply(second.clone()))
        .await
        .expect("PostgreSQL commit plus Redis failure must not hang")
        .unwrap_err();
    assert!(matches!(error, StorageError::CacheSync(_)));

    let durable = store.load(&key).await.unwrap().unwrap();
    assert_eq!(durable.revision, Revision(2));
    redis.restart();

    let mut recovered = TieredSnapshotStore::connect(&postgres_url, &redis_url)
        .await
        .unwrap();
    assert_eq!(
        recovered.apply(second).await.unwrap(),
        TransactionalWriteOutcome::Duplicate {
            new_revision: Revision(2),
            result: b"committed-v2".to_vec(),
        }
    );
    assert_eq!(
        recovered.load(&key).await.unwrap().unwrap().revision,
        Revision(2)
    );
}

#[tokio::test]
#[ignore = "会停止并恢复本机 PostgreSQL；设置 DBPROXY_RUN_DOCKER_FAULTS=1 后显式运行"]
async fn postgres_outage_never_reports_a_successful_write() {
    if !require_opt_in() {
        return;
    }
    let (postgres_url, redis_url) = env_urls();
    let mut store = TieredSnapshotStore::connect(&postgres_url, &redis_url)
        .await
        .expect("PostgreSQL and Redis must be available");
    let key = RecordKey::new("fault-matrix", test_suffix()).unwrap();
    let first = transaction(
        &format!("first-{}", test_suffix()),
        key.clone(),
        Revision::ZERO,
        b"v1",
        b"committed-v1",
    );
    store.apply(first).await.unwrap();

    let mut postgres = RestartGuard::stop(POSTGRES_CONTAINER);
    let cached = tokio::time::timeout(Duration::from_secs(5), store.load(&key))
        .await
        .expect("Redis cache read must not hang when PostgreSQL is down")
        .unwrap()
        .expect("committed snapshot should remain in Redis");
    assert_eq!(cached.revision, Revision(1));

    let write = transaction(
        &format!("must-fail-{}", test_suffix()),
        key.clone(),
        Revision(1),
        b"must-not-commit",
        b"must-not-return-success",
    );
    let error = tokio::time::timeout(Duration::from_secs(5), store.apply(write))
        .await
        .expect("database outage must not hang the caller")
        .unwrap_err();
    assert!(matches!(error, StorageError::Postgres(_)));

    postgres.restart();
    let recovered = TieredSnapshotStore::connect(&postgres_url, &redis_url)
        .await
        .unwrap();
    assert_eq!(
        recovered.load(&key).await.unwrap().unwrap().revision,
        Revision(1)
    );
}
