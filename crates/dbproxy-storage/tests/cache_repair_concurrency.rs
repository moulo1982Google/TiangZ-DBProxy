//! Real PostgreSQL lease/merge regressions. Use a disposable database with no business workers,
//! DBPROXY_TEST_POSTGRES_URL and DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION=1, --ignored --test-threads=1.
use std::{future::Future, time::Duration};
use tiangz_dbproxy_core::{RecordKey, Revision};
use tiangz_dbproxy_storage::{CacheRepairLease, PostgresCacheRepairQueue, PostgresSnapshotStore};
use tokio::{task::JoinHandle, time::timeout};

struct Fixture {
    queue: PostgresCacheRepairQueue,
    writer: PostgresCacheRepairQueue,
    sql: tokio_postgres::Client,
    connection: JoinHandle<()>,
    record: RecordKey,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.connection.abort();
    }
}

impl Fixture {
    async fn new() -> Self {
        assert_eq!(
            std::env::var("DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION").as_deref(),
            Ok("1")
        );
        let url =
            std::env::var("DBPROXY_TEST_POSTGRES_URL").expect("dedicated test database required");
        let store = PostgresSnapshotStore::connect(&url).await.unwrap();
        let writer = PostgresSnapshotStore::connect(&url).await.unwrap();
        let (sql, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        let connection = tokio::spawn(async move { connection.await.unwrap() });
        // Only our fixture's rows; serial execution is required because claim is queue-wide.
        sql.execute(
            "DELETE FROM dbproxy_cache_repairs WHERE namespace='cache-repair-matrix'",
            &[],
        )
        .await
        .unwrap();
        Self {
            queue: store.cache_repair_queue(),
            writer: writer.cache_repair_queue(),
            sql,
            connection,
            record: RecordKey::new("cache-repair-matrix", "hot").unwrap(),
        }
    }

    async fn enqueue(&self, revision: u64) {
        self.writer
            .enqueue(&self.record, Revision(revision))
            .await
            .unwrap();
    }

    async fn claim(&self) -> CacheRepairLease {
        let lease = self
            .queue
            .claim("same-worker", 30_000)
            .await
            .unwrap()
            .expect("fixture must be ready");
        assert_eq!(lease.record, self.record);
        lease
    }

    async fn expire(&self) {
        self.sql.execute("UPDATE dbproxy_cache_repairs SET lease_until=clock_timestamp()-interval '1 second' WHERE namespace=$1 AND record_key=$2", &[&self.record.namespace, &self.record.key]).await.unwrap();
    }

    async fn state(&self) -> String {
        self.sql.query_one("SELECT row_to_json(r)::TEXT FROM dbproxy_cache_repairs r WHERE namespace=$1 AND record_key=$2", &[&self.record.namespace, &self.record.key]).await.unwrap().get(0)
    }

    async fn count(&self) -> i64 {
        self.sql
            .query_one(
                "SELECT COUNT(*) FROM dbproxy_cache_repairs WHERE namespace=$1 AND record_key=$2",
                &[&self.record.namespace, &self.record.key],
            )
            .await
            .unwrap()
            .get(0)
    }
}

async fn bounded(future: impl Future<Output = ()>) {
    timeout(Duration::from_secs(30), future)
        .await
        .expect("repair regression must not hang");
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL; run serially"]
async fn newer_targets_keep_live_leases_and_ack_uses_actual_repaired_revision() {
    bounded(async {
        let f = Fixture::new().await;
        f.enqueue(1).await;
        let lease = f.claim().await;
        f.enqueue(2).await;
        assert!(
            f.queue
                .claim("other-worker", 30_000)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            f.queue
                .acknowledge(&lease, Some(Revision(1)))
                .await
                .unwrap()
        );
        assert_eq!(f.count().await, 1);
        let next = f.claim().await; // No wait for lease expiry after partial coverage.
        assert_eq!(next.target_revision, Revision(2));
        assert!(next.lease_token > lease.lease_token);
        f.enqueue(3).await;
        // Repair loaded a revision newer than the target it originally claimed.
        assert!(f.queue.acknowledge(&next, Some(Revision(3))).await.unwrap());
        assert_eq!(f.count().await, 0);
    })
    .await;
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL; run serially"]
async fn missing_snapshot_only_settles_the_exact_claimed_target() {
    bounded(async {
        let f = Fixture::new().await;
        f.enqueue(1).await;
        let first = f.claim().await;
        assert!(f.queue.acknowledge(&first, None).await.unwrap());
        assert_eq!(f.count().await, 0);
        f.enqueue(2).await;
        let second = f.claim().await;
        f.enqueue(3).await;
        assert!(f.queue.acknowledge(&second, None).await.unwrap());
        let next = f.claim().await;
        assert_eq!(next.target_revision, Revision(3));
        assert!(f.queue.acknowledge(&next, None).await.unwrap());
        assert_eq!(f.count().await, 0);
    })
    .await;
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL; run serially"]
async fn hot_enqueues_preserve_backoff_age_dead_letters_and_failure_history() {
    bounded(async {
        let f = Fixture::new().await;
        f.enqueue(1).await;
        let first = f.claim().await;
        f.enqueue(2).await;
        assert!(f.queue.fail(&first, "cache unavailable", 60_000, 2).await.unwrap());
        let before: serde_json::Value = serde_json::from_str(&f.state().await).unwrap();
        for revision in [2, 1, 3, 4] { f.enqueue(revision).await; }
        let after: serde_json::Value = serde_json::from_str(&f.state().await).unwrap();
        for field in ["requested_at", "available_at", "attempt_count", "last_error", "lease_token", "dead_lettered_at"] {
            assert_eq!(before[field], after[field], "merge changed {field}");
        }
        assert_eq!(after["target_revision"], 4);
        assert_eq!(after["attempt_count"], 1);
        assert!(f.queue.claim("other-worker", 30_000).await.unwrap().is_none());
        f.sql.execute("UPDATE dbproxy_cache_repairs SET available_at=clock_timestamp()-interval '1 second' WHERE namespace=$1", &[&f.record.namespace]).await.unwrap();
        let second = f.claim().await;
        assert_eq!(second.attempt_count, 1);
        f.enqueue(5).await;
        assert!(f.queue.fail(&second, "still unavailable", 60_000, 2).await.unwrap());
        let before: serde_json::Value = serde_json::from_str(&f.state().await).unwrap();
        f.enqueue(6).await;
        let after: serde_json::Value = serde_json::from_str(&f.state().await).unwrap();
        for field in ["requested_at", "available_at", "attempt_count", "last_error", "lease_token", "dead_lettered_at"] { assert_eq!(before[field], after[field]); }
        assert!(!after["dead_lettered_at"].is_null());
        assert_eq!(after["attempt_count"], 2);
        assert!(f.queue.claim("other-worker", 30_000).await.unwrap().is_none());
        assert!(f.queue.requeue_dead_letter(&f.record).await.unwrap());
        let requeued: serde_json::Value = serde_json::from_str(&f.state().await).unwrap();
        assert!(requeued["lease_token"].as_i64().unwrap() > second.lease_token);
        assert_eq!(requeued["attempt_count"], 0);
        assert!(!f.queue.acknowledge(&second, Some(Revision(6))).await.unwrap());
        assert!(!f.queue.fail(&second, "late failure", 0, 1).await.unwrap());
        let next = f.claim().await;
        assert_eq!(next.target_revision, Revision(6));
        assert!(!f.queue.requeue_dead_letter(&f.record).await.unwrap());
        assert!(f.queue.acknowledge(&next, Some(Revision(6))).await.unwrap());
    }).await;
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL; run serially"]
async fn hot_merges_preserve_queue_position() {
    bounded(async {
        let f = Fixture::new().await;
        f.enqueue(1).await;
        let cold = RecordKey::new(&f.record.namespace, "cold").unwrap();
        f.writer.enqueue(&cold, Revision(1)).await.unwrap();
        f.sql.execute("UPDATE dbproxy_cache_repairs SET requested_at=to_timestamp(CASE WHEN record_key='hot' THEN 1 ELSE 2 END) WHERE namespace=$1", &[&f.record.namespace]).await.unwrap();
        f.enqueue(2).await;
        let hot = f.claim().await;
        assert_eq!(hot.target_revision, Revision(2));
        assert!(f.queue.acknowledge(&hot, Some(Revision(2))).await.unwrap());
        let next = f.queue.claim("worker", 30_000).await.unwrap().unwrap();
        assert_eq!(next.record, cold);
        assert!(f.queue.acknowledge(&next, None).await.unwrap());
    }).await;
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL; run serially"]
async fn expired_lease_cannot_ack_or_fail_before_or_after_same_worker_reclaims() {
    bounded(async {
        let f = Fixture::new().await;
        f.enqueue(1).await;
        let old = f.claim().await;
        f.expire().await;
        let expired = f.state().await;
        assert!(!f.queue.acknowledge(&old, Some(Revision(1))).await.unwrap());
        assert!(!f.queue.fail(&old, "late", 0, 1).await.unwrap());
        assert_eq!(f.state().await, expired);
        let next = f.claim().await;
        assert!(next.lease_token > old.lease_token);
        let renewed = f.state().await;
        assert!(!f.queue.acknowledge(&old, None).await.unwrap());
        assert!(!f.queue.fail(&old, "late", 0, 1).await.unwrap());
        assert_eq!(f.state().await, renewed);
        assert!(f.queue.acknowledge(&next, Some(Revision(1))).await.unwrap());
    })
    .await;
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL; run serially"]
async fn deleted_and_recreated_rows_do_not_reuse_lease_identity() {
    bounded(async {
        let f = Fixture::new().await;
        for batch in [false, true] {
            f.enqueue(1).await;
            let old = f.claim().await;
            if batch {
                let snapshot = tiangz_dbproxy_core::SnapshotEnvelope {
                    record: f.record.clone(),
                    revision: Revision(1),
                    schema: "test".into(),
                    schema_version: 1,
                    payload: vec![1],
                    updated_at_unix_ms: 1,
                };
                assert_eq!(
                    f.writer
                        .acknowledge_cached_multi(&[snapshot])
                        .await
                        .unwrap(),
                    1
                );
            } else {
                assert!(
                    f.writer
                        .acknowledge_cached(&f.record, Revision(1))
                        .await
                        .unwrap()
                );
            }
            f.enqueue(1).await; // Same record, revision AND worker: only token can distinguish it.
            let next = f.claim().await;
            assert!(next.lease_token > old.lease_token);
            let state = f.state().await;
            assert!(!f.queue.acknowledge(&old, Some(Revision(1))).await.unwrap());
            assert!(!f.queue.fail(&old, "stale incarnation", 0, 1).await.unwrap());
            assert_eq!(f.state().await, state);
            assert!(f.queue.acknowledge(&next, Some(Revision(1))).await.unwrap());
            f.enqueue(1).await; // Also cover deletion by the worker ACK itself.
            let third = f.claim().await;
            assert!(third.lease_token > next.lease_token);
            assert!(!f.queue.acknowledge(&next, Some(Revision(1))).await.unwrap());
            assert!(
                !f.queue
                    .fail(&next, "duplicate completion", 0, 1)
                    .await
                    .unwrap()
            );
            assert!(
                f.queue
                    .acknowledge(&third, Some(Revision(1)))
                    .await
                    .unwrap()
            );
        }
    })
    .await;
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL; run serially"]
async fn ack_waiting_on_a_concurrent_writer_cannot_clear_its_newer_target() {
    bounded(async {
        let mut f = Fixture::new().await;
        f.enqueue(1).await;
        let lease = f.claim().await;
        let tx = f.sql.transaction().await.unwrap();
        tx.execute("UPDATE dbproxy_cache_repairs SET target_revision=2 WHERE namespace=$1 AND record_key=$2", &[&f.record.namespace, &f.record.key]).await.unwrap();
        let queue = f.queue.clone();
        let ack = tokio::spawn(async move { queue.acknowledge(&lease, Some(Revision(1))).await.unwrap() });
        // Observe real lock contention before committing the concurrent update.
        timeout(Duration::from_secs(5), async {
            loop {
                assert!(!ack.is_finished());
                let waiting: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE NOT granted AND locktype='transactionid')", &[]).await.unwrap().get(0);
                if waiting { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("ACK must wait for the concurrent writer");
        tx.commit().await.unwrap();
        assert!(ack.await.unwrap());
        let next = f.claim().await;
        assert_eq!(next.target_revision, Revision(2));
        assert!(f.queue.acknowledge(&next, Some(Revision(2))).await.unwrap());
    }).await;
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL; run serially"]
async fn settlement_rechecks_expiry_after_waiting_for_an_unchanged_locked_row() {
    bounded(async {
        for failing in [false, true] {
            let mut f = Fixture::new().await;
            f.enqueue(1).await;
            let lease = f.queue.claim("same-worker", 1_500).await.unwrap().unwrap();
            let tx = f.sql.transaction().await.unwrap();
            tx.query_one("SELECT target_revision FROM dbproxy_cache_repairs WHERE namespace=$1 AND record_key=$2 FOR UPDATE", &[&f.record.namespace, &f.record.key]).await.unwrap();
            let queue = f.queue.clone();
            let settlement = tokio::spawn(async move {
                if failing { queue.fail(&lease, "expired while waiting", 0, 1).await.unwrap() }
                else { queue.acknowledge(&lease, Some(Revision(1))).await.unwrap() }
            });
            timeout(Duration::from_secs(5), async {
                loop {
                    assert!(!settlement.is_finished());
                    let waiting: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE NOT granted AND locktype='transactionid')", &[]).await.unwrap().get(0);
                    if waiting { break; }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                // Let the database clock expire the lease without updating its tuple.
                loop {
                    let expired: bool = tx.query_one("SELECT lease_until <= clock_timestamp() FROM dbproxy_cache_repairs WHERE namespace=$1 AND record_key=$2", &[&f.record.namespace, &f.record.key]).await.unwrap().get(0);
                    if expired { break; }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }).await.unwrap();
            tx.commit().await.unwrap();
            assert!(!settlement.await.unwrap(), "expired settlement succeeded (fail={failing})");
            let row: serde_json::Value = serde_json::from_str(&f.state().await).unwrap();
            assert_eq!(row["attempt_count"], 0);
            assert!(row["dead_lettered_at"].is_null());
            let current = f.claim().await;
            assert!(f.queue.acknowledge(&current, Some(Revision(1))).await.unwrap());
        }
    }).await;
}
