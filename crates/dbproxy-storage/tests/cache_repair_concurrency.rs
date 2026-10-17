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

/// 与请求事务内 `enqueue_in_transaction` 相同的语句，用于在事务中途停下观察锁。
/// The statement request transactions run through `enqueue_in_transaction`, issued by hand so the
/// test can pause between rows while their locks are held.
const REQUEST_ENQUEUE: &str = "INSERT INTO dbproxy_cache_repairs (namespace, record_key, target_revision) VALUES ($1, $2, $3) ON CONFLICT (namespace, record_key) DO UPDATE SET target_revision = GREATEST(dbproxy_cache_repairs.target_revision, EXCLUDED.target_revision)";

async fn raw_client() -> (tokio_postgres::Client, JoinHandle<()>) {
    let url = std::env::var("DBPROXY_TEST_POSTGRES_URL").expect("dedicated test database required");
    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    (
        client,
        tokio::spawn(async move { connection.await.unwrap() }),
    )
}

fn cached(record: &RecordKey, revision: u64) -> tiangz_dbproxy_core::SnapshotEnvelope {
    tiangz_dbproxy_core::SnapshotEnvelope {
        record: record.clone(),
        revision: Revision(revision),
        schema: "test".into(),
        schema_version: 1,
        payload: vec![1],
        updated_at_unix_ms: 1,
    }
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

/// 2026-10-16 长稳出现 40P01：批量清理按计划顺序删行，提交多条记录的请求事务按记录顺序写修复行，
/// 两边各持一行再等对方。清理改为跳过已被锁住的行后不再等待，因此不会进入死锁环。
/// The 2026-10-16 soak hit 40P01: the batch cleanup deleted rows in plan order while a multi-record
/// request transaction upserted repair rows in record order, each holding one row and waiting for the
/// other. Cleanup now skips rows that are already locked, so it never waits and cannot close a cycle.
#[tokio::test]
#[ignore = "requires dedicated PostgreSQL; run serially"]
async fn batch_cleanup_never_deadlocks_with_a_request_writing_several_repair_rows() {
    bounded(async {
        let f = Fixture::new().await;
        let a = RecordKey::new(&f.record.namespace, "deadlock-a").unwrap();
        let b = RecordKey::new(&f.record.namespace, "deadlock-b").unwrap();
        let mut outcomes = Vec::new();
        // 两个方向各跑一次：无论清理按数组顺序还是堆顺序访问，总有一个方向会先删到请求稍后要写的行。
        // Both directions: whether cleanup visits rows in array or heap order, one direction reaches
        // the row the request writes second before the row it already holds.
        for (first, second) in [(&a, &b), (&b, &a)] {
            f.sql
                .execute(
                    "DELETE FROM dbproxy_cache_repairs WHERE namespace=$1",
                    &[&f.record.namespace],
                )
                .await
                .unwrap();
            f.writer.enqueue(second, Revision(1)).await.unwrap();
            f.writer.enqueue(first, Revision(1)).await.unwrap();
            let (mut request, request_connection) = raw_client().await;
            let request_pid: i32 = request
                .query_one("SELECT pg_backend_pid()", &[])
                .await
                .unwrap()
                .get(0);
            let tx = request.transaction().await.unwrap();
            tx.execute(REQUEST_ENQUEUE, &[&first.namespace, &first.key, &2_i64])
                .await
                .unwrap();
            let queue = f.writer.clone();
            let batch = [cached(second, 1), cached(first, 1)];
            let ack = tokio::spawn(async move { queue.acknowledge_cached_multi(&batch).await });
            // 清理要么自己结束，要么真的被请求事务挡住，再让请求写第二行。
            // Let the request write its second row only once cleanup finished or is really blocked.
            timeout(Duration::from_secs(5), async {
                while !ack.is_finished() {
                    let blocked: bool = f
                        .sql
                        .query_one(
                            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)))",
                            &[&request_pid],
                        )
                        .await
                        .unwrap()
                        .get(0);
                    if blocked {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("cleanup must finish or block on the request");
            tx.execute(REQUEST_ENQUEUE, &[&second.namespace, &second.key, &2_i64])
                .await
                .unwrap();
            tx.commit().await.unwrap();
            drop(request);
            request_connection.await.unwrap();
            let mut targets = Vec::new();
            for record in [first, second] {
                let target: i64 = f
                    .sql
                    .query_one(
                        "SELECT target_revision FROM dbproxy_cache_repairs WHERE namespace=$1 AND record_key=$2",
                        &[&record.namespace, &record.key],
                    )
                    .await
                    .unwrap()
                    .get(0);
                targets.push(target);
            }
            outcomes.push((first.key.clone(), ack.await.unwrap(), targets));
        }
        // 先报死锁本身，再核对清理结果，避免把另一个方向的计数差异误当成复现。
        // Report the deadlock itself first, then the cleanup results.
        for (first, result, _) in &outcomes {
            if let Err(error) = result {
                panic!("cleanup failed while the request held {first}: {error}");
            }
        }
        for (first, result, targets) in outcomes {
            // 请求持有的行被跳过，另一行被删除后又被请求按新目标写回；两行都保留新目标。
            // The held row is skipped; the other is deleted and rewritten by the request. Both keep
            // the newer target, so no repair is lost.
            assert_eq!(result.unwrap(), 1, "request held {first}");
            assert_eq!(targets, vec![2, 2], "request held {first}");
        }
        f.sql
            .execute(
                "DELETE FROM dbproxy_cache_repairs WHERE namespace=$1",
                &[&f.record.namespace],
            )
            .await
            .unwrap();
    })
    .await;
}

/// 工作线程据此把真实的 40P01/40001 记为可重试的 WARN；其他数据库错误仍是 ERROR。
/// Workers log real 40P01/40001 failures as retryable warnings; other database errors stay errors.
#[tokio::test]
#[ignore = "requires dedicated PostgreSQL; run serially"]
async fn real_deadlock_and_serialization_failures_are_retryable_conflicts() {
    use tiangz_dbproxy_storage::StorageError;
    bounded(async {
        let f = Fixture::new().await;
        let a = RecordKey::new(&f.record.namespace, "conflict-a").unwrap();
        let b = RecordKey::new(&f.record.namespace, "conflict-b").unwrap();
        f.writer.enqueue(&a, Revision(1)).await.unwrap();
        f.writer.enqueue(&b, Revision(1)).await.unwrap();
        let bump = "UPDATE dbproxy_cache_repairs SET target_revision=target_revision+1 WHERE namespace=$1 AND record_key=$2";
        let (mut one, one_connection) = raw_client().await;
        let (mut two, two_connection) = raw_client().await;

        // 两个事务交叉更新两行，PostgreSQL 必定回滚其中一个（40P01）。
        // Two transactions update the rows crosswise; PostgreSQL must abort one with 40P01.
        let first = one.transaction().await.unwrap();
        let second = two.transaction().await.unwrap();
        first.execute(bump, &[&a.namespace, &a.key]).await.unwrap();
        second.execute(bump, &[&b.namespace, &b.key]).await.unwrap();
        type Params<'a> = [&'a (dyn tokio_postgres::types::ToSql + Sync); 2];
        let b_params: Params = [&b.namespace, &b.key];
        let a_params: Params = [&a.namespace, &a.key];
        let (left, right) = tokio::join!(
            first.execute(bump, &b_params),
            second.execute(bump, &a_params)
        );
        let deadlock = match (left, right) {
            (Err(error), Ok(_)) | (Ok(_), Err(error)) => StorageError::from(error),
            other => panic!("expected exactly one deadlock victim: {other:?}"),
        };
        assert!(deadlock.is_retryable_conflict(), "{deadlock}");
        assert!(deadlock.to_string().contains("40P01"), "{deadlock}");
        drop((first, second));

        // 可重复读事务更新一行已被他人提交修改的行，得到 40001。
        // A repeatable-read transaction updating a row changed by a later commit gets 40001.
        let reader = one
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .start()
            .await
            .unwrap();
        reader
            .query_one(
                "SELECT target_revision FROM dbproxy_cache_repairs WHERE namespace=$1 AND record_key=$2",
                &[&a.namespace, &a.key],
            )
            .await
            .unwrap();
        two.execute(bump, &[&a.namespace, &a.key]).await.unwrap();
        let serialization =
            StorageError::from(reader.execute(bump, &[&a.namespace, &a.key]).await.unwrap_err());
        assert!(serialization.is_retryable_conflict(), "{serialization}");
        assert!(serialization.to_string().contains("40001"), "{serialization}");
        drop(reader);

        // 其他数据库错误与非数据库错误不算可重试冲突。
        // Other database errors and non-database errors are not retryable conflicts.
        let division = StorageError::from(f.sql.query_one("SELECT 1/0", &[]).await.unwrap_err());
        assert!(!division.is_retryable_conflict(), "{division}");
        assert!(!StorageError::CacheFallbackGateClosed.is_retryable_conflict());

        drop((one, two));
        one_connection.await.unwrap();
        two_connection.await.unwrap();
        f.sql
            .execute(
                "DELETE FROM dbproxy_cache_repairs WHERE namespace=$1",
                &[&f.record.namespace],
            )
            .await
            .unwrap();
    })
    .await;
}
