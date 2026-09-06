//! Isolated PostgreSQL acceptance, split by failure mode; no Redis or soak access.
//! Requires DBPROXY_TEST_POSTGRES_URL and DBPROXY_TEST_ALLOW_SCHEMA_MIGRATION=1.
use std::{
    future::Future,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tiangz_dbproxy_core::{
    AppendRecord, AsyncSnapshotStore, CommitEffects, EventEnvelope, MultiRecordTransactionalWrite,
    MultiRecordTransactionalWriteOutcome as Outcome, RecordKey, Revision, StoreError,
    TransactionalRecordWrite,
};
use tiangz_dbproxy_storage::{OutboxRoute, PostgresSnapshotStore, StorageError};
use tokio::{
    sync::Barrier,
    task::{JoinHandle, JoinSet},
    time::timeout,
};

struct Fixture {
    url: String,
    id: String,
    store: PostgresSnapshotStore,
    sql: tokio_postgres::Client,
    connection: JoinHandle<()>,
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
            Ok("1"),
            "explicitly acknowledge migrations on a disposable test database"
        );
        let url = std::env::var("DBPROXY_TEST_POSTGRES_URL")
            .expect("dedicated test URL required; normal deployment URL is deliberately not read");
        let store = PostgresSnapshotStore::connect(&url).await.unwrap();
        let (sql, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        let connection = tokio::spawn(async move { connection.await.unwrap() });
        let id = format!(
            "matrix_{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let queue = store.outbox_queue();
        queue
            .register_publisher(&id, "isolated-no-mq-test")
            .await
            .unwrap();
        queue
            .register_route(&OutboxRoute {
                producer: id.clone(),
                publisher: id.clone(),
                version: 1,
                destination: format!("test:{id}"),
            })
            .await
            .unwrap();
        Self {
            url,
            id,
            store,
            sql,
            connection,
        }
    }

    fn request(
        &self,
        name: &str,
        partition: &str,
    ) -> (MultiRecordTransactionalWrite, CommitEffects) {
        let id = format!("{}-{name}", self.id);
        let request = MultiRecordTransactionalWrite {
            operation_id: id.clone(),
            writes: vec![TransactionalRecordWrite {
                record: RecordKey::new("relay_matrix", &id).unwrap(),
                schema: "opaque".into(),
                schema_version: 1,
                expected_revision: Revision::ZERO,
                payload: vec![1],
                updated_at_unix_ms: 1,
            }],
            result: vec![2],
        };
        let event = EventEnvelope {
            event_id: id.clone(),
            producer: self.id.clone(),
            event_type: "Changed".into(),
            aggregate_type: "document".into(),
            aggregate_id: id.clone(),
            partition_key: partition.into(),
            schema_version: 1,
            content_type: "application/octet-stream".into(),
            payload: vec![3],
            occurred_at_unix_ms: 1,
            route_version: 1,
        }
        .into_outbox()
        .unwrap();
        let effects = CommitEffects {
            appends: vec![AppendRecord {
                record: RecordKey::new("relay_matrix_facts", &id).unwrap(),
                schema: "opaque".into(),
                schema_version: 1,
                payload: vec![4],
                occurred_at_unix_ms: 1,
            }],
            outbox_events: vec![event],
        };
        (request, effects)
    }

    async fn expire(&self, id: &str) {
        assert_eq!(self.sql.execute(
            "UPDATE dbproxy_outbox SET lease_until=clock_timestamp()-interval '1 second' WHERE event_id=$1",
            &[&id],
        ).await.unwrap(), 1);
    }
}

async fn bounded(future: impl Future<Output = ()>) {
    timeout(Duration::from_secs(45), future)
        .await
        .expect("acceptance must not hang on lock regressions");
}

// Mirror the existing length-prefixed lock identity to hold a record lock from a separate
// transaction. This test checks the real SQL locking behavior, not a source-text pattern.
fn lock_key(scope: &str, components: &[&str]) -> String {
    let mut key = format!("{}:{scope}:", scope.len());
    for component in components {
        key.push_str(&format!("{}:{component}:", component.len()));
    }
    key
}

async fn wait_for_blocked_lock(
    tx: &tokio_postgres::Transaction<'_>,
    key: &str,
    tasks: &mut JoinSet<()>,
) {
    timeout(Duration::from_secs(10), async {
        loop {
            assert!(tasks.try_join_next().is_none(), "a writer finished before its required lock was released");
            let waiting: bool = tx.query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND objsubid=1 AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND classid::BIGINT=((hashtextextended($1,0)>>32)&4294967295) AND objid::BIGINT=(hashtextextended($1,0)&4294967295))",
                &[&key],
            ).await.unwrap().get(0);
            if waiting { return; }
            // Poll observed database state; elapsed time alone never counts as proof of blocking.
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("writer must visibly wait on the expected advisory lock");
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL test URL and explicit migration opt-in"]
async fn fan_in_sources_share_the_writer_lock_and_order_with_route_versions() {
    bounded(async {
        let mut fixture = Fixture::new().await;
        let queue = fixture.store.outbox_queue();
        let other_producer = format!("{}b", fixture.id);
        let destination = format!("test:{}", fixture.id);
        for (producer, version) in [(other_producer.clone(), 1), (fixture.id.clone(), 2)] {
            queue
                .register_route(&OutboxRoute {
                    producer,
                    version,
                    publisher: fixture.id.clone(),
                    destination: destination.clone(),
                })
                .await
                .unwrap();
        }
        let first = fixture.request("first-source", "shared");
        let mut second = fixture.request("second-source", "shared");
        let mut envelope = EventEnvelope::from_outbox(&second.1.outbox_events[0])
            .unwrap()
            .unwrap();
        envelope.producer = other_producer;
        second.1.outbox_events[0] = envelope.into_outbox().unwrap();
        let mut third = fixture.request("next-version", "shared");
        let mut envelope = EventEnvelope::from_outbox(&third.1.outbox_events[0])
            .unwrap()
            .unwrap();
        envelope.route_version = 2;
        third.1.outbox_events[0] = envelope.into_outbox().unwrap();
        let expected_ids =
            [&first, &second, &third].map(|(_, effects)| effects.outbox_events[0].event_id.clone());
        let expected_topics =
            [&first, &second, &third].map(|(_, effects)| effects.outbox_events[0].topic.clone());
        let held_key = lock_key(
            "record",
            &[
                &first.0.writes[0].record.namespace,
                &first.0.writes[0].record.key,
            ],
        );
        let destination_scope = lock_key("relay-destination", &[&fixture.id, &destination]);
        let delivery_key = lock_key("outbox-partition", &[&destination_scope, "shared"]);
        let mut writer_one = PostgresSnapshotStore::connect_existing(&fixture.url)
            .await
            .unwrap();
        let mut writer_two = PostgresSnapshotStore::connect_existing(&fixture.url)
            .await
            .unwrap();
        let holder = fixture.sql.transaction().await.unwrap();
        holder
            .query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                &[&held_key],
            )
            .await
            .unwrap();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            assert!(matches!(
                writer_one.commit_records(first.0, first.1).await.unwrap(),
                Outcome::Applied { .. }
            ));
        });
        // The first writer has acquired its destination lock before reaching this record lock.
        wait_for_blocked_lock(&holder, &held_key, &mut tasks).await;
        tasks.spawn(async move {
            assert!(matches!(
                writer_two.commit_records(second.0, second.1).await.unwrap(),
                Outcome::Applied { .. }
            ));
        });
        wait_for_blocked_lock(&holder, &delivery_key, &mut tasks).await;
        holder.commit().await.unwrap();
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        fixture
            .store
            .commit_records(third.0, third.1)
            .await
            .unwrap();
        for (event_id, topic) in expected_ids.into_iter().zip(expected_topics) {
            let lease = queue
                .claim_for_publisher("fan-in", 30_000, Some(&fixture.id))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(lease.event.event_id, event_id);
            assert_eq!(lease.event.topic, topic);
            assert_eq!(lease.destination, destination);
            assert!(queue.acknowledge(&lease).await.unwrap());
        }
        assert!(
            queue
                .claim_for_publisher("fan-in", 30_000, Some(&fixture.id))
                .await
                .unwrap()
                .is_none()
        );
    })
    .await;
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL test URL and explicit migration opt-in"]
async fn independent_connections_commit_identical_effects_exactly_once() {
    bounded(async {
        let fixture = Fixture::new().await;
        let (request, effects) = fixture.request("race", "one");
        let barrier = Arc::new(Barrier::new(8));
        let mut tasks = JoinSet::new();
        for i in 0..8 {
            let mut store = PostgresSnapshotStore::connect_existing(&fixture.url).await.unwrap();
            let barrier = barrier.clone();
            let request = request.clone();
            let effects = effects.clone();
            tasks.spawn(async move {
                barrier.wait().await;
                (i, store.commit_records(request, effects).await.unwrap())
            });
        }
        let mut applied = 0;
        let mut duplicate = 0;
        while let Some(task) = tasks.join_next().await {
            match task.unwrap().1 { Outcome::Applied { .. } => applied += 1, Outcome::Duplicate { .. } => duplicate += 1 }
        }
        assert_eq!((applied, duplicate), (1, 7));
        let row = fixture.sql.query_one(
            "SELECT (SELECT COUNT(*) FROM dbproxy_append_records WHERE operation_id=$1), (SELECT COUNT(*) FROM dbproxy_outbox WHERE operation_id=$1), (SELECT COUNT(*) FROM dbproxy_multi_transaction_effects WHERE operation_id=$1)",
            &[&request.operation_id],
        ).await.unwrap();
        for column in 0..3 { assert_eq!(row.get::<_, i64>(column), 1); }
    }).await;
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL test URL and explicit migration opt-in"]
async fn late_event_conflict_rolls_back_all_prior_writes_and_effects() {
    bounded(async {
        let mut fixture = Fixture::new().await;
        let (first, effects) = fixture.request("first", "one");
        fixture.store.commit_records(first, effects.clone()).await.unwrap();
        let (request, mut colliding) = fixture.request("second", "one");
        let original_event = colliding.outbox_events[0].clone();
        colliding.outbox_events = effects.outbox_events;
        assert!(matches!(fixture.store.commit_records(request.clone(), colliding.clone()).await,
            Err(StorageError::Core(StoreError::OutboxEventConflict { .. }))));
        assert!(fixture.store.load(&request.writes[0].record).await.unwrap().is_none());
        let row = fixture.sql.query_one(
            "SELECT (SELECT COUNT(*) FROM dbproxy_operation_claims WHERE operation_id=$1), (SELECT COUNT(*) FROM dbproxy_multi_transactions WHERE operation_id=$1), (SELECT COUNT(*) FROM dbproxy_multi_transaction_records WHERE operation_id=$1), (SELECT COUNT(*) FROM dbproxy_multi_transaction_effects WHERE operation_id=$1), (SELECT COUNT(*) FROM dbproxy_append_records WHERE operation_id=$1), (SELECT COUNT(*) FROM dbproxy_outbox WHERE operation_id=$1)",
            &[&request.operation_id],
        ).await.unwrap();
        for column in 0..6 { assert_eq!(row.get::<_, i64>(column), 0, "rollback column {column}"); }
        assert_eq!(fixture.sql.query_one(
            "SELECT COUNT(*) FROM dbproxy_cache_repairs WHERE namespace=$1 AND record_key=$2",
            &[&request.writes[0].record.namespace, &request.writes[0].record.key],
        ).await.unwrap().get::<_, i64>(0), 0, "cache repair must roll back too");
        colliding.outbox_events = vec![original_event];
        assert!(matches!(fixture.store.commit_records(request, colliding).await.unwrap(), Outcome::Applied { .. }));
    }).await;
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL test URL and explicit migration opt-in"]
async fn workers_compete_for_one_lease_without_blocking_other_partitions() {
    bounded(async {
        let mut fixture = Fixture::new().await;
        let (request, effects) = fixture.request("head", "blocked");
        fixture
            .store
            .commit_records(request, effects)
            .await
            .unwrap();
        let barrier = Arc::new(Barrier::new(8));
        let mut tasks = JoinSet::new();
        for i in 0..8 {
            let store = PostgresSnapshotStore::connect_existing(&fixture.url)
                .await
                .unwrap();
            let barrier = barrier.clone();
            let id = fixture.id.clone();
            tasks.spawn(async move {
                barrier.wait().await;
                store
                    .outbox_queue()
                    .claim_for_publisher(&format!("worker-{i}"), 30_000, Some(&id))
                    .await
                    .unwrap()
            });
        }
        let mut claimed = Vec::new();
        while let Some(task) = tasks.join_next().await {
            if let Some(lease) = task.unwrap() {
                claimed.push(lease);
            }
        }
        assert_eq!(claimed.len(), 1);
        let queue = fixture.store.outbox_queue();
        assert!(queue.fail(&claimed[0], "poison", 1, 1).await.unwrap());
        for (name, partition) in [("following", "blocked"), ("independent", "free")] {
            let (request, effects) = fixture.request(name, partition);
            fixture
                .store
                .commit_records(request, effects)
                .await
                .unwrap();
        }
        let free = queue
            .claim_for_publisher("free-worker", 30_000, Some(&fixture.id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(free.event.partition_key, "free");
        assert!(queue.acknowledge(&free).await.unwrap());
        assert!(
            queue
                .claim_for_publisher("blocked-worker", 30_000, Some(&fixture.id))
                .await
                .unwrap()
                .is_none()
        );
    })
    .await;
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL test URL and explicit migration opt-in"]
async fn expired_lease_cannot_ack_or_fail_before_or_after_same_worker_reclaims() {
    bounded(async {
        let mut fixture = Fixture::new().await;
        let (request, effects) = fixture.request("lease", "one");
        fixture
            .store
            .commit_records(request, effects)
            .await
            .unwrap();
        let queue = fixture.store.outbox_queue();
        let old = queue
            .claim_for_publisher("same-worker", 30_000, Some(&fixture.id))
            .await
            .unwrap()
            .unwrap();
        fixture.expire(&old.event.event_id).await;
        assert!(!queue.acknowledge(&old).await.unwrap());
        assert!(!queue.fail(&old, "expired", 1, 1).await.unwrap());
        let renewed = queue
            .claim_for_publisher("same-worker", 30_000, Some(&fixture.id))
            .await
            .unwrap()
            .unwrap();
        assert!(renewed.lease_token > old.lease_token);
        assert!(!queue.acknowledge(&old).await.unwrap());
        assert!(!queue.fail(&old, "stale token", 1, 1).await.unwrap());
        assert!(queue.acknowledge(&renewed).await.unwrap());
        assert!(!queue.acknowledge(&renewed).await.unwrap());
        let state = queue
            .inspect(&renewed.event.event_id)
            .await
            .unwrap()
            .unwrap();
        assert!(state.published);
        assert_eq!(state.attempts, 0);
        assert_eq!(state.expired_leases, 1);
    })
    .await;
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL test URL and explicit migration opt-in"]
async fn simultaneous_admin_retry_has_one_winner_and_one_audit_record() {
    bounded(async {
        let mut fixture = Fixture::new().await;
        let (request, effects) = fixture.request("admin", "one");
        fixture.store.commit_records(request, effects).await.unwrap();
        let queue = fixture.store.outbox_queue();
        let lease = queue.claim_for_publisher("worker", 30_000, Some(&fixture.id)).await.unwrap().unwrap();
        assert!(queue.fail(&lease, "poison", 1, 1).await.unwrap());
        let barrier = Arc::new(Barrier::new(8));
        let mut tasks = JoinSet::new();
        for i in 0..8 {
            let store = PostgresSnapshotStore::connect_existing(&fixture.url).await.unwrap();
            let barrier = barrier.clone();
            let id = lease.event.event_id.clone();
            tasks.spawn(async move {
                barrier.wait().await;
                store.outbox_queue().retry_dead_letter(&id, &format!("operator-{i}"), "fixed").await.unwrap()
            });
        }
        let mut succeeded = 0;
        while let Some(task) = tasks.join_next().await { succeeded += usize::from(task.unwrap()); }
        assert_eq!(succeeded, 1);
        let rows = fixture.sql.query(
            "SELECT prior_attempts,prior_error,reason,database_user FROM dbproxy_outbox_admin_audit WHERE event_id=$1",
            &[&lease.event.event_id],
        ).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, i64>(0), 1);
        assert_eq!(rows[0].get::<_, String>(1), "poison");
        assert_eq!(rows[0].get::<_, String>(2), "fixed");
        assert!(!rows[0].get::<_, String>(3).is_empty());
        let state = queue.inspect(&lease.event.event_id).await.unwrap().unwrap();
        assert!(!state.dead_lettered);
        assert!(!state.published);
        assert_eq!(state.attempts, 0);
        assert_eq!(state.destination, lease.destination);
    }).await;
}
