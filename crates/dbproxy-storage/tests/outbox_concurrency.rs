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
async fn batched_claims_compete_only_for_partition_heads_and_preserve_dead_letter_fences() {
    bounded(async {
        let mut fixture = Fixture::new().await;
        let queue = fixture.store.outbox_queue();
        for invalid_limit in [0, 65] {
            assert!(
                queue
                    .claim_batch_for_publisher("invalid", 30_000, Some(&fixture.id), invalid_limit)
                    .await
                    .is_err()
            );
        }
        for partition in 0..16 {
            for suffix in ["head", "following"] {
                let (request, effects) = fixture.request(
                    &format!("{suffix}-{partition}"),
                    &format!("group-{partition}"),
                );
                fixture
                    .store
                    .commit_records(request, effects)
                    .await
                    .unwrap();
            }
        }
        let barrier = Arc::new(Barrier::new(8));
        let mut tasks = JoinSet::new();
        for worker in 0..8 {
            let store = PostgresSnapshotStore::connect_existing(&fixture.url)
                .await
                .unwrap();
            let barrier = barrier.clone();
            let publisher = fixture.id.clone();
            tasks.spawn(async move {
                barrier.wait().await;
                store
                    .outbox_queue()
                    .claim_batch_for_publisher(
                        &format!("batch-worker-{worker}"),
                        30_000,
                        Some(&publisher),
                        4,
                    )
                    .await
                    .unwrap()
            });
        }
        let mut heads = Vec::new();
        while let Some(result) = tasks.join_next().await {
            let batch = result.unwrap();
            assert!(batch.len() <= 4);
            heads.extend(batch);
        }
        assert_eq!(heads.len(), 16);
        let partitions = heads
            .iter()
            .map(|lease| &lease.event.partition_key)
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(partitions.len(), 16);
        assert!(
            heads
                .iter()
                .all(|lease| lease.event.event_id.contains("head-"))
        );
        assert!(
            queue
                .claim_batch_for_publisher("no-follower", 30_000, Some(&fixture.id), 16)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(queue.fail(&heads[0], "poison head", 1, 1).await.unwrap());
        for head in &heads[1..] {
            assert!(queue.acknowledge(head).await.unwrap());
            assert!(
                !queue.acknowledge(head).await.unwrap(),
                "ACK is still fenced per lease"
            );
        }
        let followers = queue
            .claim_batch_for_publisher("next-batch", 30_000, Some(&fixture.id), 16)
            .await
            .unwrap();
        assert_eq!(followers.len(), 15);
        assert!(
            followers
                .iter()
                .all(|lease| lease.event.event_id.contains("following-")
                    && lease.event.partition_key != heads[0].event.partition_key)
        );
        for follower in &followers {
            assert!(queue.acknowledge(follower).await.unwrap());
        }
        assert!(
            queue
                .claim_batch_for_publisher("poison-remains", 30_000, Some(&fixture.id), 16)
                .await
                .unwrap()
                .is_empty()
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
#[tokio::test]
#[ignore = "requires dedicated PostgreSQL test URL and explicit migration opt-in"]
async fn blocked_heads_never_allow_followers_to_overtake() {
    bounded(async {
        for mode in ["backoff", "leased", "locked", "dead"] {
            let mut fixture = Fixture::new().await;
            let mut messages = vec![("head".to_string(), "ordered")];
            messages.extend((0..40).map(|n| (format!("follower-{n}"), "ordered")));
            messages.push(("free".to_string(), "independent"));
            for (name, partition) in messages {
                let (request, effects) = fixture.request(&name, partition);
                fixture.store.commit_records(request, effects).await.unwrap();
            }
            let head = format!("{}-head", fixture.id);
            let queue = fixture.store.outbox_queue();
            match mode {
                "backoff" => {
                    fixture.sql.execute("UPDATE dbproxy_outbox SET available_at=clock_timestamp()+interval '1 hour' WHERE event_id=$1", &[&head]).await.unwrap();
                }
                "leased" | "dead" => {
                    let lease = queue.claim_for_publisher("head-worker", 300_000, Some(&fixture.id)).await.unwrap().unwrap();
                    assert_eq!(lease.event.event_id, head);
                    if mode == "dead" {
                        assert!(queue.fail(&lease, "test dead head", 1, 1).await.unwrap());
                    }
                }
                _ => {}
            }
            let lock = fixture.sql.transaction().await.unwrap();
            if mode == "locked" {
                lock.query_one("SELECT event_id FROM dbproxy_outbox WHERE event_id=$1 FOR UPDATE", &[&head]).await.unwrap();
            }
            let free = queue.claim_for_publisher("free-worker", 300_000, Some(&fixture.id)).await.unwrap().unwrap();
            assert_eq!(free.event.partition_key, "independent", "{mode}");
            assert!(queue.acknowledge(&free).await.unwrap());
            assert!(queue.claim_for_publisher("follower-worker", 300_000, Some(&fixture.id)).await.unwrap().is_none(), "{mode}");
            assert!(queue.claim_for_publisher("unknown-worker", 300_000, Some("missing-publisher")).await.unwrap().is_none());
            lock.rollback().await.unwrap();
        }
    }).await;
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL test URL and explicit migration opt-in"]
async fn locked_prefix_falls_back_and_continuous_claims_preserve_order() {
    bounded(async {
        for prefix in [31_i64, 32, 33, 40] {
        let mut fixture = Fixture::new().await;
        for n in 1..=70 {
            let (request, effects) = fixture.request(&format!("event-{n:03}"), &format!("partition-{n:03}"));
            fixture.store.commit_records(request, effects).await.unwrap();
        }
        let queue = fixture.store.outbox_queue();
        let lock = fixture.sql.transaction().await.unwrap();
        let rows = lock.query("SELECT event_id FROM dbproxy_outbox WHERE publisher_id=$1 ORDER BY enqueue_order LIMIT $2 FOR UPDATE", &[&fixture.id, &prefix]).await.unwrap();
        assert_eq!(rows.len(), prefix as usize);
        let barrier = Arc::new(Barrier::new(8));
        let mut tasks = JoinSet::new();
        for n in 0..8 {
            let store = PostgresSnapshotStore::connect_existing(&fixture.url).await.unwrap();
            let barrier = barrier.clone();
            let publisher = fixture.id.clone();
            tasks.spawn(async move {
                barrier.wait().await;
                store.outbox_queue().claim_for_publisher(&format!("prefix-worker-{n}"), 300_000, Some(&publisher)).await.unwrap().unwrap()
            });
        }
        let mut claimed = Vec::new();
        while let Some(result) = tasks.join_next().await {
            claimed.push(result.unwrap());
        }
        let mut ids: Vec<_> = claimed.iter().map(|lease| lease.event.event_id.clone()).collect();
        ids.sort();
        assert_eq!(ids, ((prefix+1)..=(prefix+8)).map(|n| format!("{}-event-{n:03}", fixture.id)).collect::<Vec<_>>());
        for lease in claimed {
            assert!(queue.acknowledge(&lease).await.unwrap());
        }
        lock.rollback().await.unwrap();
        for n in (1..=70).filter(|n| !((prefix+1)..=(prefix+8)).contains(n)) {
            let lease = queue.claim_for_publisher("prefix-worker", 300_000, Some(&fixture.id)).await.unwrap().unwrap();
            assert_eq!(lease.event.event_id, format!("{}-event-{n:03}", fixture.id));
            assert!(queue.acknowledge(&lease).await.unwrap());
        }
        assert!(queue.claim_for_publisher("prefix-worker", 300_000, Some(&fixture.id)).await.unwrap().is_none());
        }
    }).await;
}

#[tokio::test]
#[ignore = "requires dedicated PostgreSQL test URL and explicit migration opt-in"]
async fn publisher_destination_matrix_keeps_same_key_groups_independent() {
    bounded(async {
        let mut cases = 0;
        for mode in ["backoff", "leased", "locked", "dead"] {
            for blocked_group in 0..4 {
                let mut fixture = Fixture::new().await;
                let queue = fixture.store.outbox_queue();
                let publishers = [format!("{}-p0", fixture.id), format!("{}-p1", fixture.id)];
                let destinations = [format!("{}-d0", fixture.id), format!("{}-d1", fixture.id)];
                for publisher in &publishers { queue.register_publisher(publisher, "isolated-no-mq-test").await.unwrap(); }
                let mut ids = vec![vec![String::new(); 2]; 4];
                // The blocked group's head is first for its publisher, so acquiring its lease
                // below cannot accidentally block a different destination.
                for group in std::iter::once(blocked_group).chain((0..4).filter(|g| *g != blocked_group)) {
                    let producer = format!("{}-source-{group}", fixture.id);
                    queue.register_route(&OutboxRoute { producer: producer.clone(), publisher: publishers[group / 2].clone(), destination: destinations[group % 2].clone(), version: 1 }).await.unwrap();
                    for (sequence, event_id) in ids[group].iter_mut().enumerate() {
                        let (request, mut effects) = fixture.request(&format!("g{group}-{sequence}"), "same-key");
                        let mut envelope = EventEnvelope::from_outbox(&effects.outbox_events[0]).unwrap().unwrap();
                        envelope.producer = producer.clone();
                        *event_id = envelope.event_id.clone();
                        effects.outbox_events[0] = envelope.into_outbox().unwrap();
                        fixture.store.commit_records(request, effects).await.unwrap();
                    }
                }
                let head = &ids[blocked_group][0];
                match mode {
                    "backoff" => { fixture.sql.execute("UPDATE dbproxy_outbox SET available_at=clock_timestamp()+interval '1 hour' WHERE event_id=$1", &[head]).await.unwrap(); },
                    "leased" | "dead" => {
                        let lease = queue.claim_for_publisher("blocked-head", 300_000, Some(&publishers[blocked_group / 2])).await.unwrap().unwrap();
                        assert_eq!(&lease.event.event_id, head);
                        if mode == "dead" { assert!(queue.fail(&lease, "route matrix head", 1, 1).await.unwrap()); }
                    },
                    _ => {},
                }
                let lock = fixture.sql.transaction().await.unwrap();
                if mode == "locked" { lock.query_one("SELECT event_id FROM dbproxy_outbox WHERE event_id=$1 FOR UPDATE", &[head]).await.unwrap(); }
                for sequence in 0..2 {
                    let mut leases = Vec::new();
                    for publisher in &publishers {
                        // Leave all returned leases active. Followers cannot be returned until
                        // acknowledgement, even when another publisher/destination has the same key.
                        for _ in 0..4 {
                            match queue.claim_for_publisher("matrix-worker", 300_000, Some(publisher)).await.unwrap() {
                                Some(lease) => {
                                    let group = ids.iter().position(|pair| pair[sequence] == lease.event.event_id).expect("follower overtook its head or an unexpected event was claimed");
                                    assert_ne!(group, blocked_group, "blocked group advanced: {mode}");
                                    assert_eq!(&lease.publisher_id, publisher);
                                    assert_eq!(lease.destination, destinations[group % 2]);
                                    leases.push(lease);
                                },
                                None => break,
                            }
                        }
                        assert!(queue.claim_for_publisher("duplicate-check", 300_000, Some(publisher)).await.unwrap().is_none());
                    }
                    let mut actual: Vec<_> = leases.iter().map(|l| l.event.event_id.clone()).collect();
                    let mut expected: Vec<_> = (0..4).filter(|g| *g != blocked_group).map(|g| ids[g][sequence].clone()).collect();
                    actual.sort(); expected.sort();
                    assert_eq!(actual, expected, "route group isolation failed: {mode}, blocked={blocked_group}");
                    for lease in leases { assert!(queue.acknowledge(&lease).await.unwrap()); }
                }
                lock.rollback().await.unwrap();
                match mode {
                    "backoff" => { fixture.sql.execute("UPDATE dbproxy_outbox SET available_at=clock_timestamp() WHERE event_id=$1", &[head]).await.unwrap(); },
                    "leased" => fixture.expire(head).await,
                    "dead" => { assert!(queue.retry_dead_letter(head, "route-matrix", "repaired").await.unwrap()); },
                    _ => {},
                }
                for expected in &ids[blocked_group] {
                    let lease = queue.claim_for_publisher("repaired-worker", 300_000, Some(&publishers[blocked_group / 2])).await.unwrap().unwrap();
                    assert_eq!(&lease.event.event_id, expected);
                    assert_eq!(lease.destination, destinations[blocked_group % 2]);
                    assert!(queue.acknowledge(&lease).await.unwrap());
                }
                for publisher in &publishers { assert!(queue.claim_for_publisher("empty", 300_000, Some(publisher)).await.unwrap().is_none()); }
                cases += 1;
                println!("A12_ROUTE_CASE mode={mode} blocked_group={blocked_group} groups=4 events=8 passed");
            }
        }
        assert_eq!(cases, 16);
        println!("A12_ROUTE_RESULT cases={cases} groups_per_case=4 events_per_case=8");
    }).await;
}
