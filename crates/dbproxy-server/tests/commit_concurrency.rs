//! Real TCP, independent clients, production MemoryBackend; no external services.
use std::{sync::Arc, time::Duration};
use tiangz_dbproxy_client::{ClientConfig, ClientError, DbProxyClient};
use tiangz_dbproxy_core::{
    AppendRecord, CommitEffects, MultiRecordTransactionalWrite,
    MultiRecordTransactionalWriteOutcome as Outcome, OutboxEvent, RecordKey, Revision,
    TransactionalRecordWrite,
};
use tiangz_dbproxy_protocol::wire::ErrorCode;
use tiangz_dbproxy_server::{DbProxyServer, MemoryBackend, ServerConfig};
use tokio::{
    sync::{Barrier, watch},
    task::{JoinHandle, JoinSet},
    time::timeout,
};

const CLIENTS: usize = 8;
const DEADLINE: Duration = Duration::from_secs(15);
type Request = (MultiRecordTransactionalWrite, CommitEffects);

struct Harness {
    clients: Vec<DbProxyClient>,
    stop: watch::Sender<bool>,
    server: JoinHandle<()>,
}

impl Harness {
    async fn new() -> Self {
        let token = "local-concurrent-test-token";
        let server = DbProxyServer::bind(
            ServerConfig::new("127.0.0.1:0".parse().unwrap(), token),
            Arc::new(MemoryBackend::new(4).unwrap()),
        )
        .await
        .unwrap();
        let endpoint = server.local_addr().unwrap().to_string();
        let (stop, rx) = watch::channel(false);
        let server = tokio::spawn(async move { server.serve(rx).await.unwrap() });
        let mut clients = Vec::new();
        for i in 0..CLIENTS {
            clients.push(
                DbProxyClient::connect(ClientConfig::new(
                    &endpoint,
                    token,
                    format!("concurrent-{i}"),
                ))
                .await
                .unwrap(),
            );
        }
        Self {
            clients,
            stop,
            server,
        }
    }

    async fn race(&self, requests: Vec<Request>) -> Vec<(usize, Result<Outcome, ClientError>)> {
        assert_eq!(requests.len(), self.clients.len());
        let barrier = Arc::new(Barrier::new(requests.len()));
        let mut tasks = JoinSet::new();
        for (index, (client, (request, effects))) in self.clients.iter().zip(requests).enumerate() {
            let client = client.clone();
            let barrier = barrier.clone();
            tasks.spawn(async move {
                barrier.wait().await;
                (index, client.commit_records(request, effects).await)
            });
        }
        timeout(DEADLINE, async {
            let mut results = Vec::new();
            while let Some(task) = tasks.join_next().await {
                results.push(task.unwrap());
            }
            results
        })
        .await
        .expect("concurrent commits must finish without deadlock")
    }

    async fn finish(self) {
        drop(self.clients);
        self.stop.send(true).unwrap();
        timeout(DEADLINE, self.server).await.unwrap().unwrap();
    }
}

fn request(id: &str) -> Request {
    let writes = ["left", "right"]
        .into_iter()
        .map(|key| TransactionalRecordWrite {
            record: RecordKey::new("concurrent", key).unwrap(),
            schema: "opaque".into(),
            schema_version: 1,
            expected_revision: Revision::ZERO,
            payload: vec![1],
            updated_at_unix_ms: 1,
        })
        .collect();
    let effects = CommitEffects {
        appends: ["first", "second"]
            .into_iter()
            .map(|key| AppendRecord {
                record: RecordKey::new("facts", format!("{id}-{key}")).unwrap(),
                schema: "opaque".into(),
                schema_version: 1,
                payload: vec![2],
                occurred_at_unix_ms: 1,
            })
            .collect(),
        outbox_events: ["first", "second"]
            .into_iter()
            .map(|key| OutboxEvent {
                event_id: format!("{id}-{key}"),
                topic: "document.changed".into(),
                partition_key: "shared".into(),
                payload: vec![3],
                occurred_at_unix_ms: 1,
            })
            .collect(),
    };
    (
        MultiRecordTransactionalWrite {
            operation_id: id.into(),
            writes,
            result: vec![4],
        },
        effects,
    )
}

fn assert_error(result: &Result<Outcome, ClientError>, expected: ErrorCode) {
    match result {
        Err(ClientError::Remote(error)) => assert_eq!(error.code, expected, "{error:?}"),
        other => panic!("expected {expected:?}, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simultaneous_identical_operations_apply_once_even_with_reordered_effects() {
    let harness = Harness::new().await;
    let (original, effects) = request("same-operation");
    let candidates = (0..CLIENTS)
        .map(|i| {
            let mut candidate = original.clone();
            let mut effects = effects.clone();
            if i % 2 == 0 {
                candidate.writes.reverse();
                effects.appends.reverse();
                effects.outbox_events.reverse();
            }
            (candidate, effects)
        })
        .collect();
    let results = harness.race(candidates).await;
    assert_eq!(
        results
            .iter()
            .filter(|(_, r)| matches!(r, Ok(Outcome::Applied { .. })))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|(_, r)| matches!(r, Ok(Outcome::Duplicate { .. })))
            .count(),
        CLIENTS - 1
    );
    for (_, result) in results {
        let (records, receipt) = match result.unwrap() {
            Outcome::Applied { records, result } | Outcome::Duplicate { records, result } => {
                (records, result)
            }
        };
        assert_eq!(receipt, original.result);
        assert!(records.iter().all(|r| r.new_revision == Revision(1)));
        assert_eq!(records.len(), 2);
    }
    for write in original.writes {
        let snapshot = harness.clients[0]
            .load(&write.record)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.revision, Revision(1));
        assert_eq!(snapshot.payload, write.payload);
    }
    harness.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simultaneous_same_id_with_different_effects_rejects_every_loser() {
    let harness = Harness::new().await;
    let candidates: Vec<_> = (0..CLIENTS)
        .map(|i| {
            let (candidate, mut effects) = request("conflicting-operation");
            effects.appends[0].payload = vec![i as u8];
            (candidate, effects)
        })
        .collect();
    let results = harness.race(candidates.clone()).await;
    let winners: Vec<_> = results
        .iter()
        .filter(|(_, r)| matches!(r, Ok(Outcome::Applied { .. })))
        .collect();
    assert_eq!(winners.len(), 1);
    let winner = winners[0].0;
    for (i, result) in &results {
        if *i != winner {
            assert_error(result, ErrorCode::OperationConflict);
        }
    }
    let (original, effects) = candidates[winner].clone();
    assert!(matches!(
        harness.clients[0]
            .commit_records(original, effects)
            .await
            .unwrap(),
        Outcome::Duplicate { .. }
    ));
    harness.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn competing_cas_rolls_back_private_records_and_does_not_reserve_effect_ids() {
    let harness = Harness::new().await;
    let candidates: Vec<_> = (0..CLIENTS)
        .map(|i| {
            let (mut candidate, effects) = request(&format!("cas-{i}"));
            candidate.writes[1].record.key = format!("private-{i}");
            (candidate, effects)
        })
        .collect();
    let results = harness.race(candidates.clone()).await;
    assert_eq!(
        results
            .iter()
            .filter(|(_, r)| matches!(r, Ok(Outcome::Applied { .. })))
            .count(),
        1
    );
    let mut revision = 1;
    for (i, result) in results {
        if matches!(result, Ok(Outcome::Applied { .. })) {
            continue;
        }
        assert_error(&result, ErrorCode::RevisionConflict);
        let (mut candidate, effects) = candidates[i].clone();
        assert!(
            harness.clients[0]
                .load(&candidate.writes[1].record)
                .await
                .unwrap()
                .is_none()
        );
        let keys: Vec<_> = candidate.writes.iter().map(|w| w.record.clone()).collect();
        assert!(
            harness.clients[0]
                .load_multi_transaction(&candidate.operation_id, &keys)
                .await
                .unwrap()
                .is_none()
        );
        candidate.writes[0].expected_revision = Revision(revision);
        // Reusing the original operation, fact and event IDs must be legal after rollback.
        assert!(matches!(
            harness.clients[0]
                .commit_records(candidate, effects)
                .await
                .unwrap(),
            Outcome::Applied { .. }
        ));
        revision += 1;
    }
    assert_eq!(
        harness.clients[0]
            .load(&RecordKey::new("concurrent", "left").unwrap())
            .await
            .unwrap()
            .unwrap()
            .revision,
        Revision(CLIENTS as u64)
    );
    harness.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_identity_conflicts_roll_back_unrelated_records_and_append_facts() {
    let harness = Harness::new().await;
    let candidates: Vec<_> = (0..CLIENTS)
        .map(|i| {
            let (mut candidate, mut effects) = request(&format!("unique-{i}"));
            for write in &mut candidate.writes {
                write.record.key.push_str(&format!("-{i}"));
            }
            effects.outbox_events[0].event_id = "global-event-identity".into();
            (candidate, effects)
        })
        .collect();
    let results = harness.race(candidates.clone()).await;
    assert_eq!(
        results
            .iter()
            .filter(|(_, r)| matches!(r, Ok(Outcome::Applied { .. })))
            .count(),
        1
    );
    for (i, result) in results {
        if matches!(result, Ok(Outcome::Applied { .. })) {
            continue;
        }
        assert_error(&result, ErrorCode::OutboxConflict);
        let (candidate, mut effects) = candidates[i].clone();
        for write in &candidate.writes {
            assert!(
                harness.clients[0]
                    .load(&write.record)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        effects.outbox_events[0].event_id = format!("repaired-{i}");
        assert!(matches!(
            harness.clients[0]
                .commit_records(candidate, effects)
                .await
                .unwrap(),
            Outcome::Applied { .. }
        ));
    }
    harness.finish().await;
}
