//! Six-operation workload foundation. This smoke verifies semantics, not performance.
#[path = "mixed_paced.rs"]
mod paced;
use super::*;
use std::io::Write;
use tiangz_dbproxy_core::{
    AppendRecord, CommitEffects, MultiRecordTransactionalWrite,
    MultiRecordTransactionalWriteOutcome, OutboxEvent, TransactionRecordReceipt,
    TransactionalRecordWrite, TransactionalWrite, TransactionalWriteOutcome,
};

const BATCH: usize = 30;
const KINDS: [&str; 6] = [
    "load",
    "load_multi",
    "save",
    "save_multi",
    "transaction",
    "commit_records",
];

fn kind(n: u64) -> usize {
    match n % 20 {
        0..=7 => 0,
        8..=11 => 1,
        12..=15 => 2,
        16..=17 => 3,
        18 => 4,
        _ => 5,
    }
}

fn writes(run: &str, n: u64, amount: usize) -> Vec<SnapshotWrite> {
    (0..amount)
        .map(|i| SnapshotWrite {
            request_id: format!("{run}-{n}-{i}"),
            record: RecordKey::new(format!("mix-{run}"), format!("{n}-{i}")).unwrap(),
            schema: "accept-mixed".into(),
            schema_version: 1,
            payload: (0..1024)
                .map(|b| ((n.wrapping_add(i as u64).wrapping_add(b)) % 251) as u8)
                .collect(),
            expected_revision: Some(Revision::ZERO),
            updated_at_unix_ms: 1,
        })
        .collect()
}

fn single(w: &SnapshotWrite) -> TransactionalWrite {
    TransactionalWrite {
        operation_id: w.request_id.clone(),
        record: w.record.clone(),
        schema: w.schema.clone(),
        schema_version: w.schema_version,
        expected_revision: Revision::ZERO,
        payload: w.payload.clone(),
        result: w.request_id.as_bytes().to_vec(),
        updated_at_unix_ms: 1,
    }
}

fn multi(ws: &[SnapshotWrite]) -> MultiRecordTransactionalWrite {
    MultiRecordTransactionalWrite {
        operation_id: ws[0].request_id.clone(),
        result: ws[0].request_id.as_bytes().to_vec(),
        writes: ws
            .iter()
            .map(|w| TransactionalRecordWrite {
                record: w.record.clone(),
                schema: w.schema.clone(),
                schema_version: w.schema_version,
                expected_revision: Revision::ZERO,
                payload: w.payload.clone(),
                updated_at_unix_ms: 1,
            })
            .collect(),
    }
}

fn effects(w: &SnapshotWrite) -> CommitEffects {
    CommitEffects {
        appends: vec![AppendRecord {
            record: w.record.clone(),
            schema: w.schema.clone(),
            schema_version: 1,
            payload: w.payload.clone(),
            occurred_at_unix_ms: 1,
        }],
        outbox_events: vec![OutboxEvent {
            event_id: w.request_id.clone(),
            topic: if std::env::var("MIX_OUTBOX_AUDIT").as_deref() == Ok("1") {
                w.record.namespace.clone()
            } else {
                "accept-mixed".into()
            },
            partition_key: if matches!(
                std::env::var("MIX_OUTBOX_STATS").as_deref(),
                Ok("off" | "on")
            ) {
                "ordered".into()
            } else {
                w.record.key.clone()
            },
            payload: w.payload.clone(),
            occurred_at_unix_ms: 1,
        }],
    }
}

async fn execute(
    client: &DbProxyClient,
    run: &str,
    n: u64,
    seeds: &[SnapshotWrite],
) -> Vec<SnapshotWrite> {
    let k = kind(n);
    let ws = writes(
        run,
        n,
        if k == 3 {
            BATCH
        } else if k == 5 {
            2
        } else {
            1
        },
    );
    match k {
        0 | 1 => {
            let expected = if k == 0 { &seeds[..1] } else { seeds };
            let values = if k == 0 {
                vec![client.load(&expected[0].record).await.unwrap()]
            } else {
                client
                    .load_multi(
                        &expected
                            .iter()
                            .map(|w| w.record.clone())
                            .collect::<Vec<_>>(),
                    )
                    .await
                    .unwrap()
            };
            assert_eq!(values.len(), expected.len());
            for (value, w) in values.into_iter().zip(expected) {
                let value = value.unwrap();
                assert_eq!(value.record, w.record);
                assert_eq!(value.revision, Revision(1));
                assert_eq!(value.payload, w.payload);
                assert_eq!(value.schema, w.schema);
            }
            return vec![];
        }
        2 => assert_eq!(
            client.save(ws[0].clone()).await.unwrap(),
            SnapshotWriteOutcome::Applied {
                revision: Revision(1)
            }
        ),
        3 => {
            let outcomes = client.save_multi(&ws).await.unwrap();
            assert_eq!(outcomes.len(), ws.len());
            for outcome in outcomes {
                assert_eq!(
                    outcome.unwrap(),
                    SnapshotWriteOutcome::Applied {
                        revision: Revision(1)
                    }
                );
            }
        }
        4 => assert_eq!(
            client.apply_transaction(single(&ws[0])).await.unwrap(),
            TransactionalWriteOutcome::Applied {
                new_revision: Revision(1),
                result: ws[0].request_id.as_bytes().to_vec()
            }
        ),
        5 => assert_eq!(
            client
                .commit_records(multi(&ws), effects(&ws[0]))
                .await
                .unwrap(),
            MultiRecordTransactionalWriteOutcome::Applied {
                records: ws
                    .iter()
                    .map(|w| TransactionRecordReceipt {
                        record: w.record.clone(),
                        new_revision: Revision(1)
                    })
                    .collect(),
                result: ws[0].request_id.as_bytes().to_vec()
            }
        ),
        _ => unreachable!(),
    }
    ws
}

#[tokio::test]
#[ignore = "fresh PG, production release process and real SDK six-operation smoke"]
async fn six_operations_preserve_snapshots_receipts_and_effects() {
    let env = env();
    let db = format!("{}_mix", env.run_id);
    let owner: String = sql(&format!("{}/postgres", env.admin_base))
        .await
        .query_one("SELECT current_user::text", &[])
        .await
        .unwrap()
        .get(0);
    create_database(&env.admin_base, &db, &owner).await;
    let url = format!("{}/{db}", env.admin_base);
    let dir = env.artifacts.join("mixed-smoke");
    std::fs::create_dir(&dir).unwrap();
    let endpoint = free_port();
    tenant_config(&dir, "A", &endpoint, &free_port());
    let deploy = deployment(&dir, &endpoint, &["A"]);
    let mut server = spawn(&deploy, &dir, "mixed", &env, &url, &url);
    let client = client(&endpoint, TOKEN_A, &mut server).await;
    let seeds = writes(&env.run_id, u64::MAX, BATCH);
    for seed in &seeds {
        assert_eq!(
            client.save(seed.clone()).await.unwrap(),
            SnapshotWriteOutcome::Applied {
                revision: Revision(1)
            }
        );
    }
    let mut ledger = std::fs::File::create(dir.join("requests.jsonl")).unwrap();
    let mut counts = [0_u64; 6];
    let mut expected = seeds.clone();
    for n in 0..60 {
        let k = kind(n);
        counts[k] += 1;
        writeln!(ledger,"{}",serde_json::json!({"kind":"intent","n":n,"operation":KINDS[k],"run":env.run_id,"payload_rule":"(n+i+byte)%251;1024 bytes","batch":BATCH})).unwrap();
        ledger.sync_data().unwrap();
        let started = Instant::now();
        let ws = execute(&client, &env.run_id, n, &seeds).await;
        writeln!(ledger,"{}",serde_json::json!({"kind":"response","n":n,"operation":KINDS[k],"ok":true,"elapsed_us":started.elapsed().as_micros()})).unwrap();
        expected.extend(ws);
    }
    ledger.sync_all().unwrap();
    assert_eq!(counts, [24, 12, 12, 6, 3, 3]);
    let pg = sql(&url).await;
    for w in &expected {
        let row=pg.query_one("SELECT payload,revision,schema_name,schema_version,updated_at_unix_ms FROM dbproxy_snapshots WHERE namespace=$1 AND record_key=$2",&[&w.record.namespace,&w.record.key]).await.unwrap();
        assert_eq!(row.get::<_, Vec<u8>>(0), w.payload);
        assert_eq!(row.get::<_, i64>(1), 1);
        assert_eq!(row.get::<_, String>(2), w.schema);
        assert_eq!(row.get::<_, i64>(3), 1);
        assert_eq!(row.get::<_, i64>(4), 1);
    }
    assert_eq!(
        count(&pg, "SELECT count(*) FROM dbproxy_snapshots").await,
        expected.len() as i64
    );
    for n in [18, 38, 58] {
        let w = &writes(&env.run_id, n, 1)[0];
        let receipt = client
            .load_transaction(&w.request_id, &w.record)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipt.new_revision, Revision(1));
        assert_eq!(receipt.result, w.request_id.as_bytes());
        assert_eq!(
            client.apply_transaction(single(w)).await.unwrap(),
            TransactionalWriteOutcome::Duplicate {
                new_revision: Revision(1),
                result: receipt.result
            }
        );
    }
    for n in [19, 39, 59] {
        let ws = writes(&env.run_id, n, 2);
        let request = multi(&ws);
        let receipt = client
            .load_multi_transaction(
                &request.operation_id,
                &ws.iter().map(|w| w.record.clone()).collect::<Vec<_>>(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipt.result, request.result);
        assert_eq!(receipt.records.len(), 2);
        assert_eq!(
            client
                .commit_records(request.clone(), effects(&ws[0]))
                .await
                .unwrap(),
            MultiRecordTransactionalWriteOutcome::Duplicate {
                records: receipt.records,
                result: request.result
            }
        );
        for table in ["dbproxy_append_records", "dbproxy_outbox"] {
            let row = pg
                .query_one(
                    &format!("SELECT payload FROM {table} WHERE operation_id=$1"),
                    &[&ws[0].request_id],
                )
                .await
                .unwrap();
            assert_eq!(row.get::<_, Vec<u8>>(0), ws[0].payload);
        }
        let row = pg
            .query_one(
                "SELECT event_id,topic,partition_key FROM dbproxy_outbox WHERE operation_id=$1",
                &[&ws[0].request_id],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(0), ws[0].request_id);
        assert_eq!(row.get::<_, String>(1), "accept-mixed");
        assert_eq!(row.get::<_, String>(2), ws[0].record.key);
    }
    assert_eq!(
        count(&pg, "SELECT count(*) FROM dbproxy_append_records").await,
        3
    );
    assert_eq!(count(&pg, "SELECT count(*) FROM dbproxy_outbox").await, 3);
    let result = serde_json::json!({"status":"FUNCTIONAL_SMOKE_ONLY","operations":counts,"snapshots":expected.len(),"append_records":3,"outbox_events":3,"transaction_replays":6,"batch_size":BATCH,"payload_bytes":1024,"timed_acceptance":false});
    std::fs::write(
        dir.join("result.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    println!("MIXED_SMOKE_RESULT {result}");
}
