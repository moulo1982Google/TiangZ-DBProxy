//! Host handshake restricts ENOSPC to the dedicated PostgreSQL WAL or log tmpfs.
use super::*;
use tiangz_dbproxy_core::{
    CommitEffects, MultiRecordTransactionalWrite, MultiRecordTransactionalWriteOutcome,
    TransactionalRecordWrite,
};

#[tokio::test]
#[ignore = "F10: dedicated bounded WAL/log filesystem, real DP process and original PG data"]
async fn f10_bounded_wal_or_log_full_recovers_without_false_success() {
    assert_eq!(std::env::var("FAULT_DEDICATED_SPACE").as_deref(), Ok("1"));
    let kind = std::env::var("FAULT_SPACE_KIND").unwrap();
    assert!(["wal", "log"].contains(&kind.as_str()));
    let env = env();
    let db = format!("{}_f10", env.run_id);
    let owner: String = sql(&format!("{}/postgres", env.admin_base))
        .await
        .query_one("SELECT current_user::text", &[])
        .await
        .unwrap()
        .get(0);
    create_database(&env.admin_base, &db, &owner).await;
    let url = format!("{}/{db}", env.admin_base);
    let dir = env.artifacts.join("f10-space");
    std::fs::create_dir_all(&dir).unwrap();
    let endpoint = free_port();
    let obs = free_port();
    tenant_config(&dir, "A", &endpoint, &obs);
    let config = deployment(&dir, &endpoint, &["A"]);
    let mut server = spawn(&config, &dir, "space", &env, &url, &url);
    let client = client(&endpoint, TOKEN_A, &mut server).await;
    let admin = sql(&url).await;
    let system_id: String = admin
        .query_one(
            "SELECT system_identifier::text FROM pg_control_system()",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let requests: Vec<_> = (0..30u8)
        .map(|n| write("f10-space", &format!("r-{n}"), n))
        .collect();
    let transaction = MultiRecordTransactionalWrite {
        operation_id: format!("{}-atomic", env.run_id),
        writes: (0..2u8)
            .map(|n| TransactionalRecordWrite {
                record: RecordKey::new("f10-atomic", format!("r-{n}")).unwrap(),
                schema: "test".into(),
                schema_version: 1,
                expected_revision: Revision::ZERO,
                payload: vec![n],
                updated_at_unix_ms: 1,
            })
            .collect(),
        result: vec![42],
    };
    let effects = CommitEffects {
        appends: vec![],
        outbox_events: vec![],
    };
    for request in &requests[..20] {
        assert_eq!(
            client.save(request.clone()).await.unwrap(),
            SnapshotWriteOutcome::Applied {
                revision: Revision(1)
            }
        );
    }
    insert_expired(&admin, "f10-expired", 10000).await;
    // An empty worker may be in its 60-second idle interval; wait for real progress first.
    wait_until(
        Duration::from_secs(70),
        "cleanup made progress before filesystem fault",
        || async {
            count(
                &admin,
                "SELECT count(*) FROM dbproxy_idempotency WHERE namespace='f10-expired'",
            )
            .await
                < 10000
        },
    )
    .await;
    std::fs::write(dir.join("fault-go"), &kind).unwrap();
    wait_until(
        Duration::from_secs(30),
        "bounded filesystem filled",
        || async { dir.join("fault-active").exists() },
    )
    .await;
    let mut outcomes = Vec::new();
    let mut errors = 0;
    for request in &requests[20..] {
        let started = Instant::now();
        let outcome =
            tokio::time::timeout(Duration::from_secs(4), client.save(request.clone())).await;
        let description = match outcome {
            Ok(Ok(result)) => {
                assert!(matches!(
                    result,
                    SnapshotWriteOutcome::Applied {
                        revision: Revision(1)
                    } | SnapshotWriteOutcome::Duplicate {
                        revision: Revision(1)
                    }
                ));
                format!("{result:?}")
            }
            Ok(Err(error)) => {
                errors += 1;
                format!("{error:?}")
            }
            Err(_) => {
                errors += 1;
                "timeout_result_unknown".into()
            }
        };
        outcomes.push(serde_json::json!({"request_id":request.request_id,"outcome":description,"seconds":started.elapsed().as_secs_f64()}));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    std::fs::write(
        dir.join("outage-outcomes.json"),
        serde_json::to_vec_pretty(&outcomes).unwrap(),
    )
    .unwrap();
    let transaction_outcome = tokio::time::timeout(
        Duration::from_secs(4),
        client.commit_records(transaction.clone(), effects.clone()),
    )
    .await;
    let transaction_accepted = matches!(&transaction_outcome, Ok(Ok(_)));
    if kind == "log" {
        assert!(
            transaction_accepted,
            "logging-only fault disrupted atomic commit"
        );
    }
    std::fs::write(
        dir.join("outage-transaction.txt"),
        format!("{transaction_outcome:?}"),
    )
    .unwrap();
    if kind == "wal" {
        assert!(errors > 0, "WAL exhaustion never reached the write path");
    } else {
        assert_eq!(
            errors, 0,
            "logging-only exhaustion disrupted durable writes"
        );
    }
    std::fs::write(
        dir.join("fault-observed"),
        "outage request outcomes recorded",
    )
    .unwrap();
    wait_until(
        Duration::from_secs(60),
        "space restored and dedicated PG ready",
        || async { dir.join("fault-release").exists() },
    )
    .await;
    let restored = sql(&url).await;
    let restored_id: String = restored
        .query_one(
            "SELECT system_identifier::text FROM pg_control_system()",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(system_id, restored_id, "PG data directory was replaced");
    let atomic_before = count(
        &restored,
        "SELECT count(*) FROM dbproxy_snapshots WHERE namespace='f10-atomic'",
    )
    .await;
    assert!(
        [0, 2].contains(&atomic_before),
        "partial atomic transaction after crash"
    );
    if transaction_accepted {
        assert_eq!(atomic_before, 2, "acknowledged transaction lost");
    }
    // Check every successful receipt before any retry can repair a missing result.
    for request in &requests[..20] {
        let row = restored.query_one("SELECT revision,payload FROM dbproxy_snapshots WHERE namespace=$1 AND record_key=$2", &[&request.record.namespace,&request.record.key]).await.unwrap();
        assert_eq!(row.get::<_, i64>(0), 1);
        assert_eq!(row.get::<_, Vec<u8>>(1), request.payload);
    }
    for (request, outcome) in requests[20..].iter().zip(&outcomes) {
        let accepted = outcome["outcome"].as_str().unwrap();
        if accepted.starts_with("Applied") || accepted.starts_with("Duplicate") {
            let row = restored.query_one("SELECT revision,payload FROM dbproxy_snapshots WHERE namespace=$1 AND record_key=$2", &[&request.record.namespace,&request.record.key]).await.unwrap();
            assert_eq!(row.get::<_, i64>(0), 1);
            assert_eq!(row.get::<_, Vec<u8>>(1), request.payload);
        }
    }
    for request in &requests {
        let result = tokio::time::timeout(Duration::from_secs(25), async {
            loop {
                if let Ok(result) = client.save(request.clone()).await {
                    break result;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            result,
            SnapshotWriteOutcome::Applied {
                revision: Revision(1)
            } | SnapshotWriteOutcome::Duplicate {
                revision: Revision(1)
            }
        ));
        let row = restored.query_one("SELECT revision,payload FROM dbproxy_snapshots WHERE namespace=$1 AND record_key=$2", &[&request.record.namespace,&request.record.key]).await.unwrap();
        assert_eq!(row.get::<_, i64>(0), 1);
        assert_eq!(row.get::<_, Vec<u8>>(1), request.payload);
    }
    tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            if client
                .commit_records(transaction.clone(), effects.clone())
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        client
            .commit_records(transaction.clone(), effects)
            .await
            .unwrap(),
        MultiRecordTransactionalWriteOutcome::Duplicate { .. }
    ));
    for write in &transaction.writes {
        let row = restored.query_one("SELECT revision,payload FROM dbproxy_snapshots WHERE namespace=$1 AND record_key=$2", &[&write.record.namespace,&write.record.key]).await.unwrap();
        assert_eq!(row.get::<_, i64>(0), 1);
        assert_eq!(row.get::<_, Vec<u8>>(1), write.payload);
    }
    assert_eq!(
        count(&restored, "SELECT count(*) FROM dbproxy_snapshots").await,
        32
    );
    wait_until(
        Duration::from_secs(90),
        "cleanup resumed after filesystem fault",
        || async {
            count(
                &restored,
                "SELECT count(*) FROM dbproxy_idempotency WHERE namespace='f10-expired'",
            )
            .await
                == 0
        },
    )
    .await;
    assert!(
        server.0.try_wait().unwrap().is_none(),
        "DP restarted or exited"
    );
    let result = serde_json::json!({"kind":kind,"confirmed_before":20,"outage_requests":10,"outage_errors":errors,"final_verified":32,"atomic_records_before_replay":atomic_before,"atomic_commit_accepted_during_fault":transaction_accepted,"revision":1,"cleanup_deleted":10000,"system_id":system_id,"dp_restarted":false});
    std::fs::write(
        dir.join("result.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    println!("F10_SPACE_RESULT {result}");
}
