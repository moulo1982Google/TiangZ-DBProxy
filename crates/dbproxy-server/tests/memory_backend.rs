use tiangz_dbproxy_core::{
    MultiRecordTransactionalWrite, RecordKey, Revision, SnapshotWrite, SnapshotWriteOutcome,
    StoreError, TransactionalRecordWrite, TransactionalWrite, TransactionalWriteOutcome,
};
use tiangz_dbproxy_server::{BackendError, DbProxyBackend, MemoryBackend};

fn snapshot(record: RecordKey, request_id: &str, revision: Revision) -> SnapshotWrite {
    SnapshotWrite {
        request_id: request_id.to_string(),
        record,
        schema: "player.domain".to_string(),
        schema_version: 1,
        payload: b"inventory=1".to_vec(),
        expected_revision: Some(revision),
        updated_at_unix_ms: 100,
    }
}

fn transaction(record: RecordKey, operation_id: &str, revision: Revision) -> TransactionalWrite {
    TransactionalWrite {
        operation_id: operation_id.to_string(),
        record,
        schema: "player.domain".to_string(),
        schema_version: 1,
        expected_revision: revision,
        payload: b"inventory=2".to_vec(),
        result: b"picked-up".to_vec(),
        updated_at_unix_ms: 200,
    }
}

#[tokio::test]
async fn all_write_modes_share_one_authoritative_snapshot() {
    let backend = MemoryBackend::new(4).unwrap();
    let player = RecordKey::new("player-economy", "1001").unwrap();
    assert_eq!(
        backend
            .save(snapshot(player.clone(), "create-1001", Revision::ZERO))
            .await
            .unwrap(),
        SnapshotWriteOutcome::Applied {
            revision: Revision(1)
        }
    );
    assert!(matches!(
        backend
            .apply_transaction(transaction(player.clone(), "pickup-1001", Revision(1)))
            .await
            .unwrap(),
        TransactionalWriteOutcome::Applied {
            new_revision: Revision(2),
            ..
        }
    ));

    let merchant = RecordKey::new("player-economy", "merchant").unwrap();
    let outcome = backend
        .apply_multi_transaction(MultiRecordTransactionalWrite {
            operation_id: "shop-1001".to_string(),
            writes: vec![
                TransactionalRecordWrite {
                    record: player.clone(),
                    schema: "player.domain".to_string(),
                    schema_version: 1,
                    expected_revision: Revision(2),
                    payload: b"coins=80".to_vec(),
                    updated_at_unix_ms: 300,
                },
                TransactionalRecordWrite {
                    record: merchant.clone(),
                    schema: "merchant.domain".to_string(),
                    schema_version: 1,
                    expected_revision: Revision::ZERO,
                    payload: b"coins=20".to_vec(),
                    updated_at_unix_ms: 300,
                },
            ],
            result: b"bought-potion".to_vec(),
        })
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        tiangz_dbproxy_core::MultiRecordTransactionalWriteOutcome::Applied { .. }
    ));
    assert_eq!(
        backend.load(&player).await.unwrap().unwrap().revision,
        Revision(3)
    );
    assert_eq!(
        backend.load(&merchant).await.unwrap().unwrap().revision,
        Revision(1)
    );
}

#[tokio::test]
async fn queued_snapshot_is_immediately_visible_but_remains_volatile() {
    let backend = MemoryBackend::new(2).unwrap();
    let player = RecordKey::new("player-runtime", "1001").unwrap();
    let mut request = snapshot(player.clone(), "queued-1001", Revision::ZERO);
    request.expected_revision = None;
    backend.enqueue_snapshot(request).await.unwrap();
    assert_eq!(
        backend.load(&player).await.unwrap().unwrap().revision,
        Revision(1)
    );
}

#[tokio::test]
async fn compact_receipt_fingerprints_still_reject_changed_payloads() {
    let backend = MemoryBackend::new(4).unwrap();
    let inventory = RecordKey::new("player", "1001:inventory").unwrap();
    let wallet = RecordKey::new("player", "1001:wallet").unwrap();
    let mut request = MultiRecordTransactionalWrite {
        operation_id: "shop-conflict-1001".to_string(),
        writes: vec![
            TransactionalRecordWrite {
                record: inventory,
                schema: "player.inventory".to_string(),
                schema_version: 1,
                expected_revision: Revision::ZERO,
                payload: b"items=1".to_vec(),
                updated_at_unix_ms: 100,
            },
            TransactionalRecordWrite {
                record: wallet,
                schema: "player.wallet".to_string(),
                schema_version: 1,
                expected_revision: Revision::ZERO,
                payload: b"coins=80".to_vec(),
                updated_at_unix_ms: 100,
            },
        ],
        result: b"bought-potion".to_vec(),
    };
    backend
        .apply_multi_transaction(request.clone())
        .await
        .unwrap();
    request.writes[0].payload = b"items=999".to_vec();
    assert!(matches!(
        backend.apply_multi_transaction(request).await.unwrap_err(),
        BackendError::Core(StoreError::OperationIdConflict { .. })
    ));
}
