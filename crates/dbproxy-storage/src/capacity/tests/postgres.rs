//! 仅在专用临时数据库运行；创建夹具与只读角色，不访问常规 DBProxy URL。
//! Runs only in the dedicated disposable database, never the normal DBProxy URL.

use super::*;
use crate::PostgresSnapshotStore;
use tokio_postgres::Client;

/// 每个测试连接都随作用域取消，失败也不遗留驱动任务。
/// Test drivers are cancelled with their scope, including assertion failures.
async fn connect(config: &Config) -> (Client, ConnectionTask) {
    let (client, connection) = config.connect(NoTls).await.unwrap();
    let task = ConnectionTask(tokio::spawn(async move {
        let _ = connection.await;
    }));
    (client, task)
}

/// 固定逻辑表在报告中应始终存在，包括 missing 状态。
/// Every logical table remains in the report, including missing relations.
fn table<'a>(snapshot: &'a CapacitySnapshot, name: &str) -> &'a TableCapacity {
    snapshot.tables.iter().find(|t| t.table == name).unwrap()
}

#[tokio::test]
#[ignore = "requires DBPROXY_CAPACITY_TEST_URL pointing to disposable database v07_capacity; creates fixtures and a read-only role"]
async fn postgres_capacity_is_read_only_partition_aware_and_bounded() {
    timeout(Duration::from_secs(30), async {
    let url = std::env::var("DBPROXY_CAPACITY_TEST_URL").expect("dedicated fixture URL required");
    let config: Config = url.parse().unwrap();
    assert_eq!(
        config.get_dbname(),
        Some("v07_capacity"),
        "refuse any other database"
    );
    let store = PostgresSnapshotStore::connect(&url).await.unwrap();
    drop(store);
    let (mut admin, _admin_task) = connect(&config).await;
    let suffix = std::process::id();
    let role = format!("capacity_reader_{suffix}");
    admin.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'capacity_fixture_only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT ON ALL TABLES IN SCHEMA public TO {role};
        INSERT INTO dbproxy_snapshots(namespace,record_key,schema_name,schema_version,revision,payload,updated_at_unix_ms)
        SELECT 'capacity', g::text, 'fixture', 1, 1, decode(repeat('ab',1024),'hex'), 123 FROM generate_series(1,100) g ON CONFLICT DO NOTHING;
        INSERT INTO dbproxy_operation_claims(operation_id,operation_kind) VALUES ('capacity-claim','single') ON CONFLICT DO NOTHING;
        ANALYZE dbproxy_snapshots;")).await.unwrap();
    let mut reader = config.clone();
    reader.user(&role).password("capacity_fixture_only");
    let snapshot = sample_capacity(&reader, &CapacityOptions::default())
        .await
        .unwrap();
    assert!(snapshot.read_only);
    assert_eq!(snapshot.tables.len(), 18);
    assert!(snapshot.tables.iter().all(|t| t.status == "measured"));
    let snapshots = table(&snapshot, "dbproxy_snapshots");
    assert_eq!(snapshots.physical_relations, 32);
    assert_eq!(snapshots.estimated_rows, Some(100.0));
    assert_eq!(snapshots.unknown_estimate_relations, 0);
    assert_eq!(snapshots.oldest_server_time.status, "no-server-clock");
    let expected: i64 = admin.query_one("SELECT SUM(pg_total_relation_size(relid))::bigint FROM pg_partition_tree('dbproxy_snapshots') WHERE isleaf", &[]).await.unwrap().get(0);
    assert_eq!(snapshots.total_bytes, Some(expected as u64));
    assert_eq!(
        table(&snapshot, "dbproxy_operation_claims")
            .oldest_server_time
            .status,
        "not-requested"
    );
    let aged = sample_capacity(
        &reader,
        &CapacityOptions {
            include_server_age: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let claim = &table(&aged, "dbproxy_operation_claims").oldest_server_time;
    assert_eq!(claim.status, "measured");
    assert!(claim.unix_ms.unwrap() <= aged.sampled_at_unix_ms);
    assert_eq!(
        table(&aged, "dbproxy_outbox").oldest_server_time.status,
        "empty"
    );
    assert_eq!(
        admin
            .query_one("SELECT COUNT(*) FROM dbproxy_snapshots", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        100
    );

    // 空 schema 与特殊字符均可观察，工具不补建任何框架表。
    // Empty and quoted schemas are observed without creating framework tables.
    let schema = format!("capacity \"; {suffix}");
    let quoted = quote_identifier(&schema);
    admin
        .batch_execute(&format!(
            "CREATE SCHEMA {quoted}; GRANT USAGE ON SCHEMA {quoted} TO {role}"
        ))
        .await
        .unwrap();
    let options = CapacityOptions {
        schema: schema.clone(),
        include_server_age: true,
        ..Default::default()
    };
    let empty = sample_capacity(&reader, &options).await.unwrap();
    assert!(
        empty.tables.iter().all(|t| t.status == "missing"
            && t.total_bytes.is_none()
            && t.estimated_rows.is_none())
    );
    assert_eq!(admin.query_one("SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid WHERE n.nspname=$1", &[&schema]).await.unwrap().get::<_, i64>(0), 0);
    admin.batch_execute(&format!("CREATE TABLE {quoted}.dbproxy_snapshots(value bigint);
        CREATE VIEW {quoted}.dbproxy_idempotency AS SELECT 1 AS value;
        CREATE TABLE {quoted}.dbproxy_operation_claims(claimed_at timestamptz DEFAULT clock_timestamp());
        INSERT INTO {quoted}.dbproxy_operation_claims DEFAULT VALUES;
        GRANT SELECT ON ALL TABLES IN SCHEMA {quoted} TO {role};
        ALTER TABLE {quoted}.dbproxy_operation_claims ENABLE ROW LEVEL SECURITY;
        CREATE POLICY hidden ON {quoted}.dbproxy_operation_claims USING (false);")).await.unwrap();
    let partial = sample_capacity(&reader, &options).await.unwrap();
    assert_eq!(table(&partial, "dbproxy_snapshots").estimated_rows, None);
    assert_eq!(
        table(&partial, "dbproxy_snapshots").unknown_estimate_relations,
        1
    );
    assert_eq!(
        table(&partial, "dbproxy_idempotency").status,
        "unsupported-relation"
    );
    let filtered = &table(&partial, "dbproxy_operation_claims").oldest_server_time;
    assert_eq!(filtered.status, "query-error");
    assert_eq!(filtered.sqlstate.as_deref(), Some("42501")); // RLS must not report a false empty table.
    assert!(matches!(
        sample_capacity(
            &reader,
            &CapacityOptions {
                schema: "capacity_missing_schema".into(),
                ..Default::default()
            }
        )
        .await,
        Err(CapacityError::MissingSchema)
    ));

    // 使用真实表锁直接验证年龄查询的250ms期限与savepoint恢复；外层采样也必须有界。
    // A real table lock verifies the age deadline/savepoint recovery and whole-call deadline.
    let blocker = admin.transaction().await.unwrap();
    blocker
        .batch_execute("LOCK TABLE dbproxy_outbox_admin_audit IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let (mut age_client, _age_task) = connect(&reader).await;
    let tx = age_client
        .build_transaction()
        .read_only(true)
        .start()
        .await
        .unwrap();
    let expired = timeout(
        Duration::from_secs(2),
        sample_oldest(
            &tx,
            "public",
            "dbproxy_outbox_admin_audit",
            "created_at",
            Duration::from_secs(10),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(expired.status, "query-timeout");
    assert_eq!(expired.sqlstate.as_deref(), Some("57014"));
    assert!(expired.unix_ms.is_none());
    assert_eq!(
        tx.query_one("SELECT 1::int", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        1
    );
    tx.rollback().await.unwrap();
    assert!(matches!(
        sample_capacity(
            &reader,
            &CapacityOptions {
                timeout: Duration::from_millis(100),
                ..Default::default()
            }
        )
        .await,
        Err(CapacityError::Timeout)
    ));
    blocker.rollback().await.unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            let remaining: i64 = admin.query_one("SELECT COUNT(*) FROM pg_stat_activity WHERE application_name='tiangz-dbproxy-capacity'", &[]).await.unwrap().get(0);
            if remaining == 0 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("sample connections must disappear after completion/timeout");
    }).await.expect("dedicated PostgreSQL capacity fixture deadline");
}
