//! 比较角色冷加载的存储阶段；不包含鉴权、路由、Actor构建或保活重连。
//! Compare the storage stage only, using identical records and batch shape.
use std::{
    env,
    error::Error,
    sync::Arc,
    time::{Duration, Instant},
};
use tiangz_dbproxy_core::{AsyncSnapshotStore, RecordKey, Revision, SnapshotWrite};
use tiangz_dbproxy_storage::{
    PostgresSnapshotStore, StorageMetrics, TieredSnapshotStore, TieredSnapshotStoreConfig,
};
use tokio::{sync::Barrier, task::JoinSet};
type Failure = Box<dyn Error + Send + Sync>;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Failure> {
    let pg = env::var("DBPROXY_POSTGRES_URL")?;
    let redis = env::var("DBPROXY_CACHE_REDIS_URL")?;
    let sweep = env::var("LOGIN_STORAGE_SWEEP").as_deref() == Ok("1");
    let concurrencies: Vec<usize> = if sweep {
        vec![1, 2, 4, 8, 12, 16]
    } else {
        vec![1, 16, 64]
    };
    let rounds = if sweep { 3 } else { 2 };
    let seconds = 5;
    let shapes = [2048usize, 1024, 1024, 2048, 256];
    for domains in [5usize, 30] {
        let mut seed = TieredSnapshotStore::connect(&pg, &redis).await?;
        let mut players = Vec::new();
        for player in 0..128usize {
            let mut records = Vec::new();
            for domain in 0..domains {
                let key = RecordKey::new(
                    format!("login-compare-{domains}"),
                    format!("{player}:{domain}"),
                )?;
                seed.save(SnapshotWrite {
                    request_id: format!("seed-{domains}-{player}-{domain}"),
                    record: key.clone(),
                    schema: "benchmark".into(),
                    schema_version: 1,
                    payload: vec![(player % 251) as u8; shapes[domain % 5]],
                    expected_revision: Some(Revision::ZERO),
                    updated_at_unix_ms: 1,
                })
                .await?;
                records.push(key);
            }
            players.push(records);
        }
        let players = Arc::new(players);
        if sweep {
            // 固定统计信息后再测；只作用于控制器创建的独立测试数据库。
            let (client, connection) = tokio_postgres::connect(&pg, tokio_postgres::NoTls).await?;
            let task = tokio::spawn(connection);
            client.batch_execute("ANALYZE dbproxy_snapshots").await?;
            let namespaces: Vec<String> = players[0].iter().map(|r| r.namespace.clone()).collect();
            let keys: Vec<String> = players[0].iter().map(|r| r.key.clone()).collect();
            let rows = client.query("EXPLAIN (ANALYZE, BUFFERS) SELECT namespace, record_key, schema_name, schema_version, revision, payload, updated_at_unix_ms FROM dbproxy_snapshots WHERE (namespace, record_key) IN (SELECT * FROM unnest($1::TEXT[], $2::TEXT[]))", &[&namespaces, &keys]).await?;
            println!(
                "QUERY_PLAN {}",
                serde_json::json!({"domains":domains,"lines":rows.iter().map(|r|r.get::<_,String>(0)).collect::<Vec<_>>()})
            );
            drop(client);
            task.await??;
        }
        for &concurrency in &concurrencies {
            // 两次相反顺序，减小固定执行顺序的偏差；PG为已预热数据库，不是磁盘冷启动。
            for round in 0..rounds {
                for mode in if round % 2 == 0 {
                    ["cache", "postgres"]
                } else {
                    ["postgres", "cache"]
                } {
                    let barrier = Arc::new(Barrier::new(concurrency + 1));
                    let mut tasks = JoinSet::new();
                    for worker in 0..concurrency {
                        let pg_store = PostgresSnapshotStore::connect(&pg).await?;
                        let metrics = Arc::new(StorageMetrics::default());
                        let cache_store = TieredSnapshotStore::connect_with_config(
                            &pg,
                            &redis,
                            TieredSnapshotStoreConfig::default(),
                            metrics.clone(),
                        )
                        .await?;
                        // 两种模式都预热同一组数据；初始化/连接不计入请求延迟。
                        for records in players.iter().skip(worker).step_by(concurrency) {
                            cache_store.load_cached_multi(records).await?;
                        }
                        let players = players.clone();
                        let barrier = barrier.clone();
                        tasks.spawn(async move {
                            barrier.wait().await;
                            let before = metrics.snapshot();
                            let start = Instant::now();
                            let mut samples = Vec::new();
                            let mut sequence = worker;
                            while start.elapsed() < Duration::from_secs(seconds) {
                                let player = sequence % players.len();
                                let records = &players[player];
                                let request_start = Instant::now();
                                let snapshots = if mode == "cache" {
                                    cache_store.load_cached_multi(records).await?
                                } else {
                                    pg_store.load_multi(records).await?
                                };
                                let latency = request_start.elapsed().as_micros() as u64;
                                if snapshots.len() != domains {
                                    return Err::<_, Failure>("snapshot count mismatch".into());
                                }
                                for (domain, snapshot) in snapshots.into_iter().enumerate() {
                                    let snapshot = snapshot.ok_or("missing snapshot")?;
                                    if snapshot.revision != Revision(1)
                                        || snapshot.payload.len() != shapes[domain % 5]
                                        || snapshot
                                            .payload
                                            .iter()
                                            .any(|b| *b != (player % 251) as u8)
                                    {
                                        return Err("snapshot content mismatch".into());
                                    }
                                }
                                samples.push(latency);
                                sequence += concurrency;
                            }
                            let after = metrics.snapshot();
                            Ok::<_, Failure>((
                                samples,
                                after.cache_hits - before.cache_hits,
                                after.postgres_fallbacks - before.postgres_fallbacks,
                                after.cache_stale_hits - before.cache_stale_hits,
                            ))
                        });
                    }
                    println!(
                        "CASE_START {}",
                        serde_json::json!({"domains":domains,"concurrency":concurrency,"round":round,"mode":mode})
                    );
                    let start = Instant::now();
                    barrier.wait().await;
                    let mut samples = Vec::new();
                    let (mut cache_hits, mut fallbacks, mut stale_hits) = (0, 0, 0);
                    while let Some(result) = tasks.join_next().await {
                        let (values, hits, falls, stale) = result??;
                        samples.extend(values);
                        cache_hits += hits;
                        fallbacks += falls;
                        stale_hits += stale;
                    }
                    let elapsed = start.elapsed().as_secs_f64();
                    samples.sort_unstable();
                    let percentile =
                        |p: usize| samples[((samples.len() - 1) * p) / 100] as f64 / 1000.0;
                    println!(
                        "CASE_RESULT {}",
                        serde_json::json!({"domains":domains,"payloadBytes":shapes.iter().sum::<usize>() * domains / 5,
                        "concurrency":concurrency,"round":round,"mode":mode,"operations":samples.len(),"elapsedSeconds":elapsed,
                        "cacheHits":cache_hits,"postgresFallbacks":fallbacks,"cacheStaleHits":stale_hits,
                        "loadsPerSecond":samples.len() as f64 / elapsed,"p50Ms":percentile(50),"p95Ms":percentile(95),"p99Ms":percentile(99)})
                    );
                }
            }
        }
    }
    Ok(())
}
