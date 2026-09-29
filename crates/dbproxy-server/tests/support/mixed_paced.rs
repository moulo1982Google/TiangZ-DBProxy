//! Fixed-rate mixed requests with bounded concurrency and post-run PG reconciliation.
use super::*;
use serde_json::{Value, json};
use tiangz_dbproxy_client::ClientError;
use tiangz_dbproxy_protocol::wire::ErrorCode;

fn failure(error: ClientError) -> Value {
    let definite = matches!(&error, ClientError::Remote(e) if matches!(e.code,
        ErrorCode::InvalidRequest | ErrorCode::Unauthorized | ErrorCode::ProtocolMismatch |
        ErrorCode::RevisionConflict | ErrorCode::IdempotencyConflict | ErrorCode::OperationConflict |
        ErrorCode::TradeConflict | ErrorCode::LedgerConflict | ErrorCode::OutboxConflict));
    json!({"status":if definite {"failed"} else {"unknown"},"detail":error.to_string()})
}
fn checked(valid: bool) -> Value {
    json!({"status":if valid {"success"} else {"mismatch"}})
}

#[test]
fn uncertain_storage_and_transport_results_are_not_definite_failures() {
    assert_eq!(failure(ClientError::RequestTimeout)["status"], "unknown");
    assert_eq!(failure(ClientError::ConnectionClosed)["status"], "unknown");
    for (code, expected) in [
        (ErrorCode::StorageUnavailable, "unknown"),
        (ErrorCode::Internal, "unknown"),
        (ErrorCode::InvalidRequest, "failed"),
    ] {
        let error = ClientError::Remote(tiangz_dbproxy_client::RemoteError {
            code,
            message: "classification probe".into(),
            actual_revision: None,
        });
        assert_eq!(failure(error)["status"], expected);
    }
}

async fn issue(
    c: &DbProxyClient,
    run: &str,
    n: u64,
    seeds: &[SnapshotWrite],
) -> Result<Value, ClientError> {
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
    Ok(match k {
        0 | 1 => {
            let expected = if k == 0 { &seeds[..1] } else { seeds };
            let rows = if k == 0 {
                vec![c.load(&expected[0].record).await?]
            } else {
                c.load_multi(
                    &expected
                        .iter()
                        .map(|w| w.record.clone())
                        .collect::<Vec<_>>(),
                )
                .await?
            };
            checked(
                rows.len() == expected.len()
                    && rows.iter().zip(expected).all(|(r, w)| {
                        r.as_ref().is_some_and(|r| {
                            r.record == w.record
                                && r.payload == w.payload
                                && r.revision == Revision(1)
                                && r.schema == w.schema
                                && r.schema_version == 1
                        })
                    }),
            )
        }
        2 => checked(
            c.save(ws[0].clone()).await?
                == SnapshotWriteOutcome::Applied {
                    revision: Revision(1),
                },
        ),
        3 => {
            let items: Vec<Value> = c
                .save_multi(&ws)
                .await?
                .into_iter()
                .map(|v| match v {
                    Ok(v) => checked(
                        v == SnapshotWriteOutcome::Applied {
                            revision: Revision(1),
                        },
                    ),
                    Err(e) => failure(ClientError::Remote(e)),
                })
                .collect();
            json!({"status":if items.len()==BATCH && items.iter().all(|i|i["status"]=="success") {"success"} else {"partial"},"items":items})
        }
        4 => checked(
            c.apply_transaction(single(&ws[0])).await?
                == TransactionalWriteOutcome::Applied {
                    new_revision: Revision(1),
                    result: ws[0].request_id.as_bytes().to_vec(),
                },
        ),
        5 => checked(
            c.commit_records(multi(&ws), effects(&ws[0])).await?
                == MultiRecordTransactionalWriteOutcome::Applied {
                    records: ws
                        .iter()
                        .map(|w| TransactionRecordReceipt {
                            record: w.record.clone(),
                            new_revision: Revision(1),
                        })
                        .collect(),
                    result: ws[0].request_id.as_bytes().to_vec(),
                },
        ),
        _ => unreachable!(),
    })
}

fn number(key: &str, default: u64) -> u64 {
    std::env::var(key).map_or(default, |v| v.parse().unwrap())
}
fn append(file: &mut std::fs::File, row: &Value) {
    writeln!(file, "{row}").unwrap();
    file.flush().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "new PG database and real process; bounded mixed fixed-rate driver"]
async fn fixed_rate_six_operations() {
    let rate = number("MIX_RATE", 20);
    let warm = number("MIX_WARMUP", 2);
    let sample = number("MIX_SAMPLE", 5);
    let concurrency = number("MIX_CONCURRENCY", 8) as usize;
    assert!(
        (1..=400).contains(&rate)
            && warm <= 120
            && (1..=300).contains(&sample)
            && (1..=64).contains(&concurrency)
    );
    let total = (warm + sample) * rate;
    assert_eq!(total % 20, 0, "complete mix cycles required");
    let env = env();
    let db = format!("{}_pace", env.run_id);
    let owner: String = sql(&format!("{}/postgres", env.admin_base))
        .await
        .query_one("SELECT current_user::text", &[])
        .await
        .unwrap()
        .get(0);
    create_database(&env.admin_base, &db, &owner).await;
    let url = format!("{}/{db}", env.admin_base);
    let dir = env.artifacts.join("mixed-paced");
    std::fs::create_dir(&dir).unwrap();
    let endpoint = free_port();
    tenant_config(&dir, "A", &endpoint, &free_port());
    let deploy = deployment(&dir, &endpoint, &["A"]);
    let mut server = spawn(&deploy, &dir, "paced", &env, &url, &url);
    let c = client(&endpoint, TOKEN_A, &mut server).await;
    // Four actual SDK connections; in-flight concurrency is a separate limit.
    let mut clients = vec![c];
    for _ in 1..4 {
        clients.push(client(&endpoint, TOKEN_A, &mut server).await);
    }
    let seeds = Arc::new(writes(&env.run_id, u64::MAX, BATCH));
    for w in seeds.iter() {
        assert_eq!(
            clients[0].save(w.clone()).await.unwrap(),
            SnapshotWriteOutcome::Applied {
                revision: Revision(1)
            }
        );
    }
    let mut ledger = std::fs::File::create(dir.join("requests.jsonl")).unwrap();
    append(
        &mut ledger,
        &json!({"kind":"manifest","run":env.run_id,"rate":rate,"warmup":warm,"sample":sample,"concurrency":concurrency,"connections":4,"mix":[40,20,20,10,5,5],"batch":BATCH,"payload_bytes":1024,"payload_rule":"(n+i+byte)%251 wrapping u64","cleanup":"production-enabled","full_timing":warm==120&&sample==300}),
    );
    let start = tokio::time::Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    let mut completed = Vec::new();
    let mut not_sent = 0_u64;
    for n in 0..total {
        let scheduled = start + Duration::from_secs_f64(n as f64 / rate as f64);
        tokio::time::sleep_until(scheduled).await;
        while let Some(row) = tasks.try_join_next() {
            let row = row.unwrap();
            append(&mut ledger, &row);
            completed.push(row);
        }
        if tasks.len() >= concurrency {
            append(
                &mut ledger,
                &json!({"kind":"not_sent","n":n,"op":KINDS[kind(n)],"sample":n>=warm*rate,"reason":"in_flight_limit"}),
            );
            not_sent += 1;
            continue;
        }
        append(
            &mut ledger,
            &json!({"kind":"intent","n":n,"op":KINDS[kind(n)],"sample":n>=warm*rate,"scheduled_us":scheduled.duration_since(start).as_micros()}),
        );
        ledger.sync_data().unwrap();
        let c = clients[n as usize % 4].clone();
        let run = env.run_id.clone();
        let seeds = seeds.clone();
        tasks.spawn(async move {
            let sent=tokio::time::Instant::now();
            let outcome=match tokio::time::timeout(Duration::from_secs(10),issue(&c,&run,n,&seeds)).await {
                Ok(Ok(v))=>v,Ok(Err(e))=>failure(e),Err(_)=>json!({"status":"unknown","detail":"driver timeout; write may have committed"})};
            json!({"kind":"response","n":n,"op":KINDS[kind(n)],"sample":n>=warm*rate,"dispatch_us":sent.duration_since(scheduled).as_micros(),"rpc_us":sent.elapsed().as_micros(),"end_to_end_us":scheduled.elapsed().as_micros(),"outcome":outcome})
        });
    }
    while let Some(row) = tasks.join_next().await {
        let row = row.unwrap();
        append(&mut ledger, &row);
        completed.push(row);
    }
    tokio::time::sleep_until(start + Duration::from_secs(warm + sample)).await;
    ledger.sync_all().unwrap();
    // All sent writes are checked, including failed/partial/unknown responses. Never resend here.
    let pg = sql(&url).await;
    pg.batch_execute("SET statement_timeout='5s'")
        .await
        .unwrap();
    let mut checks = std::fs::File::create(dir.join("reconciliation.jsonl")).unwrap();
    let mut mismatches = 0_u64;
    let mut present = 0_u64;
    let mut effects_found = 0_u64;
    for row in &completed {
        let n = row["n"].as_u64().unwrap();
        let k = kind(n);
        if k < 2 {
            continue;
        }
        let ws = writes(
            &env.run_id,
            n,
            if k == 3 {
                BATCH
            } else if k == 5 {
                2
            } else {
                1
            },
        );
        let keys = ws.iter().map(|w| w.record.key.clone()).collect::<Vec<_>>();
        let rows=pg.query("SELECT record_key,payload,revision,schema_name,schema_version,updated_at_unix_ms FROM dbproxy_snapshots WHERE namespace=$1 AND record_key=ANY($2)",&[&ws[0].record.namespace,&keys]).await.unwrap();
        let mut valid = true;
        for (i, w) in ws.iter().enumerate() {
            let found = rows.iter().find(|r| r.get::<_, String>(0) == w.record.key);
            let status = row["outcome"]["items"].get(i).unwrap_or(&row["outcome"])["status"]
                .as_str()
                .unwrap_or("unknown");
            valid &= match found {
                Some(r) => {
                    present += 1;
                    status != "failed"
                        && r.get::<_, Vec<u8>>(1) == w.payload
                        && r.get::<_, i64>(2) == 1
                        && r.get::<_, String>(3) == w.schema
                        && r.get::<_, i64>(4) == 1
                        && r.get::<_, i64>(5) == 1
                }
                None => status != "success",
            };
        }
        if k >= 4 {
            let table = if k == 4 {
                "dbproxy_transactions"
            } else {
                "dbproxy_multi_transactions"
            };
            let receipt = pg
                .query_opt(
                    &format!("SELECT result FROM {table} WHERE operation_id=$1"),
                    &[&ws[0].request_id],
                )
                .await
                .unwrap();
            valid &= receipt.is_some() == !rows.is_empty();
            if let Some(receipt) = receipt {
                valid &= receipt.get::<_, Vec<u8>>(0) == ws[0].request_id.as_bytes();
            }
        }
        if k == 5 {
            valid &= rows.is_empty() || rows.len() == 2;
            for table in ["dbproxy_append_records", "dbproxy_outbox"] {
                let effect = pg
                    .query_opt(
                        &format!("SELECT payload FROM {table} WHERE operation_id=$1"),
                        &[&ws[0].request_id],
                    )
                    .await
                    .unwrap();
                valid &= effect.is_some() == !rows.is_empty();
                if let Some(effect) = effect {
                    valid &= effect.get::<_, Vec<u8>>(0) == ws[0].payload;
                    effects_found += 1;
                }
            }
        }
        mismatches += u64::from(!valid);
        append(
            &mut checks,
            &json!({"n":n,"valid":valid,"present":rows.len(),"expected":ws.len(),"response":row["outcome"]}),
        );
    }
    for seed in seeds.iter() {
        let r=pg.query_one("SELECT payload,revision FROM dbproxy_snapshots WHERE namespace=$1 AND record_key=$2",&[&seed.record.namespace,&seed.record.key]).await.unwrap();
        mismatches += u64::from(r.get::<_, Vec<u8>>(0) != seed.payload || r.get::<_, i64>(1) != 1);
    }
    mismatches += u64::from(
        count(&pg, "SELECT count(*) FROM dbproxy_snapshots").await != present as i64 + BATCH as i64,
    );
    mismatches += u64::from(
        count(&pg, "SELECT count(*) FROM dbproxy_append_records").await
            + count(&pg, "SELECT count(*) FROM dbproxy_outbox").await
            != effects_found as i64,
    );
    checks.sync_all().unwrap();
    let errors = completed
        .iter()
        .filter(|r| r["outcome"]["status"] != "success")
        .count();
    let result = json!({"full_timing":warm==120&&sample==300,"rounds":1,"rate":rate,"concurrency":concurrency,"scheduled":total,"responses":completed.len(),"not_sent":not_sent,"errors":errors,"reconciliation_mismatches":mismatches,"snapshots":present+BATCH as u64,"effect_rows":effects_found,"capacity_proven":false});
    std::fs::write(
        dir.join("result.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    println!("MIXED_PACED_RESULT {result}");
    assert_eq!(completed.len() as u64 + not_sent, total);
    assert_eq!(mismatches, 0);
    assert_eq!(errors, 0);
    assert_eq!(not_sent, 0);
}
