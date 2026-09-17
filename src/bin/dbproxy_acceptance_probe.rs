//! 隔离验收辅助进程：使用正式协议与缓存编码，不输出认证内容。
//! Isolated acceptance helper using official wire/cache codecs; never emits credentials.
use prost::Message;
use serde_json::{Value, json};
use std::io::{self, BufRead, Write};
use std::sync::Arc;
use std::time::Duration;
use tiangz_dbproxy_client::{ClientConfig, ClientError, DbProxyClient};
use tiangz_dbproxy_core::{
    MultiRecordTransactionalWrite, RecordKey, Revision, SnapshotEnvelope, SnapshotWrite,
    TransactionalRecordWrite,
};
use tiangz_dbproxy_protocol::{
    DEFAULT_MAX_FRAME_BYTES, PRE_AUTHORITATIVE_PROTOCOL_FINGERPRINT_V2, PROTOCOL_FINGERPRINT,
    PROTOCOL_VERSION, read_message, wire, write_message,
};
use tiangz_dbproxy_storage::{RedisSnapshotCache, SnapshotCacheConfig, StorageMetrics};

type Failure = Box<dyn std::error::Error + Send + Sync>;

fn text<'a>(v: &'a Value, key: &str) -> Result<&'a str, Failure> {
    v[key]
        .as_str()
        .ok_or_else(|| format!("missing {key}").into())
}

fn record(v: &Value) -> Result<RecordKey, Failure> {
    let r: RecordKey = serde_json::from_value(v.clone())?;
    if !matches!(
        r.namespace.as_str(),
        "slg.demo.player.v1" | "slg.demo.world.v1"
    ) && !r.namespace.starts_with("acceptance.")
    {
        return Err("record outside isolated acceptance namespaces".into());
    }
    Ok(RecordKey::new(r.namespace, r.key)?)
}

// 单请求解码只返回验收所需字段；Hello中的密钥从不序列化。
// Decode only evidence fields; authentication tokens in Hello are never serialized.
fn decode(v: &Value) -> Result<Value, Failure> {
    let bytes: Vec<u8> = serde_json::from_value(v["bytes"].clone())?;
    if bytes.len() > 8 * 1024 * 1024 {
        return Err("frame too large".into());
    }
    if text(v, "direction")? == "request" {
        let frame = wire::ClientFrame::decode(bytes.as_slice())?;
        let Some(wire::client_frame::Body::Request(r)) = frame.body else {
            return Ok(json!({"kind":"hello"}));
        };
        let mut result = json!({"kind":"other", "rpcId":r.rpc_id});
        match r.body {
            Some(wire::request_envelope::Body::LoadSnapshot(r)) => {
                result["kind"] = json!("load");
                result["records"] = json!(
                    r.record
                        .map(|k| json!({"namespace":k.namespace,"key":k.key}))
                        .into_iter()
                        .collect::<Vec<_>>()
                );
            }
            Some(wire::request_envelope::Body::LoadMultiSnapshot(r)) => {
                result["kind"] = json!("load_multi");
                result["records"] = json!(
                    r.records
                        .iter()
                        .map(|k| json!({"namespace":k.namespace,"key":k.key}))
                        .collect::<Vec<_>>()
                );
            }
            Some(wire::request_envelope::Body::CommitRecords(r)) => {
                result["kind"] = json!("commit");
                result["operationId"] = json!(r.operation_id);
                result["writes"] = json!(r.writes.iter().map(|w| json!({
                    "record":w.record.as_ref().map(|k| json!({"namespace":k.namespace,"key":k.key})),
                    "expectedRevision":w.expected_revision,
                    "payload":serde_json::from_slice::<Value>(&w.payload).unwrap_or(Value::Null)
                })).collect::<Vec<_>>());
            }
            _ => {}
        }
        Ok(result)
    } else {
        let frame = wire::ServerFrame::decode(bytes.as_slice())?;
        match frame.body {
            Some(wire::server_frame::Body::Response(r)) => {
                Ok(json!({"kind":"response", "rpcId":r.rpc_id,
                "error":r.error.map(|e| e.code)}))
            }
            _ => Ok(json!({"kind":"hello"})),
        }
    }
}

async fn client(v: &Value) -> Result<DbProxyClient, Failure> {
    let endpoint = text(v, "endpoint")?;
    if !endpoint.starts_with("127.0.0.1:") {
        return Err("only loopback endpoints are allowed".into());
    }
    let mut config = ClientConfig::new(
        endpoint,
        std::env::var("DBPROXY_AUTH_TOKEN")?,
        "acceptance-probe",
    );
    config.request_timeout = Duration::from_secs(5);
    Ok(DbProxyClient::connect(config).await?)
}

async fn execute(v: &Value) -> Result<Value, Failure> {
    match text(v, "command")? {
        "decode" => decode(v),
        "info" => Ok(json!({"fingerprint":PROTOCOL_FINGERPRINT})),
        "cache_get" | "cache_restore" | "cache_negative" | "cache_put" => {
            let r = record(&v["record"])?;
            let cache = RedisSnapshotCache::connect_with_metrics_and_policy(
                &std::env::var("DBPROXY_CACHE_REDIS_URL")?,
                Arc::new(StorageMetrics::default()),
                SnapshotCacheConfig {
                    negative_ttl: Duration::from_secs(60),
                    ..Default::default()
                },
            )
            .await?;
            match text(v, "command")? {
                "cache_restore" | "cache_put" => {
                    let snapshot: SnapshotEnvelope = serde_json::from_value(v["snapshot"].clone())?;
                    if snapshot.record != r {
                        return Err("snapshot identity mismatch".into());
                    }
                    if v["command"] == "cache_restore" {
                        cache.delete(&r).await?;
                    }
                    cache.put(&snapshot).await?;
                }
                "cache_negative" => {
                    cache.delete(&r).await?;
                    cache.put_negative(&r).await?;
                }
                _ => {}
            }
            Ok(serde_json::to_value(cache.get(&r).await?)?)
        }
        "read" => {
            let c = client(v).await?;
            let r = record(&v["record"])?;
            let value = if v["cached"] == true {
                c.load_cached(&r, v["minimum"].as_u64().map(Revision))
                    .await?
            } else {
                c.load(&r).await?
            };
            Ok(serde_json::to_value(value)?)
        }
        "batch" => {
            let c = client(v).await?;
            let records = v["records"]
                .as_array()
                .ok_or("records missing")?
                .iter()
                .map(record)
                .collect::<Result<Vec<_>, _>>()?;
            let minima: Vec<u64> =
                serde_json::from_value(v.get("minima").cloned().unwrap_or(json!([])))?;
            let values = if v["cached"] == true {
                c.load_cached_multi(
                    &records,
                    &minima.into_iter().map(Revision).collect::<Vec<_>>(),
                )
                .await?
            } else {
                c.load_multi(&records).await?
            };
            Ok(serde_json::to_value(values)?)
        }
        "seed" => {
            let c = client(v).await?;
            let r = record(&v["record"])?;
            if !r.namespace.starts_with("acceptance.") {
                return Err("seed cannot modify SLG assets".into());
            }
            let revision = c
                .save(SnapshotWrite {
                    request_id: text(v, "requestId")?.into(),
                    record: r,
                    schema: "acceptance".into(),
                    schema_version: 1,
                    payload: vec![1],
                    expected_revision: Some(Revision(
                        v["expected"].as_u64().ok_or("expected missing")?,
                    )),
                    updated_at_unix_ms: 1,
                })
                .await?;
            Ok(json!({"outcome":format!("{revision:?}")}))
        }
        "atomic_batch" => {
            let reader = client(v).await?;
            let writer = client(v).await?;
            let keys = [
                RecordKey::new("acceptance.atomic", "left")?,
                RecordKey::new("acceptance.atomic", "right")?,
            ];
            let writes = |revision| {
                keys.iter()
                    .map(|record| TransactionalRecordWrite {
                        record: record.clone(),
                        schema: "test".into(),
                        schema_version: 1,
                        expected_revision: Revision(revision),
                        payload: vec![revision as u8],
                        updated_at_unix_ms: 1,
                    })
                    .collect()
            };
            writer
                .apply_multi_transaction(MultiRecordTransactionalWrite {
                    operation_id: "acceptance:seed".into(),
                    writes: writes(0),
                    result: vec![],
                })
                .await?;
            let write_keys = keys.clone();
            let updates = async move {
                for revision in 1..=100 {
                    writer
                        .apply_multi_transaction(MultiRecordTransactionalWrite {
                            operation_id: format!("acceptance:update:{revision}"),
                            writes: write_keys
                                .iter()
                                .map(|record| TransactionalRecordWrite {
                                    record: record.clone(),
                                    schema: "test".into(),
                                    schema_version: 1,
                                    expected_revision: Revision(revision),
                                    payload: vec![revision as u8],
                                    updated_at_unix_ms: 1,
                                })
                                .collect(),
                            result: vec![],
                        })
                        .await?;
                }
                Ok::<_, Failure>(())
            };
            let reads = async {
                for _ in 0..100 {
                    let values = reader.load_multi(&keys).await?;
                    if values[0].as_ref().ok_or("missing left")?.revision
                        != values[1].as_ref().ok_or("missing right")?.revision
                    {
                        return Err("mixed atomic batch".into());
                    }
                }
                Ok::<_, Failure>(())
            };
            tokio::try_join!(updates, reads)?;
            let final_values = reader.load_multi(&keys).await?;
            if final_values.iter().any(
                |v| !matches!(v, Some(s) if s.revision == Revision(101) && s.payload == vec![100]),
            ) {
                return Err("atomic writer did not commit every update".into());
            }
            Ok(json!({"updates":100,"reads":100,"finalRevision":101}))
        }
        "legacy_read" | "invalid_batch" => {
            let endpoint = text(v, "endpoint")?;
            if !endpoint.starts_with("127.0.0.1:") {
                return Err("loopback required".into());
            }
            let mut stream = tokio::net::TcpStream::connect(endpoint).await?;
            write_message(
                &mut stream,
                &wire::ClientFrame {
                    body: Some(wire::client_frame::Body::Hello(wire::ClientHello {
                        protocol_version: PROTOCOL_VERSION,
                        protocol_fingerprint: PRE_AUTHORITATIVE_PROTOCOL_FINGERPRINT_V2.into(),
                        auth_token: std::env::var("DBPROXY_AUTH_TOKEN")?,
                        client_name: "legacy-acceptance".into(),
                    })),
                },
                DEFAULT_MAX_FRAME_BYTES,
            )
            .await?;
            let hello: wire::ServerFrame = read_message(&mut stream, DEFAULT_MAX_FRAME_BYTES)
                .await?
                .ok_or("missing hello")?;
            if !matches!(hello.body, Some(wire::server_frame::Body::Hello(h)) if h.accepted && h.protocol_fingerprint == PRE_AUTHORITATIVE_PROTOCOL_FINGERPRINT_V2)
            {
                return Err("legacy hello rejected".into());
            }
            let r = record(&v["record"])?;
            let body = if v["command"] == "legacy_read" {
                wire::request_envelope::Body::LoadSnapshot(wire::LoadSnapshotRequest {
                    record: Some((&r).into()),
                    ..Default::default()
                })
            } else {
                wire::request_envelope::Body::LoadMultiSnapshot(wire::LoadMultiSnapshotRequest {
                    records: vec![(&r).into()],
                    allow_stale: true,
                    min_revisions: vec![1, 2],
                })
            };
            write_message(
                &mut stream,
                &wire::ClientFrame {
                    body: Some(wire::client_frame::Body::Request(wire::RequestEnvelope {
                        rpc_id: 1,
                        body: Some(body),
                    })),
                },
                DEFAULT_MAX_FRAME_BYTES,
            )
            .await?;
            let response: wire::ServerFrame = read_message(&mut stream, DEFAULT_MAX_FRAME_BYTES)
                .await?
                .ok_or("missing response")?;
            let Some(wire::server_frame::Body::Response(response)) = response.body else {
                return Err("wrong response".into());
            };
            if v["command"] == "invalid_batch" {
                return Ok(json!({"code":response.error.ok_or("invalid batch accepted")?.code}));
            }
            let Some(wire::response_envelope::Body::LoadSnapshot(loaded)) = response.body else {
                return Err("legacy read failed".into());
            };
            let snapshot = loaded
                .snapshot
                .map(SnapshotEnvelope::try_from)
                .transpose()?;
            Ok(serde_json::to_value(snapshot)?)
        }
        "reject_old_handshake" => {
            // 旧握手夹具只模拟兼容边界，不冒充历史服务端业务实现。
            // Simulate only the legacy handshake boundary, not a historical storage server.
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let endpoint = listener.local_addr()?.to_string();
            let fixture = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await?;
                let _: Option<wire::ClientFrame> =
                    read_message(&mut stream, DEFAULT_MAX_FRAME_BYTES).await?;
                write_message(
                    &mut stream,
                    &wire::ServerFrame {
                        body: Some(wire::server_frame::Body::Hello(wire::ServerHello {
                            protocol_version: PROTOCOL_VERSION,
                            protocol_fingerprint: PRE_AUTHORITATIVE_PROTOCOL_FINGERPRINT_V2.into(),
                            accepted: true,
                            error: None,
                            supports_outbox_relay: true,
                        })),
                    },
                    DEFAULT_MAX_FRAME_BYTES,
                )
                .await?;
                Ok::<_, Failure>(())
            });
            let result = client(&json!({"endpoint":endpoint})).await;
            fixture.await??;
            if !matches!(
                result
                    .as_ref()
                    .err()
                    .and_then(|e| e.downcast_ref::<ClientError>()),
                Some(ClientError::UnexpectedResponse(
                    "server accepted a different protocol"
                ))
            ) {
                return Err("legacy handshake did not reach expected protocol rejection".into());
            }
            Ok(json!({"rejected":true,"scope":"legacy handshake simulator"}))
        }
        _ => Err("unknown command".into()),
    }
}

fn main() -> Result<(), Failure> {
    if std::env::var("DBPROXY_ACCEPTANCE_ISOLATED").as_deref() != Ok("1") {
        return Err("isolated fixture opt-in required".into());
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    for line in io::stdin().lock().lines() {
        let v: Value = serde_json::from_str(&line?)?;
        let result = runtime
            .block_on(async { tokio::time::timeout(Duration::from_secs(30), execute(&v)).await });
        // 错误不回显可能含连接串的底层内容。
        // Never echo underlying errors that may contain sensitive connection strings.
        let response = match result {
            Ok(Ok(value)) => json!({"id":v["id"],"ok":true,"value":value}),
            Ok(Err(error)) => {
                let code = match error.downcast_ref::<ClientError>() {
                    Some(ClientError::Remote(remote)) => Some(remote.code as i32),
                    _ => None,
                };
                json!({"id":v["id"],"ok":false,"error":"probe operation failed","code":code})
            }
            Err(_) => json!({"id":v["id"],"ok":false,"error":"probe operation timed out"}),
        };
        println!("{response}");
        io::stdout().flush()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decoded_hello_never_contains_credentials() {
        let frame = wire::ClientFrame {
            body: Some(wire::client_frame::Body::Hello(wire::ClientHello {
                auth_token: "must-not-appear-in-report".into(),
                ..Default::default()
            })),
        };
        let decoded =
            decode(&json!({"direction":"request","bytes":frame.encode_to_vec()})).unwrap();
        assert_eq!(decoded, json!({"kind":"hello"}));
    }

    #[test]
    fn response_keeps_rpc_identity_and_remote_failure_code() {
        let frame = wire::ServerFrame {
            body: Some(wire::server_frame::Body::Response(wire::ResponseEnvelope {
                rpc_id: 42,
                error: Some(wire::RpcError {
                    code: 3001,
                    ..Default::default()
                }),
                ..Default::default()
            })),
        };
        let decoded =
            decode(&json!({"direction":"response","bytes":frame.encode_to_vec()})).unwrap();
        assert_eq!(decoded["rpcId"], 42);
        assert_eq!(decoded["error"], 3001);
    }

    #[test]
    fn cache_mutations_reject_unrelated_namespaces() {
        assert!(record(&json!({"namespace":"production.player","key":"alice"})).is_err());
        assert!(record(&json!({"namespace":"slg.demo.player.v1","key":"realm-41/alice"})).is_ok());
    }
}
