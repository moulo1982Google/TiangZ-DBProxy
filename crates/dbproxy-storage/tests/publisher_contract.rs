//! Bounded local RESP fixture: validates our wire behavior, not Redis durability.
use std::time::Duration;
use tiangz_dbproxy_core::{EventEnvelope, OutboxEvent};
use tiangz_dbproxy_storage::{PublishError, PublishMessage, Publisher, RedisStreamPublisher};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    task::JoinHandle,
    time::timeout,
};

#[derive(Clone, Copy)]
enum Reply {
    Success,
    SlowAof,
    RejectXadd,
    DisconnectAfterXadd,
}

struct Fixture {
    url: String,
    task: JoinHandle<Vec<Vec<Vec<u8>>>>,
}

impl Fixture {
    async fn new(reply: Reply) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("redis://{}/", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            timeout(Duration::from_secs(15), async move {
                let (socket, _) = listener.accept().await.unwrap();
                let (input, mut output) = socket.into_split();
                let mut input = BufReader::new(input);
                let mut commands = Vec::new();
                let mut sends = 0;
                loop {
                    let mut line = String::new();
                    match input.read_line(&mut line).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                    let count: usize = line.trim().strip_prefix('*').unwrap().parse().unwrap();
                    let mut args = Vec::new();
                    for _ in 0..count {
                        line.clear();
                        input.read_line(&mut line).await.unwrap();
                        let len: usize = line.trim().strip_prefix('$').unwrap().parse().unwrap();
                        let mut value = vec![0; len + 2];
                        input.read_exact(&mut value).await.unwrap();
                        assert_eq!(&value[len..], b"\r\n");
                        value.truncate(len);
                        args.push(value);
                    }
                    let command = args[0].clone();
                    commands.push(args);
                    match command.as_slice() {
                        b"XADD" => {
                            sends += 1;
                            match reply {
                                Reply::Success | Reply::SlowAof => output
                                    .write_all(format!("$3\r\n{sends}-0\r\n").as_bytes())
                                    .await
                                    .unwrap(),
                                Reply::RejectXadd => output
                                    .write_all(b"-ERR simulated-sensitive-detail\r\n")
                                    .await
                                    .unwrap(),
                                Reply::DisconnectAfterXadd => break,
                            }
                        }
                        b"WAITAOF" => {
                            if matches!(reply, Reply::SlowAof) {
                                tokio::time::sleep(Duration::from_millis(750)).await;
                            }
                            if output.write_all(b"*2\r\n:1\r\n:0\r\n").await.is_err() {
                                break;
                            }
                        }
                        _ => output.write_all(b"+OK\r\n").await.unwrap(),
                    }
                }
                commands
            })
            .await
            .expect("RESP fixture must terminate")
        });
        Self { url, task }
    }

    async fn finish(self) -> Vec<Vec<Vec<u8>>> {
        timeout(Duration::from_secs(15), self.task)
            .await
            .unwrap()
            .unwrap()
    }
}

#[tokio::test]
async fn aof_confirmation_can_exceed_the_redis_clients_default_half_second_timeout() {
    let fixture = Fixture::new(Reply::SlowAof).await;
    let publisher = RedisStreamPublisher::connect(&fixture.url, "legacy:")
        .await
        .unwrap();
    let result = Publisher::publish(&publisher, message(&legacy())).await;
    drop(publisher);
    fixture.finish().await;
    assert!(
        result.is_ok(),
        "valid AOF confirmation within 2 seconds must not time out after 500ms"
    );
}

fn legacy() -> OutboxEvent {
    OutboxEvent {
        event_id: "legacy-event".into(),
        topic: "old.trade".into(),
        partition_key: "one".into(),
        payload: vec![0, 255],
        occurred_at_unix_ms: 1,
    }
}

fn message(event: &OutboxEvent) -> PublishMessage<'_> {
    PublishMessage {
        event,
        destination: "exact.destination",
        operation_id: "operation",
        trade_id: "legacy-trade",
    }
}

#[tokio::test]
async fn confirmed_publications_reuse_connection_and_preserve_both_wire_formats() {
    let fixture = Fixture::new(Reply::Success).await;
    let publisher = RedisStreamPublisher::connect(&fixture.url, "must-not-override:")
        .await
        .unwrap();
    let old = legacy();
    let envelope = EventEnvelope {
        event_id: "new-event".into(),
        producer: "game".into(),
        event_type: "Changed".into(),
        aggregate_type: "document".into(),
        aggregate_id: "1".into(),
        partition_key: "one".into(),
        schema_version: 1,
        content_type: "application/octet-stream".into(),
        payload: vec![0, 255],
        occurred_at_unix_ms: 1,
        route_version: 1,
    }
    .into_outbox()
    .unwrap();
    for (i, event) in [&old, &envelope].into_iter().enumerate() {
        let receipt = timeout(
            Duration::from_secs(5),
            Publisher::publish(&publisher, message(event)),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(receipt.message_id, format!("{}-0", i + 1));
    }
    drop(publisher);
    let commands: Vec<_> = fixture
        .finish()
        .await
        .into_iter()
        .filter(|args| args[0] == b"XADD" || args[0] == b"WAITAOF")
        .collect();
    assert_eq!(commands.len(), 4);
    for (pair, event) in commands.as_chunks::<2>().0.iter().zip([&old, &envelope]) {
        assert_eq!(
            &pair[0][..3],
            &[
                b"XADD".to_vec(),
                b"exact.destination".to_vec(),
                b"*".to_vec()
            ]
        );
        let fields: std::collections::BTreeMap<_, _> = pair[0][3..]
            .as_chunks::<2>()
            .0
            .iter()
            .map(|field| {
                (
                    String::from_utf8(field[0].clone()).unwrap(),
                    field[1].clone(),
                )
            })
            .collect();
        assert_eq!(fields["event_id"], event.event_id.as_bytes());
        assert_eq!(fields["operation_id"], b"operation");
        assert_eq!(fields["trade_id"], b"legacy-trade");
        assert_eq!(fields["partition_key"], event.partition_key.as_bytes());
        assert_eq!(fields["occurred_at_unix_ms"], b"1");
        assert_eq!(fields["payload"], event.payload);
        if event.event_id == old.event_id {
            assert!(!fields.contains_key("event"));
            assert_eq!(fields.len(), 6);
        } else {
            assert_eq!(fields["event"], event.payload);
            assert_eq!(fields.len(), 7);
        }
        assert_eq!(
            pair[1],
            [
                b"WAITAOF".to_vec(),
                b"1".to_vec(),
                b"0".to_vec(),
                b"2000".to_vec()
            ]
        );
    }
}

async fn failure_case(reply: Reply) {
    let fixture = Fixture::new(reply).await;
    let publisher = RedisStreamPublisher::connect(&fixture.url, "legacy:")
        .await
        .unwrap();
    let error = timeout(
        Duration::from_secs(5),
        Publisher::publish(&publisher, message(&legacy())),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(matches!(error, PublishError::Transient(_)));
    assert!(!error.to_string().contains("simulated-sensitive-detail"));
    drop(publisher);
    let commands = fixture.finish().await;
    assert_eq!(
        commands.iter().filter(|args| args[0] == b"XADD").count(),
        1,
        "publisher must not silently retry"
    );
    assert!(
        !commands.iter().any(|args| args[0] == b"WAITAOF"),
        "failed XADD cannot be confirmed"
    );
}

#[tokio::test]
async fn xadd_error_is_redacted_and_never_followed_by_aof_ack() {
    failure_case(Reply::RejectXadd).await;
}

#[tokio::test]
async fn lost_xadd_response_is_transient_without_hidden_retry() {
    failure_case(Reply::DisconnectAfterXadd).await;
}

#[tokio::test]
async fn malformed_reserved_envelope_is_permanent_and_never_sent() {
    let fixture = Fixture::new(Reply::Success).await;
    let publisher = RedisStreamPublisher::connect(&fixture.url, "legacy:")
        .await
        .unwrap();
    let mut event = legacy();
    event.topic = "dbproxy.relay.v1.game.1".into();
    let error = Publisher::publish(&publisher, message(&event))
        .await
        .unwrap_err();
    assert!(matches!(error, PublishError::Permanent(_)));
    drop(publisher);
    assert!(
        !fixture
            .finish()
            .await
            .iter()
            .any(|args| args[0] == b"XADD" || args[0] == b"WAITAOF")
    );
}
