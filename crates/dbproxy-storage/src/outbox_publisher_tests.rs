use super::*;
use crate::{PublishMessage, Publisher};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    task::JoinSet,
};

// A bounded RESP fixture tests our connection/cancellation behavior, not Redis durability.
async fn fixture(stall: bool) -> (String, tokio::task::JoinHandle<()>, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("redis://{}/", listener.local_addr().unwrap());
    let sends = Arc::new(AtomicUsize::new(0));
    let observed = sends.clone();
    let task = tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        for index in 0..2 {
            let (socket, _) = listener.accept().await.unwrap();
            let sends = observed.clone();
            tasks.spawn(async move {
                let (input, mut output) = socket.into_split();
                let mut input = BufReader::new(input);
                loop {
                    let mut line = String::new();
                    if input.read_line(&mut line).await.unwrap() == 0 {
                        break;
                    }
                    let count: usize = line.trim().strip_prefix('*').unwrap().parse().unwrap();
                    let mut args = Vec::new();
                    for _ in 0..count {
                        line.clear();
                        input.read_line(&mut line).await.unwrap();
                        let size: usize = line.trim().strip_prefix('$').unwrap().parse().unwrap();
                        let mut value = vec![0; size + 2];
                        input.read_exact(&mut value).await.unwrap();
                        value.truncate(size);
                        args.push(value);
                    }
                    match args[0].as_slice() {
                        b"XADD" => {
                            assert_eq!(args[1], b"fixture.destination");
                            sends.fetch_add(1, Ordering::SeqCst);
                            output.write_all(b"$3\r\n1-0\r\n").await.unwrap();
                        }
                        b"WAITAOF" if index == 0 && stall => {
                            let mut probe = [0];
                            let _ = input.read(&mut probe).await;
                            break;
                        }
                        b"WAITAOF" => output
                            .write_all(if index == 0 {
                                b"*2\r\n:0\r\n:0\r\n"
                            } else {
                                b"*2\r\n:1\r\n:0\r\n"
                            })
                            .await
                            .unwrap(),
                        _ => output.write_all(b"+OK\r\n").await.unwrap(),
                    }
                }
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap()
        }
    });
    (url, task, sends)
}

async fn publication_case(stall: bool) {
    let (url, task, sends) = fixture(stall).await;
    let publisher = RedisStreamPublisher::connect(&url, "legacy-prefix:")
        .await
        .unwrap();
    let event = OutboxEvent {
        event_id: "fixture-event".into(),
        topic: "legacy.topic".into(),
        partition_key: "one".into(),
        payload: vec![1],
        occurred_at_unix_ms: 1,
    };
    let message = || PublishMessage {
        event: &event,
        destination: "fixture.destination",
        operation_id: "operation",
        trade_id: "old-trade",
    };
    let first = tokio::time::timeout(
        Duration::from_millis(100),
        Publisher::publish(&publisher, message()),
    )
    .await;
    if stall {
        assert!(first.is_err())
    } else {
        assert!(first.unwrap().is_err())
    }
    let receipt = tokio::time::timeout(
        Duration::from_secs(2),
        Publisher::publish(&publisher, message()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(receipt.message_id, "1-0");
    assert_eq!(sends.load(Ordering::SeqCst), 2);
    drop(publisher);
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn aof_failure_requires_a_new_connection_and_republication() {
    publication_case(false).await;
}
#[tokio::test]
async fn publication_timeout_discards_the_in_flight_connection() {
    publication_case(true).await;
}

#[test]
fn endpoint_identity_excludes_credentials_but_includes_database() {
    use crate::redis_endpoint_fingerprint as fingerprint;
    assert_eq!(
        fingerprint("redis://alice:old@127.0.0.1:6379/1").unwrap(),
        fingerprint("redis://bob:new@127.0.0.1:6379/1").unwrap()
    );
    assert_ne!(
        fingerprint("redis://127.0.0.1:6379/1").unwrap(),
        fingerprint("redis://127.0.0.1:6379/2").unwrap()
    );
}
