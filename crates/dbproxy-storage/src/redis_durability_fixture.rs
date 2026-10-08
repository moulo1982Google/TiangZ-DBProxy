//! 有界 RESP 替身只验证连接、期限与命令；不能证明 Redis 的真实持久性。
//! Bounded RESP fixture for connections, budgets and commands, never real Redis durability.
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    task::{JoinHandle, JoinSet},
};

#[derive(Clone, Copy)]
pub(crate) enum Failure {
    Unconfirmed,
    Stall,
    Disconnect,
    WriteError,
}
pub(crate) struct Plan {
    pub write: &'static [u8],
    pub max_ack_ms: u64,
    pub failure: Failure,
    pub first_write_delay: Duration,
    pub second_ack_delay: Duration,
}
#[derive(Default)]
pub(crate) struct Observed {
    pub writes: Vec<usize>,
    pub ack_ms: Vec<(usize, u64)>,
    pub commands: Vec<(usize, Vec<Vec<u8>>)>,
}
pub(crate) struct Fixture {
    pub url: String,
    pub observed: Arc<Mutex<Observed>>,
    task: JoinHandle<()>,
}
impl Fixture {
    pub async fn start(plan: Plan) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("redis://{}/", listener.local_addr().unwrap());
        let observed = Arc::new(Mutex::new(Observed::default()));
        let captured = observed.clone();
        let task = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            for index in 0..2 {
                let (socket, _) = listener.accept().await.unwrap();
                let observed = captured.clone();
                tasks.spawn(async move {
                    let (input, mut output) = socket.into_split();
                    let mut input = BufReader::new(input);
                    loop {
                        let mut line = String::new();
                        match input.read_line(&mut line).await {
                            Ok(0) => break,
                            Ok(_) => (),
                            Err(error)
                                if matches!(
                                    error.kind(),
                                    std::io::ErrorKind::ConnectionReset
                                        | std::io::ErrorKind::ConnectionAborted
                                ) =>
                            {
                                break;
                            }
                            Err(error) => panic!("RESP fixture read failed: {error}"),
                        }
                        let count: usize = line.trim().strip_prefix('*').unwrap().parse().unwrap();
                        let mut args = Vec::new();
                        for _ in 0..count {
                            line.clear();
                            input.read_line(&mut line).await.unwrap();
                            let size: usize =
                                line.trim().strip_prefix('$').unwrap().parse().unwrap();
                            let mut value = vec![0; size + 2];
                            input.read_exact(&mut value).await.unwrap();
                            value.truncate(size);
                            args.push(value);
                        }
                        let reply: &[u8] = if args[0] == plan.write {
                            observed.lock().unwrap().writes.push(index);
                            observed
                                .lock()
                                .unwrap()
                                .commands
                                .push((index, args.clone()));
                            if index == 0 && !plan.first_write_delay.is_zero() {
                                tokio::time::sleep(plan.first_write_delay).await;
                            }
                            if plan.write == b"XADD" {
                                if index == 0
                                    && matches!(plan.failure, Failure::WriteError)
                                    && args[4] == b"batch-event-1"
                                {
                                    b"-WRONGTYPE fixture write failure\r\n"
                                } else {
                                    b"$3\r\n1-0\r\n"
                                }
                            } else {
                                b":1\r\n"
                            }
                        } else if args[0] == b"WAITAOF" {
                            assert_eq!(&args[1..3], &[b"1".to_vec(), b"0".to_vec()]);
                            let ms = std::str::from_utf8(&args[3])
                                .unwrap()
                                .parse::<u64>()
                                .unwrap();
                            assert!((1..=plan.max_ack_ms).contains(&ms));
                            {
                                let mut observations = observed.lock().unwrap();
                                assert_eq!(
                                    observations.writes.last(),
                                    Some(&index),
                                    "must write on this connection before ACK"
                                );
                                observations.ack_ms.push((index, ms));
                            }
                            if index == 0 {
                                match plan.failure {
                                    Failure::Disconnect => break,
                                    Failure::Stall => {
                                        let mut probe = [0];
                                        let _ = input.read(&mut probe).await;
                                        break;
                                    }
                                    Failure::Unconfirmed => b"*2\r\n:0\r\n:0\r\n",
                                    Failure::WriteError => {
                                        panic!("failed pipeline cannot be acknowledged")
                                    }
                                }
                            } else {
                                tokio::time::sleep(plan.second_ack_delay).await;
                                b"*2\r\n:1\r\n:0\r\n"
                            }
                        } else {
                            b"+OK\r\n"
                        };
                        if output.write_all(reply).await.is_err() {
                            break;
                        }
                    }
                });
            }
            while let Some(result) = tasks.join_next().await {
                result.unwrap();
            }
        });
        Self {
            url,
            observed,
            task,
        }
    }
    pub async fn finish(mut self) {
        tokio::time::timeout(Duration::from_secs(3), &mut self.task)
            .await
            .unwrap()
            .unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
