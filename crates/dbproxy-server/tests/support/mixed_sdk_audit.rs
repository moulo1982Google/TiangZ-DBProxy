//! Per-call acceptance timing through the existing synchronous SDK callback.
use serde_json::{Value, json};
use std::{cell::RefCell, future::Future, time::Duration};
use tiangz_dbproxy_client::{
    ClientConnectionOutcome, ClientObserver, ClientRequestOutcome, ClientRequestTiming,
};

tokio::task_local! {
    static ATTEMPTS: RefCell<Vec<Value>>;
}

pub struct Observer;
impl ClientObserver for Observer {
    fn connection_attempt(&self, _: usize, _: Duration, _: ClientConnectionOutcome) {}
    fn endpoint_failover(&self, _: usize, _: usize) {}
    fn request_attempt(&self, _: usize, _: &'static str, _: Duration, _: ClientRequestOutcome) {
        panic!("SDK acceptance audit requires the timed callback");
    }
    fn request_attempt_timed(
        &self,
        endpoint: usize,
        operation: &'static str,
        timing: ClientRequestTiming,
        outcome: ClientRequestOutcome,
    ) {
        // Callback executes in the caller's task: no lock, channel, file IO or await.
        // Seed/setup calls outside the explicit measurement scope are excluded.
        let _ = ATTEMPTS.try_with(|rows| {
            let mut rows = rows.borrow_mut();
            assert!(rows.len() < 2, "SDK retry bound changed");
            rows.push(json!({"operation":operation,"endpoint":endpoint,
                "queue_wait_us":timing.queue_wait.as_micros(),
                "exchange_us":timing.exchange.as_micros(),
                "outcome":format!("{outcome:?}")}));
        });
    }
}

pub async fn capture<T>(enabled: bool, future: impl Future<Output = T>) -> (T, Value) {
    if !enabled {
        return (future.await, Value::Null);
    }
    ATTEMPTS
        .scope(RefCell::new(Vec::with_capacity(2)), async {
            let result = future.await;
            let rows = ATTEMPTS.with(|rows| rows.take());
            (result, json!(rows))
        })
        .await
}

#[tokio::test]
async fn task_scopes_keep_simultaneous_calls_separate() {
    let one = tokio::spawn(capture(true, async {
        tokio::task::yield_now().await;
        Observer.request_attempt_timed(
            0,
            "load",
            ClientRequestTiming {
                queue_wait: Duration::from_micros(3),
                exchange: Duration::from_micros(7),
            },
            ClientRequestOutcome::Success,
        );
    }));
    let two = tokio::spawn(capture(true, async {
        Observer.request_attempt_timed(
            0,
            "save",
            ClientRequestTiming {
                queue_wait: Duration::ZERO,
                exchange: Duration::from_micros(9),
            },
            ClientRequestOutcome::Success,
        );
        tokio::task::yield_now().await;
    }));
    let (_, a) = one.await.unwrap();
    let (_, b) = two.await.unwrap();
    assert_eq!(a.as_array().unwrap().len(), 1);
    assert_eq!(b.as_array().unwrap().len(), 1);
    assert_eq!(a[0]["operation"], "load");
    assert_eq!(b[0]["operation"], "save");
    assert_eq!(a[0]["queue_wait_us"], 3);
    assert!(capture(false, async {}).await.1.is_null());
}
