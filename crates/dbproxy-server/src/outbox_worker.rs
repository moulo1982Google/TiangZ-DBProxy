//! Bounded Outbox publication; broker confirmation precedes each fenced PostgreSQL ACK.
use crate::{BackendError, DurableQueueProcessOutcome, RetryWorkerPolicy, StorageBackend};
use std::time::Instant;
use tiangz_dbproxy_storage::{OutboxLease, PublishError, PublishMessage, PublishReceipt};
use tokio::time::timeout;

const OUTBOX_PUBLISH_BATCH_SIZE: usize = 16;

impl StorageBackend {
    pub async fn process_outbox_once(
        &self,
        worker_id: &str,
        policy: RetryWorkerPolicy,
    ) -> Result<DurableQueueProcessOutcome, BackendError> {
        let policy = policy.validate()?;
        let Some(lease) = self.outbox.claim(worker_id, policy.lease_ms).await? else {
            return Ok(DurableQueueProcessOutcome::Empty);
        };
        let publisher = self
            .outbox_publishers
            .get(&lease.publisher_id)
            .ok_or(BackendError::InvalidConfig("outbox publisher is missing"))?;
        let message = tiangz_dbproxy_storage::PublishMessage {
            event: &lease.event,
            destination: &lease.destination,
            operation_id: &lease.operation_id,
            trade_id: &lease.trade_id,
        };
        let started = Instant::now();
        let published = timeout(self.outbox_publish_timeout, publisher.publish(message)).await;
        let status = match &published {
            Ok(Ok(_)) => "success",
            Ok(Err(_)) => "error",
            Err(_) => "timeout",
        };
        self.outbox_relay_metrics.record(
            &lease.producer,
            &lease.publisher_id,
            status,
            started.elapsed().as_secs_f64(),
        );
        let publication =
            published.unwrap_or(Err(tiangz_dbproxy_storage::PublishError::Transient(
                "publication deadline exceeded; result may be unknown",
            )));
        self.finish_outbox_publication(&lease, publication, policy)
            .await
    }

    /// One publisher per batch, and only one head from each independent ordering group.
    pub async fn process_outbox_batch(
        &self,
        worker_id: &str,
        policy: RetryWorkerPolicy,
    ) -> Result<Vec<Result<DurableQueueProcessOutcome, BackendError>>, BackendError> {
        let policy = policy.validate()?;
        let Some(first) = self.outbox.claim(worker_id, policy.lease_ms).await? else {
            return Ok(Vec::new());
        };
        let publisher = self
            .outbox_publishers
            .get(&first.publisher_id)
            .ok_or(BackendError::InvalidConfig("outbox publisher is missing"))?;
        // Claiming the first head already fences its followers out of the second query.
        let mut leases = self
            .outbox
            .claim_batch_for_publisher(
                worker_id,
                policy.lease_ms,
                Some(&first.publisher_id),
                OUTBOX_PUBLISH_BATCH_SIZE - 1,
            )
            .await?;
        leases.insert(0, first);
        let messages = leases
            .iter()
            .map(|lease| PublishMessage {
                event: &lease.event,
                destination: &lease.destination,
                operation_id: &lease.operation_id,
                trade_id: &lease.trade_id,
            })
            .collect::<Vec<_>>();
        let started = Instant::now();
        let published = timeout(
            self.outbox_publish_timeout,
            publisher.publish_batch(&messages),
        )
        .await;
        let timed_out = published.is_err();
        let publications = match published {
            Ok(results) if results.len() == leases.len() => results,
            _ => leases
                .iter()
                .map(|_| {
                    Err(PublishError::Transient(
                        "batch publication incomplete; result may be unknown",
                    ))
                })
                .collect(),
        };
        let elapsed = started.elapsed().as_secs_f64();
        let mut outcomes = Vec::with_capacity(leases.len());
        for (lease, publication) in leases.iter().zip(publications) {
            let status = if timed_out {
                "timeout"
            } else if publication.is_ok() {
                "success"
            } else {
                "error"
            };
            self.outbox_relay_metrics
                .record(&lease.producer, &lease.publisher_id, status, elapsed);
            outcomes.push(
                self.finish_outbox_publication(lease, publication, policy)
                    .await,
            );
        }
        Ok(outcomes)
    }

    async fn finish_outbox_publication(
        &self,
        lease: &OutboxLease,
        publication: Result<PublishReceipt, PublishError>,
        policy: RetryWorkerPolicy,
    ) -> Result<DurableQueueProcessOutcome, BackendError> {
        match publication {
            Ok(_) => {
                if self.outbox.acknowledge(lease).await? {
                    Ok(DurableQueueProcessOutcome::Committed)
                } else {
                    self.outbox_relay_metrics.record(
                        &lease.producer,
                        &lease.publisher_id,
                        "lease_lost",
                        0.0,
                    );
                    Ok(DurableQueueProcessOutcome::LeaseLost)
                }
            }
            Err(error) => {
                let max_attempts =
                    if matches!(error, tiangz_dbproxy_storage::PublishError::Permanent(_)) {
                        1
                    } else {
                        policy.max_attempts
                    };
                let dead_lettered =
                    lease.attempt_count.saturating_add(1) >= u64::from(max_attempts);
                let retry_delay =
                    policy.outbox_retry_delay_ms(&lease.event.event_id, lease.attempt_count);
                if !self
                    .outbox
                    .fail(lease, &error.to_string(), retry_delay, max_attempts)
                    .await?
                {
                    self.outbox_relay_metrics.record(
                        &lease.producer,
                        &lease.publisher_id,
                        "lease_lost",
                        0.0,
                    );
                    return Ok(DurableQueueProcessOutcome::LeaseLost);
                }
                self.outbox_relay_metrics.record(
                    &lease.producer,
                    &lease.publisher_id,
                    if dead_lettered { "dead" } else { "retry" },
                    0.0,
                );
                if dead_lettered {
                    Ok(DurableQueueProcessOutcome::DeadLettered)
                } else {
                    Ok(DurableQueueProcessOutcome::RetryScheduled)
                }
            }
        }
    }
}
