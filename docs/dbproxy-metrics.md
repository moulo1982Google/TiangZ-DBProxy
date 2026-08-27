# DBProxy storage metrics

The `/metrics` endpoint exports bounded Prometheus metrics for the PostgreSQL/Redis path. No
player ID, `RecordKey`, namespace, request ID, or operation ID is used as a label.

`dbproxy_rpc_payload_bytes_total{operation="..."}` records binary request payload and transaction
result bytes by the fixed RPC operation name, making payload amplification visible without exposing
business identifiers.

## Readiness and dependency health

`dbproxy_live` reports process liveness. For the PostgreSQL/Redis backend, `dbproxy_ready` is one
only while the service lifecycle is ready and both required dependencies passed the most recent
storage-metrics sample. `dbproxy_dependency_up{dependency="postgresql"}` and
`dbproxy_dependency_up{dependency="redis"}` expose the individual bounded states. The
`/dependencies` endpoint returns the same state as JSON and uses HTTP 503 while degraded.

Dependency state reuses the five-second durable-queue sampling loop rather than opening extra
probe connections. A transition can therefore take up to roughly one sample interval. A failed
PostgreSQL sample skips the second queue query in that poll, avoiding duplicate reconnect attempts.
Memory backends do not require these dependencies and return `not-configured` from the endpoint.

## Cache and fallback counters

- `dbproxy_cache_hits_total` and `dbproxy_cache_misses_total` count the initial logical cache
  result once per requested record. Hits include fresh, stale, and negative results; the stale and
  negative counters below are subsets. Internal singleflight and lease-lock rechecks are not
  counted again, so the hit ratio remains request-oriented.
- `dbproxy_cache_negative_hits_total` counts requests answered by a cached missing-record marker,
  while `dbproxy_cache_negative_writes_total` counts markers written after PostgreSQL confirms that
  a record does not exist.
- `dbproxy_cache_stale_hits_total` counts snapshots served during the stale-while-revalidate window.
- `dbproxy_cache_refresh_started_total`, `dbproxy_cache_refresh_completed_total`, and
  `dbproxy_cache_refresh_errors_total` describe background refresh attempts for stale entries.
- `dbproxy_cache_read_errors_total` counts Redis command failures and snapshot decode/protocol
  failures.
- `dbproxy_cache_writes_total` counts successful cache writes. `dbproxy_cache_write_errors_total`
  counts failed writes or deletes after a durable operation.
- `dbproxy_postgres_fallbacks_total` counts PostgreSQL fallback attempts after a cache miss.
  `dbproxy_postgres_fallback_errors_total` and
  `dbproxy_postgres_fallback_timeouts_total` split failed and timed-out fallback reads.
- `dbproxy_postgres_fallback_circuit_open_total` counts fallback requests rejected while the
  PostgreSQL circuit breaker is open.
- `dbproxy_cache_fallback_lock_acquired_total` and
  `dbproxy_cache_fallback_lock_contention_total` show cross-instance Redis lease-lock activity.
  `dbproxy_cache_fallback_lock_timeouts_total` counts waits that expired before a lock became
  available, while `dbproxy_cache_fallback_lock_errors_total` and
  `dbproxy_cache_fallback_lock_release_errors_total` identify Redis coordination failures.

The fallback path is bounded by `storage.cacheFallbackConcurrency` and
`storage.cacheFallbackTimeoutMs`; the same timeout also bounds Redis lookup, warmup, delete, and
lock-release operations so a half-open Redis connection cannot hang a request indefinitely. The circuit breaker opens after
`cacheFallbackCircuitFailureThreshold` consecutive PostgreSQL fallback failures, rejects new
fallbacks during `cacheFallbackCircuitCooldownMs`, then permits one half-open probe. A successful
probe closes the circuit; another failure reopens it. A sustained increase in timeout or circuit
open rate means the limit, PostgreSQL capacity, or Redis availability needs attention.

The defaults are five consecutive failures and a 5-second cooldown. The breaker is in-process and
shared by all `TieredSnapshotStore` clones belonging to one configured connection shard.

The distributed lock is configured with `cacheFallbackLockLeaseMs`,
`cacheFallbackLockWaitMs`, and `cacheFallbackLockPollMs`; it defaults to a 3-second lease, waits
up to 1 second, and polls the cache every 25 milliseconds. Lock acquisition is best-effort: if Redis is unavailable or the wait expires,
the request still falls back to PostgreSQL so cache coordination cannot make durable data
unavailable. A batch shares one lock-wait budget rather than multiplying it by record count. A
request that observes another instance's newly populated cache returns without a PostgreSQL read.
Lock release is token-checked and automatic expiry recovers abandoned leases.

## Cache lifecycle

Snapshot-cache lifetime is configured under `storage` when `backend` is `postgresRedis`:

- `cacheTtlMs` defaults to 300000 milliseconds and defines the base fresh period.
- `cacheTtlJitterMs` defaults to 30000 milliseconds. A stable per-`RecordKey` value between zero
  and this limit is added to the fresh period so many records do not expire together.
- `cacheStaleWhileRevalidateMs` defaults to 30000 milliseconds. After freshness expires, the stale
  snapshot can still be returned while DBProxy refreshes it in the background. Set it to zero to
  disable stale serving.
- `cacheNegativeTtlMs` defaults to 5000 milliseconds. PostgreSQL misses are cached for this period
  to absorb repeated reads for nonexistent records. Set it to zero to disable negative caching.

For a positive entry, the Redis hard TTL is `cacheTtlMs + per-record jitter +
cacheStaleWhileRevalidateMs`. Freshness is a separate Redis marker with its own relative TTL, so
classification does not depend on application-host clocks. When the marker expires but the payload
still exists, the entry is stale. Entries written by older DBProxy versions have no freshness
marker; they are treated as stale, served once, and migrated by background refresh instead of
causing an immediate synchronized PostgreSQL miss.

Stale refreshes are deduplicated by `RecordKey` inside each process, bounded by the configured
fallback concurrency, and reuse the Redis lease lock for cross-instance coordination. If all
refresh slots are occupied, the stale value is still returned and a later read retries scheduling.
The request that observed the stale value does not wait for the refresh. A durable write removes any negative marker, and a negative marker is written only after
PostgreSQL confirms absence; revision-aware scripts prevent an absence result from deleting a newer
positive cache entry.

## Durable backlog gauges

- `dbproxy_backlog_pending` is the current number of ordinary snapshots waiting for a worker.
- `dbproxy_backlog_processing` is the number of leased snapshots currently being written.
- `dbproxy_backlog_oldest_pending_age_seconds` is the age of the oldest pending item. It is zero
  when the pending set is empty. These gauges are sampled every five seconds by the DBProxy
  process; a Redis sampling error leaves the last successful value in place, marks the Redis
  dependency down, and is logged.

The local alert rules warn when pending depth exceeds 1000, the oldest item exceeds five minutes,
fallback reads keep timing out, or background refreshes keep failing. Stale hits are intentionally
not alerted on their own: a stale hit can be the expected low-latency behavior during the SWR
window. Interpret stale rate together with refresh errors, PostgreSQL fallbacks, and RPC latency.
Tune thresholds to the game's write rate and recovery objective before production deployment.

## Durable cache-repair queue

Every authoritative PostgreSQL write creates or advances a repair target in the same transaction.
The metrics therefore describe committed data that Redis has not yet confirmed, rather than an
in-memory best-effort task:

- `dbproxy_cache_repair_pending`, `dbproxy_cache_repair_processing`, and
  `dbproxy_cache_repair_dead_lettered` are the current queue states.
- `dbproxy_cache_repair_oldest_age_seconds` is the age of the oldest non-dead repair target.
- `dbproxy_cache_repair_worker_polls_total{result="..."}` uses the fixed result set
  `committed`, `retry_scheduled`, `dead_lettered`, `lease_lost`, `empty`, and `failure`.

A cache write error can be transient without affecting PostgreSQL durability. A growing oldest
age or any dead letter is the actionable signal. The local rules warn above 60 seconds for five
minutes and alert critically on any dead letter.

## PostgreSQL outbox

- `dbproxy_outbox_pending`, `dbproxy_outbox_processing`, and
  `dbproxy_outbox_dead_lettered` expose current event states.
- `dbproxy_outbox_oldest_age_seconds` is the age of the oldest unpublished, non-dead event.
- `dbproxy_outbox_worker_polls_total{result="..."}` has the same bounded result set as cache
  repair.

Publication is at least once. `committed` means the Redis Stream append received a local AOF ACK
and PostgreSQL recorded `published_at`; an ACK failure after publication can still produce a
duplicate event on retry. Consumers deduplicate by event ID. No metric label contains event,
operation, trade, account, or partition identifiers.
