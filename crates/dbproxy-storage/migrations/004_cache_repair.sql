CREATE TABLE IF NOT EXISTS dbproxy_cache_repairs (
    namespace TEXT NOT NULL,
    record_key TEXT NOT NULL,
    target_revision BIGINT NOT NULL CHECK (target_revision >= 0),
    requested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    attempt_count BIGINT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    lease_owner TEXT NULL,
    lease_until TIMESTAMPTZ NULL,
    last_error TEXT NULL,
    dead_lettered_at TIMESTAMPTZ NULL,
    PRIMARY KEY (namespace, record_key)
);

CREATE INDEX IF NOT EXISTS dbproxy_cache_repairs_ready
    ON dbproxy_cache_repairs (available_at, requested_at)
    WHERE dead_lettered_at IS NULL;

CREATE INDEX IF NOT EXISTS dbproxy_cache_repairs_dead_lettered
    ON dbproxy_cache_repairs (dead_lettered_at)
    WHERE dead_lettered_at IS NOT NULL;
