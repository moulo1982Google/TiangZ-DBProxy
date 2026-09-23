-- Time columns are keys so eligibility filters before heap access.
CREATE INDEX dbproxy_cache_repairs_unleased_order
ON dbproxy_cache_repairs (requested_at, namespace, record_key, available_at)
WHERE dead_lettered_at IS NULL AND lease_until IS NULL;

CREATE INDEX dbproxy_cache_repairs_expired
ON dbproxy_cache_repairs (lease_until, requested_at)
WHERE dead_lettered_at IS NULL AND lease_until IS NOT NULL;

CREATE INDEX dbproxy_cache_repairs_leased_order
ON dbproxy_cache_repairs (requested_at, namespace, record_key, lease_until, available_at)
WHERE dead_lettered_at IS NULL AND lease_until IS NOT NULL;
