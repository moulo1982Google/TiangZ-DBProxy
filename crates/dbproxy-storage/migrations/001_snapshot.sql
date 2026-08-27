CREATE TABLE IF NOT EXISTS dbproxy_snapshots (
    namespace TEXT NOT NULL,
    record_key TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    schema_version BIGINT NOT NULL,
    revision BIGINT NOT NULL,
    payload BYTEA NOT NULL,
    updated_at_unix_ms BIGINT NOT NULL,
    PRIMARY KEY (namespace, record_key)
) PARTITION BY HASH (namespace, record_key);

-- DBProxy is still in development, so partition support starts with a fresh schema instead of
-- attempting an implicit online rewrite of an existing heap table. Failing here is safer than
-- silently running without partitioning when CREATE TABLE IF NOT EXISTS encounters an old table.
DO $$
DECLARE
    snapshot_relation_kind "char";
BEGIN
    SELECT relkind
    INTO snapshot_relation_kind
    FROM pg_class
    WHERE oid = to_regclass('dbproxy_snapshots');

    IF snapshot_relation_kind IS DISTINCT FROM 'p'::"char" THEN
        RAISE EXCEPTION
            'dbproxy_snapshots exists but is not partitioned; reset the development database before starting this DBProxy version';
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p00 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 0);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p01 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 1);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p02 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 2);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p03 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 3);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p04 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 4);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p05 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 5);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p06 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 6);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p07 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 7);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p08 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 8);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p09 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 9);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p10 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 10);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p11 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 11);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p12 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 12);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p13 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 13);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p14 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 14);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p15 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 15);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p16 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 16);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p17 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 17);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p18 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 18);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p19 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 19);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p20 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 20);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p21 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 21);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p22 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 22);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p23 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 23);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p24 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 24);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p25 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 25);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p26 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 26);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p27 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 27);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p28 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 28);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p29 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 29);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p30 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 30);
CREATE TABLE IF NOT EXISTS dbproxy_snapshots_p31 PARTITION OF dbproxy_snapshots
    FOR VALUES WITH (MODULUS 32, REMAINDER 31);

CREATE TABLE IF NOT EXISTS dbproxy_idempotency (
    request_id TEXT PRIMARY KEY,
    namespace TEXT NOT NULL,
    record_key TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    schema_version BIGINT NOT NULL,
    payload BYTEA NOT NULL,
    expected_revision BIGINT NULL,
    revision BIGINT NOT NULL,
    updated_at_unix_ms BIGINT NULL
);
