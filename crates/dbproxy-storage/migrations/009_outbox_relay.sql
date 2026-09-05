CREATE TABLE dbproxy_outbox_publishers (
    publisher_id TEXT PRIMARY KEY,
    backend TEXT NOT NULL,
    endpoint_fingerprint TEXT NOT NULL
);
CREATE TABLE dbproxy_outbox_routes (
    route_key TEXT PRIMARY KEY,
    producer TEXT NOT NULL,
    route_version BIGINT NOT NULL,
    publisher_id TEXT NOT NULL REFERENCES dbproxy_outbox_publishers(publisher_id),
    backend TEXT NOT NULL,
    destination TEXT NOT NULL,
    UNIQUE(producer, route_version)
);
CREATE TRIGGER dbproxy_publishers_immutable BEFORE UPDATE OR DELETE ON dbproxy_outbox_publishers
    FOR EACH ROW EXECUTE FUNCTION dbproxy_reject_append_mutation();
CREATE TRIGGER dbproxy_routes_immutable BEFORE UPDATE OR DELETE ON dbproxy_outbox_routes
    FOR EACH ROW EXECUTE FUNCTION dbproxy_reject_append_mutation();
CREATE TRIGGER dbproxy_publishers_no_truncate BEFORE TRUNCATE ON dbproxy_outbox_publishers
    FOR EACH STATEMENT EXECUTE FUNCTION dbproxy_reject_append_mutation();
CREATE TRIGGER dbproxy_routes_no_truncate BEFORE TRUNCATE ON dbproxy_outbox_routes
    FOR EACH STATEMENT EXECUTE FUNCTION dbproxy_reject_append_mutation();

ALTER TABLE dbproxy_outbox ADD COLUMN producer TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE dbproxy_outbox ADD COLUMN publisher_id TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE dbproxy_outbox ADD COLUMN backend TEXT NOT NULL DEFAULT 'redisStream';
ALTER TABLE dbproxy_outbox ADD COLUMN destination TEXT;
ALTER TABLE dbproxy_outbox ADD COLUMN enqueue_order BIGSERIAL;
ALTER TABLE dbproxy_outbox ADD COLUMN lease_token BIGINT NOT NULL DEFAULT 0;
ALTER TABLE dbproxy_outbox ADD COLUMN expired_leases BIGINT NOT NULL DEFAULT 0;
WITH ordered AS (SELECT event_id, row_number() OVER (ORDER BY created_at,event_id) AS ordinal FROM dbproxy_outbox)
UPDATE dbproxy_outbox e SET enqueue_order=o.ordinal FROM ordered o WHERE e.event_id=o.event_id;
SELECT setval(pg_get_serial_sequence('dbproxy_outbox','enqueue_order'), COALESCE(MAX(enqueue_order),0)+1, false) FROM dbproxy_outbox;
UPDATE dbproxy_outbox SET destination = 'dbproxy:outbox:' || topic;
ALTER TABLE dbproxy_outbox ALTER COLUMN destination SET NOT NULL;

-- Old writers keep their exact stream address. Reserved relay topics require a registered route.
CREATE FUNCTION dbproxy_resolve_outbox_route() RETURNS TRIGGER AS $$
DECLARE route dbproxy_outbox_routes%ROWTYPE;
BEGIN
    IF NEW.topic LIKE 'dbproxy.relay.v1.%' THEN
        SELECT * INTO route FROM dbproxy_outbox_routes WHERE route_key = NEW.topic;
        IF NOT FOUND THEN RAISE EXCEPTION 'outbox route is not registered'; END IF;
        NEW.producer := route.producer;
        NEW.publisher_id := route.publisher_id;
        NEW.backend := route.backend;
        NEW.destination := route.destination;
    ELSE
        NEW.producer := 'legacy'; NEW.publisher_id := 'legacy'; NEW.backend := 'redisStream';
        NEW.destination := 'dbproxy:outbox:' || NEW.topic;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER dbproxy_outbox_route BEFORE INSERT ON dbproxy_outbox
    FOR EACH ROW EXECUTE FUNCTION dbproxy_resolve_outbox_route();
CREATE FUNCTION dbproxy_reject_outbox_route_mutation() RETURNS TRIGGER AS $$
BEGIN
    IF (OLD.producer, OLD.publisher_id, OLD.backend, OLD.destination, OLD.enqueue_order)
        IS DISTINCT FROM (NEW.producer, NEW.publisher_id, NEW.backend, NEW.destination, NEW.enqueue_order)
    THEN RAISE EXCEPTION 'outbox delivery route is immutable'; END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER dbproxy_outbox_route_immutable BEFORE UPDATE ON dbproxy_outbox
    FOR EACH ROW EXECUTE FUNCTION dbproxy_reject_outbox_route_mutation();
CREATE INDEX dbproxy_outbox_order ON dbproxy_outbox(publisher_id, destination, partition_key, enqueue_order)
    WHERE published_at IS NULL;

CREATE TABLE dbproxy_outbox_admin_audit (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    operator_name TEXT NOT NULL,
    database_user TEXT NOT NULL DEFAULT current_user,
    reason TEXT NOT NULL,
    prior_attempts BIGINT NOT NULL,
    prior_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER dbproxy_outbox_admin_immutable BEFORE UPDATE OR DELETE ON dbproxy_outbox_admin_audit
    FOR EACH ROW EXECUTE FUNCTION dbproxy_reject_append_mutation();
CREATE TRIGGER dbproxy_outbox_admin_no_truncate BEFORE TRUNCATE ON dbproxy_outbox_admin_audit
    FOR EACH STATEMENT EXECUTE FUNCTION dbproxy_reject_append_mutation();
