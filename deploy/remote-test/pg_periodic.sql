-- One JSON line of PostgreSQL state for long runs; executed every PG_SAMPLE_SECONDS against the
-- round database. Cheap on purpose: statistics views and sizes, plus one retention-index range
-- count for the expired-receipt backlog; no full-table counts. Needs psql -v retention_hours=N.
SELECT json_build_object(
  'at', clock_timestamp(),
  'db_bytes', pg_database_size(current_database()),
  'idempotency', (
    SELECT json_build_object(
      'total_bytes', pg_total_relation_size(s.relid), 'index_bytes', pg_indexes_size(s.relid),
      'live', s.n_live_tup, 'dead', s.n_dead_tup, 'ins', s.n_tup_ins, 'del', s.n_tup_del,
      'autovacuum_count', s.autovacuum_count, 'autoanalyze_count', s.autoanalyze_count,
      'last_autovacuum', s.last_autovacuum)
    FROM pg_stat_user_tables s WHERE s.relname = 'dbproxy_idempotency'),
  'snapshots', (
    SELECT json_build_object(
      'total_bytes', sum(pg_total_relation_size(s.relid)), 'index_bytes', sum(pg_indexes_size(s.relid)),
      'live', sum(s.n_live_tup), 'dead', sum(s.n_dead_tup), 'ins', sum(s.n_tup_ins),
      'autovacuum_count', sum(s.autovacuum_count))
    FROM pg_stat_user_tables s WHERE s.relname LIKE 'dbproxy\_snapshots\_p%'),
  'cache_repairs', (
    SELECT json_build_object('total_bytes', pg_total_relation_size(s.relid), 'live', s.n_live_tup, 'dead', s.n_dead_tup)
    FROM pg_stat_user_tables s WHERE s.relname = 'dbproxy_cache_repairs'),
  'expired_receipts_pending', (
    SELECT count(*) FROM dbproxy_idempotency WHERE recorded_at < statement_timestamp() - make_interval(hours => :retention_hours)),
  'connections', (
    SELECT json_object_agg(k, c) FROM (
      SELECT coalesce(nullif(application_name, ''), backend_type) || ':' || coalesce(state, '-') AS k, count(*) AS c
      FROM pg_stat_activity WHERE datname = current_database() GROUP BY 1) x),
  'xact', (
    SELECT json_build_object('commit', xact_commit, 'rollback', xact_rollback, 'deadlocks', deadlocks,
                             'temp_bytes', temp_bytes, 'blks_read', blks_read, 'blks_hit', blks_hit)
    FROM pg_stat_database WHERE datname = current_database()),
  'wal', (SELECT row_to_json(w) FROM pg_stat_wal w),
  'wal_io', (
    SELECT json_agg(json_build_object('backend', backend_type, 'context', context, 'writes', writes,
      'write_time', write_time, 'fsyncs', fsyncs, 'fsync_time', fsync_time))
    FROM pg_stat_io WHERE object = 'wal' AND (writes > 0 OR fsyncs > 0)),
  'checkpointer', (SELECT row_to_json(c) FROM pg_stat_checkpointer c),
  'wal_segments', (SELECT count(*) FROM pg_ls_waldir()));
