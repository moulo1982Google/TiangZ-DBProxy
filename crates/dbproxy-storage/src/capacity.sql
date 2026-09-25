-- Read catalog estimates and physical storage; never count or load business rows.
WITH RECURSIVE roots AS (
    SELECT names.name, c.oid, c.relkind::text AS root_kind
    FROM pg_catalog.unnest($2::text[]) AS names(name)
    LEFT JOIN pg_catalog.pg_namespace n ON n.nspname = $1
    LEFT JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = names.name
), tree(name, oid) AS (
    SELECT name, oid FROM roots WHERE root_kind IN ('r', 'p')
    UNION
    SELECT tree.name, i.inhrelid
    FROM tree JOIN pg_catalog.pg_inherits i ON i.inhparent = tree.oid
)
SELECT roots.name, roots.root_kind, c.relkind::text AS kind,
       c.reltuples::double precision AS estimated_rows,
       CASE WHEN c.relkind = 'r' THEN pg_catalog.pg_table_size(c.oid) END AS table_bytes,
       CASE WHEN c.relkind = 'r' THEN pg_catalog.pg_indexes_size(c.oid) END AS index_bytes
FROM roots
LEFT JOIN tree ON tree.name = roots.name
LEFT JOIN pg_catalog.pg_class c ON c.oid = tree.oid
ORDER BY roots.name, tree.oid
LIMIT 10001
