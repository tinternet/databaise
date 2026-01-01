SELECT
    schemaname AS schema,
    relname AS table_name,
    seq_scan AS seq_scans,
    seq_tup_read AS seq_tuples_read,
    COALESCE(idx_scan, 0) AS index_scans,
    ROUND((pg_total_relation_size(schemaname||'.'||relname)::numeric / 1024 / 1024)::numeric, 2) AS table_size_mb,
    ROUND((seq_scan::numeric * seq_tup_read::numeric / NULLIF(seq_scan::numeric + COALESCE(idx_scan, 0)::numeric, 0))::numeric, 2) AS estimated_impact,
    'Consider adding indexes on frequently filtered columns' AS create_suggestion
FROM pg_stat_user_tables
WHERE seq_scan > 0
  AND schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY seq_scan * seq_tup_read DESC
LIMIT 25
