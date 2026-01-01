SELECT
    t.table_schema,
    t.table_name,
    COALESCE(SUM(tio.count_read), 0) AS full_table_scans,
    COALESCE(SUM(tio.count_read), 0) AS rows_read,
    COALESCE(SUM(tio.count_write), 0) AS rows_changed,
    ROUND((t.data_length + t.index_length) / 1024 / 1024, 2) AS table_size_mb,
    ROUND(COALESCE(SUM(tio.count_read), 0) * ((t.data_length + t.index_length) / 1024 / 1024), 2) AS estimated_impact,
    'Consider adding indexes on frequently filtered columns' AS create_suggestion
FROM information_schema.tables t
LEFT JOIN performance_schema.table_io_waits_summary_by_index_usage tio
    ON t.table_schema = tio.object_schema
    AND t.table_name = tio.object_name
    AND tio.index_name IS NULL
WHERE t.table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
  AND t.table_type = 'BASE TABLE'
GROUP BY t.table_schema, t.table_name, t.data_length, t.index_length
HAVING full_table_scans > 0
ORDER BY estimated_impact DESC
LIMIT 25
