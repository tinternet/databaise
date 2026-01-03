SELECT
    queryid::text AS query_hash,
    calls,
    ROUND(total_exec_time::numeric / 1000, 3) AS total_time_sec,
    ROUND((total_exec_time / NULLIF(calls, 0))::numeric / 1000, 3) AS avg_time_sec,
    ROUND(min_exec_time::numeric / 1000, 3) AS min_time_sec,
    ROUND(max_exec_time::numeric / 1000, 3) AS max_time_sec,
    ROUND(mean_exec_time::numeric / 1000, 3) AS mean_time_sec,
    ROUND(stddev_exec_time::numeric / 1000, 3) AS stddev_time_sec,
    rows AS total_rows_returned,
    shared_blks_hit,
    shared_blks_read,
    shared_blks_dirtied,
    shared_blks_written,
    temp_blks_read,
    temp_blks_written,
    CASE WHEN shared_blks_hit + shared_blks_read > 0
         THEN ROUND((shared_blks_hit::numeric / (shared_blks_hit + shared_blks_read) * 100)::numeric, 2)
         ELSE 100
    END AS cache_hit_pct,
    query
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 25
