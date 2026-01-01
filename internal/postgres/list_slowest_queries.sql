SELECT
    queryid::text AS query_hash,
    calls,
    ROUND(total_exec_time::numeric / 1000, 3) AS total_time_sec,
    ROUND((total_exec_time / calls)::numeric / 1000, 3) AS avg_time_sec,
    ROUND(min_exec_time::numeric / 1000, 3) AS min_time_sec,
    ROUND(max_exec_time::numeric / 1000, 3) AS max_time_sec,
    shared_blks_hit AS shared_blocks_hit,
    shared_blks_read AS shared_blocks_read,
    shared_blks_written AS shared_blocks_written,
    query AS query_text
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 20
