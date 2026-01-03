SELECT
    SUBSTRING(digest_text, 1, 500) AS query,
    schema_name,
    count_star AS calls,
    ROUND(sum_timer_wait / 1000000000000, 3) AS total_time_sec,
    ROUND(avg_timer_wait / 1000000000000, 3) AS avg_time_sec,
    ROUND(max_timer_wait / 1000000000000, 3) AS max_time_sec,
    ROUND(sum_lock_time / 1000000000000, 3) AS lock_time_sec,
    sum_rows_examined AS rows_examined,
    sum_rows_sent AS rows_sent,
    sum_rows_affected AS rows_affected,
    sum_no_index_used AS no_index_used,
    sum_no_good_index_used AS no_good_index_used,
    sum_select_full_join AS full_join,
    sum_select_scan AS full_scan,
    sum_created_tmp_tables AS tmp_tables,
    sum_created_tmp_disk_tables AS tmp_disk_tables,
    sum_errors AS errors,
    sum_warnings AS warnings,
    first_seen,
    last_seen
FROM performance_schema.events_statements_summary_by_digest
WHERE schema_name IS NOT NULL
  AND schema_name NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
ORDER BY sum_timer_wait DESC
LIMIT 25
