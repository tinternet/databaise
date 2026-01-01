SELECT
    SUBSTRING(digest_text, 1, 500) AS digest_text,
    schema_name,
    count_star,
    ROUND(sum_timer_wait / 1000000000000, 3) AS total_time_sec,
    ROUND(avg_timer_wait / 1000000000000, 3) AS avg_time_sec,
    ROUND(min_timer_wait / 1000000000000, 3) AS min_time_sec,
    ROUND(max_timer_wait / 1000000000000, 3) AS max_time_sec,
    sum_rows_examined AS rows_examined,
    sum_rows_sent AS rows_sent,
    sum_rows_affected AS rows_affected
FROM performance_schema.events_statements_summary_by_digest
WHERE schema_name IS NOT NULL
  AND schema_name NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
ORDER BY sum_timer_wait DESC
LIMIT 20
