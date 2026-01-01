SELECT
    pid,
    usename AS username,
    datname AS database_name,
    application_name,
    state,
    wait_event,
    wait_event_type,
    query_start::text,
    EXTRACT(EPOCH FROM (NOW() - query_start)) AS query_duration_sec,
    (SELECT blocking_pid FROM (SELECT pid, unnest(pg_blocking_pids(pid)) AS blocking_pid FROM pg_stat_activity) AS blocks WHERE blocks.pid = a.pid LIMIT 1) AS blocking_pid,
    query AS query_text
FROM pg_stat_activity a
WHERE wait_event IS NOT NULL
  AND state != 'idle'
  AND pid != pg_backend_pid()
ORDER BY query_start ASC
