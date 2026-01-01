SELECT
    t.processlist_id AS thread_id,
    t.processlist_user AS username,
    t.processlist_db AS database_name,
    t.processlist_command AS command,
    t.processlist_state AS state,
    ew.event_name AS wait_event,
    'Lock' AS wait_event_type,
    t.processlist_time AS time_seconds,
    NULL AS blocking_thread_id,
    t.processlist_info AS query_text
FROM performance_schema.threads t
LEFT JOIN performance_schema.events_waits_current ew
    ON t.thread_id = ew.thread_id
WHERE t.processlist_state IS NOT NULL
  AND t.processlist_state != ''
  AND t.type = 'FOREGROUND'
  AND t.processlist_command != 'Sleep'
  AND ew.event_name IS NOT NULL
ORDER BY t.processlist_time DESC
