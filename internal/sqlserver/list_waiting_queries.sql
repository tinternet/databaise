SELECT
    r.start_time AS [start_time],
    --session_ID AS [SPID],
    --DB_NAME(database_id) AS [Database],
    SUBSTRING(
        t.text,
        (r.statement_start_offset / 2) + 1,
        CASE
            WHEN r.statement_end_offset = -1 OR r.statement_end_offset = 0
                THEN (DATALENGTH(t.text) - r.statement_start_offset / 2) + 1
            ELSE (r.statement_end_offset - r.statement_start_offset) / 2 + 1
        END
    ) AS [query_text],
    r.status,
    r.command,
    r.wait_type,
    r.wait_time,
    r.wait_resource,
    r.last_wait_type
FROM sys.dm_exec_requests AS r
OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) AS t
WHERE r.session_id != @@SPID       -- don't show this query
  AND r.session_id > 50            -- don't show system queries
  AND r.wait_type IS NOT NULL
ORDER BY r.start_time ASC;