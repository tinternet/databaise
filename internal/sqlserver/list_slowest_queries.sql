SELECT TOP 25
    SUBSTRING(
        st.text,
        (qs.statement_start_offset / 2) + 1,
        (
            (
                CASE
                    WHEN qs.statement_end_offset = -1
                        THEN DATALENGTH(st.text)
                    ELSE qs.statement_end_offset
                END
                - qs.statement_start_offset
            ) / 2
        ) + 1
    ) AS query,
    qs.execution_count AS calls,
    ROUND(qs.total_elapsed_time / 1000000.0, 3) AS total_time_sec,
    ROUND((qs.total_elapsed_time / qs.execution_count) / 1000000.0, 3) AS avg_time_sec,
    ROUND(qs.min_elapsed_time / 1000000.0, 3) AS min_time_sec,
    ROUND(qs.max_elapsed_time / 1000000.0, 3) AS max_time_sec,
    ROUND(qs.total_worker_time / 1000000.0, 3) AS total_cpu_time_sec,
    ROUND((qs.total_worker_time / qs.execution_count) / 1000000.0, 3) AS avg_cpu_time_sec,
    qs.total_logical_reads,
    qs.total_logical_writes,
    qs.total_physical_reads,
    ROUND((qs.total_logical_reads / qs.execution_count), 0) AS avg_logical_reads,
    qs.total_rows AS total_rows_returned,
    ROUND((qs.total_rows * 1.0 / qs.execution_count), 0) AS avg_rows_returned,
    qs.total_grant_kb AS total_memory_grant_kb,
    qs.creation_time,
    qs.last_execution_time
FROM sys.dm_exec_query_stats AS qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
WHERE qs.execution_count > 0
ORDER BY qs.total_elapsed_time DESC;
