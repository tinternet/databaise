SELECT TOP 20
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
    ) AS statement_text,
    r.wait_type,
    qs.creation_time,
    qs.last_execution_time,
    qs.total_physical_reads,
    qs.total_logical_reads,
    qs.total_logical_writes,
    qs.execution_count,
    qs.total_worker_time,
    qs.total_elapsed_time,
    qs.total_elapsed_time / qs.execution_count AS avg_elapsed_time
FROM sys.dm_exec_query_stats AS qs
JOIN sys.dm_exec_requests AS r 
    ON qs.sql_handle = r.sql_handle
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
ORDER BY qs.total_elapsed_time / qs.execution_count DESC;