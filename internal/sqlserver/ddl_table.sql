SELECT
  'CREATE TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + ' (' + CHAR(10) +
  STRING_AGG(
    '  ' + QUOTENAME(c.name) + ' ' +
    TYPE_NAME(c.user_type_id) +
    CASE
      WHEN TYPE_NAME(c.user_type_id) IN ('varchar','char','varbinary')
        THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS varchar) END + ')'
      WHEN TYPE_NAME(c.user_type_id) IN ('nvarchar','nchar')
        THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length / 2 AS varchar) END + ')'
      WHEN TYPE_NAME(c.user_type_id) IN ('decimal','numeric')
        THEN '(' + CAST(c.precision AS varchar) + ',' + CAST(c.scale AS varchar) + ')'
      ELSE ''
    END +
    CASE WHEN c.is_identity = 1
      THEN ' IDENTITY(' + CAST(ic.seed_value AS varchar) + ',' + CAST(ic.increment_value AS varchar) + ')'
      ELSE ''
    END +
    CASE WHEN c.is_nullable = 0 THEN ' NOT NULL' ELSE ' NULL' END +
    CASE WHEN dc.definition IS NOT NULL THEN ' DEFAULT ' + dc.definition ELSE '' END,
    ',' + CHAR(10)
  ) + CHAR(10) + ');' AS ddl
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
JOIN sys.columns c ON c.object_id = t.object_id
LEFT JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
LEFT JOIN sys.default_constraints dc ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
WHERE t.name = ?
  AND s.name = ISNULL(NULLIF(?, ''), s.name)
GROUP BY s.name, t.name;
