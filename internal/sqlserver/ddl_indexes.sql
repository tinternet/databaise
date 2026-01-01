SELECT
  'CREATE ' +
  CASE WHEN i.is_unique = 1 THEN 'UNIQUE ' ELSE '' END +
  i.type_desc COLLATE SQL_Latin1_General_CP1_CI_AS + ' INDEX ' + QUOTENAME(i.name) +
  ' ON ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) +
  ' (' + STRING_AGG(QUOTENAME(c.name), ', ') + ');'
FROM sys.indexes i
JOIN sys.tables t ON t.object_id = i.object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE i.is_primary_key = 0
  AND i.is_unique_constraint = 0
  AND t.name = ?
  AND s.name = ISNULL(NULLIF(?, ''), s.name)
GROUP BY i.name, i.is_unique, i.type_desc, s.name, t.name;
