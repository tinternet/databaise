SELECT
  'ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + '.' +
  QUOTENAME(OBJECT_NAME(parent_object_id)) +
  ' ADD CONSTRAINT ' + QUOTENAME(name) +
  ' ' + definition COLLATE Latin1_General_CI_AS_KS_WS + ';'
FROM sys.check_constraints
WHERE parent_object_id = OBJECT_ID(?)

UNION ALL

SELECT
  'ALTER TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) +
  ' ADD CONSTRAINT ' + QUOTENAME(k.name) +
  ' ' + k.type_desc COLLATE Latin1_General_CI_AS_KS_WS +
  ' (' +
  STRING_AGG(QUOTENAME(c.name), ', ') +
  ');'
FROM sys.key_constraints k
JOIN sys.tables t ON t.object_id = k.parent_object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
JOIN sys.index_columns ic ON ic.object_id = t.object_id AND ic.index_id = k.unique_index_id
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE t.name = ?
  AND s.name = ?
GROUP BY s.name, t.name, k.name, k.type_desc;
