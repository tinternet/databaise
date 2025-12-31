SELECT
    i.name AS name,
    CAST(
        N'CREATE ' +
        CASE WHEN i.is_unique = 1 THEN N'UNIQUE ' ELSE N'' END +
        i.type_desc COLLATE DATABASE_DEFAULT +
        N' INDEX ' + QUOTENAME(i.name) COLLATE DATABASE_DEFAULT +
        N' ON ' +
        QUOTENAME(s.name) COLLATE DATABASE_DEFAULT + N'.' +
        QUOTENAME(t.name) COLLATE DATABASE_DEFAULT +
        N' (' +
            STUFF((
                SELECT N', ' + QUOTENAME(c.name) COLLATE DATABASE_DEFAULT
                FROM sys.index_columns ic
                JOIN sys.columns c
                  ON c.object_id = ic.object_id
                 AND c.column_id = ic.column_id
                WHERE ic.object_id = i.object_id
                  AND ic.index_id  = i.index_id
                  AND ic.key_ordinal > 0
                ORDER BY ic.key_ordinal
                FOR XML PATH(''), TYPE
            ).value('.', 'nvarchar(max)'), 1, 2, N'') +
        N')' +
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM sys.index_columns ic
                WHERE ic.object_id = i.object_id
                  AND ic.index_id  = i.index_id
                  AND ic.is_included_column = 1
            )
            THEN
                N' INCLUDE (' +
                STUFF((
                    SELECT N', ' + QUOTENAME(c.name) COLLATE DATABASE_DEFAULT
                    FROM sys.index_columns ic
                    JOIN sys.columns c
                      ON c.object_id = ic.object_id
                     AND c.column_id = ic.column_id
                    WHERE ic.object_id = i.object_id
                      AND ic.index_id  = i.index_id
                      AND ic.is_included_column = 1
                    ORDER BY c.column_id
                    FOR XML PATH(''), TYPE
                ).value('.', 'nvarchar(max)'), 1, 2, N'') +
                N')'
            ELSE N''
        END
    AS nvarchar(max)) AS definition
FROM sys.indexes i
JOIN sys.tables  t ON t.object_id = i.object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE i.is_hypothetical = 0
  AND i.index_id > 0
  AND s.name = CASE @schema WHEN '' THEN s.name ELSE @schema END
  AND t.name = @table;