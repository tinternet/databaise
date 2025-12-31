SELECT 
    CONCAT(
        'CREATE ',
        IF(non_unique = 0, 'UNIQUE ', 'NONUNIQUE'),
        'INDEX ',
        index_name,
        ' ON ',
        table_name,
        ' (',
        GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ', '),
        ')'
    ) AS Definition,
    index_name AS Name
FROM information_schema.statistics
WHERE table_name = ?
    AND table_schema = DATABASE()
GROUP BY index_name, non_unique, index_type, table_name
ORDER BY 
    IF(index_name = 'PRIMARY', 0, 1),
    index_name;
