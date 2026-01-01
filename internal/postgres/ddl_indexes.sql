SELECT pg_get_indexdef(indexrelid) || ';'
FROM pg_index
WHERE indrelid = ?::regclass;
