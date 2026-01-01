SELECT table_schema as schema, table_name as name
FROM information_schema.tables
WHERE table_schema = COALESCE(NULLIF($1, ''), 'public') AND table_type = 'BASE TABLE'
ORDER BY table_name
