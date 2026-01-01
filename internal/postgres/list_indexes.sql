SELECT
	i.relname as name,
	pg_get_indexdef(i.oid) as definition
FROM pg_index x
JOIN pg_class i ON i.oid = x.indexrelid
JOIN pg_class t ON t.oid = x.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = COALESCE(NULLIF($1, ''), 'public') AND t.relname = $2
