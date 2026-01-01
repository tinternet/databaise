SELECT
	column_name as name,
	data_type as database_type,
	is_nullable = 'YES' as is_nullable,
	column_default as default_value
FROM information_schema.columns
WHERE table_schema = COALESCE(NULLIF($1, ''), 'public') AND table_name = $2
ORDER BY ordinal_position
