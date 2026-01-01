SELECT
  'COMMENT ON COLUMN ' || attrelid::regclass || '.' || quote_ident(attname) ||
  ' IS ' || quote_literal(description) || ';'
FROM pg_description d
JOIN pg_attribute a
  ON a.attrelid = d.objoid AND a.attnum = d.objsubid
WHERE d.objoid = ?::regclass;
