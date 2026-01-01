SELECT
  'ALTER TABLE ' || conrelid::regclass ||
  ' ADD CONSTRAINT ' || quote_ident(conname) ||
  ' ' || pg_get_constraintdef(oid) || ';'
FROM pg_constraint
WHERE conrelid = ?::regclass;
