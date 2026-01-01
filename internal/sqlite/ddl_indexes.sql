SELECT sql
FROM sqlite_master
WHERE type = 'index'
  AND tbl_name = ?
  AND sql IS NOT NULL
