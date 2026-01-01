SELECT
    datname AS database_name,
    deadlocks AS deadlock_count,
    stats_reset::text AS last_stats_reset
FROM pg_stat_database
WHERE datname IS NOT NULL
  AND deadlocks > 0
ORDER BY deadlocks DESC
