SELECT NOT (
    -- 1. Superuser check
    (SELECT rolsuper FROM pg_roles WHERE rolname = current_user)
    OR
    -- 2. Database creation check
    has_database_privilege(current_user, current_database(), 'CREATE')
    OR
    -- 3. Schema creation check
    EXISTS (
        SELECT 1 FROM pg_namespace
        WHERE nspname NOT LIKE 'pg_%'
          AND nspname != 'information_schema'
          AND has_schema_privilege(current_user, nspname, 'CREATE')
    )
    OR
    -- 4. Table/View DML check
    EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname NOT LIKE 'pg_%'
          AND n.nspname != 'information_schema'
          AND c.relkind IN ('r', 'v', 'm', 'p', 'f')
          AND (
              has_table_privilege(current_user, c.oid, 'INSERT') OR
              has_table_privilege(current_user, c.oid, 'UPDATE') OR
              has_table_privilege(current_user, c.oid, 'DELETE') OR
              has_table_privilege(current_user, c.oid, 'TRUNCATE')
          )
    )
    OR
    -- 5. Sequence modification check
    EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'S'
          AND n.nspname NOT LIKE 'pg_%'
          AND has_sequence_privilege(current_user, c.oid, 'UPDATE')
    )
    OR
    -- 6. Ownership check (direct & indirect via group membership)
    EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname NOT LIKE 'pg_%'
          AND n.nspname != 'information_schema'
          AND pg_has_role(current_user, c.relowner, 'MEMBER')
    )
    OR EXISTS (
        SELECT 1 FROM pg_namespace n
        WHERE n.nspname NOT LIKE 'pg_%'
          AND n.nspname != 'information_schema'
          AND pg_has_role(current_user, n.nspowner, 'MEMBER')
    )
) AS is_readonly
