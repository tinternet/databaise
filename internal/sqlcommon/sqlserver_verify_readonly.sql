SELECT
    CASE
        WHEN (
            -- 1. High-Level Server/Role Check
            -- ISNULL defaults to 1 (has role) if check fails, erring on the side of caution
            ISNULL(IS_SRVROLEMEMBER('sysadmin'), 1) = 1
            OR ISNULL(IS_SRVROLEMEMBER('serveradmin'), 1) = 1
            OR ISNULL(IS_SRVROLEMEMBER('dbcreator'), 1) = 1

            -- 2. Dangerous Database Roles
            OR EXISTS (SELECT 1 FROM sys.database_role_members drm
                       JOIN sys.database_principals dp ON drm.role_principal_id = dp.principal_id
                       WHERE drm.member_principal_id = DATABASE_PRINCIPAL_ID()
                         AND dp.name IN ('db_owner', 'db_datawriter', 'db_ddladmin', 'db_accessadmin'))

            -- 3. Database-level Write/Alter/Execute
            OR EXISTS (SELECT 1 FROM fn_my_permissions(NULL, 'DATABASE')
                       WHERE permission_name IN ('INSERT', 'UPDATE', 'DELETE', 'ALTER', 'CONTROL', 'EXECUTE'))

            -- 4. Schema-level Write/Alter/Execute (excluding fixed database role schemas)
            -- Fixed role schemas (db_datareader, etc.) grant permissions on themselves but not on user data
            OR EXISTS (SELECT 1 FROM sys.schemas s
                       CROSS APPLY fn_my_permissions(QUOTENAME(s.name), 'SCHEMA') p
                       WHERE s.name NOT IN ('sys', 'information_schema',
                             'db_owner', 'db_accessadmin', 'db_securityadmin', 'db_ddladmin',
                             'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_denydatareader', 'db_denydatawriter')
                         AND p.permission_name IN ('INSERT', 'UPDATE', 'DELETE', 'ALTER', 'CONTROL', 'EXECUTE'))

            -- 5. Ownership (If they own it, they can change it)
            OR EXISTS (SELECT 1 FROM sys.objects WHERE principal_id = DATABASE_PRINCIPAL_ID() AND is_ms_shipped = 0)
            OR EXISTS (SELECT 1 FROM sys.schemas WHERE principal_id = DATABASE_PRINCIPAL_ID())

        ) THEN 0 -- NOT read-only
        ELSE 1    -- IS read-only
    END AS is_readonly;
