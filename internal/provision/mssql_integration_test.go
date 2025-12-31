//go:build integration

package provision

import (
	"log"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tinternet/databaise/internal/sqltest"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func setupSqlServerDatabase(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := sqltest.SetupSqlServerContainer(t)
	provisioner := SqlServerProvisioner{}

	password, err := GeneratePassword()
	require.NoError(t, err)
	require.NoError(t, provisioner.Connect(dsn))
	require.NoError(t, provisioner.CreateUser(t.Context(), "testuser", password))
	require.NoError(t, provisioner.GrantReadOnly(t.Context(), "testuser", AccessScope{
		Groups:    []string{"dbo"},
		Resources: []string{},
	}))

	migrateGormDatabase(t, provisioner.db)

	db, err := gorm.Open(sqlserver.Open(sqltest.ReplaceURLCredentials(t, dsn, "testuser", password)), &gorm.Config{
		Logger: logger.New(
			log.New(t.Output(), "", log.LstdFlags),
			logger.Config{LogLevel: logger.Error, Colorful: true},
		),
	})
	require.NoError(t, err)
	return db
}

func TestSqlServer_ReadOnlyAccess(t *testing.T) {
	t.Parallel()
	db := setupSqlServerDatabase(t)
	testReadonlySchemaScope(t, db)
}

func TestSqlServer_BadInputs(t *testing.T) {
	t.Parallel()
	dsn := sqltest.SetupSqlServerContainer(t)
	provisioner := SqlServerProvisioner{}
	testBadInputs(t, &provisioner, dsn)
}

func TestSqlServer_DropUser(t *testing.T) {
	t.Parallel()
	dsn := sqltest.SetupSqlServerContainer(t)
	provisioner := SqlServerProvisioner{}
	testDropUser(t, &provisioner, dsn)
}

func TestSqlServer_BypassAttempts(t *testing.T) {
	t.Parallel()
	db := setupSqlServerDatabase(t)

	assertBlocked := func(t *testing.T, query string, emsg string) {
		t.Helper()
		err := db.Exec(query).Error
		require.Error(t, err, "Action should have been blocked")
		require.ErrorContains(t, err, emsg)
	}

	// Verify that the table we will test against is actually there.
	t.Run("Check table is readable", func(t *testing.T) {
		var count int
		require.NoError(t, db.Raw("SELECT COUNT(*) FROM dbo.test_data").Scan(&count).Error)
		require.Greater(t, count, 0)
	})

	t.Run("EXEC string", func(t *testing.T) {
		assertBlocked(t, "EXEC('INSERT INTO dbo.test_data (id, text) VALUES (1, ''exec_hack'')')", "The INSERT permission was denied")
	})

	t.Run("sp_executesql", func(t *testing.T) {
		assertBlocked(t, "EXEC sp_executesql N'INSERT INTO dbo.test_data (id, text) VALUES (@v, @n)', N'@n NVARCHAR(100), @v INT', @n='sp_exec_hack', @v=1", "The INSERT permission was denied")
	})

	t.Run("MERGE Statement", func(t *testing.T) {
		assertBlocked(t, "MERGE dbo.test_data AS target USING (SELECT 'merge_hack' as text, 5 as id) AS source ON target.id = source.id WHEN NOT MATCHED THEN INSERT (id, text) VALUES (source.id, source.text);", "The INSERT permission was denied")
	})

	t.Run("TRUNCATE", func(t *testing.T) {
		assertBlocked(t, "TRUNCATE TABLE dbo.test_data", "Cannot find the object")
	})

	t.Run("ALTER TABLE", func(t *testing.T) {
		assertBlocked(t, "ALTER TABLE dbo.test_data ADD evil_col NVARCHAR(100)", "Cannot find the object")
	})

	t.Run("CREATE INDEX", func(t *testing.T) {
		assertBlocked(t, "CREATE INDEX evil_idx ON dbo.test_data (text)", "you do not have permissions")
	})

	t.Run("DROP TABLE", func(t *testing.T) {
		assertBlocked(t, "DROP TABLE dbo.test_data", "Cannot drop the table")
	})

	t.Run("CREATE PROCEDURE", func(t *testing.T) {
		assertBlocked(t, "CREATE PROCEDURE evil_proc AS INSERT INTO dbo.test_data (id, text) VALUES (5, 'proc_hack')", "permission denied")
	})

	// --- Privilege Escalation & System Interaction ---

	t.Run("EXECUTE AS (Impersonation)", func(t *testing.T) {
		// Tries to impersonate the table owner or admin
		assertBlocked(t, "EXECUTE AS USER = 'dbo'; INSERT INTO dbo.test_data (id, text) VALUES (5, 'execas_hack'); REVERT;", "you do not have permission")
	})

	t.Run("xp_cmdshell (OS Command)", func(t *testing.T) {
		// Even if enabled on server, this user shouldn't have exec rights
		assertBlocked(t, "EXEC xp_cmdshell 'whoami'", "The EXECUTE permission was denied")
	})

	t.Run("CREATE USER", func(t *testing.T) {
		assertBlocked(t, "CREATE USER evil_user WITHOUT LOGIN", "User does not have permission to perform this action")
	})

	t.Run("ALTER ROLE", func(t *testing.T) {
		assertBlocked(t, `
			DECLARE @user sysname = USER_NAME();
			DECLARE @sql NVARCHAR(MAX) = N'ALTER ROLE db_owner ADD MEMBER ' + QUOTENAME(@user);
			EXEC sp_executesql @sql;`, "Cannot alter the role")
	})
}
