//go:build integration

package provision

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tinternet/databaise/internal/sqltest"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func setupMySqlDatabase(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := sqltest.SetupMySqlContainer(t)
	provisioner := MySqlProvisioner{}

	password, err := GeneratePassword()
	require.NoError(t, err)
	require.NoError(t, provisioner.Connect(dsn))
	require.NoError(t, provisioner.CreateUser(t.Context(), "testuser", password))
	require.NoError(t, provisioner.GrantReadOnly(t.Context(), "testuser", AccessScope{
		Groups:    []string{"test"},
		Resources: []string{},
	}))

	migrateGormDatabase(t, provisioner.db)

	dsn = strings.Replace(dsn, "test", password, 1)
	dsn = strings.Replace(dsn, "root", "testuser", 1)

	db, err := gorm.Open(mysql.Open(dsn))
	require.NoError(t, err)
	return db
}

func TestMySql_ReadOnlyAccess(t *testing.T) {
	t.Parallel()
	db := setupMySqlDatabase(t)
	testReadonlySchemaScope(t, db)
}

func TestMySql_BadInputs(t *testing.T) {
	t.Parallel()
	dsn := sqltest.SetupMySqlContainer(t)
	provisioner := MySqlProvisioner{}
	testBadInputs(t, &provisioner, dsn)
}

func TestMySql_DropUser(t *testing.T) {
	t.Parallel()
	dsn := sqltest.SetupMySqlContainer(t)
	provisioner := MySqlProvisioner{}
	testDropUser(t, &provisioner, dsn)
}

func TestMySQL_BypassAttempts(t *testing.T) {
	t.Parallel()
	db := setupMySqlDatabase(t)

	assertBlocked := func(t *testing.T, query string, emsg string) {
		t.Helper()
		err := db.Exec(query).Error
		require.Error(t, err, "Action should have been blocked")
		require.ErrorContains(t, err, emsg)
	}

	// Verify that the table we will test against is actually there.
	t.Run("Check table is readable", func(t *testing.T) {
		var count int
		require.NoError(t, db.Raw("SELECT COUNT(*) FROM test.test_data").Scan(&count).Error)
		require.Greater(t, count, 0)
	})

	// --- DDL & Schema Modifications ---

	t.Run("TRUNCATE", func(t *testing.T) {
		// Often requires DROP privilege in MySQL
		assertBlocked(t, "TRUNCATE TABLE test_data", "DROP command denied to user")
	})

	t.Run("ALTER TABLE", func(t *testing.T) {
		assertBlocked(t, "ALTER TABLE test_data ADD evil_col VARCHAR(100)", "ALTER command denied to user")
	})

	t.Run("CREATE INDEX", func(t *testing.T) {
		assertBlocked(t, "CREATE INDEX evil_idx ON test_data (name)", "INDEX command denied to user")
	})

	t.Run("DROP TABLE", func(t *testing.T) {
		assertBlocked(t, "DROP TABLE test_data", "DROP command denied to user")
	})

	t.Run("CREATE VIEW", func(t *testing.T) {
		// Creating a view can sometimes obscure data access
		assertBlocked(t, "CREATE VIEW evil_view AS SELECT * FROM test_data", "CREATE VIEW command denied to user")
	})

	// --- Privilege Escalation & Administration ---

	t.Run("CREATE USER", func(t *testing.T) {
		assertBlocked(t, "CREATE USER 'evil_user'@'%' IDENTIFIED BY 'password'", "Access denied; you need (at least one of) the CREATE USER")
	})

	t.Run("GRANT to self", func(t *testing.T) {
		assertBlocked(t, "GRANT ALL ON *.* TO CURRENT_USER()", "Access denied for user")
	})

	// --- Code Execution & File Access ---

	t.Run("CREATE PROCEDURE", func(t *testing.T) {
		assertBlocked(t, "CREATE PROCEDURE evil_proc() INSERT INTO test_data (name, value) VALUES ('proc_hack', 1)", "Access denied for user")
	})

	t.Run("CREATE TRIGGER", func(t *testing.T) {
		assertBlocked(t, "CREATE TRIGGER evil_trigger BEFORE INSERT ON test_data FOR EACH ROW SET NEW.value = 0", "You do not have the SUPER privilege")
	})

	t.Run("LOAD DATA (File Access)", func(t *testing.T) {
		assertBlocked(t, "LOAD DATA INFILE '/tmp/evil.csv' INTO TABLE test_data", "Access denied for user")
	})
}
