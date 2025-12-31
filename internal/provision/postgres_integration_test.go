//go:build integration

package provision

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tinternet/databaise/internal/sqltest"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func setupPostgresDatabase(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := sqltest.SetupPostgresContainer(t)
	provisioner := PostgresProvisioner{}

	require.NoError(t, provisioner.Connect(dsn))
	require.NoError(t, provisioner.CreateUser(t.Context(), "testuser", "testpass"))
	require.NoError(t, provisioner.GrantReadOnly(t.Context(), "testuser", AccessScope{
		Groups:    []string{"public"},
		Resources: []string{},
	}))

	migrateGormDatabase(t, provisioner.db)

	db, err := gorm.Open(postgres.Open(sqltest.ReplaceURLCredentials(t, dsn, "testuser", "testpass")))
	require.NoError(t, err)
	return db
}

func TestPostgres_ReadOnlyAccess(t *testing.T) {
	t.Parallel()
	db := setupPostgresDatabase(t)
	testReadonlySchemaScope(t, db)
}

func TestPostgres_BadInputs(t *testing.T) {
	t.Parallel()
	dsn := sqltest.SetupPostgresContainer(t)
	provisioner := PostgresProvisioner{}
	testBadInputs(t, &provisioner, dsn)
}

func TestPostgres_DropUser(t *testing.T) {
	t.Parallel()
	dsn := sqltest.SetupPostgresContainer(t)
	provisioner := PostgresProvisioner{}
	testDropUser(t, &provisioner, dsn)
}

func TestPostgres_BypassAttempts(t *testing.T) {
	t.Parallel()
	db := setupPostgresDatabase(t)

	assertBlocked := func(t *testing.T, sql string, emsg string) {
		t.Helper()
		err := db.Exec(sql).Error
		require.Error(t, err, "SQL should have failed but succeeded")
		require.ErrorContains(t, err, emsg)
	}

	// Verify that the table we will test against is actually there.
	t.Run("Check table is readable", func(t *testing.T) {
		var count int
		require.NoError(t, db.Raw("SELECT COUNT(*) FROM public.test_data").Scan(&count).Error)
		require.Greater(t, count, 0)
	})

	t.Run("CTE INSERT", func(t *testing.T) {
		assertBlocked(t,
			"SET TRANSACTION READ WRITE; WITH inserted AS (INSERT INTO public.test_data (id, text) VALUES (5, 'hack') RETURNING *) SELECT * FROM inserted",
			"permission denied")
	})

	t.Run("TRUNCATE", func(t *testing.T) {
		assertBlocked(t, "SET TRANSACTION READ WRITE; TRUNCATE public.test_data", "permission denied")
	})

	t.Run("DROP TABLE", func(t *testing.T) {
		assertBlocked(t, "SET TRANSACTION READ WRITE; DROP TABLE public.test_data", "must be owner of")
	})

	t.Run("ALTER TABLE", func(t *testing.T) {
		assertBlocked(t, "SET TRANSACTION READ WRITE; ALTER TABLE public.test_data ADD COLUMN evil_col TEXT", "must be owner of")
	})

	t.Run("CREATE INDEX", func(t *testing.T) {
		assertBlocked(t, "SET TRANSACTION READ WRITE; CREATE INDEX evil_idx ON public.test_data (text)", "must be owner of")
	})
}
