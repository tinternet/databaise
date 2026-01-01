package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tinternet/databaise/internal/config"
)

func TestExtractDatabaseName(t *testing.T) {
	t.Run("Valid DSN with database in path", func(t *testing.T) {
		dbName, err := extractPostgresDatabaseName("postgres://user:pass@localhost:5432/testdb")
		require.NoError(t, err)
		require.Equal(t, "/testdb", dbName)
	})

	t.Run("Valid DSN with parameters", func(t *testing.T) {
		dbName, err := extractPostgresDatabaseName("postgres://user:pass@localhost:5432/orders?sslmode=disable")
		require.NoError(t, err)
		require.Equal(t, "/orders", dbName)
	})

	t.Run("Empty path", func(t *testing.T) {
		dbName, err := extractPostgresDatabaseName("postgres://user:pass@localhost:5432")
		require.NoError(t, err)
		require.Equal(t, "", dbName)
	})
}

func TestGetDatabaseIdentifier(t *testing.T) {
	t.Run("Same database", func(t *testing.T) {
		dsn := "postgres://user:pass@localhost:5432/testdb"
		r := ReadConfig{ReadConfig: config.ReadConfig{DSN: dsn}}
		w := WriteConfig{WriteConfig: config.WriteConfig{DSN: dsn}}
		a := AdminConfig{AdminConfig: config.AdminConfig{DSN: dsn}}

		rID, err := r.GetDatabaseIdentifier()
		require.NoError(t, err)
		wID, err := w.GetDatabaseIdentifier()
		require.NoError(t, err)
		aID, err := a.GetDatabaseIdentifier()
		require.NoError(t, err)

		require.Equal(t, "/testdb", rID)
		require.Equal(t, rID, wID)
		require.Equal(t, rID, aID)
	})
}
