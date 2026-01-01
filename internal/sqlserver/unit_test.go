package sqlserver

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tinternet/databaise/internal/config"
)

func TestExtractDatabaseName(t *testing.T) {
	t.Run("Valid DSN with database parameter", func(t *testing.T) {
		dbName, err := extractSQLServerDatabaseName("sqlserver://sa:password@localhost:1433?database=testdb")
		require.NoError(t, err)
		require.Equal(t, "testdb", dbName)
	})

	t.Run("Valid DSN with multiple parameters", func(t *testing.T) {
		dbName, err := extractSQLServerDatabaseName("sqlserver://sa:password@localhost:1433?database=orders&timeout=30")
		require.NoError(t, err)
		require.Equal(t, "orders", dbName)
	})

	t.Run("Missing database parameter", func(t *testing.T) {
		_, err := extractSQLServerDatabaseName("sqlserver://sa:password@localhost:1433")
		require.ErrorContains(t, err, "database parameter not specified")
	})

	t.Run("Malformed DSN", func(t *testing.T) {
		_, err := extractSQLServerDatabaseName("not a valid url")
		require.Error(t, err)
	})
}

func TestGetDatabaseIdentifier(t *testing.T) {
	t.Run("Same database", func(t *testing.T) {
		dsn := "sqlserver://sa:password@localhost:1433?database=testdb"
		r := ReadConfig{ReadConfig: config.ReadConfig{DSN: dsn}}
		w := WriteConfig{WriteConfig: config.WriteConfig{DSN: dsn}}
		a := AdminConfig{AdminConfig: config.AdminConfig{DSN: dsn}}

		rID, err := r.GetDatabaseIdentifier()
		require.NoError(t, err)
		wID, err := w.GetDatabaseIdentifier()
		require.NoError(t, err)
		aID, err := a.GetDatabaseIdentifier()
		require.NoError(t, err)

		require.Equal(t, "testdb", rID)
		require.Equal(t, rID, wID)
		require.Equal(t, rID, aID)
	})
}
