package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tinternet/databaise/internal/config"
)

func TestExtractDatabaseName(t *testing.T) {
	t.Run("Malformed dsn", func(t *testing.T) {
		_, err := extractMySQLDatabaseName("dsadsads")
		require.ErrorContains(t, err, "could not parse mysql dsn")
	})
	t.Run("No database specified", func(t *testing.T) {
		_, err := extractMySQLDatabaseName("root:test@tcp(localhost:5000)/?param=true")
		require.ErrorContains(t, err, "database not specified")
	})
	t.Run("Valid DSN", func(t *testing.T) {
		dbName, err := extractMySQLDatabaseName("root:test@tcp(localhost:5000)/testdb?param=true")
		require.NoError(t, err)
		require.Equal(t, "testdb", dbName)
	})
}

func TestEnableParseTimeParam(t *testing.T) {
	t.Run("Not present", func(t *testing.T) {
		dsn := enableParseTime("root:test@tcp(localhost:5000)/test")
		require.Equal(t, "root:test@tcp(localhost:5000)/test?parseTime=true", dsn)
	})
	t.Run("Already present", func(t *testing.T) {
		dsn := enableParseTime("root:test@tcp(localhost:5000)/test?parseTime=true")
		require.Equal(t, "root:test@tcp(localhost:5000)/test?parseTime=true", dsn)
	})
	t.Run("Multiple params", func(t *testing.T) {
		dsn := enableParseTime("root:test@tcp(localhost:5000)/test?multiStatements=true")
		require.Equal(t, "root:test@tcp(localhost:5000)/test?multiStatements=true&parseTime=true", dsn)
	})
}

func TestGetDatabaseIdentifier(t *testing.T) {
	t.Run("Same database", func(t *testing.T) {
		dsn := "root:test@tcp(localhost:5000)/test?multiStatements=true&parseTime=true"
		r := ReadConfig{ReadConfig: config.ReadConfig{DSN: dsn}}
		w := WriteConfig{WriteConfig: config.WriteConfig{DSN: dsn}}
		a := AdminConfig{AdminConfig: config.AdminConfig{DSN: dsn}}

		rID, err := r.GetDatabaseIdentifier()
		require.NoError(t, err)
		wID, err := w.GetDatabaseIdentifier()
		require.NoError(t, err)
		aID, err := a.GetDatabaseIdentifier()
		require.NoError(t, err)

		require.Equal(t, "test", rID)
		require.Equal(t, rID, wID)
		require.Equal(t, rID, aID)
	})
}
