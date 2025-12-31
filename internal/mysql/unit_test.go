package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractDatabaseName(t *testing.T) {
	t.Run("Nil value", func(t *testing.T) {
		require.NoError(t, extractDatabaseName(nil, make(map[string]bool)))
	})
	t.Run("Malformed dsn", func(t *testing.T) {
		cfg := ReadConfig{DSN: "dsadsads"}
		err := extractDatabaseName(cfg, make(map[string]bool))
		require.ErrorContains(t, err, "could not parse mysql dsn")
	})
	t.Run("No database specified", func(t *testing.T) {
		cfg := ReadConfig{DSN: "root:test@tcp(localhost:5000)/?param=true"}
		err := extractDatabaseName(cfg, make(map[string]bool))
		require.ErrorContains(t, err, "database not specified")
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

func TestValidateConfig(t *testing.T) {
	t.Run("Same database", func(t *testing.T) {
		dsn := "root:test@tcp(localhost:5000)/test?multiStatements=true&parseTime=true"
		r := ReadConfig{DSN: dsn}
		w := WriteConfig{DSN: dsn}
		a := AdminConfig{DSN: dsn}
		require.NoError(t, Connector{}.ValidateConfig(&r, &w, &a))
	})
	t.Run("Different read database", func(t *testing.T) {
		dsn := "root:test@tcp(localhost:5000)/test?multiStatements=true&parseTime=true"
		r := ReadConfig{DSN: "oot:test@tcp(localhost:5000)/different?multiStatements=true&parseTime=true"}
		w := WriteConfig{DSN: dsn}
		a := AdminConfig{DSN: dsn}
		require.Error(t, Connector{}.ValidateConfig(&r, &w, &a))
	})
	t.Run("Different write database", func(t *testing.T) {
		dsn := "root:test@tcp(localhost:5000)/test?multiStatements=true&parseTime=true"
		r := ReadConfig{DSN: dsn}
		w := WriteConfig{DSN: "oot:test@tcp(localhost:5000)/different?multiStatements=true&parseTime=true"}
		a := AdminConfig{DSN: dsn}
		require.Error(t, Connector{}.ValidateConfig(&r, &w, &a))
	})
	t.Run("Different admin database", func(t *testing.T) {
		dsn := "root:test@tcp(localhost:5000)/test?multiStatements=true&parseTime=true"
		r := ReadConfig{DSN: dsn}
		w := WriteConfig{DSN: dsn}
		a := AdminConfig{DSN: "oot:test@tcp(localhost:5000)/different?multiStatements=true&parseTime=true"}
		require.Error(t, Connector{}.ValidateConfig(&r, &w, &a))
	})
}
