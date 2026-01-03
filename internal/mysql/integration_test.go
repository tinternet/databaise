//go:build integration

package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tinternet/databaise/internal/backend"
	"github.com/tinternet/databaise/internal/sqltest"
)

func openTestConnection(t *testing.T) *Backend {
	t.Helper()
	dsn := sqltest.SetupMySqlContainer(t)
	db, err := Connector{}.ConnectAdmin(AdminConfig{DSN: dsn})
	require.NoError(t, err)
	sqltest.Seed(t, db)
	return &Backend{db: db}
}

func TestConnect(t *testing.T) {
	t.Parallel()
	dsn := sqltest.SetupMySqlContainer(t)

	t.Run("ReadOnly With BypassReadonlyCheck=false", func(t *testing.T) {
		t.Parallel()
		_, err := Connector{}.ConnectRead(ReadConfig{DSN: dsn})
		require.ErrorContains(t, err, "read DSN user has write permissions")
	})

	t.Run("ReadOnly With BypassReadonlyCheck=true", func(t *testing.T) {
		t.Parallel()
		db, err := Connector{}.ConnectRead(ReadConfig{DSN: dsn, BypassReadonlyCheck: true})
		require.NotNil(t, db)
		require.NoError(t, err)
	})

	t.Run("Admin", func(t *testing.T) {
		t.Parallel()
		db, err := Connector{}.ConnectAdmin(AdminConfig{DSN: dsn})
		require.NotNil(t, db)
		require.NoError(t, err)
	})
}

func TestListTables(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)
	tables, err := b.ListTables(t.Context(), backend.ListTablesIn{})
	require.NoError(t, err)
	require.ElementsMatch(t, tables, []backend.Table{
		{Name: "orders"},
		{Name: "users"},
	})
}

func TestDescribeTable(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		res, err := b.DescribeTable(t.Context(), backend.DescribeTableIn{Table: "orders"})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Contains(t, res.CreateTable, "CREATE TABLE")
		require.Contains(t, res.CreateTable, "KEY") // indexes are included in the table DDL
	})
	t.Run("NonExistentTable", func(t *testing.T) {
		t.Parallel()
		res, err := b.DescribeTable(t.Context(), backend.DescribeTableIn{Table: "nonexistent"})
		require.Nil(t, res)
		require.ErrorContains(t, err, "doesn't exist")
	})
}

func TestExecuteQuery(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)

	res, err := b.ExecuteQuery(t.Context(), backend.ReadQueryIn{Query: "SELECT * FROM orders"})
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)

	t.Run("Malformed SQL", func(t *testing.T) {
		_, err := b.ExecuteQuery(t.Context(), backend.ReadQueryIn{Query: "SELECT NOT SELECT"})
		require.ErrorContains(t, err, "You have an error in your SQL")
	})

	t.Run("Empty Result", func(t *testing.T) {
		t.Parallel()
		res, err := b.ExecuteQuery(t.Context(), backend.ReadQueryIn{Query: "SELECT * FROM orders WHERE id = 999"})
		require.NoError(t, err)
		require.Len(t, res.Rows, 0)
	})
}

func TestExecuteDDL(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)

	res, err := b.ExecuteDDL(t.Context(), backend.ExecuteDDLIn{
		DDL: "CREATE UNIQUE INDEX ix_someindex1 ON orders (id, created_at)",
	})

	require.NoError(t, err)
	require.True(t, res.Success)

	t.Run("IndexAlreadyExists", func(t *testing.T) {
		_, err := b.ExecuteDDL(t.Context(), backend.ExecuteDDLIn{
			DDL: "CREATE UNIQUE INDEX ix_someindex1 ON orders (id, created_at)",
		})
		require.ErrorContains(t, err, "Duplicate key name")
	})

	t.Run("DropIndex", func(t *testing.T) {
		res, err := b.ExecuteDDL(t.Context(), backend.ExecuteDDLIn{
			DDL: "DROP INDEX ix_someindex1 ON orders",
		})
		require.NoError(t, err)
		require.True(t, res.Success)
	})
}

func TestExplainQuery(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)

	t.Run("Explain", func(t *testing.T) {
		t.Parallel()
		res, err := b.ExplainQuery(t.Context(), backend.ExplainQueryIn{Query: "SELECT * FROM orders"})
		require.NoError(t, err)
		require.Equal(t, "json", res.Format)
		require.Greater(t, len(res.Result), 1)
	})
	t.Run("ExplainAnalyze", func(t *testing.T) {
		t.Parallel()
		res, err := b.ExplainQuery(t.Context(), backend.ExplainQueryIn{Query: "SELECT * FROM orders", Analyze: true})
		require.NoError(t, err)
		require.Equal(t, "json", res.Format)
		require.Greater(t, len(res.Result), 1)
	})

	t.Run("MalformedQuery", func(t *testing.T) {
		t.Parallel()
		res, err := b.ExplainQuery(t.Context(), backend.ExplainQueryIn{Query: "SELECT NOT SELECT"})
		require.Nil(t, res)
		require.ErrorContains(t, err, "You have an error in your SQL")
	})
}

func TestListMissingIndexes(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)
	_, err := b.ListMissingIndexes(t.Context())
	require.ErrorContains(t, err, "does not provide automatic index recommendations")
}

func TestListWaitingQueries(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)
	_, err := b.ListWaitingQueries(t.Context())
	require.NoError(t, err)
}

func TestListSlowestQueries(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)
	res, err := b.ListSlowestQueries(t.Context())
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Columns)
	require.NotNil(t, res.Queries)
	// Verify expected columns are documented
	require.Contains(t, res.Columns, "query")
	require.Contains(t, res.Columns, "calls")
	require.Contains(t, res.Columns, "total_time_sec")
	require.Contains(t, res.Columns, "no_index_used")
	require.Contains(t, res.Columns, "full_scan")
}

func TestListDeadlocks(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)
	res, err := b.ListDeadlocks(t.Context())
	require.NoError(t, err)
	require.NotNil(t, res)
}
