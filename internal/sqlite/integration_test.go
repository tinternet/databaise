//go:build integration

package sqlite

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tinternet/databaise/internal/backend"
	"github.com/tinternet/databaise/internal/sqlcommon"
	"github.com/tinternet/databaise/internal/sqltest"
)

func createFile(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	f, err := os.CreateTemp(dir, "*.db")
	require.NoError(t, err)
	defer f.Close()
	return f.Name()
}

func openTestConnection(t *testing.T) *Backend {
	t.Helper()
	path := createFile(t)
	db, err := Connector{}.ConnectAdmin(AdminConfig{Path: path})
	require.NoError(t, err)
	sqltest.Seed(t, db)
	return &Backend{db: db}
}

func TestConnect(t *testing.T) {
	t.Parallel()
	file := createFile(t)

	t.Run("ReadOnly", func(t *testing.T) {
		t.Parallel()
		db, err := Connector{}.ConnectRead(ReadConfig{Path: file})
		require.NotNil(t, db)
		require.NoError(t, err)
	})

	t.Run("Admin", func(t *testing.T) {
		t.Parallel()
		db, err := Connector{}.ConnectAdmin(AdminConfig{Path: file})
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

		assert.Contains(t, res.CreateTable, "CREATE TABLE")
		assert.Contains(t, res.CreateTable, "orders")
		assert.Greater(t, len(res.CreateIndexes), 0)

		for _, i := range res.CreateIndexes {
			assert.Contains(t, i, "CREATE")
			assert.Contains(t, i, "INDEX")
		}
	})
	t.Run("NonExistentTable", func(t *testing.T) {
		t.Parallel()
		res, err := b.DescribeTable(t.Context(), backend.DescribeTableIn{Table: "nonexistent"})
		require.Nil(t, res)
		require.ErrorIs(t, err, sqlcommon.ErrTableNotFound)
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
		require.ErrorContains(t, err, "syntax error")
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
		require.ErrorContains(t, err, "already exists")
	})

	t.Run("DropIndex", func(t *testing.T) {
		res, err := b.ExecuteDDL(t.Context(), backend.ExecuteDDLIn{
			DDL: "DROP INDEX ix_someindex1",
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
	t.Run("ExplainQueryPlan", func(t *testing.T) {
		t.Parallel()
		res, err := b.ExplainQuery(t.Context(), backend.ExplainQueryIn{Query: "SELECT * FROM orders", Analyze: true})
		require.NoError(t, err)
		require.Equal(t, "json", res.Format)
		require.GreaterOrEqual(t, len(res.Result), 1)
	})
	t.Run("MalformedQuery", func(t *testing.T) {
		t.Parallel()
		_, err := b.ExplainQuery(t.Context(), backend.ExplainQueryIn{Query: "SELECT NOT SELECT"})
		require.ErrorContains(t, err, "syntax error")
	})
}

func TestListMissingIndexes(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)
	_, err := b.ListMissingIndexes(t.Context())
	require.ErrorContains(t, err, "not available for SQLite")
}

func TestListWaitingQueries(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)
	_, err := b.ListWaitingQueries(t.Context())
	require.ErrorContains(t, err, "not available for SQLite")
}

func TestListSlowestQueries(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)
	_, err := b.ListSlowestQueries(t.Context())
	require.ErrorContains(t, err, "not available for SQLite")
}

func TestListDeadlocks(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)
	_, err := b.ListDeadlocks(t.Context())
	require.ErrorContains(t, err, "not available for SQLite")
}
