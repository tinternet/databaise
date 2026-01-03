//go:build integration

package sqlserver

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tinternet/databaise/internal/backend"
	"github.com/tinternet/databaise/internal/sqlcommon"
	"github.com/tinternet/databaise/internal/sqltest"
)

func openTestConnection(t *testing.T) *Backend {
	t.Helper()
	dsn := sqltest.SetupSqlServerContainer(t)
	db, err := Connector{}.ConnectAdmin(AdminConfig{DSN: dsn})
	require.NoError(t, err)
	sqltest.Seed(t, db)
	return &Backend{db: db}
}

func TestConnect(t *testing.T) {
	t.Parallel()
	dsn := sqltest.SetupSqlServerContainer(t)

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
	assert.Contains(t, tables, backend.Table{Schema: "dbo", Name: "orders"})
	assert.Contains(t, tables, backend.Table{Schema: "dbo", Name: "users"})
}

func TestDescribeTable(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		res, err := b.DescribeTable(t.Context(), backend.DescribeTableIn{Schema: "dbo", Table: "orders"})

		require.NoError(t, err)
		require.NotNil(t, res)

		assert.Contains(t, res.CreateTable, "CREATE TABLE")
		assert.True(t, slices.ContainsFunc(res.CreateIndexes, func(v string) bool {
			return strings.Contains(v, "CREATE") && strings.Contains(v, "INDEX")
		}))
		assert.True(t, slices.ContainsFunc(res.CreateConstraints, func(v string) bool {
			return strings.Contains(v, "ADD CONSTRAINT")
		}))
	})
	t.Run("NonExistentTable", func(t *testing.T) {
		t.Parallel()
		res, err := b.DescribeTable(t.Context(), backend.DescribeTableIn{Schema: "dbo", Table: "nonexistent"})
		require.Nil(t, res)
		require.ErrorIs(t, err, sqlcommon.ErrTableNotFound)
	})
}

func TestExecuteQuery(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)

	res, err := b.ExecuteQuery(t.Context(), backend.ReadQueryIn{Query: "SELECT * FROM dbo.orders"})
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)

	t.Run("Malformed SQL", func(t *testing.T) {
		_, err := b.ExecuteQuery(t.Context(), backend.ReadQueryIn{Query: "SELECT NOT SELECT"})
		require.ErrorContains(t, err, "Incorrect syntax near")
	})

	t.Run("Empty Result", func(t *testing.T) {
		t.Parallel()
		res, err := b.ExecuteQuery(t.Context(), backend.ReadQueryIn{Query: "SELECT * FROM dbo.orders WHERE id = 999"})
		require.NoError(t, err)
		require.Len(t, res.Rows, 0)
	})
}

func TestExecuteDDL(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)

	res, err := b.ExecuteDDL(t.Context(), backend.ExecuteDDLIn{
		DDL: "CREATE UNIQUE NONCLUSTERED INDEX ix_someindex1 ON dbo.orders (id, created_at)",
	})

	require.NoError(t, err)
	require.True(t, res.Success)

	t.Run("IndexAlreadyExists", func(t *testing.T) {
		_, err := b.ExecuteDDL(t.Context(), backend.ExecuteDDLIn{
			DDL: "CREATE UNIQUE NONCLUSTERED INDEX ix_someindex1 ON dbo.orders (id, created_at)",
		})
		require.ErrorContains(t, err, "already exists")
	})

	t.Run("DropIndex", func(t *testing.T) {
		res, err := b.ExecuteDDL(t.Context(), backend.ExecuteDDLIn{
			DDL: "DROP INDEX ix_someindex1 ON dbo.orders",
		})
		require.NoError(t, err)
		require.True(t, res.Success)
	})
}

func TestExplainQuery(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)

	t.Run("Estimated", func(t *testing.T) {
		res, err := b.ExplainQuery(t.Context(), backend.ExplainQueryIn{Query: "SELECT * FROM dbo.orders", Analyze: false})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, "xml", res.Format)
		require.Contains(t, res.Result, "<ShowPlanXML")
	})
	t.Run("Actual", func(t *testing.T) {
		res, err := b.ExplainQuery(t.Context(), backend.ExplainQueryIn{Query: "SELECT id FROM dbo.orders", Analyze: true})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, "xml", res.Format)
		require.Contains(t, res.Result, "<ShowPlanXML")
	})
}

func TestListMissingIndexes(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)
	_, err := b.ListMissingIndexes(t.Context())
	require.NoError(t, err)
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
	_, err := b.ListSlowestQueries(t.Context())
	require.NoError(t, err)
}

func TestListDeadlocks(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)
	_, err := b.ListDeadlocks(t.Context())
	require.NoError(t, err)
}
