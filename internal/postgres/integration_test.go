//go:build integration

package postgres

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tinternet/databaise/internal/backend"
	"github.com/tinternet/databaise/internal/sqltest"
)

func openTestConnection(t *testing.T) *Backend {
	t.Helper()
	dsn := sqltest.SetupPostgresContainer(t)
	db, err := Connector{}.ConnectAdmin(AdminConfig{DSN: dsn})
	require.NoError(t, err)
	sqltest.Seed(t, db.DB)
	return &Backend{db: db}
}

func TestConnect(t *testing.T) {
	t.Parallel()
	dsn := sqltest.SetupPostgresContainer(t)

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

	t.Run("ReadOnly With UseReadonlyTx", func(t *testing.T) {
		t.Parallel()
		db, err := Connector{}.ConnectRead(ReadConfig{DSN: dsn, UseReadonlyTx: true})
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
		{Schema: "public", Name: "orders"},
		{Schema: "public", Name: "users"},
	})
}

func TestDescribeTable(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		res, err := b.DescribeTable(t.Context(), backend.DescribeTableIn{Schema: "public", Table: "orders"})

		require.NoError(t, err)
		require.NotNil(t, res)

		assert.Contains(t, res.CreateTable, "CREATE TABLE public.orders")
		assert.True(t, slices.ContainsFunc(res.CreateIndexes, func(v string) bool {
			return strings.Contains(v, "CREATE INDEX")
		}))
		assert.True(t, slices.ContainsFunc(res.CreateConstraints, func(v string) bool {
			return strings.Contains(v, "ADD CONSTRAINT")
		}))
	})
	t.Run("MissingSchema", func(t *testing.T) {
		t.Parallel()
		res, err := b.DescribeTable(t.Context(), backend.DescribeTableIn{Schema: "", Table: "orders"})
		require.NoError(t, err)
		require.NotNil(t, res)
	})
	t.Run("NonExistentTable", func(t *testing.T) {
		t.Parallel()
		res, err := b.DescribeTable(t.Context(), backend.DescribeTableIn{Schema: "public", Table: "nonexistent"})
		require.Nil(t, res)
		require.ErrorContains(t, err, "does not exist")
	})
}

func TestExecuteQuery(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)

	res, err := b.ExecuteQuery(t.Context(), backend.ReadQueryIn{Query: "SELECT * FROM public.orders"})
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)

	t.Run("Malformed SQL", func(t *testing.T) {
		_, err := b.ExecuteQuery(t.Context(), backend.ReadQueryIn{Query: "SELECT NOT SELECT"})
		require.ErrorContains(t, err, "syntax error at or near")
	})

	t.Run("Empty Result", func(t *testing.T) {
		t.Parallel()
		res, err := b.ExecuteQuery(t.Context(), backend.ReadQueryIn{Query: "SELECT * FROM public.orders WHERE id = 999"})
		require.NoError(t, err)
		require.Len(t, res.Rows, 0)
	})
}

func TestExecuteDDL(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)

	res, err := b.ExecuteDDL(t.Context(), backend.ExecuteDDLIn{
		DDL: "CREATE UNIQUE INDEX ix_someindex1 ON public.orders (id, created_at)",
	})

	require.NoError(t, err)
	require.True(t, res.Success)

	t.Run("IndexAlreadyExists", func(t *testing.T) {
		_, err := b.ExecuteDDL(t.Context(), backend.ExecuteDDLIn{
			DDL: "CREATE UNIQUE INDEX ix_someindex1 ON public.orders (id, created_at)",
		})
		require.ErrorContains(t, err, "already exists")
	})

	t.Run("DropIndex", func(t *testing.T) {
		res, err := b.ExecuteDDL(t.Context(), backend.ExecuteDDLIn{
			DDL: "DROP INDEX public.ix_someindex1",
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
		res, err := b.ExplainQuery(t.Context(), backend.ExplainQueryIn{Query: "SELECT * FROM public.orders"})
		require.NoError(t, err)
		require.Equal(t, "json", res.Format)
		require.Greater(t, len(res.Result), 1)
	})
	t.Run("ExplainAnalyze", func(t *testing.T) {
		t.Parallel()
		res, err := b.ExplainQuery(t.Context(), backend.ExplainQueryIn{Query: "SELECT * FROM public.orders", Analyze: true})
		require.NoError(t, err)
		require.Equal(t, "json", res.Format)
		require.Greater(t, len(res.Result), 1)
	})

	t.Run("MalformedQuery", func(t *testing.T) {
		t.Parallel()
		res, err := b.ExplainQuery(t.Context(), backend.ExplainQueryIn{Query: "SELECT NOT SELECT"})
		require.Nil(t, res)
		require.ErrorContains(t, err, "syntax error at or near")
	})
}

func TestReadonlyTransactionEnforcement(t *testing.T) {
	t.Parallel()

	dsn := sqltest.SetupPostgresContainer(t)
	adminDB, err := Connector{}.ConnectAdmin(AdminConfig{DSN: dsn})
	require.NoError(t, err)
	sqltest.Seed(t, adminDB.DB)

	readDB, err := Connector{}.ConnectRead(ReadConfig{DSN: dsn, UseReadonlyTx: true})
	require.NoError(t, err)
	b := &Backend{db: readDB}

	t.Run("ReadOnlyTransactionAllowed", func(t *testing.T) {
		t.Parallel()
		_, err := b.ExecuteQuery(t.Context(), backend.ReadQueryIn{Query: "SELECT * FROM public.orders"})
		require.NoError(t, err)
	})

	t.Run("WriteInReadOnlyTransaction", func(t *testing.T) {
		t.Parallel()
		res, err := b.ExecuteQuery(t.Context(), backend.ReadQueryIn{Query: "DELETE FROM public.orders"})
		require.Nil(t, res)
		require.ErrorContains(t, err, "cannot execute DELETE in a read-only transaction")
	})

	t.Run("Bypass readonly tx", func(t *testing.T) {
		t.Parallel()
		res, err := b.ExecuteQuery(t.Context(), backend.ReadQueryIn{Query: "COMMIT; DELETE FROM public.orders"})
		require.Nil(t, res)
		require.ErrorContains(t, err, "cannot insert multiple commands into a prepared statement")
	})
}

func TestListMissingIndexes(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)
	res, err := b.ListMissingIndexes(t.Context())
	require.NoError(t, err)
	require.NotNil(t, res)
}

func TestListWaitingQueries(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)
	res, err := b.ListWaitingQueries(t.Context())
	require.NoError(t, err)
	require.NotNil(t, res)
}

func TestListSlowestQueries(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)
	require.NoError(t, b.db.Exec("CREATE EXTENSION IF NOT EXISTS pg_stat_statements").Error)
	res, err := b.ListSlowestQueries(t.Context())
	require.NoError(t, err)
	require.NotNil(t, res)
}

func TestListDeadlocks(t *testing.T) {
	t.Parallel()
	b := openTestConnection(t)
	res, err := b.ListDeadlocks(t.Context())
	require.NoError(t, err)
	require.NotNil(t, res)
}
