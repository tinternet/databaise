//go:build integration

package postgres

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tinternet/databaise/internal/config"
	"github.com/tinternet/databaise/internal/provision"
	"github.com/tinternet/databaise/internal/sqltest"
)

func openTestConnection(t *testing.T) DB {
	t.Helper()
	dsn := sqltest.SetupPostgresContainer(t)
	db, err := Connector{}.ConnectAdmin(AdminConfig{AdminConfig: config.AdminConfig{DSN: dsn}})
	sqltest.Seed(t, db.DB)
	require.NoError(t, err)
	return db
}

func TestConnect(t *testing.T) {
	t.Parallel()
	dsn := sqltest.SetupPostgresContainer(t)

	t.Run("ReadOnly", func(t *testing.T) {
		t.Parallel()
		provisioner := provision.PostgresProvisioner{}
		require.NoError(t, provisioner.Connect(dsn))
		require.NoError(t, provisioner.CreateUser(t.Context(), "testuser", "testpass"))
		rodsn := sqltest.ReplaceURLCredentials(t, dsn, "testuser", "testpass")
		db, err := Connector{}.ConnectRead(ReadConfig{ReadConfig: config.ReadConfig{DSN: rodsn}})
		require.NotNil(t, db)
		require.NoError(t, err)
	})

	t.Run("ReadOnly With EnforceReadonly=true", func(t *testing.T) {
		t.Parallel()
		db, err := Connector{}.ConnectRead(ReadConfig{ReadConfig: config.ReadConfig{DSN: dsn}})
		require.NotNil(t, db)
		require.ErrorContains(t, err, "read DSN user has write permissions")
	})

	t.Run("ReadOnly With EnforceReadonly=false", func(t *testing.T) {
		t.Parallel()
		db, err := Connector{}.ConnectRead(ReadConfig{ReadConfig: config.ReadConfig{DSN: dsn, EnforceReadonly: ptr(false)}})
		require.NotNil(t, db)
		require.NoError(t, err)
	})

	t.Run("ReadOnly With UseReadonlyTx", func(t *testing.T) {
		t.Parallel()
		db, err := Connector{}.ConnectRead(ReadConfig{ReadConfig: config.ReadConfig{DSN: dsn}, UseReadonlyTx: true})
		require.NotNil(t, db)
		require.NoError(t, err)
	})

	t.Run("Write", func(t *testing.T) {
		t.Parallel()
		db, err := Connector{}.ConnectWrite(WriteConfig{WriteConfig: config.WriteConfig{DSN: dsn}})
		require.NotNil(t, db)
		require.NoError(t, err)
	})

	t.Run("Admin", func(t *testing.T) {
		t.Parallel()
		db, err := Connector{}.ConnectAdmin(AdminConfig{AdminConfig: config.AdminConfig{DSN: dsn}})
		require.NotNil(t, db)
		require.NoError(t, err)
	})
}

func TestListTables(t *testing.T) {
	t.Parallel()
	db := openTestConnection(t)
	l, err := ListTables(t.Context(), ListTablesIn{}, db)
	require.NoError(t, err)
	require.ElementsMatch(t, l.Tables, []Table{
		{Schema: "public", Name: "orders"},
		{Schema: "public", Name: "users"},
	})
}

func TestDescribeTable(t *testing.T) {
	t.Parallel()
	db := openTestConnection(t)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		res, err := DescribeTable(t.Context(), DescribeTableIn{Schema: "public", Table: "orders"}, db)

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
	t.Run("NonExistentTable", func(t *testing.T) {
		t.Parallel()
		res, err := DescribeTable(t.Context(), DescribeTableIn{Schema: "public", Table: "nonexistend"}, db)
		require.Nil(t, res)
		require.ErrorContains(t, err, "does not exist")
	})
}

func TestExecuteQuery(t *testing.T) {
	t.Parallel()
	db := openTestConnection(t)

	res, err := ExecuteQuery(t.Context(), ExecuteQueryIn{Query: "SELECT * FROM public.orders"}, db)
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)

	t.Run("Malformed SQL", func(t *testing.T) {
		_, err := ExecuteQuery(t.Context(), ExecuteQueryIn{Query: "SELECT NOT SELECT"}, db)
		require.ErrorContains(t, err, "syntax error at or near")
	})

	t.Run("Empty Result", func(t *testing.T) {
		t.Parallel()
		res, err := ExecuteQuery(t.Context(), ExecuteQueryIn{Query: "SELECT * FROM public.orders WHERE id = 999"}, db)
		require.NoError(t, err)
		require.Len(t, res.Rows, 0)
	})
}

func TestCreateIndex(t *testing.T) {
	t.Parallel()
	db := openTestConnection(t)

	ix := CreateIndexIn{
		Schema:  "public",
		Table:   "orders",
		Name:    "ix_someindex1",
		Columns: []string{"id", "created_at"},
		Unique:  true,
	}

	res, err := CreateIndex(t.Context(), ix, db)

	require.NoError(t, err)
	require.True(t, res.Success)

	t.Run("IndexAlreadyExists", func(t *testing.T) {
		res, err := CreateIndex(t.Context(), ix, db)
		require.Nil(t, res)
		require.ErrorContains(t, err, "already exists")
	})
}

func TestDropIndex(t *testing.T) {
	db := openTestConnection(t)

	res, err := CreateIndex(t.Context(), CreateIndexIn{
		Schema:  "public",
		Table:   "orders",
		Name:    "ix_someindex1",
		Columns: []string{"id", "created_at"},
		Unique:  true,
	}, db)

	require.NoError(t, err)
	require.True(t, res.Success)

	ix, err := DropIndex(t.Context(), DropIndexIn{Schema: "public", Name: "ix_someindex1"}, db)
	require.True(t, ix.Success)
	require.NoError(t, err)

	t.Run("NonExistentIndex", func(t *testing.T) {
		ix, err := DropIndex(t.Context(), DropIndexIn{Schema: "public", Name: "ix_someindex1"}, db)
		require.Nil(t, ix)
		require.ErrorContains(t, err, "does not exist")
	})
}

func TestExplainQuery(t *testing.T) {
	t.Parallel()
	db := openTestConnection(t)

	t.Run("Explain", func(t *testing.T) {
		t.Parallel()
		res, err := ExplainQuery(t.Context(), ExplainQueryIn{Query: "SELECT * FROM public.orders"}, db)
		require.NoError(t, err)
		require.Greater(t, len(res.Plan), 1)
		t.Logf("%v", res.Plan)
	})
	t.Run("ExplainQueryPlan", func(t *testing.T) {
		t.Parallel()
		res, err := ExplainQuery(t.Context(), ExplainQueryIn{Query: "SELECT * FROM public.orders", Analyze: true}, db)
		require.NoError(t, err)
		require.Greater(t, len(res.Plan), 1)
		t.Logf("%v", res.Plan)
	})

	t.Run("MalformedQuery", func(t *testing.T) {
		t.Parallel()
		res, err := ExplainQuery(t.Context(), ExplainQueryIn{Query: "SELECT NOT SELECT"}, db)
		require.Nil(t, res)
		require.ErrorContains(t, err, "syntax error at or near")
	})
}

func TestReadonlyTransactionEnforcement(t *testing.T) {
	t.Parallel()

	dsn := sqltest.SetupPostgresContainer(t)
	adm, err := Connector{}.ConnectAdmin(AdminConfig{AdminConfig: config.AdminConfig{DSN: dsn}})
	require.NoError(t, err)
	sqltest.Seed(t, adm.DB)
	db, err := Connector{}.ConnectRead(ReadConfig{ReadConfig: config.ReadConfig{DSN: dsn}, UseReadonlyTx: true})
	require.NoError(t, err)

	t.Run("ReadOnlyTransactionAllowed", func(t *testing.T) {
		t.Parallel()
		_, err := ExecuteQuery(t.Context(), ExecuteQueryIn{Query: "SELECT * FROM public.orders"}, db)
		require.NoError(t, err)
	})

	t.Run("WriteInReadOnlyTransaction", func(t *testing.T) {
		t.Parallel()
		res, err := ExecuteQuery(t.Context(), ExecuteQueryIn{Query: "DELETE FROM public.orders"}, db)
		require.Nil(t, res)
		require.ErrorContains(t, err, "cannot execute DELETE in a read-only transaction")
	})

	t.Run("Bypass readonly tx", func(t *testing.T) {
		t.Parallel()
		res, err := ExecuteQuery(t.Context(), ExecuteQueryIn{Query: "COMMIT; DELETE FROM public.orders"}, db)
		require.Nil(t, res)
		require.ErrorContains(t, err, "cannot insert multiple commands into a prepared statement")
	})
}

func TestListMissingIndexes(t *testing.T) {
	t.Parallel()
	db := openTestConnection(t)
	res, err := ListMissingIndexes(t.Context(), struct{}{}, db)
	require.NoError(t, err)
	require.NotNil(t, res)
}

func TestListWaitingQueries(t *testing.T) {
	t.Parallel()
	db := openTestConnection(t)
	res, err := ListWaitingQueries(t.Context(), struct{}{}, db)
	require.NoError(t, err)
	require.NotNil(t, res)
}

func TestListSlowestQueries(t *testing.T) {
	t.Parallel()
	db := openTestConnection(t)
	require.NoError(t, db.Exec("CREATE EXTENSION IF NOT EXISTS pg_stat_statements").Error)
	res, err := ListSlowestQueries(t.Context(), struct{}{}, db)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Queries)
}

func TestListDeadlocks(t *testing.T) {
	t.Parallel()
	db := openTestConnection(t)
	res, err := ListDeadlocks(t.Context(), struct{}{}, db)
	require.NoError(t, err)
	require.NotNil(t, res)
}

func ptr[T any](v T) *T {
	return &v
}
