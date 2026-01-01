//go:build integration

package sqlite

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestConnect(t *testing.T) {
	t.Parallel()
	file := createFile(t)

	t.Run("ReadOnly", func(t *testing.T) {
		t.Parallel()
		db, err := Connector{}.ConnectRead(SqliteConfig{Path: file})
		require.NotNil(t, db)
		require.NoError(t, err)
	})

	t.Run("Write", func(t *testing.T) {
		t.Parallel()
		db, err := Connector{}.ConnectWrite(SqliteConfig{Path: file})
		require.NotNil(t, db)
		require.NoError(t, err)
	})

	t.Run("Admin", func(t *testing.T) {
		t.Parallel()
		db, err := Connector{}.ConnectAdmin(SqliteConfig{Path: file})
		require.NotNil(t, db)
		require.NoError(t, err)
	})
}

func TestListTables(t *testing.T) {
	t.Parallel()

	db, err := Connector{}.ConnectRead(SqliteConfig{Path: createFile(t)})
	require.NoError(t, err)
	sqltest.Seed(t, db)

	l, err := ListTables(t.Context(), ListTablesIn{}, db)
	require.NoError(t, err)
	require.ElementsMatch(t, l.Tables, []string{"orders", "users"})
}

func TestDescribeTable(t *testing.T) {
	t.Parallel()

	db, err := Connector{}.ConnectRead(SqliteConfig{Path: createFile(t)})
	require.NoError(t, err)
	sqltest.Seed(t, db)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		res, err := DescribeTable(t.Context(), DescribeTableIn{Table: "orders"}, db)

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
		res, err := DescribeTable(t.Context(), DescribeTableIn{Table: "nonexistent"}, db)
		require.Nil(t, res)
		require.ErrorIs(t, sqlcommon.ErrTableNotFound, err)
	})
}

func TestExecuteQuery(t *testing.T) {
	t.Parallel()
	db, err := Connector{}.ConnectRead(SqliteConfig{Path: createFile(t)})
	require.NoError(t, err)
	sqltest.Seed(t, db)

	res, err := ExecuteQuery(t.Context(), ExecuteQueryIn{Query: "SELECT * FROM orders"}, db)
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)

	t.Run("Malformed SQL", func(t *testing.T) {
		_, err := ExecuteQuery(t.Context(), ExecuteQueryIn{Query: "SELECT NOT SELECT"}, db)
		require.ErrorContains(t, err, "syntax error")
	})

	t.Run("Empty Result", func(t *testing.T) {
		t.Parallel()
		res, err := ExecuteQuery(t.Context(), ExecuteQueryIn{Query: "SELECT * FROM orders WHERE id = 999"}, db)
		require.NoError(t, err)
		require.Len(t, res.Rows, 0)
	})
}

func TestCreateIndex(t *testing.T) {
	t.Parallel()
	db, err := Connector{}.ConnectRead(SqliteConfig{Path: createFile(t)})
	require.NoError(t, err)
	sqltest.Seed(t, db)

	ix := CreateIndexIn{
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
	t.Parallel()
	db, err := Connector{}.ConnectRead(SqliteConfig{Path: createFile(t)})
	require.NoError(t, err)
	sqltest.Seed(t, db)

	res, err := CreateIndex(t.Context(), CreateIndexIn{
		Table:   "orders",
		Name:    "ix_someindex1",
		Columns: []string{"id", "created_at"},
		Unique:  true,
	}, db)

	require.NoError(t, err)
	require.True(t, res.Success)

	ix, err := DropIndex(t.Context(), DropIndexIn{Name: "ix_someindex1"}, db)
	require.True(t, ix.Success)
	require.NoError(t, err)

	t.Run("NonExistentIndex", func(t *testing.T) {
		ix, err := DropIndex(t.Context(), DropIndexIn{Name: "ix_someindex1"}, db)
		require.Nil(t, ix)
		require.ErrorContains(t, err, "no such index")
	})
}

func TestExplainQuery(t *testing.T) {
	t.Parallel()
	db, err := Connector{}.ConnectRead(SqliteConfig{Path: createFile(t)})
	require.NoError(t, err)
	sqltest.Seed(t, db)

	t.Run("Explain", func(t *testing.T) {
		t.Parallel()
		res, err := ExplainQuery(t.Context(), ExplainQueryIn{Query: "SELECT * FROM orders"}, db)
		require.NoError(t, err)
		require.Greater(t, len(res.Plan), 1)
	})
	t.Run("ExplainQueryPlan", func(t *testing.T) {
		t.Parallel()
		res, err := ExplainQuery(t.Context(), ExplainQueryIn{Query: "SELECT * FROM orders", QueryPlan: true}, db)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(res.Plan), 1)
	})
	t.Run("MalformedQuery", func(t *testing.T) {
		t.Parallel()
		_, err := ExplainQuery(t.Context(), ExplainQueryIn{Query: "SELECT NOT SELECT"}, db)
		require.ErrorContains(t, err, "syntax error")
	})
}
