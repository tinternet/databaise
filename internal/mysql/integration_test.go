//go:build integration

package mysql

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tinternet/databaise/internal/config"
	"github.com/tinternet/databaise/internal/provision"
	"github.com/tinternet/databaise/internal/sqltest"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func openTestConnection(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := sqltest.SetupMySqlContainer(t)
	db, err := gorm.Open(mysql.Open(dsn+"&parseTime=true"), sqltest.GormConfig(t))
	sqltest.Seed(t, db)
	require.NoError(t, err)
	return db
}

func setCredentials(t *testing.T, dsn, user, pass string) string {
	t.Helper()
	parts := strings.SplitN(dsn, "@", 2)
	require.Len(t, parts, 2)
	return fmt.Sprintf("%s:%s@%s", user, pass, parts[1])
}

func TestConnect(t *testing.T) {
	t.Parallel()
	dsn := sqltest.SetupMySqlContainer(t)

	t.Run("ReadOnly", func(t *testing.T) {
		t.Parallel()
		provisioner := provision.MySqlProvisioner{}
		require.NoError(t, provisioner.Connect(dsn))
		require.NoError(t, provisioner.CreateUser(t.Context(), "testuser", "testpass"))
		// MySql needs grant to the database
		require.NoError(t, provisioner.GrantReadOnly(t.Context(), "testuser", provision.AccessScope{Groups: []string{"test"}}))
		rodsn := setCredentials(t, dsn, "testuser", "testpass")
		db, err := Connector{}.ConnectRead(ReadConfig{ReadConfig: config.ReadConfig{DSN: rodsn}})
		require.NoError(t, err)
		require.NotNil(t, db)
	})

	t.Run("ReadOnly With EnforceReadonly=true", func(t *testing.T) {
		t.Parallel()
		db, err := Connector{}.ConnectRead(ReadConfig{ReadConfig: config.ReadConfig{DSN: dsn, EnforceReadonly: ptr(true)}})
		require.Nil(t, db)
		require.ErrorContains(t, err, "read DSN user has write permissions")
	})

	t.Run("ReadOnly With EnforceReadonly=false", func(t *testing.T) {
		t.Parallel()
		db, err := Connector{}.ConnectRead(ReadConfig{ReadConfig: config.ReadConfig{DSN: dsn, EnforceReadonly: ptr(false)}})
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
	require.ElementsMatch(t, l.Tables, []string{"orders", "users"})
}

func TestDescribeTable(t *testing.T) {
	t.Parallel()
	db := openTestConnection(t)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		res, err := DescribeTable(t.Context(), DescribeTableIn{Table: "orders"}, db)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Contains(t, res.CreateTable, "CREATE TABLE")
		require.Contains(t, res.CreateTable, "KEY") // indexes are included in the table DDL
	})
	t.Run("NonExistentTable", func(t *testing.T) {
		t.Parallel()
		res, err := DescribeTable(t.Context(), DescribeTableIn{Table: "nonexistend"}, db)
		require.Nil(t, res)
		require.ErrorContains(t, err, "doesn't exist")
	})
}

func TestExecuteQuery(t *testing.T) {
	t.Parallel()
	db := openTestConnection(t)

	res, err := ExecuteQuery(t.Context(), ExecuteQueryIn{Query: "SELECT * FROM test.orders"}, db)
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)

	t.Run("Malformed SQL", func(t *testing.T) {
		_, err := ExecuteQuery(t.Context(), ExecuteQueryIn{Query: "SELECT NOT SELECT"}, db)
		require.ErrorContains(t, err, "You have an error in your SQL")
	})

	t.Run("Empty Result", func(t *testing.T) {
		t.Parallel()
		res, err := ExecuteQuery(t.Context(), ExecuteQueryIn{Query: "SELECT * FROM test.orders WHERE id = 999"}, db)
		require.NoError(t, err)
		require.Len(t, res.Rows, 0)
	})
}

func TestCreateIndex(t *testing.T) {
	t.Parallel()
	db := openTestConnection(t)

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
		require.ErrorContains(t, err, "Duplicate key name")
	})
}

func TestDropIndex(t *testing.T) {
	db := openTestConnection(t)

	res, err := CreateIndex(t.Context(), CreateIndexIn{
		Table:   "orders",
		Name:    "ix_someindex1",
		Columns: []string{"id", "created_at"},
		Unique:  true,
	}, db)

	require.NoError(t, err)
	require.True(t, res.Success)

	ix, err := DropIndex(t.Context(), DropIndexIn{Name: "ix_someindex1", Table: "orders"}, db)
	require.NoError(t, err)
	require.True(t, ix.Success)

	t.Run("NonExistentIndex", func(t *testing.T) {
		ix, err := DropIndex(t.Context(), DropIndexIn{Name: "ix_someindex1", Table: "orders"}, db)
		require.Nil(t, ix)
		require.ErrorContains(t, err, "Can't DROP")
	})
}

func TestExplainQuery(t *testing.T) {
	t.Parallel()
	db := openTestConnection(t)

	t.Run("Explain", func(t *testing.T) {
		t.Parallel()
		res, err := ExplainQuery(t.Context(), ExplainQueryIn{Query: "SELECT * FROM test.orders"}, db)
		require.NoError(t, err)
		require.Greater(t, len(res.Plan), 1)
		t.Logf("%v", res.Plan)
	})
	t.Run("ExplainQueryPlan", func(t *testing.T) {
		t.Parallel()
		res, err := ExplainQuery(t.Context(), ExplainQueryIn{Query: "SELECT * FROM test.orders", Analyze: true}, db)
		require.NoError(t, err)
		require.Greater(t, len(res.Plan), 1)
		t.Logf("%v", res.Plan)
	})

	t.Run("MalformedQuery", func(t *testing.T) {
		t.Parallel()
		res, err := ExplainQuery(t.Context(), ExplainQueryIn{Query: "SELECT NOT SELECT"}, db)
		require.Nil(t, res)
		require.ErrorContains(t, err, "You have an error in your SQL")
	})
}

func TestListMissingIndexes(t *testing.T) {
	t.Parallel()
	db := openTestConnection(t)

	_, err := ListMissingIndexes(t.Context(), struct{}{}, db)
	require.NoError(t, err)
}

func TestListWaitingQueries(t *testing.T) {
	t.Parallel()
	db := openTestConnection(t)

	_, err := ListWaitingQueries(t.Context(), struct{}{}, db)
	require.NoError(t, err)
}

func TestListSlowestQueries(t *testing.T) {
	t.Parallel()
	db := openTestConnection(t)

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
	require.NotNil(t, res.DeadlockInfo)
}

func ptr[T any](v T) *T {
	return &v
}
