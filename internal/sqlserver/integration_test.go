//go:build integration

package sqlserver

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tinternet/databaise/internal/config"
	"github.com/tinternet/databaise/internal/provision"
	"github.com/tinternet/databaise/internal/sqlcommon"
	"github.com/tinternet/databaise/internal/sqltest"
)

func openTestConnection(t *testing.T) DB {
	t.Helper()
	dsn := sqltest.SetupSqlServerContainer(t)
	db, err := Connector{}.ConnectAdmin(AdminConfig{AdminConfig: config.AdminConfig{DSN: dsn}})
	sqltest.Seed(t, db)
	require.NoError(t, err)
	return db
}

func TestConnect(t *testing.T) {
	t.Parallel()
	dsn := sqltest.SetupSqlServerContainer(t)

	t.Run("ReadOnly", func(t *testing.T) {
		t.Parallel()
		provisioner := provision.SqlServerProvisioner{}
		require.NoError(t, provisioner.Connect(dsn))
		password, err := provision.GeneratePassword()
		require.NoError(t, err)
		require.NoError(t, provisioner.CreateUser(t.Context(), "testuser", password))
		rodsn := sqltest.ReplaceURLCredentials(t, dsn, "testuser", password)
		db, err := Connector{}.ConnectRead(ReadConfig{ReadConfig: config.ReadConfig{DSN: rodsn}})
		require.NotNil(t, db)
		require.NoError(t, err)
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
	assert.Contains(t, l.Tables, sqlcommon.Table{Schema: "dbo", Name: "orders"})
	assert.Contains(t, l.Tables, sqlcommon.Table{Schema: "dbo", Name: "users"})
}

func TestDescribeTable(t *testing.T) {
	t.Parallel()
	db := openTestConnection(t)

	t.Run("ColumnTypes", func(t *testing.T) {
		t.Parallel()
		res, err := DescribeTable(t.Context(), DescribeTableIn{Schema: "dbo", Table: "orders"}, db)

		require.NoError(t, err)
		require.NotNil(t, res)

		assert.Equal(t, "dbo", res.Schema)
		assert.Equal(t, "orders", res.Name)

		columns := []sqlcommon.Column{
			{Name: "id", DatabaseType: "bigint", IsNullable: false, DefaultValue: nil},
			{Name: "created_at", DatabaseType: "datetimeoffset", IsNullable: true, DefaultValue: nil},
			{Name: "updated_at", DatabaseType: "datetimeoffset", IsNullable: true, DefaultValue: nil},
			{Name: "deleted_at", DatabaseType: "datetimeoffset", IsNullable: true, DefaultValue: nil},
			{Name: "order_code", DatabaseType: "nvarchar", IsNullable: false, DefaultValue: nil},
			{Name: "amount", DatabaseType: "float", IsNullable: false, DefaultValue: nil},
			{Name: "user_id", DatabaseType: "bigint", IsNullable: false, DefaultValue: nil},
			{Name: "shipped_at", DatabaseType: "datetimeoffset", IsNullable: true, DefaultValue: nil},
		}
		assert.EqualValues(t, columns, res.Columns)

		require.Len(t, res.Indexes, 4)
		require.True(t, slices.ContainsFunc(res.Indexes, func(idx sqlcommon.Index) bool {
			return strings.HasPrefix(idx.Name, "PK__orders__") && strings.HasPrefix(idx.Definition, "CREATE UNIQUE CLUSTERED INDEX")
		}))
		// assert.Contains(t, res.Indexes, sqlcommon.Index{Name: "PK__orders__3213E83FFC9F3B19", Definition: "CREATE UNIQUE CLUSTERED INDEX [PK__orders__3213E83FFC9F3B19] ON [dbo].[orders] ([id])"})
		assert.Contains(t, res.Indexes, sqlcommon.Index{Name: "idx_orders_user_id", Definition: "CREATE NONCLUSTERED INDEX [idx_orders_user_id] ON [dbo].[orders] ([user_id])"})
		assert.Contains(t, res.Indexes, sqlcommon.Index{Name: "idx_orders_order_code", Definition: "CREATE UNIQUE NONCLUSTERED INDEX [idx_orders_order_code] ON [dbo].[orders] ([order_code])"})
		assert.Contains(t, res.Indexes, sqlcommon.Index{Name: "idx_orders_deleted_at", Definition: "CREATE NONCLUSTERED INDEX [idx_orders_deleted_at] ON [dbo].[orders] ([deleted_at])"})
	})
	t.Run("NonExistentTable", func(t *testing.T) {
		t.Parallel()
		res, err := DescribeTable(t.Context(), DescribeTableIn{Schema: "dbo", Table: "nonexistend"}, db)
		require.Nil(t, res)
		require.ErrorIs(t, sqlcommon.ErrTableNotFound, err)
	})
	t.Run("WithEmptySchema", func(t *testing.T) {
		t.Parallel()
		res, err := DescribeTable(t.Context(), DescribeTableIn{Schema: "", Table: "orders"}, db)
		require.NotNil(t, res)
		require.NoError(t, err)
		require.Equal(t, "orders", res.Name)
		require.Equal(t, "", res.Schema)
	})
}

func TestExecuteQuery(t *testing.T) {
	t.Parallel()
	db := openTestConnection(t)

	res, err := ExecuteQuery(t.Context(), ExecuteQueryIn{Query: "SELECT * FROM dbo.orders"}, db)
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)

	t.Run("Malformed SQL", func(t *testing.T) {
		_, err := ExecuteQuery(t.Context(), ExecuteQueryIn{Query: "SELECT NOT SELECT"}, db)
		require.ErrorContains(t, err, "Incorrect syntax near")
	})

	t.Run("Empty Result", func(t *testing.T) {
		t.Parallel()
		res, err := ExecuteQuery(t.Context(), ExecuteQueryIn{Query: "SELECT * FROM dbo.orders WHERE id = 999"}, db)
		require.NoError(t, err)
		require.Len(t, res.Rows, 0)
	})
}

func TestCreateIndex(t *testing.T) {
	t.Parallel()
	db := openTestConnection(t)

	ix := CreateIndexIn{
		CreateIndexIn: sqlcommon.CreateIndexIn{
			Schema:  "dbo",
			Table:   "orders",
			Name:    "ix_someindex1",
			Columns: []string{"id", "created_at"},
			Unique:  true,
		},
		Clustered: false,
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
		CreateIndexIn: sqlcommon.CreateIndexIn{
			Schema:  "dbo",
			Table:   "orders",
			Name:    "ix_someindex1",
			Columns: []string{"id", "created_at"},
			Unique:  true,
		},
		Clustered: false,
	}, db)

	require.NoError(t, err)
	require.True(t, res.Success)

	drop := DropIndexIn{
		DropIndexIn: sqlcommon.DropIndexIn{
			Schema: "dbo",
			Name:   "ix_someindex1",
		},
		Table: "orders",
	}

	ix, err := DropIndex(t.Context(), drop, db)
	require.True(t, ix.Success)
	require.NoError(t, err)

	t.Run("NonExistentIndex", func(t *testing.T) {
		ix, err := DropIndex(t.Context(), drop, db)
		require.Nil(t, ix)
		require.ErrorContains(t, err, "does not exist")
	})
}

func TestExplainQuery(t *testing.T) {
	t.Parallel()

	db := openTestConnection(t)

	t.Run("Estimated", func(t *testing.T) {
		res, err := ExplainQuery(t.Context(), ExplainQueryIn{Query: "SELECT * FROM dbo.orders", ActualExecutionPlan: false}, db)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Contains(t, res.Plan, "<ShowPlanXML")
	})
	t.Run("Actual", func(t *testing.T) {
		res, err := ExplainQuery(t.Context(), ExplainQueryIn{Query: "SELECT id FROM dbo.orders", ActualExecutionPlan: true}, db)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Contains(t, res.Plan, "<ShowPlanXML")
		require.Equal(t, []map[string]any{{"id": int64(1)}, {"id": int64(2)}}, res.Rows)
	})
}

func TestListMissingIndexes(t *testing.T) {
	t.Parallel()
	db := openTestConnection(t)
	_, err := ListMissingIndexes(t.Context(), struct{}{}, db)
	require.NoError(t, err)
}

func TestCreateIndex_BrokenConnection(t *testing.T) {
	t.Parallel()
	db := openTestConnection(t)
	inner, _ := db.DB()
	inner.Close()
	res, err := CreateIndex(t.Context(), CreateIndexIn{}, db)
	require.Nil(t, res)
	require.Error(t, err)
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
	_, err := ListSlowestQueries(t.Context(), struct{}{}, db)
	require.NoError(t, err)
}

func TestListDeadlocks(t *testing.T) {
	t.Parallel()
	db := openTestConnection(t)
	_, err := ListDeadlocks(t.Context(), struct{}{}, db)
	require.NoError(t, err)
}

func ptr[T any](v T) *T {
	return &v
}
