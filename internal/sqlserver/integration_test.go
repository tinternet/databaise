//go:build integration

package sqlserver

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tinternet/databaise/internal/config"
	"github.com/tinternet/databaise/internal/sqlcommon"
)

var sqlserverTestDSN = os.Getenv("TEST_MSSQL_DSN")

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func openTestConnection(t *testing.T, seed, cleanup string) DB {
	db, err := Connector{}.ConnectAdmin(config.AdminConfig{
		DSN: sqlserverTestDSN,
	})
	require.NoError(t, err)
	if seed != "" {
		require.NoError(t, db.WithContext(t.Context()).Exec(seed).Error)
	}
	t.Cleanup(func() {
		if cleanup != "" {
			db.Exec(cleanup)
		}
		db, _ := db.DB()
		db.Close()
	})
	return db
}

func TestConnectRead(t *testing.T) {
	t.Parallel()
	db, err := Connector{}.ConnectRead(config.ReadConfig{
		DSN:             sqlserverTestDSN,
		EnforceReadonly: new(bool),
	})
	t.Cleanup(func() {
		if err == nil {
			db, _ := db.DB()
			db.Close()
		}
	})
	require.NotNil(t, db)
	require.NoError(t, err)
}

func TestConnectReadEnforceReadonly(t *testing.T) {
	t.Parallel()
	db, err := Connector{}.ConnectRead(config.ReadConfig{
		DSN:             sqlserverTestDSN,
		EnforceReadonly: ptr(true),
	})
	require.Error(t, err)
	require.Nil(t, db)
}

func TestConnectWrite(t *testing.T) {
	t.Parallel()
	db, err := Connector{}.ConnectWrite(config.WriteConfig{
		DSN: sqlserverTestDSN,
	})
	t.Cleanup(func() {
		db, _ := db.DB()
		db.Close()
	})
	require.NotNil(t, db)
	require.NoError(t, err)
}

func TestConnectAdmin(t *testing.T) {
	t.Parallel()
	db, err := Connector{}.ConnectAdmin(config.AdminConfig{
		DSN: sqlserverTestDSN,
	})
	t.Cleanup(func() {
		db, _ := db.DB()
		db.Close()
	})
	require.NotNil(t, db)
	require.NoError(t, err)
}

func TestListTables(t *testing.T) {
	t.Parallel()

	seed := `
		CREATE TABLE dbo.TestListTables (
			ID int,
			Field1 VARCHAR(50) NOT NULL,
			Field2 BIT NOT NULL DEFAULT 1,
			Field3 BIGINT NULL
		);
		CREATE CLUSTERED INDEX ix_clustered ON TestListTables (ID);
	`
	cleanup := `DROP TABLE dbo.TestListTables`
	db := openTestConnection(t, seed, cleanup)

	l, err := ListTables(t.Context(), ListTablesIn{}, db)
	require.NoError(t, err)
	require.Contains(t, l.Tables, sqlcommon.Table{
		Schema: "dbo",
		Name:   "TestListTables",
	})
}

func TestDescribeTable(t *testing.T) {
	t.Parallel()

	seed := `
		CREATE TABLE dbo.TestDescribeTable (
			ID int,
			Field1 VARCHAR(50) NOT NULL,
			Field2 BIT NOT NULL DEFAULT 1,
			Field3 BIGINT NULL
		);
		CREATE CLUSTERED INDEX ix_clustered ON TestDescribeTable (ID);
	`
	cleanup := `DROP TABLE dbo.TestDescribeTable`

	db := openTestConnection(t, seed, cleanup)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		res, err := DescribeTable(t.Context(), DescribeTableIn{
			Schema: "dbo",
			Table:  "TestDescribeTable",
		}, db)

		require.NoError(t, err)
		require.NotNil(t, res)

		expected := &DescribeTableOut{
			Schema: "dbo",
			Name:   "TestDescribeTable",
			Columns: []sqlcommon.Column{
				{
					Name:         "ID",
					DatabaseType: "int",
					IsNullable:   true,
					DefaultValue: nil,
				}, {
					Name:         "Field1",
					DatabaseType: "varchar",
					IsNullable:   false,
					DefaultValue: nil,
				}, {
					Name:         "Field2",
					DatabaseType: "bit",
					IsNullable:   false,
					DefaultValue: ptr("((1))"),
				}, {
					Name:         "Field3",
					DatabaseType: "bigint",
					IsNullable:   true,
					DefaultValue: nil,
				},
			},
			Indexes: []sqlcommon.Index{
				{
					Name:       "ix_clustered",
					Definition: "INDEX ON dbo.TestDescribeTable (ID)",
				},
			},
		}

		require.EqualValues(t, expected, res)
	})
	t.Run("NonExistentTable", func(t *testing.T) {
		t.Parallel()
		res, err := DescribeTable(t.Context(), DescribeTableIn{
			Schema: "dbo",
			Table:  "nonexistend",
		}, db)
		require.Nil(t, res)
		require.ErrorIs(t, sqlcommon.ErrTableNotFound, err)
	})
	t.Run("WithEmptySchema", func(t *testing.T) {
		t.Parallel()
		res, err := DescribeTable(t.Context(), DescribeTableIn{
			Schema: "",
			Table:  "TestDescribeTable",
		}, db)
		require.NotNil(t, res)
		require.NoError(t, err)
		require.Equal(t, "TestDescribeTable", res.Name)
		require.Equal(t, "", res.Schema)
	})
	t.Run("WithEmptySchema", func(t *testing.T) {
		t.Parallel()
		res, err := DescribeTable(t.Context(), DescribeTableIn{
			Schema: "",
			Table:  "TestDescribeTable",
		}, db)
		require.NotNil(t, res)
		require.NoError(t, err)
		require.Equal(t, "TestDescribeTable", res.Name)
		require.Equal(t, "", res.Schema)
	})
	t.Run("BrokenConnection", func(t *testing.T) {
		t.Parallel()
		db := openTestConnection(t, "", "")
		inner, _ := db.DB()
		inner.Close()
		res, err := DescribeTable(t.Context(), DescribeTableIn{}, db)
		require.Nil(t, res)
		require.Error(t, err)
	})
}

func TestExecuteQuery(t *testing.T) {
	t.Parallel()

	seed := `
		IF OBJECT_ID('dbo.TestExecuteQuery ') IS NOT NULL DROP TABLE dbo.TestExecuteQuery 
		CREATE TABLE dbo.TestExecuteQuery (
			ID int,
			Field1 VARCHAR(50) NOT NULL,
			Field2 BIT NOT NULL DEFAULT 1,
			Field3 BIGINT NULL
		);
		INSERT INTO dbo.TestExecuteQuery(id, field1)
		VALUES (1, 'asd'), (2, 'qwe');
	`
	cleanup := `DROP TABLE dbo.TestExecuteQuery`
	db := openTestConnection(t, seed, cleanup)

	res, err := ExecuteQuery(t.Context(), ExecuteQueryIn{Query: "SELECT * FROM dbo.TestExecuteQuery"}, db)
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)

	t.Run("Malformed SQL", func(t *testing.T) {
		_, err := ExecuteQuery(t.Context(), ExecuteQueryIn{Query: "SELECT NOT SELECT"}, db)
		require.Error(t, err)
	})
}

func TestCreateIndex(t *testing.T) {
	t.Parallel()

	seed := `
		IF OBJECT_ID('dbo.TestCreateIndex ') IS NOT NULL DROP TABLE dbo.TestCreateIndex 
		CREATE TABLE dbo.TestCreateIndex (
			ID int,
			Field1 VARCHAR(50) NOT NULL,
			Field2 BIT NOT NULL DEFAULT 1,
			Field3 BIGINT NULL
		);
	`
	cleanup := `DROP TABLE dbo.TestCreateIndex`
	db := openTestConnection(t, seed, cleanup)

	res, err := CreateIndex(t.Context(), CreateIndexIn{
		CreateIndexIn: sqlcommon.CreateIndexIn{
			Schema:  "dbo",
			Table:   "TestCreateIndex",
			Name:    "ix_someindex",
			Columns: []string{"id", "field1"},
			Unique:  true,
		},
		Clustered: true,
	}, db)

	require.NoError(t, err)
	require.True(t, res.Success)

	t.Run("BrokenConnection", func(t *testing.T) {
		t.Parallel()
		db := openTestConnection(t, "", "")
		inner, _ := db.DB()
		inner.Close()
		res, err := CreateIndex(t.Context(), CreateIndexIn{}, db)
		require.False(t, res.Success)
		require.Error(t, err)
	})
}

func TestDropIndex(t *testing.T) {
	t.Parallel()

	seed := `
		IF OBJECT_ID('dbo.TestDropIndex ') IS NOT NULL DROP TABLE dbo.TestDropIndex 
		CREATE TABLE dbo.TestDropIndex (
			ID int,
			Field1 VARCHAR(50) NOT NULL,
			Field2 BIT NOT NULL DEFAULT 1,
			Field3 BIGINT NULL
		);
		CREATE INDEX ix_to_drop ON dbo.TestDropIndex (ID);
	`
	cleanup := `DROP TABLE dbo.TestDropIndex`
	db := openTestConnection(t, seed, cleanup)

	res, err := DropIndex(t.Context(), DropIndexIn{
		Table: "TestDropIndex",
		DropIndexIn: sqlcommon.DropIndexIn{
			Name:   "ix_to_drop",
			Schema: "dbo",
		},
	}, db)

	require.NoError(t, err)
	require.True(t, res.Success)
}

func TestExplainQuery(t *testing.T) {
	t.Parallel()

	seed := `
		IF OBJECT_ID('dbo.TestExplainQuery ') IS NOT NULL DROP TABLE dbo.TestExplainQuery 
		CREATE TABLE dbo.TestExplainQuery (
			ID int,
			Field1 VARCHAR(50) NOT NULL,
			Field2 BIT NOT NULL DEFAULT 1,
			Field3 BIGINT NULL
		);
		INSERT INTO dbo.TestExplainQuery(id, field1)
		VALUES (1, 'asd'), (2, 'qwe');
	`
	cleanup := `DROP TABLE dbo.TestExplainQuery`
	db := openTestConnection(t, seed, cleanup)

	t.Run("Estimated", func(t *testing.T) {
		res, err := ExplainQuery(t.Context(), ExplainQueryIn{Query: "SELECT * FROM dbo.TestExplainQuery", ActualExecutionPlan: false}, db)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Contains(t, res.Plan, "<ShowPlanXML")
	})
	t.Run("Actual", func(t *testing.T) {
		res, err := ExplainQuery(t.Context(), ExplainQueryIn{Query: "SELECT id FROM dbo.TestExplainQuery", ActualExecutionPlan: true}, db)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Contains(t, res.Plan, "<ShowPlanXML")
		require.Equal(t, []map[string]any{{"id": int64(1)}, {"id": int64(2)}}, res.Rows)
	})
}

func ptr[T any](v T) *T {
	return &v
}
