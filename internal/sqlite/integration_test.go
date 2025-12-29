//go:build integration

package sqlite

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tinternet/databaise/internal/sqlcommon"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func openTestConnection(t *testing.T, seed, path string) DB {
	db, err := Connector{}.ConnectAdmin(AdminConfig{
		Path: path,
	})
	require.NoError(t, err)
	if seed != "" {
		require.NoError(t, db.WithContext(t.Context()).Exec(seed).Error)
	}
	t.Cleanup(func() {
		db, _ := db.DB()
		db.Close()
		os.Remove(path)
	})
	return db
}

func TestConnectRead(t *testing.T) {
	t.Parallel()
	db, err := Connector{}.ConnectRead(ReadConfig{
		Path: os.TempDir() + "/test_readonly.db",
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

func TestConnectWrite(t *testing.T) {
	t.Parallel()
	db, err := Connector{}.ConnectWrite(WriteConfig{
		Path: os.TempDir() + "/test_write.db",
	})
	require.NoError(t, err)
	require.NotNil(t, db)
}

func TestConnectAdmin(t *testing.T) {
	t.Parallel()
	db, err := Connector{}.ConnectAdmin(AdminConfig{
		Path: os.TempDir() + "/test_admin.db",
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
		CREATE TABLE TestListTables (
			ID int,
			Field1 TEXT NOT NULL,
			Field2 BIT NOT NULL DEFAULT 1,
			Field3 BIGINT NULL
		);
		CREATE INDEX ix_table ON TestListTables (ID);
	`
	db := openTestConnection(t, seed, os.TempDir()+"/test_list_tables.db")

	l, err := ListTables(t.Context(), ListTablesIn{}, db)
	require.NoError(t, err)
	require.Contains(t, l.Tables, "TestListTables")
}

func TestDescribeTable(t *testing.T) {
	t.Parallel()

	seed := `
		CREATE TABLE TestDescribeTable (
			ID int,
			Field1 TEXT NOT NULL,
			Field2 BIT NOT NULL DEFAULT 1,
			Field3 BIGINT NULL
		);
		CREATE INDEX ix_table ON TestDescribeTable (ID);
	`

	db := openTestConnection(t, seed, os.TempDir()+"/test_describe_table.db")

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		res, err := DescribeTable(t.Context(), DescribeTableIn{
			Table: "TestDescribeTable",
		}, db)

		require.NoError(t, err)
		require.NotNil(t, res)

		expected := &DescribeTableOut{
			Name: "TestDescribeTable",
			Columns: []sqlcommon.Column{
				{
					Name:         "ID",
					DatabaseType: "INT",
					IsNullable:   true,
					DefaultValue: nil,
				}, {
					Name:         "Field1",
					DatabaseType: "TEXT",
					IsNullable:   false,
					DefaultValue: nil,
				}, {
					Name:         "Field2",
					DatabaseType: "BIT",
					IsNullable:   false,
					DefaultValue: ptr("1"),
				}, {
					Name:         "Field3",
					DatabaseType: "BIGINT",
					IsNullable:   true,
					DefaultValue: nil,
				},
			},
			Indexes: []sqlcommon.Index{
				{
					Name:       "ix_table",
					Definition: "CREATE INDEX ix_table ON TestDescribeTable (ID)",
				},
			},
		}

		require.EqualValues(t, expected, res)
	})
	t.Run("NonExistentTable", func(t *testing.T) {
		t.Parallel()
		res, err := DescribeTable(t.Context(), DescribeTableIn{
			Table: "nonexistend",
		}, db)
		require.Nil(t, res)
		require.ErrorIs(t, sqlcommon.ErrTableNotFound, err)
	})
	t.Run("WithEmptySchema", func(t *testing.T) {
		t.Parallel()
		res, err := DescribeTable(t.Context(), DescribeTableIn{
			Table: "TestDescribeTable",
		}, db)
		require.NotNil(t, res)
		require.NoError(t, err)
		require.Equal(t, "TestDescribeTable", res.Name)
	})
	t.Run("WithEmptySchema", func(t *testing.T) {
		t.Parallel()
		res, err := DescribeTable(t.Context(), DescribeTableIn{
			Table: "TestDescribeTable",
		}, db)
		require.NotNil(t, res)
		require.NoError(t, err)
		require.Equal(t, "TestDescribeTable", res.Name)
	})
	t.Run("BrokenConnection", func(t *testing.T) {
		t.Parallel()
		db := openTestConnection(t, "", os.TempDir()+"/test_broken_connection.db")
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
		CREATE TABLE TestExecuteQuery (
			ID int,
			Field1 TEXT NOT NULL,
			Field2 BIT NOT NULL DEFAULT 1,
			Field3 BIGINT NULL
		);
		INSERT INTO TestExecuteQuery(id, field1)
		VALUES (1, 'asd'), (2, 'qwe');
	`
	db := openTestConnection(t, seed, os.TempDir()+"/test_execute_query.db")

	res, err := ExecuteQuery(t.Context(), ExecuteQueryIn{Query: "SELECT * FROM TestExecuteQuery"}, db)
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
		CREATE TABLE TestCreateIndex (
			ID int,
			Field1 VARCHAR(50) NOT NULL,
			Field2 BIT NOT NULL DEFAULT 1,
			Field3 BIGINT NULL
		);
	`
	db := openTestConnection(t, seed, os.TempDir()+"/test_create_index.db")

	res, err := CreateIndex(t.Context(), CreateIndexIn{
		Table:   "TestCreateIndex",
		Name:    "ix_someindex",
		Columns: []string{"id", "field1"},
		Unique:  true,
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

func ptr[T any](v T) *T {
	return &v
}
