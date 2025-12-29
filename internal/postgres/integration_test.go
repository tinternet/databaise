//go:build integration

package postgres

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tinternet/databaise/internal/config"
	"github.com/tinternet/databaise/internal/sqlcommon"
)

var postgresTestDSN = os.Getenv("TEST_POSTGRES_DSN")

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func openTestConnection(t *testing.T, seed, cleanup string) DB {
	db, err := Connector{}.ConnectAdmin(config.AdminConfig{
		DSN: postgresTestDSN,
	})
	require.NoError(t, err)
	if seed != "" {
		require.NoError(t, db.WithContext(t.Context()).Exec(seed).Error)
	}
	t.Cleanup(func() {
		if cleanup != "" {
			db.Exec(cleanup)
		}

		db, _ := db.DB.DB()
		db.Close()
	})
	return db
}

func TestConnectRead(t *testing.T) {
	t.Parallel()
	db, err := Connector{}.ConnectRead(ReadConfig{
		ReadConfig: config.ReadConfig{
			DSN:             postgresTestDSN,
			EnforceReadonly: new(bool),
		},
	})
	t.Cleanup(func() {
		if err == nil {
			db, _ := db.DB.DB()
			db.Close()
		}
	})
	require.NotNil(t, db)
	require.NoError(t, err)
}

func TestConnectReadEnforceReadonly(t *testing.T) {
	t.Parallel()
	db, err := Connector{}.ConnectRead(ReadConfig{
		ReadConfig: config.ReadConfig{
			DSN:             postgresTestDSN,
			EnforceReadonly: ptr(true),
		},
		UseReadonlyTx: false,
	})
	require.Error(t, err)
	require.Nil(t, db.DB)
}

func TestConnectWrite(t *testing.T) {
	t.Parallel()
	db, err := Connector{}.ConnectWrite(config.WriteConfig{
		DSN: postgresTestDSN,
	})
	t.Cleanup(func() {
		if db.DB != nil {
			db, _ := db.DB.DB()
			db.Close()
		}
	})
	require.NotNil(t, db)
	require.NoError(t, err)
}

func TestConnectAdmin(t *testing.T) {
	t.Parallel()
	db, err := Connector{}.ConnectAdmin(config.AdminConfig{
		DSN: postgresTestDSN,
	})
	t.Cleanup(func() {
		db, _ := db.DB.DB()
		db.Close()
	})
	require.NotNil(t, db)
	require.NoError(t, err)
}

func TestListTables(t *testing.T) {
	t.Parallel()

	seed := `
		CREATE TABLE public.TestListTables (
			ID int,
			Field1 VARCHAR(50) NOT NULL,
			Field2 BOOLEAN NOT NULL DEFAULT TRUE,
			Field3 BIGINT NULL
		);
		CREATE INDEX ix_table ON TestListTables (ID);
	`
	cleanup := `DROP TABLE public.TestListTables`
	db := openTestConnection(t, seed, cleanup)

	l, err := ListTables(t.Context(), ListTablesIn{}, db)
	require.NoError(t, err)
	require.Contains(t, l.Tables, sqlcommon.Table{
		Schema: "public",
		Name:   "testlisttables",
	})
}

func TestDescribeTable(t *testing.T) {
	t.Parallel()

	seed := `
		CREATE TABLE public.TestDescribeTable (
			ID int,
			Field1 VARCHAR(50) NOT NULL,
			Field2 BOOLEAN NOT NULL DEFAULT TRUE,
			Field3 BIGINT NULL
		);
		CREATE INDEX ix_clustered ON TestDescribeTable (ID);
	`
	cleanup := `DROP TABLE public.TestDescribeTable`

	db := openTestConnection(t, seed, cleanup)

	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		res, err := DescribeTable(t.Context(), DescribeTableIn{
			Schema: "public",
			Table:  "testdescribetable",
		}, db)

		require.NoError(t, err)
		require.NotNil(t, res)

		expected := &DescribeTableOut{
			Schema: "public",
			Name:   "testdescribetable",
			Columns: []sqlcommon.Column{
				{
					Name:         "id",
					DatabaseType: "integer",
					IsNullable:   true,
					DefaultValue: nil,
				}, {
					Name:         "field1",
					DatabaseType: "character varying",
					IsNullable:   false,
					DefaultValue: nil,
				}, {
					Name:         "field2",
					DatabaseType: "boolean",
					IsNullable:   false,
					DefaultValue: ptr("true"),
				}, {
					Name:         "field3",
					DatabaseType: "bigint",
					IsNullable:   true,
					DefaultValue: nil,
				},
			},
			Indexes: []sqlcommon.Index{
				{
					Name:       "ix_clustered",
					Definition: "CREATE INDEX ix_clustered ON public.testdescribetable USING btree (id)",
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
			Table:  "testdescribetable",
		}, db)
		require.NotNil(t, res)
		require.NoError(t, err)
		require.Equal(t, "testdescribetable", res.Name)
		require.Equal(t, "", res.Schema)
	})
	t.Run("WithEmptySchema", func(t *testing.T) {
		t.Parallel()
		res, err := DescribeTable(t.Context(), DescribeTableIn{
			Schema: "",
			Table:  "testdescribetable",
		}, db)
		require.NotNil(t, res)
		require.NoError(t, err)
		require.Equal(t, "testdescribetable", res.Name)
		require.Equal(t, "", res.Schema)
	})
	t.Run("BrokenConnection", func(t *testing.T) {
		t.Parallel()
		db := openTestConnection(t, "", "")
		inner, _ := db.DB.DB()
		inner.Close()
		res, err := DescribeTable(t.Context(), DescribeTableIn{}, db)
		require.Nil(t, res)
		require.Error(t, err)
	})
}

func TestExecuteQuery(t *testing.T) {
	t.Parallel()

	seed := `
		CREATE TABLE public.TestExecuteQuery (
			ID int,
			Field1 VARCHAR(50) NOT NULL,
			Field2 BOOLEAN NOT NULL DEFAULT TRUE,
			Field3 BIGINT NULL
		);
		INSERT INTO public.TestExecuteQuery(id, field1)
		VALUES (1, 'asd'), (2, 'qwe');
	`
	cleanup := `DROP TABLE public.TestExecuteQuery`
	db := openTestConnection(t, seed, cleanup)

	res, err := ExecuteQuery(t.Context(), ExecuteQueryIn{Query: "SELECT * FROM public.TestExecuteQuery"}, db)
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
		CREATE TABLE public.TestCreateIndex (
			ID int,
			Field1 VARCHAR(50) NOT NULL,
			Field2 BOOLEAN NOT NULL DEFAULT TRUE,
			Field3 BIGINT NULL
		);
	`
	cleanup := `DROP TABLE public.TestCreateIndex`
	db := openTestConnection(t, seed, cleanup)

	res, err := CreateIndex(t.Context(), CreateIndexIn{
		Schema:  "public",
		Table:   "testcreateindex",
		Name:    "ix_someindex1",
		Columns: []string{"id", "field1"},
		Unique:  true,
	}, db)

	require.NoError(t, err)
	require.True(t, res.Success)

	t.Run("BrokenConnection", func(t *testing.T) {
		t.Parallel()
		db := openTestConnection(t, "", "")
		inner, _ := db.DB.DB()
		inner.Close()
		res, err := CreateIndex(t.Context(), CreateIndexIn{}, db)
		require.False(t, res.Success)
		require.Error(t, err)
	})
}

func TestReadonlyTransactionEnforcement(t *testing.T) {
	t.Parallel()

	seed := `
		CREATE TABLE public.TestReadonlyTransactionEnforcement (
			ID int,
			Field1 VARCHAR(50) NOT NULL
		);
		INSERT INTO public.TestReadonlyTransactionEnforcement(id, field1)
		VALUES (1, 'asd'), (2, 'qwe');
	`
	cleanup := `DROP TABLE public.TestReadonlyTransactionEnforcement`
	openTestConnection(t, seed, cleanup)
	db, err := Connector{}.ConnectRead(ReadConfig{
		ReadConfig: config.ReadConfig{
			DSN:             postgresTestDSN,
			EnforceReadonly: ptr(true),
		},
		UseReadonlyTx: true,
	})
	require.NoError(t, err)

	t.Run("ReadOnlyTransactionAllowed", func(t *testing.T) {
		t.Parallel()
		_, err := ExecuteQuery(t.Context(), ExecuteQueryIn{Query: "SELECT * FROM public.TestReadonlyTransactionEnforcement"}, db)
		require.NoError(t, err)
	})

	t.Run("WriteInReadOnlyTransaction", func(t *testing.T) {
		t.Parallel()
		res, err := ExecuteQuery(t.Context(), ExecuteQueryIn{Query: "DELETE FROM public.TestReadonlyTransactionEnforcement"}, db)
		require.Error(t, err)
		require.Nil(t, res)
	})

	t.Run("Bypass readonly tx", func(t *testing.T) {
		t.Parallel()
		res, err := ExecuteQuery(t.Context(), ExecuteQueryIn{Query: "COMMIT; DELETE FROM public.TestReadonlyTransactionEnforcement"}, db)
		require.Error(t, err)
		require.Nil(t, res)
	})
}

func TestDropIndex(t *testing.T) {
	t.Parallel()

	seed := `
		CREATE TABLE public.TestDropIndex (
			ID int,
			Field1 VARCHAR(50) NOT NULL,
			Field2 BOOLEAN NOT NULL DEFAULT TRUE,
			Field3 BIGINT NULL
		);
		CREATE INDEX ix_to_drop ON public.TestDropIndex (ID);
	`
	cleanup := `DROP TABLE public.TestDropIndex`
	db := openTestConnection(t, seed, cleanup)

	res, err := DropIndex(t.Context(), sqlcommon.DropIndexIn{
		Schema: "public",
		Name:   "ix_to_drop",
	}, db)

	require.NoError(t, err)
	require.True(t, res.Success)
}

func ptr[T any](v T) *T {
	return &v
}
