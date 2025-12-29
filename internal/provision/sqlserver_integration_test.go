//go:build integration

package provision

import (
	"testing"

	"github.com/tinternet/databaise/internal/sqlcommon"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
)

func setupSQLServer() {
	if sqlserverTestDSN == "" {
		return
	}

	// Normalize DSN for go-mssqldb driver
	normalizedDSN := normalizeSQLServerDSN(sqlserverTestDSN)

	db, err := gorm.Open(sqlserver.Open(normalizedDSN), &gorm.Config{})
	if err != nil {
		return
	}
	defer func() {
		sqlDB, _ := db.DB()
		sqlDB.Close()
	}()

	// Drop existing tables if any
	db.Exec(`DROP TABLE IF EXISTS test_data`)
	db.Exec(`DROP TABLE IF EXISTS secret_data`)

	// Create test tables
	db.Exec(`CREATE TABLE test_data (id INT IDENTITY(1,1) PRIMARY KEY, name NVARCHAR(100), value INT)`)
	db.Exec(`INSERT INTO test_data (name, value) VALUES ('alice', 100), ('bob', 200), ('charlie', 300)`)
	db.Exec(`CREATE TABLE secret_data (id INT IDENTITY(1,1) PRIMARY KEY, secret NVARCHAR(100))`)
	db.Exec(`INSERT INTO secret_data (secret) VALUES ('top_secret')`)
}

func cleanupSQLServer() {
	if sqlserverTestDSN == "" {
		return
	}

	normalizedDSN := normalizeSQLServerDSN(sqlserverTestDSN)

	db, err := gorm.Open(sqlserver.Open(normalizedDSN), &gorm.Config{})
	if err != nil {
		return
	}
	defer func() {
		sqlDB, _ := db.DB()
		sqlDB.Close()
	}()

	db.Exec(`DROP TABLE IF EXISTS test_data`)
	db.Exec(`DROP TABLE IF EXISTS secret_data`)
}

func TestSQLServer_SchemaScope_AllTables(t *testing.T) {
	if sqlserverTestDSN == "" {
		t.Skip("TEST_MSSQL_DSN not set")
	}

	provisioner, ok := Get("sqlserver")
	if !ok {
		t.Fatal("sqlserver provisioner not registered")
	}

	result, err := provisioner.Provision(sqlserverTestDSN, Options{
		Schemas: map[string][]string{"dbo": {}}, // all tables in dbo schema
	})
	if err != nil {
		t.Fatalf("failed to provision: %v", err)
	}
	t.Logf("Provisioned user: %s", result.User)

	db, err := gorm.Open(sqlserver.Open(result.DSN), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to connect with readonly user: %v", err)
	}
	defer func() {
		closeAndWait(db)
		provisioner.Revoke(sqlserverTestDSN, result.User)
	}()

	// Verify readonly
	var isReadonly int
	if err := db.Raw(sqlcommon.SQLServerVerifyReadonlySQL).Scan(&isReadonly).Error; err != nil {
		t.Fatalf("failed to run verify_readonly check: %v", err)
	}
	if isReadonly != 1 {
		t.Fatal("verify_readonly.sql reports user is NOT readonly")
	}
	t.Log("verify_readonly.sql confirms user is readonly")

	// Can read from test_data
	var count int64
	if err := db.Raw("SELECT COUNT(*) FROM test_data").Scan(&count).Error; err != nil {
		t.Fatalf("failed to read from test_data: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 rows, got %d", count)
	}
	t.Logf("Can read test_data: %d rows", count)

	// Can read from secret_data (schema-wide access)
	if err := db.Raw("SELECT COUNT(*) FROM secret_data").Scan(&count).Error; err != nil {
		t.Fatalf("failed to read from secret_data: %v", err)
	}
	t.Logf("Can read secret_data: %d rows", count)

	// Cannot INSERT
	if err := db.Exec("INSERT INTO test_data (name, value) VALUES ('hacker', 999)").Error; err == nil {
		t.Fatal("INSERT should have been blocked")
	}
	t.Log("INSERT blocked")

	// Cannot UPDATE
	if err := db.Exec("UPDATE test_data SET value = 0 WHERE name = 'alice'").Error; err == nil {
		t.Fatal("UPDATE should have been blocked")
	}
	t.Log("UPDATE blocked")

	// Cannot DELETE
	if err := db.Exec("DELETE FROM test_data WHERE name = 'alice'").Error; err == nil {
		t.Fatal("DELETE should have been blocked")
	}
	t.Log("DELETE blocked")

	// Cannot CREATE TABLE
	if err := db.Exec("CREATE TABLE evil_table (id int)").Error; err == nil {
		db.Exec("DROP TABLE evil_table")
		t.Fatal("CREATE TABLE should have been blocked")
	}
	t.Log("CREATE TABLE blocked")
}

func TestSQLServer_SchemaScope_SpecificTables(t *testing.T) {
	if sqlserverTestDSN == "" {
		t.Skip("TEST_MSSQL_DSN not set")
	}

	provisioner, ok := Get("sqlserver")
	if !ok {
		t.Fatal("sqlserver provisioner not registered")
	}

	result, err := provisioner.Provision(sqlserverTestDSN, Options{
		Schemas: map[string][]string{
			"dbo": {"test_data"}, // only test_data, not secret_data
		},
	})
	if err != nil {
		t.Fatalf("failed to provision: %v", err)
	}
	t.Logf("Provisioned user: %s", result.User)

	db, err := gorm.Open(sqlserver.Open(result.DSN), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to connect with readonly user: %v", err)
	}
	defer func() {
		closeAndWait(db)
		provisioner.Revoke(sqlserverTestDSN, result.User)
	}()

	// Can read from test_data (granted)
	var count int64
	if err := db.Raw("SELECT COUNT(*) FROM test_data").Scan(&count).Error; err != nil {
		t.Fatalf("failed to read from test_data: %v", err)
	}
	t.Logf("Can read test_data: %d rows", count)

	// Cannot read from secret_data (not granted)
	if err := db.Raw("SELECT COUNT(*) FROM secret_data").Scan(&count).Error; err == nil {
		t.Fatal("should NOT be able to read secret_data")
	}
	t.Log("Cannot read secret_data (as expected)")
}

func TestSQLServer_Update(t *testing.T) {
	if sqlserverTestDSN == "" {
		t.Skip("TEST_MSSQL_DSN not set")
	}

	provisioner, ok := Get("sqlserver")
	if !ok {
		t.Fatal("sqlserver provisioner not registered")
	}

	// First provision with only test_data
	result, err := provisioner.Provision(sqlserverTestDSN, Options{
		Schemas: map[string][]string{
			"dbo": {"test_data"},
		},
	})
	if err != nil {
		t.Fatalf("failed to provision: %v", err)
	}
	t.Logf("Provisioned user: %s", result.User)

	// Verify cannot read secret_data
	db, err := gorm.Open(sqlserver.Open(result.DSN), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	var count int64
	if err := db.Raw("SELECT COUNT(*) FROM secret_data").Scan(&count).Error; err == nil {
		closeAndWait(db)
		provisioner.Revoke(sqlserverTestDSN, result.User)
		t.Fatal("should NOT be able to read secret_data before update")
	}
	closeAndWait(db)

	// Update to add secret_data access
	_, err = provisioner.Provision(sqlserverTestDSN, Options{
		Username: result.User,
		Schemas: map[string][]string{
			"dbo": {"secret_data"},
		},
		Update: true,
	})
	if err != nil {
		provisioner.Revoke(sqlserverTestDSN, result.User)
		t.Fatalf("failed to update: %v", err)
	}

	// Verify can now read secret_data
	db, err = gorm.Open(sqlserver.Open(result.DSN), &gorm.Config{})
	if err != nil {
		provisioner.Revoke(sqlserverTestDSN, result.User)
		t.Fatalf("failed to connect after update: %v", err)
	}
	defer func() {
		closeAndWait(db)
		provisioner.Revoke(sqlserverTestDSN, result.User)
	}()

	if err := db.Raw("SELECT COUNT(*) FROM secret_data").Scan(&count).Error; err != nil {
		t.Fatalf("should be able to read secret_data after update: %v", err)
	}
	t.Logf("Can read secret_data after update: %d rows", count)
}

func TestSQLServer_DuplicateUserFails(t *testing.T) {
	if sqlserverTestDSN == "" {
		t.Skip("TEST_MSSQL_DSN not set")
	}

	provisioner, ok := Get("sqlserver")
	if !ok {
		t.Fatal("sqlserver provisioner not registered")
	}

	// First provision
	result, err := provisioner.Provision(sqlserverTestDSN, Options{
		Schemas: map[string][]string{"dbo": {}},
	})
	if err != nil {
		t.Fatalf("failed to provision: %v", err)
	}
	defer provisioner.Revoke(sqlserverTestDSN, result.User)

	// Try to provision with same username (should fail)
	_, err = provisioner.Provision(sqlserverTestDSN, Options{
		Username: result.User,
		Schemas:  map[string][]string{"dbo": {}},
	})
	if err == nil {
		t.Fatal("provisioning duplicate user should have failed")
	}
	t.Logf("Duplicate user correctly rejected: %v", err)
}

func TestSQLServer_BypassAttempts(t *testing.T) {
	if sqlserverTestDSN == "" {
		t.Skip("TEST_MSSQL_DSN not set")
	}

	provisioner, ok := Get("sqlserver")
	if !ok {
		t.Fatal("sqlserver provisioner not registered")
	}

	result, err := provisioner.Provision(sqlserverTestDSN, Options{
		Schemas: map[string][]string{"dbo": {}},
	})
	if err != nil {
		t.Fatalf("failed to provision: %v", err)
	}

	db, err := gorm.Open(sqlserver.Open(result.DSN), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to connect with readonly user: %v", err)
	}
	defer func() {
		closeAndWait(db)
		provisioner.Revoke(sqlserverTestDSN, result.User)
	}()

	bypassTests := []struct {
		name       string
		query      string
		acceptable bool
	}{
		{"EXEC string", "EXEC('INSERT INTO test_data (name, value) VALUES (''exec_hack'', 1)')", false},
		{"sp_executesql", "EXEC sp_executesql N'INSERT INTO test_data (name, value) VALUES (@n, @v)', N'@n NVARCHAR(100), @v INT', @n='sp_exec_hack', @v=1", false},
		{"EXECUTE AS", "EXECUTE AS USER = 'dbo'; INSERT INTO test_data (name, value) VALUES ('execas_hack', 1); REVERT;", false},
		{"CREATE PROCEDURE", "CREATE PROCEDURE evil_proc AS INSERT INTO test_data (name, value) VALUES ('proc_hack', 1)", false},
		{"xp_cmdshell", "EXEC xp_cmdshell 'whoami'", false},
		{"TRUNCATE", "TRUNCATE TABLE test_data", false},
		{"ALTER TABLE", "ALTER TABLE test_data ADD evil_col NVARCHAR(100)", false},
		{"CREATE INDEX", "CREATE INDEX evil_idx ON test_data (name)", false},
		{"DROP TABLE", "DROP TABLE test_data", false},
		{"CREATE USER", "CREATE USER evil_user WITHOUT LOGIN", false},
		{"ALTER ROLE", "ALTER ROLE db_owner ADD MEMBER CURRENT_USER", false},
		{"MERGE", "MERGE test_data AS target USING (SELECT 'merge_hack' as name, 1 as value) AS source ON target.name = source.name WHEN NOT MATCHED THEN INSERT (name, value) VALUES (source.name, source.value);", false},
	}

	for _, tc := range bypassTests {
		t.Run(tc.name, func(t *testing.T) {
			err := db.Exec(tc.query).Error
			if err == nil {
				if tc.acceptable {
					t.Logf("allowed (acceptable - doesn't bypass readonly)")
				} else {
					t.Errorf("BYPASS SUCCEEDED: query executed without error")
				}
			} else {
				t.Logf("blocked: %v", err)
			}
		})
	}

	// Final verification
	var count int64
	if err := db.Raw("SELECT COUNT(*) FROM test_data").Scan(&count).Error; err != nil {
		t.Fatalf("failed to verify data integrity: %v", err)
	}
	if count != 3 {
		t.Errorf("DATA MODIFIED: expected 3 rows, got %d", count)
	} else {
		t.Log("Final verification: test_data unchanged (3 rows)")
	}
}
