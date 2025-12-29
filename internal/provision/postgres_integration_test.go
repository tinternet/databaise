//go:build integration

package provision

import (
	"testing"

	"github.com/tinternet/databaise/internal/sqlcommon"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func setupPostgres() {
	if postgresTestDSN == "" {
		return
	}

	db, err := gorm.Open(postgres.Open(postgresTestDSN), &gorm.Config{})
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
	db.Exec(`CREATE TABLE test_data (id SERIAL PRIMARY KEY, name TEXT, value INT)`)
	db.Exec(`INSERT INTO test_data (name, value) VALUES ('alice', 100), ('bob', 200), ('charlie', 300)`)
	db.Exec(`CREATE TABLE secret_data (id SERIAL PRIMARY KEY, secret TEXT)`)
	db.Exec(`INSERT INTO secret_data (secret) VALUES ('top_secret')`)
}

func cleanupPostgres() {
	if postgresTestDSN == "" {
		return
	}

	db, err := gorm.Open(postgres.Open(postgresTestDSN), &gorm.Config{})
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

func TestPostgres_SchemaScope_AllTables(t *testing.T) {
	if postgresTestDSN == "" {
		t.Skip("TEST_POSTGRES_DSN not set")
	}

	provisioner, ok := Get("postgres")
	if !ok {
		t.Fatal("postgres provisioner not registered")
	}

	result, err := provisioner.Provision(postgresTestDSN, Options{
		Schemas: map[string][]string{"public": {}}, // all tables in public schema
	})
	if err != nil {
		t.Fatalf("failed to provision: %v", err)
	}
	t.Logf("Provisioned user: %s", result.User)

	db, err := gorm.Open(postgres.Open(result.DSN), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to connect with readonly user: %v", err)
	}
	defer func() {
		closeAndWait(db)
		provisioner.Revoke(postgresTestDSN, result.User)
	}()

	// Verify readonly
	var isReadonly bool
	if err := db.Raw(sqlcommon.PostgreSQLVerifyReadonlySQL).Scan(&isReadonly).Error; err != nil {
		t.Fatalf("failed to run verify_readonly check: %v", err)
	}
	if !isReadonly {
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

func TestPostgres_SchemaScope_SpecificTables(t *testing.T) {
	if postgresTestDSN == "" {
		t.Skip("TEST_POSTGRES_DSN not set")
	}

	provisioner, ok := Get("postgres")
	if !ok {
		t.Fatal("postgres provisioner not registered")
	}

	result, err := provisioner.Provision(postgresTestDSN, Options{
		Schemas: map[string][]string{
			"public": {"test_data"}, // only test_data, not secret_data
		},
	})
	if err != nil {
		t.Fatalf("failed to provision: %v", err)
	}
	t.Logf("Provisioned user: %s", result.User)

	db, err := gorm.Open(postgres.Open(result.DSN), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to connect with readonly user: %v", err)
	}
	defer func() {
		closeAndWait(db)
		provisioner.Revoke(postgresTestDSN, result.User)
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

func TestPostgres_Update(t *testing.T) {
	if postgresTestDSN == "" {
		t.Skip("TEST_POSTGRES_DSN not set")
	}

	provisioner, ok := Get("postgres")
	if !ok {
		t.Fatal("postgres provisioner not registered")
	}

	// First provision with only test_data
	result, err := provisioner.Provision(postgresTestDSN, Options{
		Schemas: map[string][]string{
			"public": {"test_data"},
		},
	})
	if err != nil {
		t.Fatalf("failed to provision: %v", err)
	}
	t.Logf("Provisioned user: %s", result.User)

	// Verify cannot read secret_data
	db, err := gorm.Open(postgres.Open(result.DSN), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	var count int64
	if err := db.Raw("SELECT COUNT(*) FROM secret_data").Scan(&count).Error; err == nil {
		closeAndWait(db)
		provisioner.Revoke(postgresTestDSN, result.User)
		t.Fatal("should NOT be able to read secret_data before update")
	}
	closeAndWait(db)

	// Update to add secret_data access
	_, err = provisioner.Provision(postgresTestDSN, Options{
		Username: result.User,
		Schemas: map[string][]string{
			"public": {"secret_data"},
		},
		Update: true,
	})
	if err != nil {
		provisioner.Revoke(postgresTestDSN, result.User)
		t.Fatalf("failed to update: %v", err)
	}

	// Verify can now read secret_data
	db, err = gorm.Open(postgres.Open(result.DSN), &gorm.Config{})
	if err != nil {
		provisioner.Revoke(postgresTestDSN, result.User)
		t.Fatalf("failed to connect after update: %v", err)
	}
	defer func() {
		closeAndWait(db)
		provisioner.Revoke(postgresTestDSN, result.User)
	}()

	if err := db.Raw("SELECT COUNT(*) FROM secret_data").Scan(&count).Error; err != nil {
		t.Fatalf("should be able to read secret_data after update: %v", err)
	}
	t.Logf("Can read secret_data after update: %d rows", count)
}

func TestPostgres_DuplicateUserFails(t *testing.T) {
	if postgresTestDSN == "" {
		t.Skip("TEST_POSTGRES_DSN not set")
	}

	provisioner, ok := Get("postgres")
	if !ok {
		t.Fatal("postgres provisioner not registered")
	}

	// First provision
	result, err := provisioner.Provision(postgresTestDSN, Options{
		Schemas: map[string][]string{"public": {}},
	})
	if err != nil {
		t.Fatalf("failed to provision: %v", err)
	}
	defer provisioner.Revoke(postgresTestDSN, result.User)

	// Try to provision with same username (should fail)
	_, err = provisioner.Provision(postgresTestDSN, Options{
		Username: result.User,
		Schemas:  map[string][]string{"public": {}},
	})
	if err == nil {
		t.Fatal("provisioning duplicate user should have failed")
	}
	t.Logf("Duplicate user correctly rejected: %v", err)
}

func TestPostgres_BypassAttempts(t *testing.T) {
	if postgresTestDSN == "" {
		t.Skip("TEST_POSTGRES_DSN not set")
	}

	provisioner, ok := Get("postgres")
	if !ok {
		t.Fatal("postgres provisioner not registered")
	}

	result, err := provisioner.Provision(postgresTestDSN, Options{
		Schemas: map[string][]string{"public": {}},
	})
	if err != nil {
		t.Fatalf("failed to provision: %v", err)
	}

	db, err := gorm.Open(postgres.Open(result.DSN), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to connect with readonly user: %v", err)
	}
	defer func() {
		closeAndWait(db)
		provisioner.Revoke(postgresTestDSN, result.User)
	}()

	bypassTests := []struct {
		name       string
		query      string
		acceptable bool
	}{
		{"CTE INSERT", "WITH inserted AS (INSERT INTO test_data (name, value) VALUES ('cte_hack', 1) RETURNING *) SELECT * FROM inserted", false},
		{"CTE UPDATE", "WITH updated AS (UPDATE test_data SET value = 0 RETURNING *) SELECT * FROM updated", false},
		{"DO block INSERT", "DO $$ BEGIN INSERT INTO test_data (name, value) VALUES ('do_hack', 1); END $$;", false},
		{"CREATE FUNCTION", "CREATE FUNCTION evil() RETURNS void AS $$ INSERT INTO test_data VALUES (999, 'func_hack', 1); $$ LANGUAGE sql;", false},
		{"TRUNCATE", "TRUNCATE test_data", false},
		{"ALTER TABLE", "ALTER TABLE test_data ADD COLUMN evil_col TEXT", false},
		{"CREATE INDEX", "CREATE INDEX evil_idx ON test_data (name)", false},
		{"DROP TABLE", "DROP TABLE test_data", false},
		{"CREATE ROLE", "CREATE ROLE evil_role", false},
		{"GRANT to self", "GRANT ALL ON test_data TO CURRENT_USER", false},
		{"SET ROLE", "SET ROLE postgres", false},
		{"SET search_path", "SET search_path TO pg_catalog, public", true}, // acceptable
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
	verifyDB, _ := gorm.Open(postgres.Open(result.DSN), &gorm.Config{})
	defer func() {
		sqlDB, _ := verifyDB.DB()
		sqlDB.Close()
	}()

	var count int64
	if err := verifyDB.Raw("SELECT COUNT(*) FROM test_data").Scan(&count).Error; err != nil {
		t.Fatalf("failed to verify data integrity: %v", err)
	}
	if count != 3 {
		t.Errorf("DATA MODIFIED: expected 3 rows, got %d", count)
	} else {
		t.Log("Final verification: test_data unchanged (3 rows)")
	}
}
