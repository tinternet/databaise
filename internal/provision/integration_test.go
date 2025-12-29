//go:build integration

package provision

import (
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

// Environment variables for test DSNs.
// Tests expect these databases to exist and be empty (or droppable tables).
var (
	postgresTestDSN  = os.Getenv("TEST_POSTGRES_DSN")
	mysqlTestDSN     = os.Getenv("TEST_MYSQL_DSN")
	sqlserverTestDSN = os.Getenv("TEST_MSSQL_DSN")
)

// testDBName extracts or derives the database name from a DSN for use in provisioning.
var (
	postgresTestDBName  string
	mysqlTestDBName     string
	sqlserverTestDBName string
)

// closeAndWait closes the database connection and waits briefly for the server to release it.
func closeAndWait(db *gorm.DB) {
	sqlDB, _ := db.DB()
	sqlDB.Close()
	time.Sleep(100 * time.Millisecond)
}

// extractPostgresDBName extracts the database name from a postgres DSN.
func extractPostgresDBName(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return ""
	}
	return strings.TrimPrefix(u.Path, "/")
}

// extractMySQLDBName extracts the database name from a MySQL DSN.
func extractMySQLDBName(dsn string) string {
	// Format: user:password@tcp(host:port)/dbname?params
	re := regexp.MustCompile(`/([^/?]+)(\?|$)`)
	if matches := re.FindStringSubmatch(dsn); len(matches) > 1 {
		return matches[1]
	}
	return ""
}

// extractSQLServerDBName extracts the database name from a SQL Server DSN.
func extractSQLServerDBName(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return ""
	}
	return u.Query().Get("database")
}

func TestMain(m *testing.M) {
	// Extract database names from DSNs
	if postgresTestDSN != "" {
		postgresTestDBName = extractPostgresDBName(postgresTestDSN)
	}
	if mysqlTestDSN != "" {
		mysqlTestDBName = extractMySQLDBName(mysqlTestDSN)
	}
	if sqlserverTestDSN != "" {
		sqlserverTestDBName = extractSQLServerDBName(sqlserverTestDSN)
	}

	// Setup test tables
	setupPostgres()
	setupMySQL()
	setupSQLServer()

	// Run tests
	code := m.Run()

	// Cleanup test tables
	cleanupPostgres()
	cleanupMySQL()
	cleanupSQLServer()

	os.Exit(code)
}
