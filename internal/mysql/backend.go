package mysql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/tinternet/databaise/internal/backend"
	"github.com/tinternet/databaise/internal/config"
	"github.com/tinternet/databaise/internal/logging"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var log = logging.New("mysql")

type ReadConfig struct {
	config.ReadConfig
}

type WriteConfig struct {
	config.WriteConfig
}

type AdminConfig struct {
	config.AdminConfig
}

// GetDatabaseIdentifier extracts the database name from MySQL DSN.
// MySQL DSN format: user:pass@tcp(host:port)/dbname?params
func (c ReadConfig) GetDatabaseIdentifier() (string, error) {
	return extractMySQLDatabaseName(c.DSN)
}

func (c WriteConfig) GetDatabaseIdentifier() (string, error) {
	return extractMySQLDatabaseName(c.DSN)
}

func (c AdminConfig) GetDatabaseIdentifier() (string, error) {
	return extractMySQLDatabaseName(c.DSN)
}

func extractMySQLDatabaseName(dsn string) (string, error) {
	parts := strings.Split(dsn, "/")
	if len(parts) < 2 {
		return "", errors.New("could not parse mysql dsn")
	}

	parts = strings.Split(parts[len(parts)-1], "?")
	if parts[0] == "" {
		return "", errors.New("database not specified")
	}

	return parts[0], nil
}

type Connector struct{}

func (b Connector) ConnectRead(cfg ReadConfig) (*gorm.DB, error) {
	log.Printf("Opening read connection")

	gormConfig := &gorm.Config{Logger: logging.NewGormLogger()}

	db, err := openConnection(enableParseTime(cfg.DSN), gormConfig)
	if err != nil {
		return nil, err
	}

	if cfg.ShouldEnforceReadonly() {
		var grants []string
		if err := db.Raw("SHOW GRANTS FOR CURRENT_USER;").Scan(&grants).Error; err != nil {
			return nil, fmt.Errorf("could not verify user permissions: %w", err)
		}
		for _, g := range grants {
			for _, p := range []string{"INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER"} {
				if strings.Contains(g, p) {
					return nil, fmt.Errorf("read DSN user has write permissions (set enforce_readonly: false to bypass)")
				}
			}
		}
		log.Printf("Verified read connection is readonly")
	} else {
		log.Printf("Skipping readonly verification (enforce_readonly: false)")
	}

	return db, nil
}

func (b Connector) ConnectWrite(cfg WriteConfig) (*gorm.DB, error) {
	log.Printf("Opening write connection")
	return openConnection(enableParseTime(cfg.DSN), &gorm.Config{Logger: logging.NewGormLogger()})
}

func (b Connector) ConnectAdmin(cfg AdminConfig) (*gorm.DB, error) {
	log.Printf("Opening admin connection")
	return openConnection(enableParseTime(cfg.DSN), &gorm.Config{Logger: logging.NewGormLogger()})
}

func init() {
	b := backend.NewBackend("mysql", Connector{})

	// Read tools
	backend.AddReadTool(&b, "list_tables", "[MySQL] List all tables in a schema.", ListTables)
	backend.AddReadTool(&b, "describe_table", "[MySQL] Describe a table's columns and indexes.", DescribeTable)
	backend.AddReadTool(&b, "read_query", "[MySQL] Execute a read-only SQL query.", ExecuteQuery)

	// Write tools
	// backend.AddWriteTool(&b, "write_query", "[MySQL] Execute a SQL statement that modifies data (INSERT, UPDATE, DELETE).", ExecuteWrite)

	// Admin tools
	backend.AddAdminTool(&b, "explain_query", "[MySQL] Explain a query's execution plan.", ExplainQuery)
	backend.AddAdminTool(&b, "create_index", "[MySQL] Create an index on a table.", CreateIndex)
	backend.AddAdminTool(&b, "drop_index", "[MySQL] Drop an index on a table.", DropIndex)
	backend.AddAdminTool(&b, "list_missing_indexes", "[MySQL] List tables that might benefit from indexes based on performance_schema statistics.", ListMissingIndexes)
	backend.AddAdminTool(&b, "list_waiting_queries", "[MySQL] List currently waiting queries with blocking information.", ListWaitingQueries)
	backend.AddAdminTool(&b, "list_slowest_queries", "[MySQL] List slowest queries by total elapsed time from performance_schema.", ListSlowestQueries)
	backend.AddAdminTool(&b, "list_deadlocks", "[MySQL] Show most recent deadlock information from InnoDB status.", ListDeadlocks)

	backend.Register(&b)
}

func openConnection(dsn string, gormCfg *gorm.Config) (*gorm.DB, error) {
	return gorm.Open(mysql.Open(dsn), gormCfg)
}

func enableParseTime(dsn string) string {
	if strings.Contains(dsn, "parseTime=true") {
		return dsn
	}
	if strings.Contains(dsn, "&") || strings.Contains(dsn, "?") {
		return dsn + "&parseTime=true"
	}
	return dsn + "?parseTime=true"
}
