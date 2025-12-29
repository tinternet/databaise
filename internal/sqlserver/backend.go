package sqlserver

import (
	"fmt"

	"github.com/tinternet/databaise/internal/backend"
	"github.com/tinternet/databaise/internal/config"
	"github.com/tinternet/databaise/internal/logging"
	"github.com/tinternet/databaise/internal/sqlcommon"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
)

var log = logging.New("sqlserver")

type DB = *gorm.DB

type (
	ReadConfig  = config.ReadConfig
	WriteConfig = config.WriteConfig
	AdminConfig = config.AdminConfig
)

type Connector struct{}

func (c Connector) ConnectRead(cfg ReadConfig) (DB, error) {
	log.Printf("Opening read connection")

	db, err := gorm.Open(sqlserver.Open(cfg.DSN), &gorm.Config{Logger: logging.NewGormLogger()})
	if err != nil {
		return nil, err
	}

	// Verify readonly permissions if enforcement is enabled
	if cfg.ShouldEnforceReadonly() {
		if !sqlcommon.VerifyReadonly(db, sqlcommon.SQLServerVerifyReadonlySQL) {
			return nil, fmt.Errorf("read DSN user has write permissions (set enforce_readonly: false to bypass)")
		}
		log.Printf("Verified read connection is readonly")
	} else {
		log.Printf("Skipping readonly verification (enforce_readonly: false)")
	}

	return db, nil
}

func (c Connector) ConnectWrite(cfg WriteConfig) (DB, error) {
	log.Printf("Opening write connection")
	return gorm.Open(sqlserver.Open(cfg.DSN), &gorm.Config{Logger: logging.NewGormLogger()})
}

func (c Connector) ConnectAdmin(cfg AdminConfig) (DB, error) {
	log.Printf("Opening admin connection")
	return gorm.Open(sqlserver.Open(cfg.DSN), &gorm.Config{Logger: logging.NewGormLogger()})
}

func init() {
	b := backend.NewBackend("sqlserver", Connector{})

	// Read tools
	backend.AddReadTool(&b, "list_tables", "[T-SQL] List all tables. Optionally specify a schema (default: dbo).", ListTables)
	backend.AddReadTool(&b, "describe_table", "[T-SQL] Describe a table's columns and indexes.", DescribeTable)
	backend.AddReadTool(&b, "read_query", "[T-SQL] Execute a read-only SQL query.", ExecuteQuery)

	// Write tools
	// backend.AddWriteTool(&b, "write_query", "[T-SQL] Execute a SQL statement that modifies data (INSERT, UPDATE, DELETE).", ExecuteWrite)

	// Admin tools
	// backend.AddAdminTool(&b, "admin_query", "[T-SQL] Execute administrative SQL (DDL, maintenance).", ExecuteAdmin)
	backend.AddAdminTool(&b, "create_index", "[T-SQL] Create an index on a table.", CreateIndex)

	backend.Register(&b)
}
