package sqlserver

import (
	"errors"
	"fmt"
	"net/url"

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

func (c Connector) ValidateConfig(r *ReadConfig, w *WriteConfig, a *AdminConfig) error {
	m := make(map[string]bool)

	if r != nil {
		if u, err := url.Parse(r.DSN); err != nil {
			return err
		} else {
			m[u.Path] = true
		}
	}
	if w != nil {
		if u, err := url.Parse(w.DSN); err != nil {
			return err
		} else {
			m[u.Path] = true
		}
	}
	if a != nil {
		if u, err := url.Parse(a.DSN); err != nil {
			return err
		} else {
			m[u.Path] = true
		}
	}

	if len(m) > 1 {
		return errors.New("read, write, admin configs must point to the same database")
	}

	return nil
}

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
	backend.AddReadTool(&b, "list_tables", "List all tables. Optionally specify a schema (default: dbo).", ListTables)
	backend.AddReadTool(&b, "describe_table", "Describe a table's columns and indexes.", DescribeTable)
	backend.AddReadTool(&b, "read_query", "[T-SQL] Execute a read-only SQL query.", ExecuteQuery)

	// Write tools
	// backend.AddWriteTool(&b, "write_query", "[T-SQL] Execute a SQL statement that modifies data (INSERT, UPDATE, DELETE).", ExecuteWrite)

	// Admin tools
	backend.AddAdminTool(&b, "explain_query", "[T-SQL] Run a query with execution plan enabled.", ExplainQuery)
	backend.AddAdminTool(&b, "create_index", "Create an index on a table.", CreateIndex)
	backend.AddAdminTool(&b, "drop_index", "Drop an index on a table.", DropIndex)
	backend.AddAdminTool(&b, "list_missing_indexes", "List missing indexes.", ListMissingIndexes)
	backend.AddAdminTool(&b, "list_waiting_queries", "List waiting queries.", ListWaitingQueries)
	backend.AddAdminTool(&b, "list_slowest_queries", "List slowest queries by total elapsed time.", ListSlowestQueries)
	backend.AddAdminTool(&b, "list_deadlocks", "List recent deadlocks.", ListDeadlocks)

	backend.Register(&b)
}
