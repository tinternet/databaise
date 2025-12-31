package sqlite

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/tinternet/databaise/internal/backend"
	"github.com/tinternet/databaise/internal/logging"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var log = logging.New("sqlite")

type DB = *gorm.DB

type SqliteConfig struct {
	// Path is the path to the SQLite database file.
	Path string `json:"path"`
}

type ReadConfig = SqliteConfig
type WriteConfig = SqliteConfig
type AdminConfig = SqliteConfig

type Connector struct{}

func getDatabaseName(dsn string) (string, error) {
	path, err := filepath.Abs(dsn)
	if err != nil {
		return "", fmt.Errorf("could not get db name: %w", err)
	}
	parts := strings.Split(path, "?")
	if len(parts) > 2 {
		return "", errors.New("malformed dsn")
	}
	return parts[0], nil
}

func (c Connector) ValidateConfig(r *ReadConfig, w *WriteConfig, a *AdminConfig) error {
	m := make(map[string]bool)

	if r != nil {
		if readDB, err := getDatabaseName(r.Path); err != nil {
			return err
		} else {
			m[readDB] = true
		}
	}
	if w != nil {
		if writeDB, err := getDatabaseName(w.Path); err != nil {
			return err
		} else {
			m[writeDB] = true
		}
	}
	if a != nil {
		if adminDB, err := getDatabaseName(a.Path); err != nil {
			return err
		} else {
			m[adminDB] = true
		}
	}

	if len(m) > 1 {
		return errors.New("read, write, admin configs must point to the same database")
	}

	return nil
}

func (c Connector) ConnectRead(cfg ReadConfig) (DB, error) {
	dsn := fmt.Sprintf("%s?mode=ro", cfg.Path)
	log.Printf("Opening readonly connection [path=%s]", cfg.Path)
	return gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logging.NewGormLogger()})
}

func (c Connector) ConnectWrite(cfg WriteConfig) (DB, error) {
	dsn := fmt.Sprintf("%s?mode=rw", cfg.Path)
	log.Printf("Opening read-write connection [path=%s]", cfg.Path)
	return gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logging.NewGormLogger()})
}

func (c Connector) ConnectAdmin(cfg AdminConfig) (DB, error) {
	dsn := fmt.Sprintf("%s?mode=rw", cfg.Path)
	log.Printf("Opening admin connection [path=%s]", cfg.Path)
	return gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logging.NewGormLogger()})
}

func init() {
	b := backend.NewBackend("sqlite", Connector{})

	// Read tools
	backend.AddReadTool(&b, "list_tables", "[SQLite] List all table names.", ListTables)
	backend.AddReadTool(&b, "describe_table", "[SQLite] Describe a table's columns and indexes.", DescribeTable)
	backend.AddReadTool(&b, "read_query", "[SQLite] Execute a read-only SQL query.", ExecuteQuery)

	// Write tools
	// backend.AddWriteTool(&b, "write_query", "[SQLite] Execute a SQL statement that modifies data (INSERT, UPDATE, DELETE).", ExecuteWrite)

	// Admin tools
	backend.AddAdminTool(&b, "explain_query", "[SQLite] Run a query with execution plan enabled.", ExplainQuery)
	backend.AddAdminTool(&b, "create_index", "[SQLite] Create an index on a table.", CreateIndex)
	backend.AddAdminTool(&b, "drop_index", "[SQLite] Drop an index on a table.", DropIndex)

	backend.Register(&b)
}
