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
	Path string `json:"path"`
}

// GetDatabaseIdentifier returns the absolute path of the SQLite database file.
func (c SqliteConfig) GetDatabaseIdentifier() (string, error) {
	path, err := filepath.Abs(c.Path)
	if err != nil {
		return "", fmt.Errorf("could not get database identifier: %w", err)
	}

	// Strip query parameters if present
	parts := strings.Split(path, "?")
	if len(parts) > 2 {
		return "", errors.New("malformed path")
	}

	return parts[0], nil
}

type Connector struct{}

func (c Connector) ConnectRead(cfg SqliteConfig) (DB, error) {
	dsn := fmt.Sprintf("%s?mode=ro", cfg.Path)
	log.Printf("Opening readonly connection [path=%s]", cfg.Path)
	return gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logging.NewGormLogger()})
}

func (c Connector) ConnectWrite(cfg SqliteConfig) (DB, error) {
	dsn := fmt.Sprintf("%s?mode=rw", cfg.Path)
	log.Printf("Opening read-write connection [path=%s]", cfg.Path)
	return gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logging.NewGormLogger()})
}

func (c Connector) ConnectAdmin(cfg SqliteConfig) (DB, error) {
	// TODO: check whether ConnectWrite and ConnectAdmin can cause race conditions
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
