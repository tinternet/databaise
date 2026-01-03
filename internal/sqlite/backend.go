package sqlite

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/tinternet/databaise/internal/backend"
	"github.com/tinternet/databaise/internal/logging"
	"github.com/tinternet/databaise/internal/sqlcommon"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var log = logging.New("sqlite")

// ReadConfig for read connections.
type ReadConfig struct {
	Path string `json:"path"`
}

// AdminConfig for admin connections.
type AdminConfig struct {
	Path string `json:"path"`
}

// Factory implements backend.BackendFactory for SQLite.
type Factory struct{}

func (Factory) Dialect() string { return "SQLite" }

func (Factory) New(db *gorm.DB) backend.SQLBackend {
	return &Backend{db: db}
}

// Connector implements backend.Connector for SQLite.
type Connector struct{}

func (Connector) ConnectRead(c ReadConfig) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s?mode=ro", c.Path)
	log.Printf("Opening readonly connection [path=%s]", c.Path)
	return gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logging.NewGormLogger()})
}

func (Connector) ConnectAdmin(c AdminConfig) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s?mode=rw", c.Path)
	log.Printf("Opening admin connection [path=%s]", c.Path)
	return gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logging.NewGormLogger()})
}

func init() {
	backend.RegisterFactory("sqlite", Factory{}, Connector{})
}

// Backend implements backend.SQLBackend for SQLite.
type Backend struct {
	db *gorm.DB
}

//go:embed list_tables.sql
var listTablesQuery string

func (b *Backend) ListTables(ctx context.Context, in backend.ListTablesIn) ([]backend.Table, error) {
	var tables []string
	if err := b.db.WithContext(ctx).Raw(listTablesQuery).Scan(&tables).Error; err != nil {
		return nil, err
	}

	result := make([]backend.Table, len(tables))
	for i, t := range tables {
		result[i] = backend.Table{Name: t}
	}
	return result, nil
}

//go:embed ddl_table.sql
var ddlCreateTableQuery string

//go:embed ddl_indexes.sql
var ddlCreateIndexesQuery string

func (b *Backend) DescribeTable(ctx context.Context, in backend.DescribeTableIn) (*backend.TableDescription, error) {
	var out backend.TableDescription

	if err := b.db.WithContext(ctx).Raw(ddlCreateTableQuery, in.Table).Scan(&out.CreateTable).Error; err != nil {
		return nil, err
	}
	if out.CreateTable == "" {
		return nil, sqlcommon.ErrTableNotFound
	}

	if err := b.db.WithContext(ctx).Raw(ddlCreateIndexesQuery, in.Table).Scan(&out.CreateIndexes).Error; err != nil {
		return nil, err
	}

	return &out, nil
}

func (b *Backend) ExecuteQuery(ctx context.Context, in backend.ReadQueryIn) (*backend.QueryResult, error) {
	var rows []map[string]any
	if err := b.db.WithContext(ctx).Raw(in.Query).Scan(&rows).Error; err != nil {
		return nil, err
	}
	return &backend.QueryResult{Rows: rows}, nil
}

func (b *Backend) ExplainQuery(ctx context.Context, in backend.ExplainQueryIn) (*backend.ExplainResult, error) {
	var suffix string
	if in.Analyze {
		suffix = " QUERY PLAN"
	}

	var plan []map[string]any
	if err := b.db.WithContext(ctx).Raw("EXPLAIN"+suffix+" "+in.Query).Scan(&plan).Error; err != nil {
		return nil, err
	}

	planJson, err := json.Marshal(plan)
	if err != nil {
		return nil, err
	}

	return &backend.ExplainResult{
		Format:     "json",
		Result:     string(planJson),
		ResultInfo: "The query plan of sqlite query",
	}, nil
}

func (b *Backend) ExecuteDDL(ctx context.Context, in backend.ExecuteDDLIn) (*backend.DDLResult, error) {
	if err := b.db.WithContext(ctx).Exec(in.DDL).Error; err != nil {
		return nil, err
	}
	return &backend.DDLResult{Success: true, Message: "DDL executed successfully"}, nil
}

// SQLite doesn't have built-in missing index recommendations
func (b *Backend) ListMissingIndexes(ctx context.Context) ([]backend.MissingIndex, error) {
	return nil, fmt.Errorf("missing index recommendations are not available for SQLite")
}

// SQLite doesn't have query monitoring
func (b *Backend) ListWaitingQueries(ctx context.Context) ([]backend.WaitingQuery, error) {
	return nil, fmt.Errorf("waiting query monitoring is not available for SQLite")
}

// SQLite doesn't have query statistics
func (b *Backend) ListSlowestQueries(ctx context.Context) (*backend.SlowQueryResult, error) {
	return nil, fmt.Errorf("slow query statistics are not available for SQLite")
}

// SQLite doesn't have deadlock detection
func (b *Backend) ListDeadlocks(ctx context.Context) ([]backend.Deadlock, error) {
	return nil, fmt.Errorf("deadlock detection is not available for SQLite")
}
