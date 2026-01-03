package sqlserver

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"

	"github.com/tinternet/databaise/internal/backend"
	"github.com/tinternet/databaise/internal/logging"
	"github.com/tinternet/databaise/internal/sqlcommon"
	"golang.org/x/sync/errgroup"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
)

var log = logging.New("sqlserver")

// ReadConfig for read connections.
type ReadConfig struct {
	DSN                 string `json:"dsn"`
	BypassReadonlyCheck bool   `json:"bypass_readonly_check,omitempty"`
}

// AdminConfig for admin connections.
type AdminConfig struct {
	DSN string `json:"dsn"`
}

// Factory implements backend.BackendFactory for SQL Server.
type Factory struct{}

func (Factory) Dialect() string { return "T-SQL" }

func (Factory) New(db *gorm.DB) backend.SQLBackend {
	return &Backend{db: db}
}

// Connector implements backend.Connector for SQL Server.
type Connector struct{}

func (Connector) ConnectRead(c ReadConfig) (*gorm.DB, error) {
	log.Printf("Opening read connection")
	db, err := gorm.Open(sqlserver.Open(c.DSN), &gorm.Config{Logger: logging.NewGormLogger()})
	if err != nil {
		return nil, err
	}

	if !c.BypassReadonlyCheck {
		if !sqlcommon.VerifyReadonly(db, sqlcommon.SQLServerVerifyReadonlySQL) {
			return nil, fmt.Errorf("read DSN user has write permissions (set bypass_readonly_check:true to bypass)")
		}
		log.Printf("Verified read connection is readonly")
	} else {
		log.Printf("Skipping readonly verification (bypass_readonly_check:true)")
	}

	return db, nil
}

func (Connector) ConnectAdmin(c AdminConfig) (*gorm.DB, error) {
	log.Printf("Opening admin connection")
	return gorm.Open(sqlserver.Open(c.DSN), &gorm.Config{Logger: logging.NewGormLogger()})
}

func init() {
	backend.RegisterFactory("sqlserver", Factory{}, Connector{})
}

// Backend implements backend.SQLBackend for SQL Server.
type Backend struct {
	db *gorm.DB
}

//go:embed list_tables.sql
var listTablesQuery string

func (b *Backend) ListTables(ctx context.Context, in backend.ListTablesIn) ([]backend.Table, error) {
	var tables []struct {
		Schema string `gorm:"column:schema"`
		Name   string `gorm:"column:name"`
	}
	if err := b.db.WithContext(ctx).Raw(listTablesQuery, sql.Named("schema", in.Schema)).Scan(&tables).Error; err != nil {
		return nil, err
	}

	result := make([]backend.Table, len(tables))
	for i, t := range tables {
		result[i] = backend.Table{Schema: t.Schema, Name: t.Name}
	}
	return result, nil
}

//go:embed ddl_table.sql
var ddlTableQuery string

//go:embed ddl_indexes.sql
var ddlIndexesQuery string

//go:embed ddl_constraints.sql
var ddlConstraintsQuery string

func (b *Backend) DescribeTable(ctx context.Context, in backend.DescribeTableIn) (*backend.TableDescription, error) {
	var out backend.TableDescription
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return b.db.WithContext(ctx).Raw(ddlTableQuery, in.Table, in.Schema).Scan(&out.CreateTable).Error
	})
	g.Go(func() error {
		return b.db.WithContext(ctx).Raw(ddlIndexesQuery, in.Table, in.Schema).Scan(&out.CreateIndexes).Error
	})
	g.Go(func() error {
		st := fmt.Sprintf("%s.%s", in.Schema, in.Table)
		return b.db.WithContext(ctx).Raw(ddlConstraintsQuery, st, in.Table, in.Schema).Scan(&out.CreateConstraints).Error
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}
	if out.CreateTable == "" {
		return nil, sqlcommon.ErrTableNotFound
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
	tx := b.db.WithContext(ctx).Begin()
	defer tx.Rollback()
	if err := tx.Error; err != nil {
		return nil, err
	}

	var enable, disable string
	if in.Analyze {
		enable = "SET STATISTICS XML ON;"
		disable = "SET STATISTICS XML OFF;"
	} else {
		enable = "SET SHOWPLAN_XML ON;"
		disable = "SET SHOWPLAN_XML OFF;"
	}

	if err := tx.Exec(enable).Error; err != nil {
		return nil, err
	}

	var plan string

	if in.Analyze {
		rows, err := tx.Raw(in.Query).Rows()
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		// Skip result rows
		for rows.Next() {
		}

		// The execution plan is in the next result set
		if rows.NextResultSet() {
			if rows.Next() {
				if err := rows.Scan(&plan); err != nil {
					return nil, err
				}
			}
		}
	} else {
		if err := tx.Raw(in.Query).Scan(&plan).Error; err != nil {
			return nil, err
		}
	}

	if err := tx.Exec(disable).Error; err != nil {
		return nil, err
	}

	return &backend.ExplainResult{
		Format:     "xml",
		Result:     plan,
		ResultInfo: "The mssql plan",
	}, nil
}

func (b *Backend) ExecuteDDL(ctx context.Context, in backend.ExecuteDDLIn) (*backend.DDLResult, error) {
	if err := b.db.WithContext(ctx).Exec(in.DDL).Error; err != nil {
		return nil, err
	}
	return &backend.DDLResult{Success: true, Message: "DDL executed successfully"}, nil
}

//go:embed missing_indexes.sql
var missingIndexesQuery string

func (b *Backend) ListMissingIndexes(ctx context.Context) ([]backend.MissingIndex, error) {
	var indexes []struct {
		AverageEstimatedImpact float64 `gorm:"column:average_estimated_impact"`
		CreateStatement        string  `gorm:"column:create_statement"`
		LastUserSeek           string  `gorm:"column:last_user_seek"`
		TableName              string  `gorm:"column:table_name"`
	}
	if err := b.db.WithContext(ctx).Raw(missingIndexesQuery).Scan(&indexes).Error; err != nil {
		return nil, err
	}

	result := make([]backend.MissingIndex, len(indexes))
	for i, idx := range indexes {
		result[i] = backend.MissingIndex{
			TableName:       idx.TableName,
			Reason:          fmt.Sprintf("last_user_seek=%s", idx.LastUserSeek),
			EstimatedImpact: idx.AverageEstimatedImpact,
			Suggestion:      idx.CreateStatement,
		}
	}
	return result, nil
}

//go:embed list_waiting_queries.sql
var waitingQueriesQuery string

func (b *Backend) ListWaitingQueries(ctx context.Context) ([]backend.WaitingQuery, error) {
	var queries []struct {
		StartTime    string `gorm:"column:start_time"`
		QueryText    string `gorm:"column:query_text"`
		Status       string `gorm:"column:status"`
		Command      string `gorm:"column:command"`
		WaitType     string `gorm:"column:wait_type"`
		WaitTimeMS   int    `gorm:"column:wait_time_ms"`
		WaitResource string `gorm:"column:wait_resource"`
		LastWaitType string `gorm:"column:last_wait_type"`
	}
	if err := b.db.WithContext(ctx).Raw(waitingQueriesQuery).Scan(&queries).Error; err != nil {
		return nil, err
	}

	result := make([]backend.WaitingQuery, len(queries))
	for i, q := range queries {
		result[i] = backend.WaitingQuery{
			ID:       q.StartTime,
			State:    q.Status,
			WaitType: q.WaitType,
			WaitTime: float64(q.WaitTimeMS) / 1000.0,
			Query:    q.QueryText,
		}
	}
	return result, nil
}

//go:embed list_slowest_queries.sql
var slowestQueriesQuery string

func (b *Backend) ListSlowestQueries(ctx context.Context) ([]backend.SlowQuery, error) {
	var queries []struct {
		StatementText    string `gorm:"column:statement_text"`
		CreationTime     string `gorm:"column:creation_time"`
		LastExecTime     string `gorm:"column:last_execution_time"`
		ExecutionCount   int    `gorm:"column:execution_count"`
		TotalElapsedTime int    `gorm:"column:total_elapsed_time"`
		AvgElapsedTime   int    `gorm:"column:avg_elapsed_time"`
	}
	if err := b.db.WithContext(ctx).Raw(slowestQueriesQuery).Scan(&queries).Error; err != nil {
		return nil, err
	}

	result := make([]backend.SlowQuery, len(queries))
	for i, q := range queries {
		result[i] = backend.SlowQuery{
			Calls:        int64(q.ExecutionCount),
			TotalTimeSec: float64(q.TotalElapsedTime) / 1000000.0, // microseconds to seconds
			AvgTimeSec:   float64(q.AvgElapsedTime) / 1000000.0,
			Query:        q.StatementText,
		}
	}
	return result, nil
}

//go:embed list_deadlocks.sql
var deadlocksQuery string

func (b *Backend) ListDeadlocks(ctx context.Context) ([]backend.Deadlock, error) {
	var deadlocks []struct {
		DeadlockReport string `gorm:"column:DeadlockGraph"`
	}
	if err := b.db.WithContext(ctx).Raw(deadlocksQuery).Scan(&deadlocks).Error; err != nil {
		return nil, err
	}

	result := make([]backend.Deadlock, len(deadlocks))
	for i, d := range deadlocks {
		result[i] = backend.Deadlock{
			Details:   d.DeadlockReport,
			Timestamp: "",
		}
	}
	return result, nil
}
