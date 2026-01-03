package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"

	"github.com/tinternet/databaise/internal/backend"
	"github.com/tinternet/databaise/internal/logging"
	"github.com/tinternet/databaise/internal/sqlcommon"
	"golang.org/x/sync/errgroup"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var log = logging.New("postgres")

// ReadConfig extends the base read config with PostgreSQL-specific options.
type ReadConfig struct {
	DSN                 string `json:"dsn"`
	UseReadonlyTx       bool   `json:"use_readonly_tx,omitempty"`
	BypassReadonlyCheck bool   `json:"bypass_readonly_check"`
}

// AdminConfig for admin connections.
type AdminConfig struct {
	DSN string `json:"dsn"`
}

// DB wraps gorm.DB with PostgreSQL-specific settings.
type DB struct {
	*gorm.DB
	UseReadonlyTx bool
}

// Factory implements backend.BackendFactory for PostgreSQL.
type Factory struct{}

func (Factory) Dialect() string { return "PostgreSQL" }

func (Factory) New(db DB) backend.SQLBackend {
	return &Backend{db: db}
}

// Connector implements backend.Connector for PostgreSQL.
type Connector struct{}

func (Connector) ConnectRead(c ReadConfig) (DB, error) {
	log.Printf("Opening read connection")
	db, err := gorm.Open(postgres.Open(c.DSN), &gorm.Config{Logger: logging.NewGormLogger()})
	if err != nil {
		return DB{}, err
	}

	if c.UseReadonlyTx {
		log.Println("Using PostgreSQL readonly transactions (use_readonly_tx: true)")
		return DB{DB: db, UseReadonlyTx: true}, nil
	}

	if !c.BypassReadonlyCheck {
		if !sqlcommon.VerifyReadonly(db, sqlcommon.PostgreSQLVerifyReadonlySQL) {
			return DB{}, fmt.Errorf("read DSN user has write permissions (set bypass_readonly_check: true to bypass)")
		}
		log.Printf("Verified read connection is readonly")
	} else {
		log.Printf("Skipping readonly verification (bypass_readonly_check: true)")
	}

	return DB{DB: db, UseReadonlyTx: false}, nil
}

func (Connector) ConnectAdmin(c AdminConfig) (DB, error) {
	log.Printf("Opening admin connection")
	db, err := gorm.Open(postgres.Open(c.DSN), &gorm.Config{Logger: logging.NewGormLogger()})
	if err != nil {
		return DB{}, err
	}

	return DB{DB: db, UseReadonlyTx: false}, nil
}

func init() {
	backend.RegisterFactory("postgres", Factory{}, Connector{})
}

// Backend implements backend.SQLBackend for PostgreSQL.
type Backend struct {
	db DB
}

//go:embed list_tables.sql
var listTablesQuery string

func (b *Backend) ListTables(ctx context.Context, in backend.ListTablesIn) ([]backend.Table, error) {
	var tables []struct {
		Schema string `gorm:"column:schema"`
		Name   string `gorm:"column:name"`
	}
	if err := b.db.WithContext(ctx).Raw(listTablesQuery, in.Schema).Scan(&tables).Error; err != nil {
		return nil, err
	}

	result := make([]backend.Table, len(tables))
	for i, t := range tables {
		result[i] = backend.Table{Schema: t.Schema, Name: t.Name}
	}
	return result, nil
}

//go:embed ddl_table.sql
var queryTableDDL string

//go:embed ddl_indexes.sql
var queryIndexesDDL string

//go:embed ddl_constraints.sql
var queryConstraintsDDL string

func (b *Backend) DescribeTable(ctx context.Context, in backend.DescribeTableIn) (*backend.TableDescription, error) {
	var out backend.TableDescription
	g, ctx := errgroup.WithContext(ctx)
	tableName := fmt.Sprintf("%s.%s", in.Schema, in.Table)

	g.Go(func() error {
		return b.db.WithContext(ctx).Raw(queryTableDDL, tableName).Scan(&out.CreateTable).Error
	})
	g.Go(func() error {
		return b.db.WithContext(ctx).Raw(queryIndexesDDL, tableName).Scan(&out.CreateIndexes).Error
	})
	g.Go(func() error {
		return b.db.WithContext(ctx).Raw(queryConstraintsDDL, tableName).Scan(&out.CreateConstraints).Error
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

	if b.db.UseReadonlyTx {
		err := b.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			return tx.Raw(in.Query).Scan(&rows).Error
		}, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return nil, err
		}
		return &backend.QueryResult{Rows: rows}, nil
	}

	if err := b.db.WithContext(ctx).Raw(in.Query).Scan(&rows).Error; err != nil {
		return nil, err
	}
	return &backend.QueryResult{Rows: rows}, nil
}

func (b *Backend) ExplainQuery(ctx context.Context, in backend.ExplainQueryIn) (*backend.ExplainResult, error) {
	var analyzeStr string
	if in.Analyze {
		analyzeStr = "ANALYZE, "
	}

	var planJSON string
	err := b.db.WithContext(ctx).Raw(fmt.Sprintf("EXPLAIN (%sFORMAT JSON) %s", analyzeStr, in.Query)).Scan(&planJSON).Error
	if err != nil {
		return nil, err
	}

	return &backend.ExplainResult{
		Format:     "json",
		Result:     planJSON,
		ResultInfo: "The postgresql query plan as returned by the database",
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
		Schema           string  `gorm:"column:schema"`
		TableName        string  `gorm:"column:table_name"`
		SeqScans         int64   `gorm:"column:seq_scans"`
		SeqTuplesRead    int64   `gorm:"column:seq_tuples_read"`
		IndexScans       int64   `gorm:"column:index_scans"`
		TableSizeMB      float64 `gorm:"column:table_size_mb"`
		EstimatedImpact  float64 `gorm:"column:estimated_impact"`
		CreateSuggestion string  `gorm:"column:create_suggestion"`
	}
	if err := b.db.WithContext(ctx).Raw(missingIndexesQuery).Scan(&indexes).Error; err != nil {
		return nil, err
	}

	result := make([]backend.MissingIndex, len(indexes))
	for i, idx := range indexes {
		result[i] = backend.MissingIndex{
			Schema:          idx.Schema,
			TableName:       idx.TableName,
			Reason:          fmt.Sprintf("seq_scans=%d, seq_tuples_read=%d, index_scans=%d, size=%.2fMB", idx.SeqScans, idx.SeqTuplesRead, idx.IndexScans, idx.TableSizeMB),
			EstimatedImpact: idx.EstimatedImpact,
			Suggestion:      idx.CreateSuggestion,
		}
	}
	return result, nil
}

//go:embed list_waiting_queries.sql
var waitingQueriesQuery string

func (b *Backend) ListWaitingQueries(ctx context.Context) ([]backend.WaitingQuery, error) {
	var queries []struct {
		PID              int     `gorm:"column:pid"`
		Username         string  `gorm:"column:username"`
		DatabaseName     string  `gorm:"column:database_name"`
		ApplicationName  string  `gorm:"column:application_name"`
		State            string  `gorm:"column:state"`
		WaitEvent        string  `gorm:"column:wait_event"`
		WaitEventType    string  `gorm:"column:wait_event_type"`
		QueryStart       string  `gorm:"column:query_start"`
		QueryDurationSec float64 `gorm:"column:query_duration_sec"`
		BlockingPID      *int    `gorm:"column:blocking_pid"`
		QueryText        string  `gorm:"column:query_text"`
	}
	if err := b.db.WithContext(ctx).Raw(waitingQueriesQuery).Scan(&queries).Error; err != nil {
		return nil, err
	}

	result := make([]backend.WaitingQuery, len(queries))
	for i, q := range queries {
		blockedBy := ""
		if q.BlockingPID != nil {
			blockedBy = fmt.Sprintf("%d", *q.BlockingPID)
		}
		result[i] = backend.WaitingQuery{
			ID:               fmt.Sprintf("%d", q.PID),
			Username:         q.Username,
			Database:         q.DatabaseName,
			State:            q.State,
			WaitType:         fmt.Sprintf("%s: %s", q.WaitEventType, q.WaitEvent),
			BlockedBy:        blockedBy,
			Query:            q.QueryText,
			QueryDurationSec: q.QueryDurationSec,
		}
	}
	return result, nil
}

//go:embed list_slowest_queries.sql
var slowestQueriesQuery string

func (b *Backend) ListSlowestQueries(ctx context.Context) ([]backend.SlowQuery, error) {
	var queries []struct {
		QueryHash         string  `gorm:"column:query_hash"`
		Calls             int64   `gorm:"column:calls"`
		TotalTimeSec      float64 `gorm:"column:total_time_sec"`
		AvgTimeSec        float64 `gorm:"column:avg_time_sec"`
		MinTimeSec        float64 `gorm:"column:min_time_sec"`
		MaxTimeSec        float64 `gorm:"column:max_time_sec"`
		SharedBlocksHit   int64   `gorm:"column:shared_blocks_hit"`
		SharedBlocksRead  int64   `gorm:"column:shared_blocks_read"`
		SharedBlocksWrite int64   `gorm:"column:shared_blocks_written"`
		QueryText         string  `gorm:"column:query_text"`
	}
	if err := b.db.WithContext(ctx).Raw(slowestQueriesQuery).Scan(&queries).Error; err != nil {
		return nil, err
	}

	result := make([]backend.SlowQuery, len(queries))
	for i, q := range queries {
		result[i] = backend.SlowQuery{
			QueryHash:    q.QueryHash,
			Calls:        q.Calls,
			TotalTimeSec: q.TotalTimeSec,
			AvgTimeSec:   q.AvgTimeSec,
			MaxTimeSec:   q.MaxTimeSec,
			Query:        q.QueryText,
		}
	}
	return result, nil
}

//go:embed list_deadlocks.sql
var deadlocksQuery string

func (b *Backend) ListDeadlocks(ctx context.Context) ([]backend.Deadlock, error) {
	var deadlocks []struct {
		DatabaseName   string `gorm:"column:database_name"`
		DeadlockCount  int64  `gorm:"column:deadlock_count"`
		LastStatsReset string `gorm:"column:last_stats_reset"`
	}
	if err := b.db.WithContext(ctx).Raw(deadlocksQuery).Scan(&deadlocks).Error; err != nil {
		return nil, err
	}

	result := make([]backend.Deadlock, len(deadlocks))
	for i, d := range deadlocks {
		result[i] = backend.Deadlock{
			Database:  d.DatabaseName,
			Count:     d.DeadlockCount,
			Timestamp: d.LastStatsReset,
		}
	}
	return result, nil
}
