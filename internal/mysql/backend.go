package mysql

import (
	"context"
	_ "embed"
	"fmt"
	"strings"

	"github.com/tinternet/databaise/internal/backend"
	"github.com/tinternet/databaise/internal/logging"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var log = logging.New("mysql")

// ReadConfig for read connections.
type ReadConfig struct {
	DSN                 string `json:"dsn"`
	BypassReadonlyCheck bool   `json:"bypass_readonly_check,omitempty"`
}

// AdminConfig for admin connections.
type AdminConfig struct {
	DSN string `json:"dsn"`
}

// Factory implements backend.BackendFactory for MySQL.
type Factory struct{}

func (Factory) Dialect() string { return "MySQL" }

func (Factory) New(db *gorm.DB) backend.SQLBackend {
	return &Backend{db: db}
}

// Connector implements backend.Connector for MySQL.
type Connector struct{}

func (Connector) ConnectRead(cfg ReadConfig) (*gorm.DB, error) {
	log.Printf("Opening read connection")
	dsn := enableParseTime(cfg.DSN)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: logging.NewGormLogger()})
	if err != nil {
		return nil, err
	}

	if !cfg.BypassReadonlyCheck {
		var grants []string
		if err := db.Raw("SHOW GRANTS FOR CURRENT_USER;").Scan(&grants).Error; err != nil {
			return nil, fmt.Errorf("could not verify user permissions: %w", err)
		}
		for _, g := range grants {
			for _, p := range []string{"INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER"} {
				if strings.Contains(g, p) {
					return nil, fmt.Errorf("read DSN user has write permissions (set bypass_readonly_check: true to bypass)")
				}
			}
		}
		log.Printf("Verified read connection is readonly")
	} else {
		log.Printf("Skipping readonly verification (bypass_readonly_check: true)")
	}

	return db, nil
}

func (Connector) ConnectAdmin(cfg AdminConfig) (*gorm.DB, error) {
	log.Printf("Opening admin connection")
	dsn := enableParseTime(cfg.DSN)
	return gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: logging.NewGormLogger()})
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

func init() {
	backend.RegisterFactory("mysql", Factory{}, Connector{})
}

// Backend implements backend.SQLBackend for MySQL.
type Backend struct {
	db *gorm.DB
}

func (b *Backend) ListTables(ctx context.Context, in backend.ListTablesIn) ([]backend.Table, error) {
	var tables []string
	if err := b.db.WithContext(ctx).Raw("SHOW TABLES;").Scan(&tables).Error; err != nil {
		return nil, err
	}

	result := make([]backend.Table, len(tables))
	for i, t := range tables {
		result[i] = backend.Table{Name: t}
	}
	return result, nil
}

func (b *Backend) DescribeTable(ctx context.Context, in backend.DescribeTableIn) (*backend.TableDescription, error) {
	var result struct {
		Table       string `gorm:"column:Table"`
		CreateTable string `gorm:"column:Create Table"`
	}
	if err := b.db.WithContext(ctx).Raw("SHOW CREATE TABLE ?", clause.Table{Name: in.Table}).Scan(&result).Error; err != nil {
		return nil, err
	}
	return &backend.TableDescription{CreateTable: result.CreateTable}, nil
}

func (b *Backend) ExecuteQuery(ctx context.Context, in backend.ReadQueryIn) (*backend.QueryResult, error) {
	var rows []map[string]any
	if err := b.db.WithContext(ctx).Raw(in.Query).Scan(&rows).Error; err != nil {
		return nil, err
	}
	return &backend.QueryResult{Rows: rows}, nil
}

func (b *Backend) ExplainQuery(ctx context.Context, in backend.ExplainQueryIn) (*backend.ExplainResult, error) {
	var explainQuery string
	if in.Analyze {
		explainQuery = fmt.Sprintf("EXPLAIN ANALYZE FORMAT=JSON %s", in.Query)
	} else {
		explainQuery = fmt.Sprintf("EXPLAIN FORMAT=JSON %s", in.Query)
	}

	var planJSON string
	if err := b.db.WithContext(ctx).Raw(explainQuery).Scan(&planJSON).Error; err != nil {
		return nil, err
	}

	return &backend.ExplainResult{
		Format:     "json",
		Result:     planJSON,
		ResultInfo: "The MySQL query plan as returned from the database",
	}, nil
}

func (b *Backend) ExecuteDDL(ctx context.Context, in backend.ExecuteDDLIn) (*backend.DDLResult, error) {
	if err := b.db.WithContext(ctx).Exec(in.DDL).Error; err != nil {
		return nil, err
	}
	return &backend.DDLResult{Success: true, Message: "DDL executed successfully"}, nil
}

func (b *Backend) ListMissingIndexes(ctx context.Context) ([]backend.MissingIndex, error) {
	return nil, fmt.Errorf("MySQL does not provide automatic index recommendations. Use list_slowest_queries to identify queries that may benefit from indexing - look for queries with high no_index_used or full_scan counts")
}

//go:embed list_waiting_queries.sql
var waitingQueriesQuery string

func (b *Backend) ListWaitingQueries(ctx context.Context) ([]backend.WaitingQuery, error) {
	var queries []struct {
		ThreadID         int64   `gorm:"column:thread_id"`
		Username         string  `gorm:"column:username"`
		DatabaseName     string  `gorm:"column:database_name"`
		Command          string  `gorm:"column:command"`
		State            string  `gorm:"column:state"`
		WaitEvent        string  `gorm:"column:wait_event"`
		WaitEventType    string  `gorm:"column:wait_event_type"`
		TimeSeconds      float64 `gorm:"column:time_seconds"`
		BlockingThreadID *int64  `gorm:"column:blocking_thread_id"`
		QueryText        string  `gorm:"column:query_text"`
	}
	if err := b.db.WithContext(ctx).Raw(waitingQueriesQuery).Scan(&queries).Error; err != nil {
		return nil, err
	}

	result := make([]backend.WaitingQuery, len(queries))
	for i, q := range queries {
		blockedBy := ""
		if q.BlockingThreadID != nil {
			blockedBy = fmt.Sprintf("%d", *q.BlockingThreadID)
		}
		result[i] = backend.WaitingQuery{
			ID:               fmt.Sprintf("%d", q.ThreadID),
			Username:         q.Username,
			Database:         q.DatabaseName,
			State:            q.State,
			WaitType:         fmt.Sprintf("%s: %s", q.WaitEventType, q.WaitEvent),
			BlockedBy:        blockedBy,
			Query:            q.QueryText,
			QueryDurationSec: q.TimeSeconds,
		}
	}
	return result, nil
}

//go:embed list_slowest_queries.sql
var slowestQueriesQuery string

func (b *Backend) ListSlowestQueries(ctx context.Context) (*backend.SlowQueryResult, error) {
	var queries []map[string]any
	if err := b.db.WithContext(ctx).Raw(slowestQueriesQuery).Scan(&queries).Error; err != nil {
		return nil, err
	}

	return &backend.SlowQueryResult{
		Columns: map[string]string{
			"query":              "The normalized SQL query text",
			"schema_name":        "Database schema name",
			"calls":              "Number of times this query was executed",
			"total_time_sec":     "Total execution time in seconds",
			"avg_time_sec":       "Average execution time in seconds",
			"max_time_sec":       "Maximum execution time in seconds",
			"lock_time_sec":      "Total time spent waiting for locks in seconds",
			"rows_examined":      "Total rows examined across all executions",
			"rows_sent":          "Total rows returned to client",
			"rows_affected":      "Total rows affected (INSERT/UPDATE/DELETE)",
			"no_index_used":      "Number of executions where no index was used",
			"no_good_index_used": "Number of executions where no good index was found",
			"full_join":          "Number of full joins (joins without indexes)",
			"full_scan":          "Number of full table scans",
			"tmp_tables":         "Number of temporary tables created",
			"tmp_disk_tables":    "Number of temporary tables created on disk (memory exceeded)",
			"errors":             "Number of errors",
			"warnings":           "Number of warnings",
			"first_seen":         "When this query was first seen",
			"last_seen":          "When this query was last seen",
		},
		Queries: queries,
	}, nil
}

func (b *Backend) ListDeadlocks(ctx context.Context) ([]backend.Deadlock, error) {
	type InnoDBStatus struct {
		Type   string `gorm:"column:Type"`
		Name   string `gorm:"column:Name"`
		Status string `gorm:"column:Status"`
	}

	var status InnoDBStatus
	if err := b.db.WithContext(ctx).Raw("SHOW ENGINE INNODB STATUS").Scan(&status).Error; err != nil {
		return nil, err
	}

	return []backend.Deadlock{
		{
			Details: status.Status,
		},
	}, nil
}
