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

//go:embed missing_indexes.sql
var missingIndexesQuery string

func (b *Backend) ListMissingIndexes(ctx context.Context) ([]backend.MissingIndex, error) {
	var indexes []struct {
		TableSchema      string  `gorm:"column:table_schema"`
		TableName        string  `gorm:"column:table_name"`
		FullTableScans   int64   `gorm:"column:full_table_scans"`
		RowsRead         int64   `gorm:"column:rows_read"`
		RowsChanged      int64   `gorm:"column:rows_changed"`
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
			Schema:          idx.TableSchema,
			TableName:       idx.TableName,
			Reason:          fmt.Sprintf("full_scans=%d, rows_read=%d, rows_changed=%d, size=%.2fMB", idx.FullTableScans, idx.RowsRead, idx.RowsChanged, idx.TableSizeMB),
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

func (b *Backend) ListSlowestQueries(ctx context.Context) ([]backend.SlowQuery, error) {
	var queries []struct {
		DigestText   string  `gorm:"column:digest_text"`
		SchemaName   string  `gorm:"column:schema_name"`
		CountStar    int64   `gorm:"column:count_star"`
		TotalTimeSec float64 `gorm:"column:total_time_sec"`
		AvgTimeSec   float64 `gorm:"column:avg_time_sec"`
		MinTimeSec   float64 `gorm:"column:min_time_sec"`
		MaxTimeSec   float64 `gorm:"column:max_time_sec"`
	}
	if err := b.db.WithContext(ctx).Raw(slowestQueriesQuery).Scan(&queries).Error; err != nil {
		return nil, err
	}

	result := make([]backend.SlowQuery, len(queries))
	for i, q := range queries {
		result[i] = backend.SlowQuery{
			QueryHash:    q.DigestText[:min(50, len(q.DigestText))],
			Calls:        q.CountStar,
			TotalTimeSec: q.TotalTimeSec,
			AvgTimeSec:   q.AvgTimeSec,
			MaxTimeSec:   q.MaxTimeSec,
			Query:        q.DigestText,
		}
	}
	return result, nil
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
