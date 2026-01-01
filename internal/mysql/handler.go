package mysql

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/tinternet/databaise/internal/sqlcommon"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

//go:embed missing_indexes.sql
var missingIndexesQuery string

//go:embed list_waiting_queries.sql
var waitingQueriesQuery string

//go:embed list_slowest_queries.sql
var slowestQueriesQuery string

// ListTablesIn is the input for the list_tables tool.
type ListTablesIn struct{}

// ListTablesOut is the output for the list_tables tool.
type ListTablesOut struct {
	Tables []string `json:"tables" jsonschema:"The list of table names"`
}

func ListTables(ctx context.Context, in ListTablesIn, db *gorm.DB) (out ListTablesOut, err error) {
	err = db.WithContext(ctx).Raw("SHOW TABLES;").Scan(&out.Tables).Error
	return
}

// DescribeTableIn is the input for the describe_table tool.
type DescribeTableIn struct {
	Table string `json:"table" jsonschema:"The name of the table to describe,required"`
}

// DescribeTableOut is the output for the describe_table tool.
type DescribeTableOut struct {
	Table       string `gorm:"column:Table" json:"table" jsonschema:"The table name"`
	CreateTable string `gorm:"column:Create Table" json:"ddl" jsonschema:"The CREATE TABLE statement for this table"`
}

func DescribeTable(ctx context.Context, in DescribeTableIn, db *gorm.DB) (*DescribeTableOut, error) {
	var result DescribeTableOut
	err := db.WithContext(ctx).Raw("SHOW CREATE TABLE ?", clause.Table{Name: in.Table}).Scan(&result).Error
	if err != nil {
		return nil, err
	}
	return &result, nil
}

type (
	ExecuteQueryIn  = sqlcommon.ExecuteQueryIn
	ExecuteQueryOut = sqlcommon.ExecuteQueryOut
)

func ExecuteQuery(ctx context.Context, in ExecuteQueryIn, db *gorm.DB) (*ExecuteQueryOut, error) {
	var out ExecuteQueryOut
	if err := db.WithContext(ctx).Raw(in.Query).Scan(&out.Rows).Error; err != nil {
		return nil, err
	}
	return &out, nil
}

// ExplainQueryIn is the input for the explain_query tool.
type ExplainQueryIn struct {
	Query   string `json:"query" jsonschema:"The SQL query to explain,required"`
	Analyze bool   `json:"analyze" jsonschema:"Whether to execute the query for actual runtime statistics"`
}

// ExplainQueryOut is the output for the explain_query tool.
type ExplainQueryOut struct {
	Plan map[string]any `json:"plan" jsonschema:"The execution plan of the query"`
}

func ExplainQuery(ctx context.Context, in ExplainQueryIn, db *gorm.DB) (*ExplainQueryOut, error) {
	var query string
	if in.Analyze {
		query = fmt.Sprintf("EXPLAIN ANALYZE FORMAT=JSON %s", in.Query)
	} else {
		query = fmt.Sprintf("EXPLAIN FORMAT=JSON %s", in.Query)
	}

	var planJSON string
	err := db.WithContext(ctx).Raw(query).Scan(&planJSON).Error
	if err != nil {
		return nil, err
	}

	var result ExplainQueryOut
	if err := json.Unmarshal([]byte(planJSON), &result.Plan); err != nil {
		return nil, fmt.Errorf("failed to parse explain JSON: %w", err)
	}

	return &result, nil
}

// CreateIndexIn is the input for the create_index tool.
type CreateIndexIn struct {
	Table   string   `json:"table" jsonschema:"The table to create the index on,required"`
	Name    string   `json:"name" jsonschema:"The name of the index,required"`
	Columns []string `json:"columns" jsonschema:"The columns to include in the index,required"`
	Unique  bool     `json:"unique,omitempty" jsonschema:"Whether to create a unique index"`
}

// CreateIndexOut is the output for the create_index tool.
type CreateIndexOut struct {
	Success bool   `json:"success" jsonschema:"Whether the index was created successfully"`
	Message string `json:"message,omitempty" jsonschema:"A message describing the result"`
}

func CreateIndex(ctx context.Context, in CreateIndexIn, db *gorm.DB) (*CreateIndexOut, error) {
	if len(in.Columns) == 0 {
		return nil, fmt.Errorf("at least one column is required to create an index")
	}

	unique := ""
	if in.Unique {
		unique = "UNIQUE "
	}

	exprs := make([]clause.Expression, len(in.Columns))
	for i, col := range in.Columns {
		exprs[i] = clause.Expr{SQL: "?", Vars: []any{clause.Column{Name: col}}}
	}

	err := db.WithContext(ctx).Exec(
		fmt.Sprintf("CREATE %s INDEX ? ON ? (?)", unique),
		clause.Column{Name: in.Name},
		clause.Table{Name: in.Table},
		clause.CommaExpression{Exprs: exprs},
	).Error
	if err != nil {
		return nil, err
	}

	return &CreateIndexOut{Success: true, Message: fmt.Sprintf("Created index %s on %s", in.Name, in.Table)}, nil
}

// DropIndexIn is the input for the drop_index tool.
type DropIndexIn struct {
	Name  string `json:"name" jsonschema:"The name of the index to drop,required"`
	Table string `json:"table" jsonschema:"The name of the index's table,required"`
}

// DropIndexOut is the output for the drop_index tool.
type DropIndexOut struct {
	Success bool   `json:"success" jsonschema:"Whether the index was dropped successfully"`
	Message string `json:"message,omitempty" jsonschema:"A message describing the result"`
}

func DropIndex(ctx context.Context, in DropIndexIn, db *gorm.DB) (*DropIndexOut, error) {
	err := db.WithContext(ctx).Exec(
		"DROP INDEX ? ON ?;",
		clause.Table{Name: in.Name},
		clause.Table{Name: in.Table},
	).Error
	if err != nil {
		return nil, err
	}
	return &DropIndexOut{Success: true, Message: fmt.Sprintf("Dropped index %s", in.Name)}, nil
}

// IndexRecommendation represents a missing index recommendation.
type IndexRecommendation struct {
	TableSchema      string  `json:"table_schema" jsonschema:"The schema name of the table"`
	TableName        string  `json:"table_name" jsonschema:"The name of the table on which the index is recommended"`
	FullTableScans   int64   `json:"full_table_scans" jsonschema:"Number of full table scans performed"`
	RowsRead         int64   `json:"rows_read" jsonschema:"Number of rows read from this table"`
	RowsChanged      int64   `json:"rows_changed" jsonschema:"Number of rows changed in this table"`
	TableSizeMB      float64 `json:"table_size_mb" jsonschema:"Size of the table in megabytes"`
	EstimatedImpact  float64 `json:"estimated_impact" jsonschema:"Estimated impact score based on table scan frequency"`
	CreateSuggestion string  `json:"create_suggestion" jsonschema:"Suggestion for which columns might benefit from indexing"`
}

// MissingIndexesOut is the output for the list_missing_indexes tool.
type MissingIndexesOut struct {
	Indexes []IndexRecommendation `json:"indexes" jsonschema:"List of missing indexes with details"`
}

func ListMissingIndexes(ctx context.Context, _ struct{}, db *gorm.DB) (out MissingIndexesOut, err error) {
	err = db.WithContext(ctx).Raw(missingIndexesQuery).Scan(&out.Indexes).Error
	return
}

// WaitingQuery represents a currently waiting query.
type WaitingQuery struct {
	ThreadID         int64   `json:"thread_id" jsonschema:"Thread ID of the connection"`
	Username         string  `json:"username" jsonschema:"Database user name"`
	DatabaseName     string  `json:"database_name" jsonschema:"Database name"`
	Command          string  `json:"command" jsonschema:"Command type being executed"`
	State            string  `json:"state" jsonschema:"Current state of the thread"`
	WaitEvent        string  `json:"wait_event" jsonschema:"The wait event the thread is waiting for"`
	WaitEventType    string  `json:"wait_event_type" jsonschema:"The type of wait event"`
	TimeSeconds      float64 `json:"time_seconds" jsonschema:"Time the query has been in this state in seconds"`
	BlockingThreadID *int64  `json:"blocking_thread_id" jsonschema:"Thread ID of the blocking connection if any"`
	QueryText        string  `json:"query_text" jsonschema:"The SQL text of the waiting query"`
}

// WaitingQueriesOut is the output for the list_waiting_queries tool.
type WaitingQueriesOut struct {
	Queries []WaitingQuery `json:"queries" jsonschema:"List of currently waiting queries"`
}

func ListWaitingQueries(ctx context.Context, _ struct{}, db *gorm.DB) (out WaitingQueriesOut, err error) {
	err = db.WithContext(ctx).Raw(waitingQueriesQuery).Scan(&out.Queries).Error
	return
}

// SlowestQuery represents a slow query.
type SlowestQuery struct {
	DigestText   string  `json:"digest_text" jsonschema:"Normalized query text"`
	SchemaName   string  `json:"schema_name" jsonschema:"Schema name where query was executed"`
	CountStar    int64   `json:"count_star" jsonschema:"Number of times this query has been executed"`
	TotalTimeSec float64 `json:"total_time_sec" jsonschema:"Total time spent executing this query in seconds"`
	AvgTimeSec   float64 `json:"avg_time_sec" jsonschema:"Average time per execution in seconds"`
	MinTimeSec   float64 `json:"min_time_sec" jsonschema:"Minimum execution time in seconds"`
	MaxTimeSec   float64 `json:"max_time_sec" jsonschema:"Maximum execution time in seconds"`
	RowsExamined int64   `json:"rows_examined" jsonschema:"Total rows examined"`
	RowsSent     int64   `json:"rows_sent" jsonschema:"Total rows sent to client"`
	RowsAffected int64   `json:"rows_affected" jsonschema:"Total rows affected"`
}

// SlowestQueriesOut is the output for the list_slowest_queries tool.
type SlowestQueriesOut struct {
	Queries []SlowestQuery `json:"queries" jsonschema:"List of slowest queries by total elapsed time"`
}

func ListSlowestQueries(ctx context.Context, _ struct{}, db *gorm.DB) (out SlowestQueriesOut, err error) {
	err = db.WithContext(ctx).Raw(slowestQueriesQuery).Scan(&out.Queries).Error
	return
}

// DeadlockInfo represents deadlock information.
type DeadlockInfo struct {
	DeadlockReport string `json:"deadlock_report" jsonschema:"The deadlock information from SHOW ENGINE INNODB STATUS"`
	Note           string `json:"note" jsonschema:"Note about the data source"`
}

// DeadlocksOut is the output for the list_deadlocks tool.
type DeadlocksOut struct {
	DeadlockInfo DeadlockInfo `json:"deadlock_info" jsonschema:"Most recent deadlock information from InnoDB status"`
}

func ListDeadlocks(ctx context.Context, _ struct{}, db *gorm.DB) (DeadlocksOut, error) {
	type InnoDBStatus struct {
		Type   string `gorm:"column:Type"`
		Name   string `gorm:"column:Name"`
		Status string `gorm:"column:Status"`
	}

	var status InnoDBStatus
	err := db.WithContext(ctx).Raw("SHOW ENGINE INNODB STATUS").Scan(&status).Error
	if err != nil {
		return DeadlocksOut{}, err
	}

	// Extract deadlock section from the status output
	// The status contains various sections, we're interested in the LATEST DETECTED DEADLOCK section
	out := DeadlocksOut{
		DeadlockInfo: DeadlockInfo{
			DeadlockReport: status.Status,
			Note:           "Shows the most recent deadlock from SHOW ENGINE INNODB STATUS. For detailed deadlock history, check MySQL error logs.",
		},
	}
	return out, nil
}
