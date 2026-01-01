package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/tinternet/databaise/internal/sqlcommon"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Table represents a database table with schema.
type Table struct {
	Schema string `json:"schema" jsonschema:"The schema name"`
	Name   string `json:"name" jsonschema:"The table name"`
}

// ListTablesIn is the input for the list_tables tool.
type ListTablesIn struct {
	Schema string `json:"schema,omitempty" jsonschema:"Schema to list tables from"`
}

// ListTablesOut is the output for the list_tables tool.
type ListTablesOut struct {
	Tables []Table `json:"tables" jsonschema:"The list of tables"`
}

//go:embed list_tables.sql
var listTablesQuery string

func ListTables(ctx context.Context, in ListTablesIn, db DB) (out ListTablesOut, err error) {
	err = db.WithContext(ctx).Raw(listTablesQuery, in.Schema).Scan(&out.Tables).Error
	return
}

// DescribeTableIn is the input for the describe_table tool.
type DescribeTableIn struct {
	Schema string `json:"schema" jsonschema:"The schema the table is in,required"`
	Table  string `json:"table" jsonschema:"The name of the table to describe,required"`
}

// DescribeTableOut is the output for the describe_table tool.
type DescribeTableOut struct {
	CreateTable       string   `json:"create_table" jsonschema:"The CREATE TABLE statement for this table"`
	CreateIndexes     []string `json:"create_indexes,omitempty" jsonschema:"CREATE INDEX statements for indexes on this table"`
	CreateConstraints []string `json:"create_constraints,omitempty" jsonschema:"CREATE CONSTRAINT statements for constraints on this table"`
}

//go:embed ddl_table.sql
var queryTableDDL string

//go:embed ddl_indexes.sql
var queryIndexesDDL string

//go:embed ddl_constraints.sql
var queryConstraintsDDL string

func DescribeTable(ctx context.Context, in DescribeTableIn, db DB) (*DescribeTableOut, error) {
	var out DescribeTableOut
	g, ctx := errgroup.WithContext(ctx)
	table := fmt.Sprintf("%s.%s", in.Schema, in.Table)
	g.Go(func() error {
		return db.WithContext(ctx).Raw(queryTableDDL, table).Scan(&out.CreateTable).Error
	})
	g.Go(func() error {
		return db.WithContext(ctx).Raw(queryIndexesDDL, table).Scan(&out.CreateIndexes).Error
	})
	g.Go(func() error {
		return db.WithContext(ctx).Raw(queryConstraintsDDL, table).Scan(&out.CreateConstraints).Error
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if out.CreateTable == "" {
		return nil, sqlcommon.ErrTableNotFound
	}
	return &out, nil
}

type (
	ExecuteQueryIn  = sqlcommon.ExecuteQueryIn
	ExecuteQueryOut = sqlcommon.ExecuteQueryOut
)

func ExecuteQuery(ctx context.Context, in ExecuteQueryIn, db DB) (*ExecuteQueryOut, error) {
	var out ExecuteQueryOut

	if db.UseReadonlyTx {
		err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			return tx.Raw(in.Query).Scan(&out.Rows).Error
		}, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return nil, err
		}
		return &out, nil
	}

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

func ExplainQuery(ctx context.Context, in ExplainQueryIn, db DB) (*ExplainQueryOut, error) {
	var analyze string
	if in.Analyze {
		analyze = "ANALYZE, "
	}

	var planJSON string
	err := db.WithContext(ctx).Raw(fmt.Sprintf("EXPLAIN (%sFORMAT JSON) %s", analyze, in.Query)).Scan(&planJSON).Error
	if err != nil {
		return nil, err
	}

	var result []ExplainQueryOut
	if err := json.Unmarshal([]byte(planJSON), &result); err != nil {
		return nil, fmt.Errorf("failed to parse explain JSON: %w", err)
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("empty explain result")
	}

	return &result[0], nil
}

// CreateIndexIn is the input for the create_index tool.
type CreateIndexIn struct {
	Schema  string   `json:"schema" jsonschema:"The schema the table is in,required"`
	Table   string   `json:"table" jsonschema:"The table to create the index on,required"`
	Name    string   `json:"name" jsonschema:"The name of the index,required"`
	Columns []string `json:"columns" jsonschema:"The columns to include in the index,required"`
	Unique  bool     `json:"unique" jsonschema:"Whether to create a unique index,required"`
}

// CreateIndexOut is the output for the create_index tool.
type CreateIndexOut struct {
	Success bool   `json:"success" jsonschema:"Whether the index was created successfully"`
	Message string `json:"message,omitempty" jsonschema:"A message describing the result"`
}

func CreateIndex(ctx context.Context, in CreateIndexIn, db DB) (*CreateIndexOut, error) {
	if len(in.Columns) == 0 {
		return nil, fmt.Errorf("at least one column is required to create an index")
	}

	schema := in.Schema
	if schema == "" {
		schema = "public"
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
		fmt.Sprintf("CREATE %sINDEX ? ON ?.? (?)", unique),
		clause.Column{Name: in.Name},
		clause.Table{Name: schema},
		clause.Table{Name: in.Table},
		clause.CommaExpression{Exprs: exprs},
	).Error

	if err != nil {
		return nil, err
	}
	return &CreateIndexOut{Success: true, Message: fmt.Sprintf("Created index %s on %s.%s", in.Name, schema, in.Table)}, nil
}

// DropIndexIn is the input for the drop_index tool.
type DropIndexIn struct {
	Schema string `json:"schema" jsonschema:"The schema the index is in,required"`
	Name   string `json:"name" jsonschema:"The name of the index to drop,required"`
}

// DropIndexOut is the output for the drop_index tool.
type DropIndexOut struct {
	Success bool   `json:"success" jsonschema:"Whether the index was dropped successfully"`
	Message string `json:"message,omitempty" jsonschema:"A message describing the result"`
}

func DropIndex(ctx context.Context, in DropIndexIn, db DB) (*DropIndexOut, error) {
	schema := in.Schema
	if schema == "" {
		schema = "public"
	}

	err := db.WithContext(ctx).Exec(
		"DROP INDEX ?.? ",
		clause.Table{Name: schema},
		clause.Column{Name: in.Name},
	).Error

	if err != nil {
		return nil, err
	}
	return &DropIndexOut{Success: true, Message: fmt.Sprintf("Dropped index %s.%s", schema, in.Name)}, nil
}

// IndexRecommendation represents a missing index recommendation.
type IndexRecommendation struct {
	Schema           string  `json:"schema" jsonschema:"The schema name of the table"`
	TableName        string  `json:"table_name" jsonschema:"The name of the table on which the index is recommended"`
	SeqScans         int64   `json:"seq_scans" jsonschema:"Number of sequential scans on this table"`
	SeqTuplesRead    int64   `json:"seq_tuples_read" jsonschema:"Number of tuples read by sequential scans"`
	IndexScans       int64   `json:"index_scans" jsonschema:"Number of index scans on this table"`
	TableSizeMB      float64 `json:"table_size_mb" jsonschema:"Size of the table in megabytes"`
	EstimatedImpact  float64 `json:"estimated_impact" jsonschema:"Estimated impact score based on sequential scan frequency"`
	CreateSuggestion string  `json:"create_suggestion" jsonschema:"Suggestion for which columns might benefit from indexing"`
}

// MissingIndexesOut is the output for the list_missing_indexes tool.
type MissingIndexesOut struct {
	Indexes []IndexRecommendation `json:"indexes" jsonschema:"List of missing indexes with details"`
}

//go:embed missing_indexes.sql
var missingIndexesQuery string

func ListMissingIndexes(ctx context.Context, _ struct{}, db DB) (out MissingIndexesOut, err error) {
	err = db.WithContext(ctx).Raw(missingIndexesQuery).Scan(&out.Indexes).Error
	return
}

// WaitingQuery represents a currently waiting query.
type WaitingQuery struct {
	PID              int     `json:"pid" jsonschema:"Process ID of the backend"`
	Username         string  `json:"username" jsonschema:"Database user name"`
	DatabaseName     string  `json:"database_name" jsonschema:"Database name"`
	ApplicationName  string  `json:"application_name" jsonschema:"Application name connected to this backend"`
	State            string  `json:"state" jsonschema:"Current state of the backend"`
	WaitEvent        string  `json:"wait_event" jsonschema:"The wait event the backend is waiting for"`
	WaitEventType    string  `json:"wait_event_type" jsonschema:"The type of wait event"`
	QueryStart       string  `json:"query_start" jsonschema:"Time when the query started"`
	QueryDurationSec float64 `json:"query_duration_sec" jsonschema:"Duration of the query in seconds"`
	BlockingPID      *int    `json:"blocking_pid" jsonschema:"PID of the blocking process if any"`
	QueryText        string  `json:"query_text" jsonschema:"The SQL text of the waiting query"`
}

// WaitingQueriesOut is the output for the list_waiting_queries tool.
type WaitingQueriesOut struct {
	Queries []WaitingQuery `json:"queries" jsonschema:"List of currently waiting queries"`
}

//go:embed list_waiting_queries.sql
var waitingQueriesQuery string

func ListWaitingQueries(ctx context.Context, _ struct{}, db DB) (out WaitingQueriesOut, err error) {
	err = db.WithContext(ctx).Raw(waitingQueriesQuery).Scan(&out.Queries).Error
	return
}

// SlowestQuery represents a slow query.
type SlowestQuery struct {
	QueryHash         string  `json:"query_hash" jsonschema:"Hash of the query for identification"`
	Calls             int64   `json:"calls" jsonschema:"Number of times this query has been executed"`
	TotalTimeSec      float64 `json:"total_time_sec" jsonschema:"Total time spent executing this query in seconds"`
	AvgTimeSec        float64 `json:"avg_time_sec" jsonschema:"Average time per execution in seconds"`
	MinTimeSec        float64 `json:"min_time_sec" jsonschema:"Minimum execution time in seconds"`
	MaxTimeSec        float64 `json:"max_time_sec" jsonschema:"Maximum execution time in seconds"`
	SharedBlocksHit   int64   `json:"shared_blocks_hit" jsonschema:"Shared blocks hit from cache"`
	SharedBlocksRead  int64   `json:"shared_blocks_read" jsonschema:"Shared blocks read from disk"`
	SharedBlocksWrite int64   `json:"shared_blocks_written" jsonschema:"Shared blocks written to disk"`
	QueryText         string  `json:"query_text" jsonschema:"The SQL text of the query"`
}

// SlowestQueriesOut is the output for the list_slowest_queries tool.
type SlowestQueriesOut struct {
	Queries []SlowestQuery `json:"queries" jsonschema:"List of slowest queries by total elapsed time"`
}

//go:embed list_slowest_queries.sql
var slowestQueriesQuery string

func ListSlowestQueries(ctx context.Context, _ struct{}, db DB) (out SlowestQueriesOut, err error) {
	err = db.WithContext(ctx).Raw(slowestQueriesQuery).Scan(&out.Queries).Error
	return
}

// DeadlockInfo represents deadlock information.
type DeadlockInfo struct {
	DatabaseName   string `json:"database_name" jsonschema:"Database name where deadlocks occurred"`
	DeadlockCount  int64  `json:"deadlock_count" jsonschema:"Total number of deadlocks detected"`
	LastStatsReset string `json:"last_stats_reset" jsonschema:"When the statistics were last reset"`
}

// DeadlocksOut is the output for the list_deadlocks tool.
type DeadlocksOut struct {
	Deadlocks []DeadlockInfo `json:"deadlocks" jsonschema:"List of deadlock information"`
}

//go:embed list_deadlocks.sql
var deadlocksQuery string

func ListDeadlocks(ctx context.Context, _ struct{}, db DB) (out DeadlocksOut, err error) {
	err = db.WithContext(ctx).Raw(deadlocksQuery).Scan(&out.Deadlocks).Error
	return
}
