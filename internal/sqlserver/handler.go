package sqlserver

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"

	"github.com/tinternet/databaise/internal/sqlcommon"
	"golang.org/x/sync/errgroup"
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
	err = db.WithContext(ctx).Raw(listTablesQuery, sql.Named("schema", in.Schema)).Scan(&out.Tables).Error
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
var ddlTableQuery string

//go:embed ddl_indexes.sql
var ddlIndexesQuery string

//go:embed ddl_constraints.sql
var ddlConstraintsQuery string

func DescribeTable(ctx context.Context, in DescribeTableIn, db DB) (*DescribeTableOut, error) {
	var result DescribeTableOut
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return db.WithContext(ctx).Raw(ddlTableQuery, in.Table, in.Schema).Scan(&result.CreateTable).Error
	})
	g.Go(func() error {
		return db.WithContext(ctx).Raw(ddlIndexesQuery, in.Table, in.Schema).Scan(&result.CreateIndexes).Error
	})
	g.Go(func() error {
		st := fmt.Sprintf("%s.%s", in.Schema, in.Table)
		return db.WithContext(ctx).Raw(ddlConstraintsQuery, st, in.Table, in.Schema).Scan(&result.CreateConstraints).Error
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if result.CreateTable == "" {
		return nil, sqlcommon.ErrTableNotFound
	}
	return &result, nil
}

type (
	ExecuteQueryIn  = sqlcommon.ExecuteQueryIn
	ExecuteQueryOut = sqlcommon.ExecuteQueryOut
)

func ExecuteQuery(ctx context.Context, in ExecuteQueryIn, db DB) (out ExecuteQueryOut, err error) {
	err = db.WithContext(ctx).Raw(in.Query).Scan(&out.Rows).Error
	return
}

// ExplainQueryIn is the input for the explain_query tool.
type ExplainQueryIn struct {
	Query               string `json:"query" jsonschema:"The SQL query to explain,required"`
	ActualExecutionPlan bool   `json:"actual_execution_plan,omitempty" jsonschema:"Whether to execute the query and get the actual execution plan"`
}

// ExplainQueryOut is the output for the explain_query tool.
type ExplainQueryOut struct {
	Plan string           `json:"plan" jsonschema:"The execution plan for the query in XML format"`
	Rows []map[string]any `json:"rows,omitempty" jsonschema:"The result rows if actual_execution_plan is true"`
}

func ExplainQuery(ctx context.Context, in ExplainQueryIn, db DB) (*ExplainQueryOut, error) {
	tx := db.WithContext(ctx).Begin()
	defer tx.Rollback()
	if err := tx.Error; err != nil {
		return nil, err
	}
	var enable, disable string
	if in.ActualExecutionPlan {
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
	var results []map[string]any

	if in.ActualExecutionPlan {
		// For actual execution plan, we need to execute the query
		rows, err := tx.Raw(in.Query).Rows()
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			if err := db.ScanRows(rows, &results); err != nil {
				return nil, err
			}
		}

		// The execution plan is returned in the next result set
		if rows.NextResultSet() {
			if rows.Next() {
				if err := rows.Scan(&plan); err != nil {
					return nil, err
				}
			}
		}
	} else {
		// For estimated plan, the plan is in the first result set
		if err := tx.Raw(in.Query).Scan(&plan).Error; err != nil {
			return nil, err
		}
	}

	if err := tx.Exec(disable).Error; err != nil {
		return nil, err
	}
	return &ExplainQueryOut{Plan: plan, Rows: results}, nil
}

// CreateIndexIn is the input for the create_index tool.
type CreateIndexIn struct {
	Schema    string   `json:"schema" jsonschema:"The schema the table is in,required"`
	Table     string   `json:"table" jsonschema:"The table to create the index on,required"`
	Name      string   `json:"name" jsonschema:"The name of the index,required"`
	Columns   []string `json:"columns" jsonschema:"The columns to include in the index,required"`
	Unique    bool     `json:"unique" jsonschema:"Whether to create a unique index,required"`
	Clustered bool     `json:"clustered" jsonschema:"Whether to create a clustered index,required"`
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
		schema = "dbo"
	}

	unique := ""
	if in.Unique {
		unique = "UNIQUE"
	}

	clustered := "NONCLUSTERED"
	if in.Clustered {
		clustered = "CLUSTERED"
	}

	exprs := make([]clause.Expression, len(in.Columns))
	for i, col := range in.Columns {
		exprs[i] = clause.Expr{SQL: "?", Vars: []any{clause.Column{Name: col}}}
	}

	err := db.WithContext(ctx).Exec(
		fmt.Sprintf("CREATE %s %s INDEX ? ON ?.? (?)", unique, clustered),
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
	Table  string `json:"table" jsonschema:"The name of the table the index is on,required"`
}

// DropIndexOut is the output for the drop_index tool.
type DropIndexOut struct {
	Success bool   `json:"success" jsonschema:"Whether the index was dropped successfully"`
	Message string `json:"message,omitempty" jsonschema:"A message describing the result"`
}

func DropIndex(ctx context.Context, in DropIndexIn, db DB) (*DropIndexOut, error) {
	err := db.WithContext(ctx).Exec(
		"DROP INDEX ? ON ?.?",
		clause.Column{Name: in.Name},
		clause.Table{Name: in.Schema},
		clause.Table{Name: in.Table},
	).Error
	if err != nil {
		return nil, err
	}
	return &DropIndexOut{Success: true, Message: fmt.Sprintf("Dropped index %s on %s.%s", in.Name, in.Schema, in.Table)}, nil
}

// IndexRecommendation represents a missing index recommendation.
type IndexRecommendation struct {
	AverageEstimatedImpact float64 `json:"average_estimated_impact" jsonschema:"The average estimated impact score of the missing index"`
	CreateStatement        string  `json:"create_statement" jsonschema:"The CREATE INDEX statement to create the missing index"`
	LastUserSeek           string  `json:"last_user_seek" jsonschema:"The timestamp of the last user seek that would have benefited from the index"`
	TableName              string  `json:"table_name" jsonschema:"The name of the table on which the index is recommended"`
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
	StartTime    string `json:"start_time" jsonschema:"The time when the query started execution"`
	QueryText    string `json:"query_text" jsonschema:"The SQL text of the waiting query"`
	Status       string `json:"status" jsonschema:"The current status of the query"`
	Command      string `json:"command" jsonschema:"The command type of the query"`
	WaitType     string `json:"wait_type" jsonschema:"The type of wait the query is experiencing"`
	WaitTimeMS   int    `json:"wait_time_ms" jsonschema:"The total wait time in milliseconds"`
	WaitResource string `json:"wait_resource" jsonschema:"The resource the query is waiting on"`
	LastWaitType string `json:"last_wait_type" jsonschema:"The last wait type experienced by the query"`
}

// WaitingQueriesOut is the output for the list_waiting_queries tool.
type WaitingQueriesOut struct {
	Queries []WaitingQuery `json:"queries" jsonschema:"List of currently waiting queries"`
}

//go:embed list_waiting_queries.sql
var waitingQueriesSQL string

func ListWaitingQueries(ctx context.Context, _ struct{}, db DB) (out WaitingQueriesOut, err error) {
	err = db.WithContext(ctx).Raw(waitingQueriesSQL).Scan(&out.Queries).Error
	return
}

// SlowestQuery represents a slow query.
type SlowestQuery struct {
	StatementText      string `json:"statement_text" jsonschema:"The SQL text of the query"`
	WaitType           string `json:"wait_type" jsonschema:"The wait type of the query"`
	CreationTime       string `json:"creation_time" jsonschema:"The time when the query plan was created"`
	LastExecTime       string `json:"last_execution_time" jsonschema:"The last time the query was executed"`
	TotalPhysicalReads int    `json:"total_physical_reads" jsonschema:"The total number of physical reads performed by the query"`
	TotalLogicalReads  int    `json:"total_logical_reads" jsonschema:"The total number of logical reads performed by the query"`
	TotalLogicalWrites int    `json:"total_logical_writes" jsonschema:"The total number of logical writes performed by the query"`
	ExecutionCount     int    `json:"execution_count" jsonschema:"The total number of times the query has been executed"`
	TotalWorkerTime    int    `json:"total_worker_time" jsonschema:"The total worker time in milliseconds consumed by the query"`
	TotalElapsedTime   int    `json:"total_elapsed_time" jsonschema:"The total elapsed time in milliseconds for the query"`
	AvgElapsedTime     int    `json:"avg_elapsed_time" jsonschema:"The average elapsed time in milliseconds per execution of the query"`
}

// SlowestQueriesOut is the output for the list_slowest_queries tool.
type SlowestQueriesOut struct {
	Queries []SlowestQuery `json:"queries" jsonschema:"List of slowest queries by total elapsed time"`
}

//go:embed list_slowest_queries.sql
var slowestQueriesSQL string

func ListSlowestQueries(ctx context.Context, _ struct{}, db DB) (out SlowestQueriesOut, err error) {
	err = db.WithContext(ctx).Raw(slowestQueriesSQL).Scan(&out.Queries).Error
	return
}

// DeadlockInfo represents deadlock information.
type DeadlockInfo struct {
	DeadlockReport string `json:"deadlock_report" jsonschema:"The XML report of the deadlock"`
	ExecutionTime  string `json:"execution_time" jsonschema:"The time when the deadlock occurred"`
}

// DeadlocksOut is the output for the list_deadlocks tool.
type DeadlocksOut struct {
	Deadlocks []DeadlockInfo `json:"deadlocks" jsonschema:"List of recent deadlocks"`
}

//go:embed list_deadlocks.sql
var listDeadlocksSQL string

func ListDeadlocks(ctx context.Context, _ struct{}, db DB) (out DeadlocksOut, err error) {
	err = db.WithContext(ctx).Raw(listDeadlocksSQL).Scan(&out.Deadlocks).Error
	return
}
