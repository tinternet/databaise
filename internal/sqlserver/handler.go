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

type (
	ListTablesIn     = sqlcommon.ListTablesIn
	ListTablesOut    = sqlcommon.ListTablesOut
	DescribeTableIn  = sqlcommon.DescribeTableIn
	DescribeTableOut = sqlcommon.DescribeTableOut
	ExecuteQueryIn   = sqlcommon.ExecuteQueryIn
	ExecuteQueryOut  = sqlcommon.ExecuteQueryOut
	CreateIndexOut   = sqlcommon.CreateIndexOut
	DropIndexOut     = sqlcommon.DropIndexOut
)

type DropIndexIn struct {
	sqlcommon.DropIndexIn
	Table string `json:"table" jsonschema:"The name of the table the index is on,required"`
}

type CreateIndexIn struct {
	sqlcommon.CreateIndexIn
	Clustered bool `json:"clustered" jsonschema:"Whether to create a clustered index,required"`
}

const listTablesQuery = `
	SELECT TABLE_SCHEMA as [schema], TABLE_NAME as name
	FROM INFORMATION_SCHEMA.TABLES
	WHERE TABLE_SCHEMA = CASE @schema WHEN '' THEN TABLE_SCHEMA ELSE @schema END AND TABLE_TYPE = 'BASE TABLE'
	ORDER BY TABLE_NAME
`

const listColumnsQuery = `
	SELECT
		COLUMN_NAME as name,
		DATA_TYPE as database_type,
		CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END as is_nullable,
		COLUMN_DEFAULT as default_value
	FROM INFORMATION_SCHEMA.COLUMNS
	WHERE TABLE_SCHEMA = CASE @schema WHEN '' THEN TABLE_SCHEMA ELSE @schema END AND TABLE_NAME = @table
	ORDER BY ORDINAL_POSITION
`

//go:embed list_indexes.sql
var listIndexesQuery string

func ListTables(ctx context.Context, in ListTablesIn, db DB) (out ListTablesOut, err error) {
	err = db.WithContext(ctx).Raw(listTablesQuery, sql.Named("schema", in.Schema)).Scan(&out.Tables).Error
	return
}

func DescribeTable(ctx context.Context, in DescribeTableIn, db DB) (*DescribeTableOut, error) {
	out := DescribeTableOut{Schema: in.Schema, Name: in.Table}
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(2)
	g.Go(func() error {
		return db.WithContext(ctx).Raw(listColumnsQuery, sql.Named("schema", in.Schema), sql.Named("table", in.Table)).Scan(&out.Columns).Error
	})
	g.Go(func() error {
		return db.WithContext(ctx).Raw(listIndexesQuery, sql.Named("schema", in.Schema), sql.Named("table", in.Table)).Scan(&out.Indexes).Error
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if len(out.Columns) == 0 {
		return nil, sqlcommon.ErrTableNotFound
	}
	return &out, nil
}

func ExecuteQuery(ctx context.Context, in ExecuteQueryIn, db DB) (out ExecuteQueryOut, err error) {
	err = db.WithContext(ctx).Raw(in.Query).Scan(&out.Rows).Error
	return
}

func CreateIndex(ctx context.Context, in CreateIndexIn, db DB) (*CreateIndexOut, error) {
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
	return &CreateIndexOut{Success: true, Message: fmt.Sprintf("Created index %s on %s.%s", in.Name, schema, in.Name)}, nil
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

type ExplainQueryIn struct {
	Query               string `json:"query" jsonschema:"The SQL query to explain,required"`
	ActualExecutionPlan bool   `json:"actual_execution_plan,omitempty" jsonschema:"Whether to execute the query and get the actual execution plan"`
}

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

//go:embed missing_indexes.sql
var missingIndexesQuery string

type MissingIndexesOut struct {
	Indexes []IndexRecommendation `json:"indexes" jsonschema:"List of missing indexes with details"`
}

type IndexRecommendation struct {
	AverageEstimatedImpact float64 `json:"average_estimated_impact" jsonschema:"The average estimated impact score of the missing index"`
	CreateStatement        string  `json:"create_statement" jsonschema:"The CREATE INDEX statement to create the missing index"`
	LastUserSeek           string  `json:"last_user_seek" jsonschema:"The timestamp of the last user seek that would have benefited from the index"`
	TableName              string  `json:"table_name" jsonschema:"The name of the table on which the index is recommended"`
}

func ListMissingIndexes(ctx context.Context, _ struct{}, db DB) (out MissingIndexesOut, err error) {
	err = db.WithContext(ctx).Raw(missingIndexesQuery).Scan(&out.Indexes).Error
	return
}

type WaitingQueriesOut struct {
	Queries []WaitingQuery `json:"queries" jsonschema:"List of currently waiting queries"`
}

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

//go:embed list_waiting_queries.sql
var waitingQueriesSQL string

func ListWaitingQueries(ctx context.Context, _ struct{}, db DB) (out WaitingQueriesOut, err error) {
	err = db.WithContext(ctx).Raw(waitingQueriesSQL).Scan(&out.Queries).Error
	return
}

type SlowestQueriesOut struct {
	Queries []SlowestQuery `json:"queries" jsonschema:"List of slowest queries by total elapsed time"`
}

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

//go:embed list_slowest_queries.sql
var slowestQueriesSQL string

func ListSlowestQueries(ctx context.Context, _ struct{}, db DB) (out SlowestQueriesOut, err error) {
	err = db.WithContext(ctx).Raw(slowestQueriesSQL).Scan(&out.Queries).Error
	return
}

type DeadlocksOut struct {
	Deadlocks []DeadlockInfo `json:"deadlocks" jsonschema:"List of recent deadlocks"`
}

type DeadlockInfo struct {
	DeadlockReport string `json:"deadlock_report" jsonschema:"The XML report of the deadlock"`
	ExecutionTime  string `json:"execution_time" jsonschema:"The time when the deadlock occurred"`
}

//go:embed list_deadlocks.sql
var listDeadlocksSQL string

func ListDeadlocks(ctx context.Context, _ struct{}, db DB) (out DeadlocksOut, err error) {
	err = db.WithContext(ctx).Raw(listDeadlocksSQL).Scan(&out.Deadlocks).Error
	return
}
