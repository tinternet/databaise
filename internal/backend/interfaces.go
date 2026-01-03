package backend

import "context"

// Table represents a database table.
type Table struct {
	Schema string `json:"schema,omitempty" jsonschema:"The schema name (if applicable)"`
	Name   string `json:"name" jsonschema:"The table name"`
}

// TableDescription represents a table's DDL.
type TableDescription struct {
	CreateTable       string   `json:"create_table" jsonschema:"The CREATE TABLE statement"`
	CreateIndexes     []string `json:"create_indexes,omitempty" jsonschema:"CREATE INDEX statements"`
	CreateConstraints []string `json:"create_constraints,omitempty" jsonschema:"CREATE CONSTRAINT statements"`
}

// QueryResult represents query results.
type QueryResult struct {
	Rows []map[string]any `json:"rows" jsonschema:"The result rows as key-value pairs"`
}

// ExplainResult represents an execution plan.
type ExplainResult struct {
	Format     string `jsonschema:"Plan format: text | json | xml | table"`
	Result     string `jsonschema:"Raw execution plan as returned by the database"`
	ResultInfo string `jsonschema:"How to interpret this plan and key fields to look at"`
}

// DDLResult represents the result of a DDL operation.
type DDLResult struct {
	Success bool   `json:"success" jsonschema:"Whether the operation succeeded"`
	Message string `json:"message,omitempty" jsonschema:"A message describing the result"`
}

// MissingIndex represents a missing index recommendation.
type MissingIndex struct {
	Schema          string  `json:"schema,omitempty" jsonschema:"The schema name"`
	TableName       string  `json:"table_name" jsonschema:"The table name"`
	Reason          string  `json:"reason,omitempty" jsonschema:"Why this index is recommended"`
	EstimatedImpact float64 `json:"estimated_impact,omitempty" jsonschema:"Estimated impact score"`
	Suggestion      string  `json:"suggestion,omitempty" jsonschema:"Suggested CREATE INDEX statement"`
}

// WaitingQuery represents a currently waiting/blocked query.
type WaitingQuery struct {
	ID               string  `json:"id" jsonschema:"Query or process identifier"`
	Username         string  `json:"username,omitempty" jsonschema:"Database user"`
	Database         string  `json:"database,omitempty" jsonschema:"Database name"`
	State            string  `json:"state,omitempty" jsonschema:"Current state"`
	WaitType         string  `json:"wait_type,omitempty" jsonschema:"Type of wait"`
	WaitTime         float64 `json:"wait_time_sec,omitempty" jsonschema:"Wait time in seconds"`
	BlockedBy        string  `json:"blocked_by,omitempty" jsonschema:"ID of blocking query"`
	Query            string  `json:"query" jsonschema:"The SQL query text"`
	QueryDurationSec float64 `json:"query_duration_sec,omitempty" jsonschema:"Query duration in seconds"`
}

// SlowQuery represents a slow query from statistics.
type SlowQuery struct {
	QueryHash    string  `json:"query_hash,omitempty" jsonschema:"Query hash for identification"`
	Calls        int64   `json:"calls,omitempty" jsonschema:"Number of executions"`
	TotalTimeSec float64 `json:"total_time_sec" jsonschema:"Total execution time in seconds"`
	AvgTimeSec   float64 `json:"avg_time_sec,omitempty" jsonschema:"Average execution time"`
	MaxTimeSec   float64 `json:"max_time_sec,omitempty" jsonschema:"Maximum execution time"`
	Query        string  `json:"query" jsonschema:"The SQL query text"`
}

// Deadlock represents deadlock information.
type Deadlock struct {
	Database  string `json:"database,omitempty" jsonschema:"Database name"`
	Count     int64  `json:"count,omitempty" jsonschema:"Number of deadlocks"`
	Details   string `json:"details,omitempty" jsonschema:"Deadlock details or log"`
	Timestamp string `json:"timestamp,omitempty" jsonschema:"When the deadlock occurred"`
}

// Backend input types

type ListTablesIn struct {
	Schema string `json:"schema,omitempty" jsonschema:"Schema to filter by (optional)"`
}

type DescribeTableIn struct {
	Schema string `json:"schema,omitempty" jsonschema:"The schema (required for PostgreSQL/SQL Server)"`
	Table  string `json:"table" jsonschema:"required,The table name"`
}

type ReadQueryIn struct {
	Query string `json:"query" jsonschema:"required,The SQL query to execute"`
}

type ExplainQueryIn struct {
	Query   string `json:"query" jsonschema:"required,The SQL query to explain"`
	Analyze bool   `json:"analyze,omitempty" jsonschema:"Execute the query for actual runtime statistics"`
}

type ExecuteDDLIn struct {
	DDL string `json:"ddl" jsonschema:"required,The DDL statement to execute (CREATE INDEX, DROP INDEX, etc)"`
}

// SQLBackend defines the interface that all SQL database backends must implement.
type SQLBackend interface {
	// ListTables returns all tables, optionally filtered by schema.
	ListTables(ctx context.Context, in ListTablesIn) ([]Table, error)

	// DescribeTable returns the DDL for a table.
	DescribeTable(ctx context.Context, in DescribeTableIn) (*TableDescription, error)

	// ExecuteQuery executes a read-only SQL query.
	ExecuteQuery(ctx context.Context, in ReadQueryIn) (*QueryResult, error)

	// ExplainQuery returns the execution plan for a query.
	ExplainQuery(ctx context.Context, in ExplainQueryIn) (*ExplainResult, error)

	// ExecuteDDL executes a DDL statement (CREATE INDEX, DROP INDEX, etc).
	ExecuteDDL(ctx context.Context, in ExecuteDDLIn) (*DDLResult, error)

	// ListMissingIndexes returns index recommendations.
	ListMissingIndexes(ctx context.Context) ([]MissingIndex, error)

	// ListWaitingQueries returns currently waiting/blocked queries.
	ListWaitingQueries(ctx context.Context) ([]WaitingQuery, error)

	// ListSlowestQueries returns the slowest queries by total time.
	ListSlowestQueries(ctx context.Context) ([]SlowQuery, error)

	// ListDeadlocks returns deadlock information.
	ListDeadlocks(ctx context.Context) ([]Deadlock, error)
}

// BackendFactory creates SQLBackend instances for a specific database type.
// The DB type parameter allows each backend to use its own connection type.
type BackendFactory[DB any] interface {
	// Dialect returns the SQL dialect name for LLM hints (e.g., "PostgreSQL", "MySQL", "T-SQL").
	Dialect() string

	// New creates a new SQLBackend instance with the given database connection.
	New(db DB) SQLBackend
}
