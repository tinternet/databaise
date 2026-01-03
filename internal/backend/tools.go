package backend

import (
	"context"

	"github.com/tinternet/databaise/internal/server"
)

type DatabaseReq struct {
	DatabaseName string `json:"database_name" jsonschema:"required,The database to operate on"`
}

type ListTablesReq struct {
	DatabaseName string `json:"database_name" jsonschema:"required,The database to operate on"`
	ListTablesIn `json:",inline"`
}

type DescribeTableReq struct {
	DatabaseName    string `json:"database_name" jsonschema:"required,The database to operate on"`
	DescribeTableIn `json:",inline"`
}

type ReadQueryReq struct {
	DatabaseName string `json:"database_name" jsonschema:"required,The database to operate on"`
	ReadQueryIn  `json:",inline"`
}

type ExplainQueryReq struct {
	DatabaseName   string `json:"database_name" jsonschema:"required,The database to operate on"`
	ExplainQueryIn `json:",inline"`
}

type ExecuteDDLReq struct {
	DatabaseName string `json:"database_name" jsonschema:"required,The database to operate on"`
	ExecuteDDLIn `json:",inline"`
}

type ListTablesOut struct {
	Tables []Table `json:"tables" jsonschema:"The list of tables"`
}

type MissingIndexesOut struct {
	Indexes []MissingIndex `json:"indexes" jsonschema:"List of missing index recommendations"`
}

type WaitingQueriesOut struct {
	Queries []WaitingQuery `json:"queries" jsonschema:"List of waiting queries"`
}

type SlowestQueriesOut struct {
	Queries []SlowQuery `json:"queries" jsonschema:"List of slowest queries"`
}

type DeadlocksOut struct {
	Deadlocks []Deadlock `json:"deadlocks" jsonschema:"List of deadlock information"`
}

// DatabaseInfo represents info about a database for list_databases.
type DatabaseInfo struct {
	Name        string `json:"name" jsonschema:"The unique identifier for this database"`
	Dialect     string `json:"dialect" jsonschema:"The SQL dialect (PostgreSQL, MySQL, T-SQL, SQLite)"`
	Description string `json:"description,omitempty" jsonschema:"Human-readable description"`
	HasAdmin    bool   `json:"has_admin" jsonschema:"Whether admin tools are available"`
}

// ListDatabasesOut is the output for the list_databases tool.
type ListDatabasesOut struct {
	Databases []DatabaseInfo `json:"databases" jsonschema:"List of all available databases"`
}

// ListDatabases returns info about all initialized databases.
func ListDatabases() ListDatabasesOut {
	instancesMu.RLock()
	defer instancesMu.RUnlock()

	result := make([]DatabaseInfo, 0, len(instances))
	for _, inst := range instances {
		result = append(result, DatabaseInfo{
			Name:        inst.Name,
			Dialect:     inst.Dialect,
			Description: inst.Description,
			HasAdmin:    inst.HasAdmin,
		})
	}
	return ListDatabasesOut{Databases: result}
}

func init() {
	server.AddTool(func(ctx context.Context, in struct{}) (ListDatabasesOut, error) {
		return ListDatabases(), nil
	}, server.Tool{
		Name:        "list_databases",
		Description: "Lists all available databases along with their SQL dialects and admin access permissions. This tool is essential for identifying the correct database to interact with before performing any operations. It helps avoid errors due to incorrect or non-existent database names and ensures that you are working within the appropriate environment.",
	})

	// Read tools
	server.AddTool(func(ctx context.Context, in ListTablesReq) (*ListTablesOut, error) {
		return Handle(ctx, in.DatabaseName, in.ListTablesIn, GetReadBackend, func(b SQLBackend, ctx context.Context, in ListTablesIn) (*ListTablesOut, error) {
			tables, err := b.ListTables(ctx, in)
			if err != nil {
				return nil, err
			}
			return &ListTablesOut{Tables: tables}, nil
		})
	}, server.Tool{
		Name:        "list_tables",
		Description: "Lists all tables in a database. Returns table names with their schemas (for PostgreSQL/SQL Server). Use the optional schema parameter to filter results. This is typically the first tool to call when exploring a new database to understand its structure.",
	})

	server.AddTool(func(ctx context.Context, in DescribeTableReq) (*TableDescription, error) {
		return Handle(ctx, in.DatabaseName, in.DescribeTableIn, GetReadBackend, SQLBackend.DescribeTable)
	}, server.Tool{
		Name:        "describe_table",
		Description: "Returns the complete DDL for a table including the CREATE TABLE statement, all indexes, and constraints. This provides the full schema definition needed to understand column types, primary keys, foreign keys, and existing indexes. For PostgreSQL/SQL Server, you must provide the schema name (e.g., 'public' or 'dbo').",
	})

	server.AddTool(func(ctx context.Context, in ReadQueryReq) (*QueryResult, error) {
		return Handle(ctx, in.DatabaseName, in.ReadQueryIn, GetReadBackend, SQLBackend.ExecuteQuery)
	}, server.Tool{
		Name:        "execute_query",
		Description: "Executes a read-only SQL query and returns the results as rows. Use the SQL dialect appropriate for the database (check list_databases to see each database's dialect: PostgreSQL, MySQL, T-SQL, or SQLite). Only SELECT queries are allowed; INSERT/UPDATE/DELETE will fail.",
	})

	// Admin tools
	server.AddTool(func(ctx context.Context, in ExplainQueryReq) (*ExplainResult, error) {
		return Handle(ctx, in.DatabaseName, in.ExplainQueryIn, GetAdminBackend, SQLBackend.ExplainQuery)
	}, server.Tool{
		Name:        "explain_query",
		Description: "Returns the execution plan for a SQL query, showing how the database will execute it. Useful for identifying performance issues like full table scans or inefficient joins. Set analyze=true to actually run the query and get real execution statistics (timing, rows processed). The output format varies by database (JSON for PostgreSQL/MySQL, XML for SQL Server).",
	})

	server.AddTool(func(ctx context.Context, in ExecuteDDLReq) (*DDLResult, error) {
		return Handle(ctx, in.DatabaseName, in.ExecuteDDLIn, GetAdminBackend, SQLBackend.ExecuteDDL)
	}, server.Tool{
		Name:        "execute_ddl",
		Description: "Executes a DDL (Data Definition Language) statement to modify database schema. Commonly used for CREATE INDEX, DROP INDEX, and other index management operations. Use the SQL dialect appropriate for the database. Examples: 'CREATE INDEX idx_name ON table(column)' or 'DROP INDEX idx_name ON table' (MySQL/SQL Server) or 'DROP INDEX schema.idx_name' (PostgreSQL).",
	})

	server.AddTool(func(ctx context.Context, in DatabaseReq) (*MissingIndexesOut, error) {
		return Handle(ctx, in.DatabaseName, struct{}{}, GetAdminBackend, func(b SQLBackend, ctx context.Context, _ struct{}) (*MissingIndexesOut, error) {
			indexes, err := b.ListMissingIndexes(ctx)
			if err != nil {
				return nil, err
			}
			return &MissingIndexesOut{Indexes: indexes}, nil
		})
	}, server.Tool{
		Name:        "list_missing_indexes",
		Description: "Analyzes query patterns and table statistics to identify tables that would benefit from additional indexes. Returns recommendations with estimated impact scores and suggested CREATE INDEX statements. For SQL Server, uses the missing index DMVs. For PostgreSQL, analyzes sequential scan statistics. For MySQL, checks performance_schema for full table scans. Not available for SQLite.",
	})

	server.AddTool(func(ctx context.Context, in DatabaseReq) (*WaitingQueriesOut, error) {
		return Handle(ctx, in.DatabaseName, struct{}{}, GetAdminBackend, func(b SQLBackend, ctx context.Context, _ struct{}) (*WaitingQueriesOut, error) {
			queries, err := b.ListWaitingQueries(ctx)
			if err != nil {
				return nil, err
			}
			return &WaitingQueriesOut{Queries: queries}, nil
		})
	}, server.Tool{
		Name:        "list_waiting_queries",
		Description: "Shows queries that are currently blocked or waiting for resources. Useful for diagnosing lock contention and identifying blocking chains. Returns the waiting query, what it's waiting for (lock type, resource), and which process is blocking it. Not available for SQLite.",
	})

	server.AddTool(func(ctx context.Context, in DatabaseReq) (*SlowestQueriesOut, error) {
		return Handle(ctx, in.DatabaseName, struct{}{}, GetAdminBackend, func(b SQLBackend, ctx context.Context, _ struct{}) (*SlowestQueriesOut, error) {
			queries, err := b.ListSlowestQueries(ctx)
			if err != nil {
				return nil, err
			}
			return &SlowestQueriesOut{Queries: queries}, nil
		})
	}, server.Tool{
		Name:        "list_slowest_queries",
		Description: "Returns the slowest queries by total execution time from query statistics. Shows execution count, total/average/max time, and the query text. Useful for identifying queries that need optimization. For PostgreSQL, requires the pg_stat_statements extension. Not available for SQLite.",
	})

	server.AddTool(func(ctx context.Context, in DatabaseReq) (*DeadlocksOut, error) {
		return Handle(ctx, in.DatabaseName, struct{}{}, GetAdminBackend, func(b SQLBackend, ctx context.Context, _ struct{}) (*DeadlocksOut, error) {
			deadlocks, err := b.ListDeadlocks(ctx)
			if err != nil {
				return nil, err
			}
			return &DeadlocksOut{Deadlocks: deadlocks}, nil
		})
	}, server.Tool{
		Name:        "list_deadlocks",
		Description: "Retrieves information about database deadlocks. For SQL Server, returns detailed deadlock graphs from extended events. For PostgreSQL, shows deadlock counts per database. For MySQL, displays the most recent deadlock from InnoDB status. Not available for SQLite.",
	})
}
