# Databaise Architecture

## Overview

Databaise is an MCP server that provides LLM-friendly database access with two operation levels:
- **Read** - Read-only operations (SELECT, list tables, describe schema)
- **Admin** - Maintenance and diagnostic operations (indexes, EXPLAIN, DBA tools)

## Directory Structure

```
cmd/
├── server/           # Main MCP server entry point
├── playground/       # Testing/development utilities
└── provision/        # CLI tool for provisioning readonly database users

internal/
├── backend/          # Backend registry, interfaces, and unified tool registration
│   ├── interfaces.go # SQLBackend interface and result types
│   ├── registry.go   # Instance management and backend registration
│   └── tools.go      # MCP tool definitions
├── config/           # Configuration loading and parsing
├── logging/          # Logging utilities
├── server/           # MCP server implementation
├── sqlcommon/        # Shared SQL utilities
├── postgres/         # PostgreSQL backend
├── sqlite/           # SQLite backend
├── sqlserver/        # SQL Server backend
├── mysql/            # MySQL backend
├── provision/        # Database user provisioning
└── sqltest/          # Testing utilities for SQL backends
```

## Config Structure

### Top-level (`config/config.go`)

```go
type Server map[string]Database  // dbName -> Database config

type Database struct {
    Backend     string          `json:"type"`        // "postgres", "sqlite", "sqlserver", "mysql"
    Description string          `json:"description"` // Human-readable for LLM context
    Read        json.RawMessage `json:"read"`        // Readonly connection config
    Admin       json.RawMessage `json:"admin"`       // Admin connection config. Optional.
}
```

### Backend-specific configs

**Postgres/MySQL/SQLServer** - DSN with optional readonly enforcement:
```json
{
    "netflix": {
        "type": "postgres",
        "description": "Netflix viewership data and catalog.",
        "read": {
            "dsn": "postgres://reader_user:pass@localhost:5432/netflix",
            "bypass_readonly_check": true
        },
        "admin": {
            "dsn": "postgres://admin_user:pass@localhost:5432/netflix"
        }
    }
}
```

**SQLite** - uses file path:
```json
{
    "cache": {
        "type": "sqlite",
        "description": "Local cache.",
        "read": { "path": "/tmp/cache.db" },
        "admin": { "path": "/tmp/cache.db" }
    }
}
```

### Config options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `dsn` | string | required | Connection string (postgres/mysql/sqlserver) |
| `path` | string | required | File path (sqlite) |
| `bypass_readonly_check` | bool | `false` | Whether to bypass the check that user has no write permissions |
| `use_readonly_tx` | bool | `false` | PostgreSQL only: wrap queries in read-only transactions |

## Backend System

The backend system uses Go generics to support different database types while exposing a unified interface.

### Core Interfaces (`backend/interfaces.go`)

```go
// SQLBackend defines the interface that all database backends must implement.
type SQLBackend interface {
    ListTables(ctx context.Context, in ListTablesIn) ([]Table, error)
    DescribeTable(ctx context.Context, in DescribeTableIn) (*TableDescription, error)
    ExecuteQuery(ctx context.Context, in ReadQueryIn) (*QueryResult, error)
    ExplainQuery(ctx context.Context, in ExplainQueryIn) (*ExplainResult, error)
    ExecuteDDL(ctx context.Context, in ExecuteDDLIn) (*DDLResult, error)
    ListMissingIndexes(ctx context.Context) ([]MissingIndex, error)
    ListWaitingQueries(ctx context.Context) ([]WaitingQuery, error)
    ListSlowestQueries(ctx context.Context) ([]SlowQuery, error)
    ListDeadlocks(ctx context.Context) ([]Deadlock, error)
}

// BackendFactory creates SQLBackend instances for a specific database type.
type BackendFactory[DB any] interface {
    Dialect() string      // Returns "PostgreSQL", "MySQL", "T-SQL", or "SQLite"
    New(db DB) SQLBackend
}

// Connector handles database connections for a backend type.
type Connector[R, A, DB any] interface {
    ConnectRead(cfg R) (DB, error)
    ConnectAdmin(cfg A) (DB, error)
}
```

### Instance Registry (`backend/registry.go`)

```go
// Instance represents a configured database instance.
type Instance struct {
    Name        string
    Description string
    Dialect     string
    HasAdmin    bool
    Read        func() SQLBackend  // Returns backend using read connection
    Admin       func() SQLBackend  // Returns backend using admin connection (nil if not configured)
}

// RegisterFactory registers a backend factory (called in each backend's init())
func RegisterFactory[R, A, DB any](backendType string, factory BackendFactory[DB], connect Connector[R, A, DB])

// Init initializes a database instance from config
func Init(name string, cfg config.Database) error

// GetReadBackend returns an SQLBackend for read operations
func GetReadBackend(databaseName string) (SQLBackend, error)

// GetAdminBackend returns an SQLBackend for admin operations
func GetAdminBackend(databaseName string) (SQLBackend, error)
```

### Tool Registration (`backend/tools.go`)

Tools are registered once at package init and route to the appropriate backend instance:

```go
// Request types embed input types with DatabaseName for routing
type DescribeTableReq struct {
    DatabaseName    string `json:"database_name" jsonschema:"required,The database to operate on"`
    DescribeTableIn `json:",inline"`
}

// Handle helper routes requests to the correct backend
func Handle[In any, Out any](
    ctx context.Context,
    databaseName string,
    in In,
    getBackend func(string) (SQLBackend, error),
    fn func(SQLBackend, context.Context, In) (Out, error),
) (Out, error)

// Tool registration example
server.AddTool(func(ctx context.Context, in DescribeTableReq) (*TableDescription, error) {
    return Handle(ctx, in.DatabaseName, in.DescribeTableIn, GetReadBackend, SQLBackend.DescribeTable)
}, server.Tool{
    Name:        "describe_table",
    Description: "Get the CREATE TABLE statement and indexes for a table.",
})
```

## Unified Tools

The `database_name` parameter routes to the correct backend instance, and `list_databases` returns the SQL dialect for each database.

| Tool | Operation | Description |
|------|-----------|-------------|
| `list_databases` | - | List all databases with their dialects |
| `list_tables` | Read | List tables, optionally filtered by schema |
| `describe_table` | Read | Get CREATE TABLE, indexes, and constraints |
| `execute_query` | Read | Execute a read-only SQL query |
| `explain_query` | Admin | Get query execution plan |
| `execute_ddl` | Admin | Execute DDL (CREATE INDEX, DROP INDEX, etc.) |
| `list_missing_indexes` | Admin | Get index recommendations |
| `list_waiting_queries` | Admin | Show blocked/waiting queries |
| `list_slowest_queries` | Admin | Show slowest queries by total time |
| `list_deadlocks` | Admin | Show deadlock information |

## Backend Implementation

Each backend implements the `SQLBackend` interface and registers itself via `init()`:

```go
// postgres/backend.go
type Factory struct{}

func (Factory) Dialect() string { return "PostgreSQL" }

func (Factory) New(db DB) backend.SQLBackend {
    return &Backend{db: db}
}

type Connector struct{}

func (Connector) ConnectRead(c ReadConfig) (DB, error) {
    // Connect and optionally verify readonly
}

func (Connector) ConnectAdmin(c AdminConfig) (DB, error) {
    // Connect with admin credentials
}

func init() {
    backend.RegisterFactory("postgres", Factory{}, Connector{})
}

// Backend implements backend.SQLBackend
type Backend struct {
    db DB
}

func (b *Backend) ListTables(ctx context.Context, in backend.ListTablesIn) ([]backend.Table, error) {
    // Implementation
}
// ... other methods
```

## Boot Sequence

1. Parse command-line flags (transport mode, config path, address)
2. Load configuration from file
3. For each database in config:
   - Look up registered backend factory by `type`
   - Call `backend.Init(dbName, dbCfg)` which:
     - Creates read connection (required)
     - Creates admin connection (if configured)
     - Stores instance in registry
4. Start MCP server (HTTP or STDIO)

**Initialization contract**: All `backend.Init()` calls must complete before the server starts. Backend state is immutable at runtime—no locking needed for request handling.

## Design Decisions

1. **Unified tools** - Tools have consistent names across all backends (e.g., `describe_table`). The `database_name` parameter routes to the correct instance.

2. **Two operation levels** - Read and Admin.

3. **Dialect in list_databases** - LLMs discover the SQL dialect via `list_databases` and write appropriate SQL for each database.

4. **Interface-based backends** - All backends implement `SQLBackend`, allowing unified tool handlers that work with any database type.

5. **DDL via execute_ddl** - A single `execute_ddl` tool accepts raw DDL statements, giving LLMs full flexibility.

6. **Flexible result types** - Types like `ExplainResult` use string fields (`Format`, `Result`, `ResultInfo`) rather than strict structures, accommodating different database formats (JSON, XML, text).

7. **Optional readonly enforcement** - Read connections verify the user has no write permissions by default. Set `bypass_readonly_check: true` to bypass.

8. **Factory pattern with generics** - `BackendFactory[DB]` and `Connector[R, A, DB]` allow type-safe handling of backend-specific connection types.
