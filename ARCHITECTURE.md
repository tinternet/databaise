# Databaise Architecture

## Overview

Databaise is an MCP server that provides LLM-friendly database access with three operation levels:
- **Read** - Read-only operations (SELECT, list tables, describe schema)
- **Write** - Data modification (INSERT, UPDATE, DELETE)
- **Admin** - Maintenance operations (indexes, EXPLAIN, slow queries, health)

## Directory Structure

```
cmd/
├── server/           # Main MCP server entry point
├── playground/       # Testing/development utilities
└── provision/        # CLI tool for provisioning readonly database users

internal/
├── backend/          # Backend registry and tool registration system
├── config/           # Configuration loading and parsing
├── logging/          # Logging utilities
├── server/           # MCP server implementation
├── sqlcommon/        # Shared SQL types and handler definitions
├── postgres/         # PostgreSQL backend
├── sqlite/           # SQLite backend
├── sqlserver/        # SQL Server backend
└── provision/        # Database user provisioning logic
```

## Config Structure

### Top-level (`config/config.go`)

```go
type Server map[string]Database  // dbName -> Database config

type Database struct {
    Backend     string          `json:"type"`        // "postgres", "sqlite", "sqlserver"
    Description string          `json:"description"` // Human-readable for LLM context
    Read        json.RawMessage `json:"read"`        // Presence = register read tools
    Write       json.RawMessage `json:"write"`       // Presence = register write tools
    Admin       json.RawMessage `json:"admin"`       // Presence = register admin tools
}
```

### Backend-specific configs

**Postgres/SQLServer** (`config/dsn.go`) - DSN with optional readonly enforcement:
```json
{
    "netflix": {
        "type": "postgres",
        "description": "Netflix viewership data and catalog.",
        "read": {
            "dsn": "postgres://reader_user:pass@localhost:5432/netflix",
            "enforce_readonly": true
        },
        "write": {
            "dsn": "postgres://app_user:pass@localhost:5432/netflix"
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
        "read": {
            "path": "/tmp/cache.db"
        }
    }
}
```

### Config options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `dsn` | string | required | Connection string (postgres/sqlserver) |
| `path` | string | required | File path (sqlite) |
| `enforce_readonly` | bool | `true` | Verify read user has no write permissions |

## Backend System (`backend/backend.go`)

The backend system uses Go generics to support different database types.

### Core types

```go
// Backend holds runtime state for a backend type
type Backend[DB any] struct {
    name            string
    tools           []toolDef
    instances       map[string]*instance
    toolsRegistered bool
    initRead        func(cfg config.Database) (DB, error)
    initWrite       func(cfg config.Database) (DB, error)
    initAdmin       func(cfg config.Database) (DB, error)
}

// Connector interface for driver-specific connection logic
type Connector[R, W, A, DB any] interface {
    ConnectRead(cfg R) (DB, error)
    ConnectWrite(cfg W) (DB, error)
    ConnectAdmin(cfg A) (DB, error)
}

// Handler signature for tool implementations
type Handler[In, Out, DB any] func(context.Context, In, DB) (Out, error)
```

### Tool registration

```go
func AddReadTool[In, Out, DB any](b *Backend[DB], name, description string, h Handler[In, Out, DB])
func AddWriteTool[In, Out, DB any](b *Backend[DB], name, description string, h Handler[In, Out, DB])
func AddAdminTool[In, Out, DB any](b *Backend[DB], name, description string, h Handler[In, Out, DB])
```

### Request routing

Tools receive requests with a `database_name` field that routes to the appropriate instance:

```go
type Request[In any] struct {
    DatabaseName string `json:"database_name"`
    Payload      In     `json:",inline"`
}
```

## Tool Registration

Tools are registered based on which config keys are present:

| Config Key | Tools Registered |
|------------|------------------|
| `read` | `list_tables`, `describe_table`, `read_query` |
| `write` | *Planned* |
| `admin` | `create_index` |

Tool names are prefixed with the backend name: `postgres_list_tables`, `sqlite_read_query`.

A global `list_databases` tool returns all configured databases with their available tools.

## Backend Implementation

Each backend implements the `Connector` interface and registers tools in its `init()` function.

Example (postgres/backend.go):
```go
type Connector struct{}

func (b Connector) ConnectRead(cfg ReadConfig) (DB, error) {
    db, err := gorm.Open(postgres.Open(cfg.DSN), &gorm.Config{})
    if err != nil {
        return nil, err
    }
    if cfg.ShouldEnforceReadonly() {
        if !sqlcommon.VerifyReadonly(db, verifyReadonlySQL) {
            return nil, fmt.Errorf("read DSN user has write permissions")
        }
    }
    return db, nil
}

func init() {
    b := backend.NewBackend("postgres", Connector{})
    backend.AddReadTool(&b, "list_tables", "[PostgreSQL] List all tables.", ListTables)
    backend.AddReadTool(&b, "describe_table", "[PostgreSQL] Describe a table.", DescribeTable)
    backend.AddReadTool(&b, "read_query", "[PostgreSQL] Execute a read-only query.", ExecuteQuery)
    backend.AddAdminTool(&b, "create_index", "[PostgreSQL] Create an index.", CreateIndex)
    backend.Register(&b)
}
```

## Boot Sequence

1. Parse command-line flags (transport mode, config path, address)
2. Load configuration from file
3. For each database in config:
   - Look up registered backend by `type`
   - Call `backend.Init(dbName, dbCfg)` which:
     - Creates connections for configured levels (read/write/admin)
     - Registers backend tools once (on first database for that backend)
     - Tracks instance for `list_databases`
4. Start MCP server (HTTP or STDIO)

**Initialization contract**: All `backend.Init()` calls must complete before the server starts. Backend state is immutable at runtime—no locking needed.

## Design Decisions

1. **Presence-based registration** - The presence of `read`, `write`, or `admin` keys determines which tools are registered. No boolean flags needed.

2. **Separate DSNs per operation** - Each operation level can use a different DSN/user with appropriate permissions.

3. **Generic backend system** - `Backend[DB]` allows type-safe handling of different database backends.

4. **Backend prefix** - Tools are named `{backend}_{action}` (e.g., `postgres_read_query`). LLM discovers databases via `list_databases`.

5. **Dialect in description** - Tool descriptions include `[PostgreSQL]`, `[T-SQL]`, or `[SQLite]` so LLMs write correct SQL.

6. **Backend owns tool definitions** - Each backend package defines its own tools, handlers, and config structures.

7. **Optional readonly enforcement** - Read connections verify the user has no write permissions by default. Set `enforce_readonly: false` to bypass.

8. **Shared types** - `internal/sqlcommon` provides common input/output structs reused across backends.
