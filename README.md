# Databaise

A Model Context Protocol (MCP) server for secure database access. Supports PostgreSQL, MySQL, SQLite, and SQL Server with unified tools and permission-based access control.

## Overview

Databaise provides a secure bridge between LLMs and databases through unified MCP tools. It uses separate database connections for read and admin operations to enforce strict permission boundaries.

## Installation

### Option 1: Download Pre-built Binary
Download the latest release from the [releases page](https://github.com/tinternet/databaise/releases).

### Option 2: Build from Source
#### Prerequisites
- Go 1.21 or higher

#### Build from source
```bash
git clone https://github.com/tinternet/databaise
cd databaise
go build -o databaise cmd/server/main.go
```

## Quick Start

1. Create a configuration file (`config.json`):
```json
{
    "netflix": {
        "type": "postgres",
        "description": "Netflix viewership data and catalog.",
        "read": {
            "dsn": "postgres://reader:pass@localhost:5432/netflix"
        }
    }
}
```

2. Run Databaise:
```bash
# For MCP clients like Claude Desktop
./databaise -transport stdio -config config.json

# For HTTP-based clients
./databaise -transport http -config config.json -address 0.0.0.0:8888
```

## Configuration

Create a `config.json` file with your database connections. See [CONFIG.md](CONFIG.md) for full details.

### Basic Structure

Each database entry has the following structure:

```json
{
    "database_name": {
        "type": "postgres",
        "description": "What data is in this database",
        "read": { ... },
        "admin": { ... }
    }
}
```

### Key Concepts

- **Config Keys**: Each database entry is identified by a key (e.g., `netflix`) - this is passed as the `database_name` parameter when calling tools
- **Backend Types**: Supported types are `postgres`, `mysql`, `sqlserver`, and `sqlite`
- **Descriptions**: Help LLMs understand what data is available
- **Operation Levels**: Only include `read` or `admin` sections for the operations you want to enable
- **Separate Connections**: Each operation level uses its own DSN/credentials

### Example Configuration

```json
{
    "netflix": {
        "type": "postgres",
        "description": "Netflix viewership data and catalog.",
        "read": {
            "dsn": "postgres://reader:pass@localhost:5432/netflix"
        },
        "admin": {
            "dsn": "postgres://admin:pass@localhost:5432/netflix"
        }
    },
    "cache": {
        "type": "sqlite",
        "description": "Local cache database.",
        "read": {
            "path": "/tmp/cache.db"
        }
    }
}
```

## Claude Desktop Setup

Add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
    "mcpServers": {
        "databaise": {
            "command": "/path/to/databaise",
            "args": ["-transport", "stdio", "-config", "/path/to/config.json"]
        }
    }
}
```

## Available Tools

All tools use a unified naming scheme. The `database_name` parameter routes requests to the correct database, and `list_databases` returns the SQL dialect for each database so LLMs can write appropriate SQL.

### Global Tools
- `list_databases` - List all configured databases with their SQL dialects and admin access

### Read Tools
Available when `read` section is configured:
- `list_tables` - List all tables in the database (optionally filter by schema)
- `describe_table` - Get CREATE TABLE statement, indexes, and constraints
- `execute_query` - Execute a read-only SQL query

### Admin Tools
Available when `admin` section is configured:
- `explain_query` - Get query execution plan (with optional ANALYZE)
- `execute_ddl` - Execute DDL statements (CREATE INDEX, DROP INDEX, etc.)
- `list_missing_indexes` - Get index recommendations based on query patterns
- `list_waiting_queries` - Show queries that are currently blocked or waiting
- `list_slowest_queries` - Display slowest queries by total execution time
- `list_deadlocks` - Retrieve deadlock information

### DBA Tool Notes

The DBA monitoring tools (`list_missing_indexes`, `list_waiting_queries`, `list_slowest_queries`, `list_deadlocks`) have database-specific implementations:

| Tool | PostgreSQL | MySQL | SQL Server | SQLite |
|------|-----------|-------|------------|--------|
| `list_missing_indexes` | pg_stat_user_tables | performance_schema | Missing index DMVs | Not supported |
| `list_waiting_queries` | pg_stat_activity | performance_schema | sys.dm_exec_requests | Not supported |
| `list_slowest_queries` | pg_stat_statements* | events_statements_summary | Query stats DMV | Not supported |
| `list_deadlocks` | pg_stat_database | INNODB STATUS | Extended events | Not supported |

*Requires pg_stat_statements extension

## Security Model

Databaise implements a robust security model to prevent unauthorized database access:

- **Presence-based registration** - Only the tools you configure are exposed
- **Separate connections** - Each operation level uses its own DSN/credentials
- **Readonly enforcement** - Read connections are verified to lack write permissions by default (set `bypass_readonly_check: true` to bypass)
- **Transaction isolation (PostgreSQL)** - Optional read-only transactions prevent query stacking attacks (`use_readonly_tx: true`)

## License

Apache 2.0
