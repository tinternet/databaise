# Databaise

A Model Context Protocol (MCP) server for secure database access. Supports PostgreSQL, SQLite, and SQL Server with permission-based tool registration.

## Overview

Databaise provides a secure bridge between LLMs and databases by exposing only the tools you explicitly configure. It uses separate database connections for read, write, and admin operations to enforce strict permission boundaries. Currently supports PostgreSQL, SQLite, SQL Server, and MySQL.

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

### Configuration Examples

### Basic Structure

Each database entry has the following structure:

```json
{
    "database_name": {
        "type": "postgres",
        "description": "What data is in this database",
        "read": { ... },
        "write": { ... },
        "admin": { ... }
    }
}
```

### Key Concepts

- **Config Keys**: Each database entry is identified by a key (e.g., `netflix`) - this is passed as the `database_name` parameter when calling tools
- **Backend Types**: The database type (`postgres`, `mysql`, `sqlserver`, `sqlite`) becomes the prefix for all tools (e.g., `postgres_list_tables`)
- **Descriptions**: Help LLMs understand what data is available
- **Operation Levels**: Only include `read`, `write`, or `admin` sections for the operations you want to enable
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
        "write": {
            "dsn": "postgres://writer:pass@localhost:5432/netflix"
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

Tools are registered based on which config sections are present. All tools use the pattern `{backend}_{tool_name}` where `{backend}` is the database type (e.g., `postgres_list_tables`, `mysql_read_query`). The config key (e.g., `netflix`) is passed as a `database_name` parameter to specify which database to query.

### Global Tools
- `list_databases` - List all configured databases with their available tools

### Read Tools
Available when `read` section is configured:
- `{backend}_list_tables` - List all tables in the database
- `{backend}_describe_table` - Get column and index information for a table
- `{backend}_read_query` - Execute a read-only SQL query

### Write Tools
*Planned for future release*

### Admin Tools
Available when `admin` section is configured:

#### Common Admin Tools (All Backends)
- `{backend}_create_index` - Create an index on a table
- `{backend}_drop_index` - Drop an index from a table
- `{backend}_explain_query` - Get query execution plan

#### DBA Monitoring Tools (SQL Server, PostgreSQL, MySQL)
Advanced diagnostic tools for production database monitoring:

- `{backend}_list_missing_indexes` - Identify tables that would benefit from additional indexes
  - **SQL Server**: Uses missing index DMVs with impact scores
  - **PostgreSQL**: Analyzes sequential scan statistics from pg_stat_user_tables
  - **MySQL**: Checks performance_schema for full table scans

- `{backend}_list_waiting_queries` - Show queries that are currently blocked or waiting
  - **SQL Server**: Uses sys.dm_exec_requests with wait types
  - **PostgreSQL**: Queries pg_stat_activity with wait events and blocking PIDs
  - **MySQL**: Leverages performance_schema threads and metadata_locks

- `{backend}_list_slowest_queries` - Display the slowest queries by total execution time
  - **SQL Server**: Queries sys.dm_exec_query_stats
  - **PostgreSQL**: Requires pg_stat_statements extension
  - **MySQL**: Uses performance_schema.events_statements_summary_by_digest

- `{backend}_list_deadlocks` - Retrieve information about database deadlocks
  - **SQL Server**: Extracts deadlock graphs from extended events
  - **PostgreSQL**: Shows deadlock counts from pg_stat_database
  - **MySQL**: Displays most recent deadlock from SHOW ENGINE INNODB STATUS

## Security Model

Databaise implements a robust security model to prevent unauthorized database access:

- **Presence-based registration** - Only the tools you configure are exposed
- **Separate connections** - Each operation level uses its own DSN/credentials  
- **Readonly enforcement** - Read connections are verified to lack write permissions by default (set `enforce_readonly: false` to bypass)
- **Transaction isolation (PostgreSQL)** - Read-only transactions prevent query stacking attacks

## Roadmap

Planned tools and features:

- **Write tools** - Insert, update, and delete operations (approach TBD - considering ORM-style `insert_record`/`update_record`/`delete_record` vs raw `write_query`)

## License

Apache 2.0