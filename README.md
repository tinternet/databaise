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

- **Database Names**: Each database is named (e.g., `netflix`) - this is passed as an argument to tools
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

## Running

### Transport Modes

```bash
# STDIO mode (for MCP clients like Claude Desktop)
./databaise -transport stdio -config config.json

# HTTP mode
./databaise -transport http -config config.json -address 0.0.0.0:8888
```

### Claude Desktop Setup

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

Tools are registered based on which config sections are present:

| Config Section | Tools Registered |
|----------------|------------------|
| `read` | `{db}_list_tables`, `{db}_describe_table`, `{db}_read_query` |
| `write` | *Planned* |
| `admin` | `{db}_create_index`, `{db}_drop_index`, `{db}_explain_query` |

### Backend-Specific Admin Tools

If the backend type is `sqlserver`, the admin section includes these additional diagnostic tools:

- `sqlserver_list_missing_indexes` - identify indexes suggested by the query optimizer
- `sqlserver_list_waiting_queries` - inspect queries currently blocked or waiting
- `sqlserver_list_slowest_queries` - retrieve top resource-consuming queries
- `sqlserver_list_deadlocks` - analyze recent deadlock events

### Global Tools
- `list_databases` - List all configured databases with their available tools

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