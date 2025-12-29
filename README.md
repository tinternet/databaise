# Databaise

A Model Context Protocol (MCP) server for secure database access. Supports PostgreSQL, SQLite, and SQL Server with permission-based tool registration.

## Installation

```bash
git clone https://github.com/tinternet/databaise
cd databaise
go build -o databaise cmd/server/main.go
```

## Configuration

Create a `config.json` file with your database connections. See [CONFIG.md](CONFIG.md) for full details.

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

**Key concepts:**
- Each database is named (e.g., `netflix`) - this is passed as an argument to tools
- The `description` helps the LLM understand what data is available
- Only include `read`, `write`, or `admin` sections for the operations you want to enable
- Each section can use a different DSN/user with appropriate permissions

## Running

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

**Global tools:**
- `list_databases` - List all configured databases with their available tools

**Example:** If you configure `netflix` with `read` and `admin` sections, these tools become available:
- `netflix_list_tables`
- `netflix_describe_table`
- `netflix_read_query`
- `netflix_create_index`

## Security Model

- **Presence-based registration** - Only the tools you configure are exposed
- **Separate connections** - Each operation level uses its own DSN/credentials
- **Readonly enforcement** - Read connections are verified to lack write permissions by default (set `enforce_readonly: false` to bypass)

## Roadmap

Planned tools and features:

**Write tools** - Insert, update, and delete operations (approach TBD - considering ORM-style `insert_record`/`update_record`/`delete_record` vs raw `write_query`)

**Admin tools:**
- `{db}_slow_queries` - Get slowest queries from database logs/stats

**Other:**
- MySQL support

## License

Apache 2.0
