# Configuration Guide

## Database Configuration

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

### Config Key (Database Identifier)

The key (e.g., `"netflix"`, `"store"`) identifies the database instance in your configuration. This key is passed as the `database_name` parameter when calling tools to specify which database to query. Choose meaningful names:

- **Good:** `netflix`, `store`, `analytics`, `users`
- **Bad:** `db1`, `my_database`, `test`

### Description

The description helps the LLM understand what data is available in this database. It is returned by the `list_databases` tool to help the LLM choose which database to query.

**Good descriptions:**
- `"Netflix shows and movies catalog"`
- `"Customer orders, payments, and shipping data"`
- `"User sessions and authentication tokens"`

**Bad descriptions:**
- `"My database"` (not helpful)
- `"PostgreSQL database"` (describes the backend, not the data)

### Backends

| Backend | `type` value | Dialect shown to LLM |
|---------|--------------|---------------------|
| PostgreSQL | `postgres` | PostgreSQL |
| SQLite | `sqlite` | SQLite |
| SQL Server | `sqlserver` | T-SQL |
| MySQL | `mysql` | MySQL |

### Operation Levels

Each operation level uses its own DSN/credentials for security isolation. The `admin` is optional, and omitting it will disable the tools for the database.

| Config Key | Tools Enabled |
|------------|---------------|
| `read` | `list_tables`, `describe_table`, `execute_query` |
| `admin` | `explain_query`, `execute_ddl`, `list_missing_indexes`, `list_waiting_queries`, `list_slowest_queries`, `list_deadlocks` |

---

## Backend-Specific Config

### PostgreSQL

```json
{
    "netflix": {
        "type": "postgres",
        "description": "Netflix viewership data and catalog.",
        "read": {
            "dsn": "postgres://reader:pass@localhost:5432/netflix",
            "bypass_readonly_check": true,
            "use_readonly_tx": false
        },
        "admin": {
            "dsn": "postgres://admin:pass@localhost:5432/netflix"
        }
    }
}
```

**Options:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `dsn` | string | required | PostgreSQL connection string |
| `bypass_readonly_check` | bool | `false` | **Startup Check**: Controls the readonly check at startup. |
| `use_readonly_tx` | bool | `false` | **Runtime Check**: Enforces read-only mode on every query (PostgreSQL). |

**Startup Check:** By default, the server connects at startup and verifies that the database user lacks write permissions. If the user does have write permissions (but you still want to proceed), set `bypass_readonly_check: true`.

**Runtime Check** When `use_readonly_tx: true`, the server skips the startup check and instead enforces safety by wrapping every query in a `READ ONLY` transaction. This uses prepared statements to strictly confine the LLM in two ways:

1) **Single Statement:** The protocol enforces a single SQL statement per request, which prevents query stacking (injecting `;COMMIT;` followed by a malicious write).
2) **Transaction Containment:** PostgreSQL's `EXECUTE` statement explicitly forbids transaction control commands, making it impossible to escape the read-only transaction context using tricks like `EXECUTE "COMMIT; DROP TABLE ..."`

### MySQL

```json
{
    "analytics": {
        "type": "mysql",
        "description": "Analytics data warehouse.",
        "read": {
            "dsn": "reader:pass@tcp(localhost:3306)/analytics",
            "bypass_readonly_check": true
        },
        "admin": {
            "dsn": "admin:pass@tcp(localhost:3306)/analytics"
        }
    }
}
```

**Options:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `dsn` | string | required | MySQL connection string (Go MySQL driver format) |
| `bypass_readonly_check` | bool | `false` | **Startup Check**: Whether to skip the readonly user check. |


### SQLite

```json
{
    "cache": {
        "type": "sqlite",
        "description": "Local cache database.",
        "read": { "path": "/data/cache.db" },
        "admin": { "path": "/data/cache.db" }
    }
}
```

SQLite automatically appends `?mode=ro` for `read` connections and `?mode=rw` for `admin` connections.

**Options:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path` | string | required | Path to SQLite database file |

### SQL Server

```json
{
    "orders": {
        "type": "sqlserver",
        "description": "Order management system.",
        "read": {
            "dsn": "sqlserver://reader:pass@localhost:1433?database=orders",
            "bypass_readonly_check": true
        },
        "admin": {
            "dsn": "sqlserver://admin:pass@localhost:1433?database=orders"
        }
    }
}
```

**Options:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `dsn` | string | required | SQL Server connection string |
| `bypass_readonly_check` | bool | `false` | Whether to skip the readonly use check. |

The readonly user should have `db_datareader` role only.

---

## Full Example

```json
{
    "customers": {
        "type": "postgres",
        "description": "Customer accounts and preferences",
        "read": {
            "dsn": "postgres://readonly:pass@prod-db.example.com/customers?sslmode=require"
        },
        "admin": {
            "dsn": "postgres://admin:pass@prod-db.example.com/customers?sslmode=require"
        }
    },
    "analytics": {
        "type": "mysql",
        "description": "Analytics events and aggregations",
        "read": {
            "dsn": "readonly:pass@tcp(analytics-db.example.com)/analytics"
        },
        "admin": {
            "dsn": "admin:pass@tcp(analytics-db.example.com)/analytics"
        }
    },
    "local_cache": {
        "type": "sqlite",
        "description": "Local development cache",
        "read": {
            "path": "./cache.db"
        }
    },
    "orders": {
        "type": "sqlserver",
        "description": "Order management system",
        "read": {
            "dsn": "sqlserver://readonly:pass@sql.example.com?database=orders"
        }
    }
}
```
