# Configuration Guide

## Database Configuration

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

Only include the sections you want to enable (`read`, `write`, `admin`). Each operation level uses its own DSN/credentials for security isolation.

For the complete list of available tools by operation level, see the [Available Tools](README.md#available-tools) section in README.md.

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
            "enforce_readonly": true,
            "use_readonly_tx": false
        },
        "write": {
            "dsn": "postgres://writer:pass@localhost:5432/netflix"
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
| `enforce_readonly` | bool | `true` | **Startup Check**: Verifies the database user cannot write. |
| `use_readonly_tx` | bool | `false` | **Runtime Check**: Enforces read-only mode on every query. |

**Startup Check:** By default, the server connects at startup and verifies that the database user lacks write permissions. If the user does have write permissions (but you still want to proceed), set `enforce_readonly: false`.

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
            "dsn": "mysql://reader:pass@localhost:3306/analytics",
            "enforce_readonly": true,
        },
        "write": {
            "dsn": "mysql://writer:pass@localhost:3306/analytics"
        },
        "admin": {
            "dsn": "mysql://admin:pass@localhost:3306/analytics"
        }
    }
}
```

**Options:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `dsn` | string | required | MySQL connection string |
| `enforce_readonly` | bool | `true` | **Startup Check**: Verifies the database user cannot write. |


### SQLite

```json
{
    "cache": {
        "type": "sqlite",
        "description": "Local cache database.",
        "read": { "path": "/data/cache.db" },
        "write": { "path": "/data/cache.db" },
        "admin": { "path": "/data/cache.db" }
    }
}
```

SQLite automatically appends the `?mode=ro` for `read` connections.

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
            "enforce_readonly": true
        },
        "write": {
            "dsn": "sqlserver://writer:pass@localhost:1433?database=orders"
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
| `enforce_readonly` | bool | `true` | Verify read user has no write permissions at startup |

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
        "type": "postgres",
        "description": "Analytics events and aggregations",
        "read": {
            "dsn": "postgres://readonly:pass@analytics-db.example.com/analytics?sslmode=require"
        },
        "write": {
            "dsn": "postgres://app:pass@analytics-db.example.com/analytics?sslmode=require"
        },
        "admin": {
            "dsn": "postgres://admin:pass@analytics-db.example.com/analytics?sslmode=require"
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
