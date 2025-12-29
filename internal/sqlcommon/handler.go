package sqlcommon

import "gorm.io/gorm"

// VerifyReadonly executes the given SQL query and checks if the result indicates readonly.
// The query should return a single boolean value (true = readonly, false = has write perms).
func VerifyReadonly(db *gorm.DB, query string) bool {
	var isReadonly bool
	if err := db.Raw(query).Scan(&isReadonly).Error; err != nil {
		return false
	}
	return isReadonly
}

// Table represents a database table with schema.
type Table struct {
	Schema string `json:"schema" jsonschema:"The schema name"`
	Name   string `json:"name" jsonschema:"The table name"`
}

// ListTablesIn is the input for the list_tables tool.
type ListTablesIn struct {
	Schema string `json:"schema,omitempty" jsonschema:"Schema to list tables from"`
}

// ListTablesOut is the output for the list_tables tool.
type ListTablesOut struct {
	Tables []Table `json:"tables" jsonschema:"The list of tables"`
}

// DescribeTableIn is the input for the describe_table tool.
type DescribeTableIn struct {
	Schema string `json:"schema,omitempty" jsonschema:"The schema the table is in"`
	Table  string `json:"table" jsonschema:"The name of the table to describe,required"`
}

// DescribeTableOut is the output for the describe_table tool.
type DescribeTableOut struct {
	Schema  string   `json:"schema" jsonschema:"The schema name"`
	Name    string   `json:"name" jsonschema:"The name of the table"`
	Columns []Column `json:"columns" jsonschema:"The columns in the table"`
	Indexes []Index  `json:"indexes" jsonschema:"The indexes on the table"`
}

// Column represents a database column.
type Column struct {
	Name         string  `json:"name" jsonschema:"The column name"`
	DatabaseType string  `json:"database_type" jsonschema:"The database-specific type"`
	IsNullable   bool    `json:"is_nullable" jsonschema:"Whether the column allows NULL values"`
	DefaultValue *string `json:"default_value,omitempty" jsonschema:"The default value if any"`
}

// Index represents a database index.
type Index struct {
	Name       string `json:"name" jsonschema:"The index name"`
	Definition string `json:"definition" jsonschema:"The index definition or column list"`
}

// ExecuteQueryIn is the input for the execute_query tool.
type ExecuteQueryIn struct {
	Query string `json:"query" jsonschema:"The SQL query to execute,required"`
}

// ExecuteQueryOut is the output for the execute_query tool.
type ExecuteQueryOut struct {
	Rows []map[string]any `json:"rows" jsonschema:"The result rows as key-value pairs"`
}

// CreateIndexIn is the input for the create_index tool.
type CreateIndexIn struct {
	Schema  string   `json:"schema,omitempty" jsonschema:"The schema the table is in"`
	Table   string   `json:"table" jsonschema:"The table to create the index on,required"`
	Name    string   `json:"name" jsonschema:"The name of the index,required"`
	Columns []string `json:"columns" jsonschema:"The columns to include in the index,required"`
	Unique  bool     `json:"unique,omitempty" jsonschema:"Whether to create a unique index"`
}

// CreateIndexOut is the output for the create_index tool.
type CreateIndexOut struct {
	Success bool   `json:"success" jsonschema:"Whether the index was created successfully"`
	Message string `json:"message,omitempty" jsonschema:"A message describing the result"`
}
