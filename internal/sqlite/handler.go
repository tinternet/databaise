package sqlite

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/tinternet/databaise/internal/sqlcommon"
	"gorm.io/gorm/clause"
)

// ListTablesIn is the input for the list_tables tool.
type ListTablesIn struct{}

// ListTablesOut is the output for the list_tables tool.
type ListTablesOut struct {
	Tables []string `json:"tables" jsonschema:"The list of table names"`
}

//go:embed list_tables.sql
var listTablesQuery string

func ListTables(ctx context.Context, in ListTablesIn, db DB) (out ListTablesOut, err error) {
	err = db.WithContext(ctx).Raw(listTablesQuery).Scan(&out.Tables).Error
	return
}

// DescribeTableIn is the input for the describe_table tool.
type DescribeTableIn struct {
	Table string `json:"table" jsonschema:"The name of the table to describe,required"`
}

// DescribeTableOut is the output for the describe_table tool.
type DescribeTableOut struct {
	CreateTable   string   `json:"create_table" jsonschema:"The CREATE TABLE statement for this table"`
	CreateIndexes []string `json:"create_indexes,omitempty" jsonschema:"CREATE INDEX statements for indexes on this table"`
}

//go:embed ddl_table.sql
var ddlCreateTableQuery string

//go:embed ddl_indexes.sql
var ddlCreateIndexesQuery string

func DescribeTable(ctx context.Context, in DescribeTableIn, db DB) (*DescribeTableOut, error) {
	var out DescribeTableOut
	err := db.WithContext(ctx).Raw(ddlCreateTableQuery, in.Table).Scan(&out.CreateTable).Error
	if err != nil {
		return nil, err
	}

	if out.CreateTable == "" {
		return nil, sqlcommon.ErrTableNotFound
	}

	err = db.WithContext(ctx).Raw(ddlCreateIndexesQuery, in.Table).Scan(&out.CreateIndexes).Error
	if err != nil {
		return nil, err
	}

	return &out, nil
}

type (
	ExecuteQueryIn  = sqlcommon.ExecuteQueryIn
	ExecuteQueryOut = sqlcommon.ExecuteQueryOut
)

func ExecuteQuery(ctx context.Context, in ExecuteQueryIn, db DB) (out ExecuteQueryOut, err error) {
	err = db.WithContext(ctx).Raw(in.Query).Scan(&out.Rows).Error
	return
}

// ExplainQueryIn is the input for the explain_query tool.
type ExplainQueryIn struct {
	Query     string `json:"query" jsonschema:"The SQL query to explain,required"`
	QueryPlan bool   `json:"query_plan,omitempty" jsonschema:"Whether to use EXPLAIN QUERY PLAN or EXPLAIN."`
}

// ExplainQueryOut is the output for the explain_query tool.
type ExplainQueryOut struct {
	Plan []map[string]any `json:"plan" jsonschema:"The query execution plan"`
}

func ExplainQuery(ctx context.Context, in ExplainQueryIn, db DB) (out ExplainQueryOut, err error) {
	var suffix string
	if in.QueryPlan {
		suffix = " QUERY PLAN"
	}
	err = db.WithContext(ctx).Raw("EXPLAIN" + suffix + " " + in.Query).Scan(&out.Plan).Error
	return
}

// CreateIndexIn is the input for the create_index tool.
type CreateIndexIn struct {
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

func CreateIndex(ctx context.Context, in CreateIndexIn, db DB) (*CreateIndexOut, error) {
	if len(in.Columns) == 0 {
		return nil, fmt.Errorf("at least one column is required to create an index")
	}

	unique := ""
	if in.Unique {
		unique = "UNIQUE"
	}

	exprs := make([]clause.Expression, len(in.Columns))
	for i, col := range in.Columns {
		exprs[i] = clause.Expr{SQL: "?", Vars: []any{clause.Column{Name: col}}}
	}

	err := db.WithContext(ctx).Exec(
		fmt.Sprintf("CREATE %s INDEX ? ON ? (?)", unique),
		clause.Column{Name: in.Name},
		clause.Table{Name: in.Table},
		clause.CommaExpression{Exprs: exprs},
	).Error
	if err != nil {
		return nil, err
	}
	return &CreateIndexOut{Success: true, Message: fmt.Sprintf("Created index %s on %s", in.Name, in.Table)}, nil
}

// DropIndexIn is the input for the drop_index tool.
type DropIndexIn struct {
	Name string `json:"name" jsonschema:"The name of the index to drop,required"`
}

// DropIndexOut is the output for the drop_index tool.
type DropIndexOut struct {
	Success bool   `json:"success" jsonschema:"Whether the index was dropped successfully"`
	Message string `json:"message,omitempty" jsonschema:"A message describing the result"`
}

func DropIndex(ctx context.Context, in DropIndexIn, db DB) (*DropIndexOut, error) {
	err := db.WithContext(ctx).Exec("DROP INDEX ?", clause.Column{Name: in.Name}).Error
	if err != nil {
		return nil, err
	}
	return &DropIndexOut{Success: true, Message: fmt.Sprintf("Dropped index %s", in.Name)}, nil
}
