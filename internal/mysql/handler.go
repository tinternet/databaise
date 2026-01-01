package mysql

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/tinternet/databaise/internal/sqlcommon"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type (
	ExecuteQueryIn  = sqlcommon.ExecuteQueryIn
	ExecuteQueryOut = sqlcommon.ExecuteQueryOut
	CreateIndexOut  = sqlcommon.CreateIndexOut
	DropIndexOut    = sqlcommon.DropIndexOut
)

const listColumnsQuery = `
	SELECT
		column_name as name,
		data_type as database_type,
		is_nullable = 'YES' as is_nullable,
		column_default as default_value
	FROM information_schema.columns
	WHERE table_name = ?
	ORDER BY ordinal_position
	`

//go:embed list_indexes.sql
var listIndexesQuery string

type ListTablesIn struct{}

type ListTablesOut struct {
	Tables []string `json:"tables" jsonschema:"The list of table names"`
}

func ListTables(ctx context.Context, in ListTablesIn, db *gorm.DB) (out ListTablesOut, err error) {
	err = db.WithContext(ctx).Raw("SHOW TABLES;").Scan(&out.Tables).Error
	return
}

// DescribeTableIn is the input for the describe_table tool.
type DescribeTableIn struct {
	Table string `json:"table" jsonschema:"The name of the table to describe,required"`
}

// DescribeTableOut is the output for the describe_table tool.
type DescribeTableOut struct {
	Schema  string             `json:"schema" jsonschema:"The schema name"`
	Name    string             `json:"name" jsonschema:"The name of the table"`
	Columns []sqlcommon.Column `json:"columns" jsonschema:"The columns in the table"`
	Indexes []sqlcommon.Index  `json:"indexes" jsonschema:"The indexes on the table"`
}

func DescribeTable(ctx context.Context, in DescribeTableIn, db *gorm.DB) (*DescribeTableOut, error) {
	out := DescribeTableOut{Name: in.Table}
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(2)
	g.Go(func() error {
		return db.WithContext(ctx).Raw(listColumnsQuery, in.Table).Scan(&out.Columns).Error
	})
	g.Go(func() error {
		return db.WithContext(ctx).Raw(listIndexesQuery, in.Table).Scan(&out.Indexes).Error
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if len(out.Columns) == 0 {
		return nil, sqlcommon.ErrTableNotFound
	}
	return &out, nil
}

func ExecuteQuery(ctx context.Context, in ExecuteQueryIn, db *gorm.DB) (*ExecuteQueryOut, error) {
	var out ExecuteQueryOut
	if err := db.WithContext(ctx).Raw(in.Query).Scan(&out.Rows).Error; err != nil {
		return nil, err
	}
	return &out, nil
}

type CreateIndexIn struct {
	Table   string   `json:"table" jsonschema:"The table to create the index on,required"`
	Name    string   `json:"name" jsonschema:"The name of the index,required"`
	Columns []string `json:"columns" jsonschema:"The columns to include in the index,required"`
	Unique  bool     `json:"unique,omitempty" jsonschema:"Whether to create a unique index"`
}

func CreateIndex(ctx context.Context, in CreateIndexIn, db *gorm.DB) (*CreateIndexOut, error) {
	if len(in.Columns) == 0 {
		return nil, fmt.Errorf("at least one column is required to create an index")
	}

	unique := ""
	if in.Unique {
		unique = "UNIQUE "
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

type DropIndexIn struct {
	Name  string `json:"name" jsonschema:"The name of the index to drop,required"`
	Table string `json:"table" jsonschema:"The name of the index's table,required"`
}

func DropIndex(ctx context.Context, in DropIndexIn, db *gorm.DB) (*DropIndexOut, error) {
	err := db.WithContext(ctx).Exec(
		"DROP INDEX ? ON ?;",
		clause.Table{Name: in.Name},
		clause.Table{Name: in.Table},
	).Error
	if err != nil {
		return nil, err
	}
	return &DropIndexOut{Success: true, Message: fmt.Sprintf("Dropped index %s", in.Name)}, nil
}

type ExplainQueryIn struct {
	Query   string `json:"query" jsonschema:"The SQL query to explain,required"`
	Analyze bool   `json:"analyze" jsonschema:"Whether to execute the query for actual runtime statistics"`
}

type ExplainQueryOut struct {
	Plan map[string]any `json:"plan" jsonschema:"The execution plan of the query"`
}

func ExplainQuery(ctx context.Context, in ExplainQueryIn, db *gorm.DB) (*ExplainQueryOut, error) {
	var query string
	if in.Analyze {
		query = fmt.Sprintf("EXPLAIN ANALYZE FORMAT=JSON %s", in.Query)
	} else {
		query = fmt.Sprintf("EXPLAIN FORMAT=JSON %s", in.Query)
	}

	var planJSON string
	err := db.WithContext(ctx).Raw(query).Scan(&planJSON).Error
	if err != nil {
		return nil, err
	}

	var result ExplainQueryOut
	if err := json.Unmarshal([]byte(planJSON), &result.Plan); err != nil {
		return nil, fmt.Errorf("failed to parse explain JSON: %w", err)
	}

	return &result, nil
}
