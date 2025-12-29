package sqlite

import (
	"context"
	"fmt"

	"github.com/tinternet/databaise/internal/sqlcommon"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm/clause"
)

type ListTablesIn struct{}

type ListTablesOut struct {
	Tables []string `json:"tables" jsonschema:"The list of table names"`
}

type DescribeTableIn struct {
	Table string `json:"table" jsonschema:"The name of the table to describe,required"`
}

type DescribeTableOut struct {
	Name    string             `json:"name" jsonschema:"The name of the table"`
	Columns []sqlcommon.Column `json:"columns" jsonschema:"The columns in the table"`
	Indexes []sqlcommon.Index  `json:"indexes" jsonschema:"The indexes on the table"`
}

type CreateIndexIn struct {
	Table   string   `json:"table" jsonschema:"The table to create the index on,required"`
	Name    string   `json:"name" jsonschema:"The name of the index,required"`
	Columns []string `json:"columns" jsonschema:"The columns to include in the index,required"`
	Unique  bool     `json:"unique,omitempty" jsonschema:"Whether to create a unique index"`
}

type DropIndexIn struct {
	Name string `json:"name" jsonschema:"The name of the index to drop,required"`
}

type (
	ExecuteQueryIn  = sqlcommon.ExecuteQueryIn
	ExecuteQueryOut = sqlcommon.ExecuteQueryOut
	CreateIndexOut  = sqlcommon.CreateIndexOut
	DropIndexOut    = sqlcommon.DropIndexOut
)

const listTablesQuery = `
	SELECT name
	FROM sqlite_master
	WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
	ORDER BY name
`

const listColumnsQuery = `
	SELECT
		name,
		type as database_type,
		"notnull" = 0 as is_nullable,
		dflt_value as default_value
	FROM pragma_table_info(?)
	ORDER BY cid
`

const listIndexesQuery = `
	SELECT
		name,
		sql as definition
	FROM sqlite_master
	WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL
`

func ListTables(ctx context.Context, in ListTablesIn, db DB) (out ListTablesOut, err error) {
	err = db.WithContext(ctx).Raw(listTablesQuery).Scan(&out.Tables).Error
	return
}

func DescribeTable(ctx context.Context, in DescribeTableIn, db DB) (*DescribeTableOut, error) {
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

func ExecuteQuery(ctx context.Context, in ExecuteQueryIn, db DB) (out ExecuteQueryOut, err error) {
	err = db.WithContext(ctx).Raw(in.Query).Scan(&out.Rows).Error
	return
}

func CreateIndex(ctx context.Context, in CreateIndexIn, db DB) (*CreateIndexOut, error) {
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
		return &CreateIndexOut{Success: false, Message: err.Error()}, err
	}
	return &CreateIndexOut{Success: true, Message: fmt.Sprintf("Created index %s on %s", in.Name, in.Table)}, nil
}

func DropIndex(ctx context.Context, in DropIndexIn, db DB) (*DropIndexOut, error) {
	err := db.WithContext(ctx).Exec(
		"DROP INDEX ?",
		clause.Column{Name: in.Name},
	).Error
	if err != nil {
		return &DropIndexOut{Success: false, Message: err.Error()}, err
	}
	return &DropIndexOut{Success: true, Message: fmt.Sprintf("Dropped index %s", in.Name)}, nil
}

type ExplainQueryIn struct {
	Query     string `json:"query" jsonschema:"The SQL query to explain,required"`
	QueryPlan bool   `json:"query_plan,omitempty" jsonschema:"Whether to use EXPLAIN QUERY PLAN or EXPLAIN."`
}

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
