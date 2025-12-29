package postgres

import (
	"context"
	"fmt"

	"github.com/tinternet/databaise/internal/sqlcommon"
	"gorm.io/gorm/clause"
)

type (
	ListTablesIn     = sqlcommon.ListTablesIn
	ListTablesOut    = sqlcommon.ListTablesOut
	DescribeTableIn  = sqlcommon.DescribeTableIn
	DescribeTableOut = sqlcommon.DescribeTableOut
	ExecuteQueryIn   = sqlcommon.ExecuteQueryIn
	ExecuteQueryOut  = sqlcommon.ExecuteQueryOut
	CreateIndexIn    = sqlcommon.CreateIndexIn
	CreateIndexOut   = sqlcommon.CreateIndexOut
)

const listTablesQuery = `
	SELECT table_schema as schema, table_name as name
	FROM information_schema.tables
	WHERE table_schema = COALESCE(NULLIF($1, ''), 'public') AND table_type = 'BASE TABLE'
	ORDER BY table_name
`

const listColumnsQuery = `
	SELECT
		column_name as name,
		data_type as database_type,
		is_nullable = 'YES' as is_nullable,
		column_default as default_value
	FROM information_schema.columns
	WHERE table_schema = COALESCE(NULLIF($1, ''), 'public') AND table_name = $2
	ORDER BY ordinal_position
`

const listIndexesQuery = `
	SELECT
		i.relname as name,
		pg_get_indexdef(i.oid) as definition
	FROM pg_index x
	JOIN pg_class i ON i.oid = x.indexrelid
	JOIN pg_class t ON t.oid = x.indrelid
	JOIN pg_namespace n ON n.oid = t.relnamespace
	WHERE n.nspname = COALESCE(NULLIF($1, ''), 'public') AND t.relname = $2
`

func ListTables(ctx context.Context, in ListTablesIn, db DB) (*ListTablesOut, error) {
	var out ListTablesOut
	if err := db.WithContext(ctx).Raw(listTablesQuery, in.Schema).Scan(&out.Tables).Error; err != nil {
		return nil, err
	}
	return &out, nil
}

func DescribeTable(ctx context.Context, in DescribeTableIn, db DB) (*DescribeTableOut, error) {
	out := DescribeTableOut{Schema: in.Schema, Name: in.Table}
	if err := db.WithContext(ctx).Raw(listColumnsQuery, in.Schema, in.Table).Scan(&out.Columns).Error; err != nil {
		return nil, err
	}
	if err := db.WithContext(ctx).Raw(listIndexesQuery, in.Schema, in.Table).Scan(&out.Indexes).Error; err != nil {
		return nil, err
	}
	return &out, nil
}

func ExecuteQuery(ctx context.Context, in ExecuteQueryIn, db DB) (*ExecuteQueryOut, error) {
	var out ExecuteQueryOut
	if err := db.WithContext(ctx).Raw(in.Query).Scan(&out.Rows).Error; err != nil {
		return nil, err
	}
	return &out, nil
}

func CreateIndex(ctx context.Context, in CreateIndexIn, db DB) (*CreateIndexOut, error) {
	schema := in.Schema
	if schema == "" {
		schema = "public"
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
		fmt.Sprintf("CREATE %sINDEX ? ON ?.? (?)", unique),
		clause.Column{Name: in.Name},
		clause.Table{Name: schema},
		clause.Table{Name: in.Table},
		clause.CommaExpression{Exprs: exprs},
	).Error

	if err != nil {
		return &CreateIndexOut{Success: false, Message: err.Error()}, err
	}
	return &CreateIndexOut{Success: true, Message: fmt.Sprintf("Created index %s on %s.%s", in.Name, schema, in.Table)}, nil
}
