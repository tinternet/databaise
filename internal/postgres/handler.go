package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/tinternet/databaise/internal/sqlcommon"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
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
	DropIndexIn      = sqlcommon.DropIndexIn
	DropIndexOut     = sqlcommon.DropIndexOut
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

func ListTables(ctx context.Context, in ListTablesIn, db DB) (out ListTablesOut, err error) {
	err = db.WithContext(ctx).Raw(listTablesQuery, in.Schema).Scan(&out.Tables).Error
	return
}

func DescribeTable(ctx context.Context, in DescribeTableIn, db DB) (*DescribeTableOut, error) {
	out := DescribeTableOut{Schema: in.Schema, Name: in.Table}
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(2)
	g.Go(func() error {
		return db.WithContext(ctx).Raw(listColumnsQuery, in.Schema, in.Table).Scan(&out.Columns).Error
	})
	g.Go(func() error {
		return db.WithContext(ctx).Raw(listIndexesQuery, in.Schema, in.Table).Scan(&out.Indexes).Error
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if len(out.Columns) == 0 {
		return nil, sqlcommon.ErrTableNotFound
	}
	return &out, nil
}

func ExecuteQuery(ctx context.Context, in ExecuteQueryIn, db DB) (*ExecuteQueryOut, error) {
	var out ExecuteQueryOut

	if db.UseReadonlyTx {
		err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			return tx.Raw(in.Query).Scan(&out.Rows).Error
		}, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return nil, err
		}
		return &out, nil
	}

	if err := db.WithContext(ctx).Raw(in.Query).Scan(&out.Rows).Error; err != nil {
		return nil, err
	}
	return &out, nil
}

func CreateIndex(ctx context.Context, in CreateIndexIn, db DB) (*CreateIndexOut, error) {
	if len(in.Columns) == 0 {
		return nil, fmt.Errorf("at least one column is required to create an index")
	}

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
		return nil, err
	}
	return &CreateIndexOut{Success: true, Message: fmt.Sprintf("Created index %s on %s.%s", in.Name, schema, in.Table)}, nil
}

func DropIndex(ctx context.Context, in DropIndexIn, db DB) (*DropIndexOut, error) {
	schema := in.Schema
	if schema == "" {
		schema = "public"
	}

	err := db.WithContext(ctx).Exec(
		"DROP INDEX ?.? ",
		clause.Table{Name: schema},
		clause.Column{Name: in.Name},
	).Error

	if err != nil {
		return nil, err
	}
	return &DropIndexOut{Success: true, Message: fmt.Sprintf("Dropped index %s.%s", schema, in.Name)}, nil
}

type ExplainQueryIn struct {
	Query   string `json:"query" jsonschema:"The SQL query to explain,required"`
	Analyze bool   `json:"analyze" jsonschema:"Whether to execute the query for actual runtime statistics"`
}

type ExplainQueryOut struct {
	Plan map[string]any `json:"plan" jsonschema:"The execution plan of the query"`
}

func ExplainQuery(ctx context.Context, in ExplainQueryIn, db DB) (*ExplainQueryOut, error) {
	var analyze string
	if in.Analyze {
		analyze = "ANALYZE, "
	}

	var planJSON string
	err := db.WithContext(ctx).Raw(fmt.Sprintf("EXPLAIN (%sFORMAT JSON) %s", analyze, in.Query)).Scan(&planJSON).Error
	if err != nil {
		return nil, err
	}

	var result []ExplainQueryOut
	if err := json.Unmarshal([]byte(planJSON), &result); err != nil {
		return nil, fmt.Errorf("failed to parse explain JSON: %w", err)
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("empty explain result")
	}

	return &result[0], nil
}
