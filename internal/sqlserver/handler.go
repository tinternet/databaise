package sqlserver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/tinternet/databaise/internal/sqlcommon"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm/clause"
)

type (
	ListTablesIn     = sqlcommon.ListTablesIn
	ListTablesOut    = sqlcommon.ListTablesOut
	DescribeTableIn  = sqlcommon.DescribeTableIn
	DescribeTableOut = sqlcommon.DescribeTableOut
	ExecuteQueryIn   = sqlcommon.ExecuteQueryIn
	ExecuteQueryOut  = sqlcommon.ExecuteQueryOut
	CreateIndexOut   = sqlcommon.CreateIndexOut
	DropIndexOut     = sqlcommon.DropIndexOut
)

type DropIndexIn struct {
	sqlcommon.DropIndexIn
	Table string `json:"table" jsonschema:"The name of the table the index is on,required"`
}

type CreateIndexIn struct {
	sqlcommon.CreateIndexIn
	Clustered bool `json:"clustered" jsonschema:"Whether to create a clustered index,required"`
}

const listTablesQuery = `
	SELECT TABLE_SCHEMA as [schema], TABLE_NAME as name
	FROM INFORMATION_SCHEMA.TABLES
	WHERE TABLE_SCHEMA = CASE @schema WHEN '' THEN TABLE_SCHEMA ELSE @schema END AND TABLE_TYPE = 'BASE TABLE'
	ORDER BY TABLE_NAME
`

const listColumnsQuery = `
	SELECT
		COLUMN_NAME as name,
		DATA_TYPE as database_type,
		CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END as is_nullable,
		COLUMN_DEFAULT as default_value
	FROM INFORMATION_SCHEMA.COLUMNS
	WHERE TABLE_SCHEMA = CASE @schema WHEN '' THEN TABLE_SCHEMA ELSE @schema END AND TABLE_NAME = @table
	ORDER BY ORDINAL_POSITION
`

const listIndexesQuery = `
	SELECT
		i.name as name,
		'INDEX ON ' + s.name + '.' + t.name + ' (' +
			STUFF((
				SELECT ', ' + c.name
				FROM sys.index_columns ic
				JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
				WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
				ORDER BY ic.key_ordinal
				FOR XML PATH('')
			), 1, 2, '') + ')' as definition
	FROM sys.indexes i
	JOIN sys.tables t ON t.object_id = i.object_id
	JOIN sys.schemas s ON s.schema_id = t.schema_id
	WHERE s.name = CASE @schema WHEN '' THEN s.name ELSE @schema END AND t.name = @table
`

func ListTables(ctx context.Context, in ListTablesIn, db DB) (out ListTablesOut, err error) {
	err = db.WithContext(ctx).Raw(listTablesQuery, sql.Named("schema", in.Schema)).Scan(&out.Tables).Error
	return
}

func DescribeTable(ctx context.Context, in DescribeTableIn, db DB) (*DescribeTableOut, error) {
	out := DescribeTableOut{Schema: in.Schema, Name: in.Table}
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(2)
	g.Go(func() error {
		return db.WithContext(ctx).Raw(listColumnsQuery, sql.Named("schema", in.Schema), sql.Named("table", in.Table)).Scan(&out.Columns).Error
	})
	g.Go(func() error {
		return db.WithContext(ctx).Raw(listIndexesQuery, sql.Named("schema", in.Schema), sql.Named("table", in.Table)).Scan(&out.Indexes).Error
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
	schema := in.Schema
	if schema == "" {
		schema = "dbo"
	}

	unique := ""
	if in.Unique {
		unique = "UNIQUE"
	}

	clustered := "NONCLUSTERED"
	if in.Clustered {
		clustered = "CLUSTERED"
	}

	exprs := make([]clause.Expression, len(in.Columns))
	for i, col := range in.Columns {
		exprs[i] = clause.Expr{SQL: "?", Vars: []any{clause.Column{Name: col}}}
	}

	err := db.WithContext(ctx).Exec(
		fmt.Sprintf("CREATE %s %s INDEX ? ON ?.? (?)", unique, clustered),
		clause.Column{Name: in.Name},
		clause.Table{Name: schema},
		clause.Table{Name: in.Table},
		clause.CommaExpression{Exprs: exprs},
	).Error
	if err != nil {
		return &CreateIndexOut{Success: false, Message: err.Error()}, err
	}
	return &CreateIndexOut{Success: true, Message: fmt.Sprintf("Created index %s on %s.%s", in.Name, schema, in.Name)}, nil
}

func DropIndex(ctx context.Context, in DropIndexIn, db DB) (*DropIndexOut, error) {
	err := db.WithContext(ctx).Exec(
		"DROP INDEX ? ON ?.?",
		clause.Column{Name: in.Name},
		clause.Table{Name: in.Schema},
		clause.Table{Name: in.Table},
	).Error
	if err != nil {
		return &DropIndexOut{Success: false, Message: err.Error()}, err
	}
	return &DropIndexOut{Success: true, Message: fmt.Sprintf("Dropped index %s on %s.%s", in.Name, in.Schema, in.Table)}, nil
}

type ExplainQueryIn struct {
	Query               string `json:"query" jsonschema:"The SQL query to explain,required"`
	ActualExecutionPlan bool   `json:"actual_execution_plan,omitempty" jsonschema:"Whether to execute the query and get the actual execution plan"`
}

type ExplainQueryOut struct {
	Plan string           `json:"plan" jsonschema:"The execution plan for the query in XML format"`
	Rows []map[string]any `json:"rows,omitempty" jsonschema:"The result rows if actual_execution_plan is true"`
}

func ExplainQuery(ctx context.Context, in ExplainQueryIn, db DB) (*ExplainQueryOut, error) {
	tx := db.WithContext(ctx).Begin()
	defer tx.Rollback()
	if err := tx.Error; err != nil {
		return nil, err
	}
	var enable, disable string
	if in.ActualExecutionPlan {
		enable = "SET STATISTICS XML ON;"
		disable = "SET STATISTICS XML OFF;"
	} else {
		enable = "SET SHOWPLAN_XML ON;"
		disable = "SET SHOWPLAN_XML OFF;"
	}
	if err := tx.Exec(enable).Error; err != nil {
		return nil, err
	}

	var plan string
	var results []map[string]any

	if in.ActualExecutionPlan {
		// For actual execution plan, we need to execute the query
		rows, err := tx.Raw(in.Query).Rows()
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			if err := db.ScanRows(rows, &results); err != nil {
				return nil, err
			}
		}

		// The execution plan is returned in the next result set
		if rows.NextResultSet() {
			if rows.Next() {
				if err := rows.Scan(&plan); err != nil {
					return nil, err
				}
			}
		}
	} else {
		// For estimated plan, the plan is in the first result set
		if err := tx.Raw(in.Query).Scan(&plan).Error; err != nil {
			return nil, err
		}
	}

	if err := tx.Exec(disable).Error; err != nil {
		return nil, err
	}
	return &ExplainQueryOut{Plan: plan, Rows: results}, nil
}
