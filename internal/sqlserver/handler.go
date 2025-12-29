package sqlserver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/tinternet/databaise/internal/sqlcommon"
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

func ListTables(ctx context.Context, in ListTablesIn, db DB) (*ListTablesOut, error) {
	var out ListTablesOut
	if err := db.WithContext(ctx).Raw(listTablesQuery, sql.Named("schema", in.Schema)).Scan(&out.Tables).Error; err != nil {
		return nil, err
	}
	return &out, nil
}

func DescribeTable(ctx context.Context, in DescribeTableIn, db DB) (*DescribeTableOut, error) {
	out := DescribeTableOut{Schema: in.Schema, Name: in.Table}
	if err := db.WithContext(ctx).Raw(listColumnsQuery, sql.Named("schema", in.Schema), sql.Named("table", in.Table)).Scan(&out.Columns).Error; err != nil {
		return nil, err
	}
	if err := db.WithContext(ctx).Raw(listIndexesQuery, sql.Named("schema", in.Schema), sql.Named("table", in.Table)).Scan(&out.Indexes).Error; err != nil {
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
		schema = "dbo"
	}

	unique := ""
	if in.Unique {
		unique = "UNIQUE "
	}

	tableName := fmt.Sprintf("[%s].[%s]", schema, in.Table)
	sql := fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
		unique, in.Name, tableName, strings.Join(in.Columns, ", "))

	if err := db.WithContext(ctx).Exec(sql).Error; err != nil {
		return &CreateIndexOut{Success: false, Message: err.Error()}, err
	}
	return &CreateIndexOut{Success: true, Message: fmt.Sprintf("Created index %s on %s", in.Name, tableName)}, nil
}
