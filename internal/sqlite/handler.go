package sqlite

import (
	"context"
	"fmt"
	"strings"

	"github.com/tinternet/databaise/internal/sqlcommon"
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

// Reuse common types where applicable
type (
	ExecuteQueryIn  = sqlcommon.ExecuteQueryIn
	ExecuteQueryOut = sqlcommon.ExecuteQueryOut
	CreateIndexOut  = sqlcommon.CreateIndexOut
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

func ListTables(ctx context.Context, in ListTablesIn, db DB) (*ListTablesOut, error) {
	var out ListTablesOut
	if err := db.WithContext(ctx).Raw(listTablesQuery).Scan(&out.Tables).Error; err != nil {
		return nil, err
	}
	return &out, nil
}

func DescribeTable(ctx context.Context, in DescribeTableIn, db DB) (*DescribeTableOut, error) {
	out := DescribeTableOut{Name: in.Table}
	if err := db.WithContext(ctx).Raw(listColumnsQuery, in.Table).Scan(&out.Columns).Error; err != nil {
		return nil, err
	}
	if err := db.WithContext(ctx).Raw(listIndexesQuery, in.Table).Scan(&out.Indexes).Error; err != nil {
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
	unique := ""
	if in.Unique {
		unique = "UNIQUE "
	}
	sql := fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
		unique, in.Name, in.Table, strings.Join(in.Columns, ", "))

	if err := db.WithContext(ctx).Exec(sql).Error; err != nil {
		return &CreateIndexOut{Success: false, Message: err.Error()}, err
	}
	return &CreateIndexOut{Success: true, Message: fmt.Sprintf("Created index %s on %s", in.Name, in.Table)}, nil
}
