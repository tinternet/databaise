package sqlcommon

import (
	"errors"

	"gorm.io/gorm"
)

var (
	ErrTableNotFound = errors.New("the table does not exist")
)

// VerifyReadonly executes the given SQL query and checks if the result indicates readonly.
// The query should return a single boolean value (true = readonly, false = has write perms).
func VerifyReadonly(db *gorm.DB, query string) bool {
	var isReadonly bool
	if err := db.Raw(query).Scan(&isReadonly).Error; err != nil {
		return false
	}
	return isReadonly
}

// ExecuteQueryIn is the input for the execute_query tool.
type ExecuteQueryIn struct {
	Query string `json:"query" jsonschema:"The SQL query to execute,required"`
}

// ExecuteQueryOut is the output for the execute_query tool.
type ExecuteQueryOut struct {
	Rows []map[string]any `json:"rows" jsonschema:"The result rows as key-value pairs"`
}
