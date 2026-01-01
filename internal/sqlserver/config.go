package sqlserver

import (
	"fmt"
	"net/url"

	"github.com/tinternet/databaise/internal/config"
)

type ReadConfig struct {
	config.ReadConfig
}

type WriteConfig struct {
	config.WriteConfig
}

type AdminConfig struct {
	config.AdminConfig
}

// GetDatabaseIdentifier extracts the database name from SQL Server DSN.
// SQL Server DSN format: sqlserver://user:pass@host:port?database=dbname
func (c ReadConfig) GetDatabaseIdentifier() (string, error) {
	return extractSQLServerDatabaseName(c.DSN)
}

func (c WriteConfig) GetDatabaseIdentifier() (string, error) {
	return extractSQLServerDatabaseName(c.DSN)
}

func (c AdminConfig) GetDatabaseIdentifier() (string, error) {
	return extractSQLServerDatabaseName(c.DSN)
}

func extractSQLServerDatabaseName(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}

	// SQL Server uses ?database=name parameter
	dbName := u.Query().Get("database")
	if dbName == "" {
		return "", fmt.Errorf("database parameter not specified in DSN")
	}

	return dbName, nil
}
