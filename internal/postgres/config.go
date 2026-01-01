package postgres

import (
	"net/url"

	"github.com/tinternet/databaise/internal/config"
)

type WriteConfig struct {
	config.WriteConfig
}

type AdminConfig struct {
	config.AdminConfig
}

// GetDatabaseIdentifier extracts the database name from Postgres DSN.
// Postgres DSN format: postgres://user:pass@host:port/dbname
func (c ReadConfig) GetDatabaseIdentifier() (string, error) {
	return extractPostgresDatabaseName(c.DSN)
}

func (c WriteConfig) GetDatabaseIdentifier() (string, error) {
	return extractPostgresDatabaseName(c.DSN)
}

func (c AdminConfig) GetDatabaseIdentifier() (string, error) {
	return extractPostgresDatabaseName(c.DSN)
}

func extractPostgresDatabaseName(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	return u.Path, nil
}
