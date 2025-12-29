package provision

import (
	"bytes"
	_ "embed"
	"fmt"
	"net/url"
	"strings"
	"text/template"

	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
)

//go:embed sqlserver_provision.sql
var sqlserverProvisionSQL string

//go:embed sqlserver_revoke.sql
var sqlserverRevokeSQL string

var sqlserverProvisionTmpl = template.Must(template.New("sqlserver_provision").Parse(sqlserverProvisionSQL))
var sqlserverRevokeTmpl = template.Must(template.New("sqlserver_revoke").Parse(sqlserverRevokeSQL))

type sqlserverProvisioner struct{}

func init() {
	Register("sqlserver", &sqlserverProvisioner{})
}

func (p *sqlserverProvisioner) Provision(adminDSN string, opts Options) (*Result, error) {
	if len(opts.Schemas) == 0 {
		return nil, fmt.Errorf("at least one schema must be specified")
	}

	normalizedDSN := normalizeSQLServerDSN(adminDSN)

	db, err := gorm.Open(sqlserver.Open(normalizedDSN), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	sqlDB, _ := db.DB()
	defer sqlDB.Close()

	username := opts.Username
	if username == "" {
		username = GenerateUsername()
	}
	password := opts.Password
	if password == "" {
		password = GeneratePassword() + "Aa1!" // SQL Server requires complex passwords
	}

	var buf bytes.Buffer
	if err := sqlserverProvisionTmpl.Execute(&buf, map[string]any{
		"Username": username,
		"Password": password,
		"Schemas":  opts.Schemas,
		"Update":   opts.Update,
	}); err != nil {
		return nil, fmt.Errorf("failed to render provision script: %w", err)
	}

	if err := db.Exec(buf.String()).Error; err != nil {
		return nil, fmt.Errorf("failed to provision user: %w", err)
	}

	readonlyDSN, err := replaceSQLServerCredentials(adminDSN, username, password)
	if err != nil {
		return nil, fmt.Errorf("failed to build readonly DSN: %w", err)
	}

	return &Result{
		User:     username,
		Password: password,
		DSN:      readonlyDSN,
		Grants:   []string{buf.String()},
	}, nil
}

func (p *sqlserverProvisioner) Revoke(adminDSN string, username string) error {
	db, err := gorm.Open(sqlserver.Open(adminDSN), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	sqlDB, _ := db.DB()
	defer sqlDB.Close()

	var buf bytes.Buffer
	if err := sqlserverRevokeTmpl.Execute(&buf, map[string]any{
		"Username": username,
	}); err != nil {
		return fmt.Errorf("failed to render revoke script: %w", err)
	}

	if err := db.Exec(buf.String()).Error; err != nil {
		return fmt.Errorf("failed to revoke user: %w", err)
	}

	return nil
}

// normalizeSQLServerDSN converts path-style database (e.g., /TestMCP) to query parameter style.
// The go-mssqldb driver requires ?database=X format, not /X format.
func normalizeSQLServerDSN(dsn string) string {
	if !strings.HasPrefix(dsn, "sqlserver://") {
		return dsn
	}

	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}

	// Handle path-style database specification (e.g., /TestMCP)
	if u.Path != "" && u.Path != "/" {
		dbName := strings.TrimPrefix(u.Path, "/")
		u.Path = ""
		q := u.Query()
		if q.Get("database") == "" {
			q.Set("database", dbName)
			u.RawQuery = q.Encode()
		}
	}

	return u.String()
}

// replaceSQLServerCredentials replaces user/password in a SQL Server DSN.
// Format: sqlserver://user:password@host:port?database=db or sqlserver://user:password@host:port/db
func replaceSQLServerCredentials(dsn, newUser, newPassword string) (string, error) {
	// First normalize the DSN
	dsn = normalizeSQLServerDSN(dsn)

	if strings.HasPrefix(dsn, "sqlserver://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", err
		}
		u.User = url.UserPassword(newUser, newPassword)
		return u.String(), nil
	}

	// ADO-style: server=host;user id=user;password=pass;database=db
	parts := strings.Split(dsn, ";")
	var result []string
	hasUser, hasPassword := false, false

	for _, part := range parts {
		lower := strings.ToLower(part)
		if strings.HasPrefix(lower, "user id=") || strings.HasPrefix(lower, "uid=") {
			result = append(result, fmt.Sprintf("user id=%s", newUser))
			hasUser = true
		} else if strings.HasPrefix(lower, "password=") || strings.HasPrefix(lower, "pwd=") {
			result = append(result, fmt.Sprintf("password=%s", newPassword))
			hasPassword = true
		} else {
			result = append(result, part)
		}
	}

	if !hasUser {
		result = append(result, fmt.Sprintf("user id=%s", newUser))
	}
	if !hasPassword {
		result = append(result, fmt.Sprintf("password=%s", newPassword))
	}

	return strings.Join(result, ";"), nil
}
