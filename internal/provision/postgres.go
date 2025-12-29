package provision

import (
	"bytes"
	_ "embed"
	"fmt"
	"net/url"
	"strings"
	"text/template"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

//go:embed postgres_provision.sql
var postgresProvisionSQL string

//go:embed postgres_revoke.sql
var postgresRevokeSQL string

var postgresProvisionTmpl = template.Must(template.New("postgres_provision").Parse(postgresProvisionSQL))
var postgresRevokeTmpl = template.Must(template.New("postgres_revoke").Parse(postgresRevokeSQL))

type postgresProvisioner struct{}

func init() {
	Register("postgres", &postgresProvisioner{})
}

func (p *postgresProvisioner) Provision(adminDSN string, opts Options) (*Result, error) {
	if len(opts.Schemas) == 0 {
		return nil, fmt.Errorf("at least one schema must be specified")
	}

	db, err := gorm.Open(postgres.Open(adminDSN), &gorm.Config{})
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
		password = GeneratePassword()
	}

	var buf bytes.Buffer
	if err := postgresProvisionTmpl.Execute(&buf, map[string]any{
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

	readonlyDSN, err := replacePostgresCredentials(adminDSN, username, password)
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

func (p *postgresProvisioner) Revoke(adminDSN string, username string) error {
	db, err := gorm.Open(postgres.Open(adminDSN), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	sqlDB, _ := db.DB()
	defer sqlDB.Close()

	// Get all user schemas for revoking default privileges
	var schemas []string
	db.Raw(`SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' AND nspname != 'information_schema'`).Scan(&schemas)

	var buf bytes.Buffer
	if err := postgresRevokeTmpl.Execute(&buf, map[string]any{
		"Username": username,
		"Schemas":  schemas,
	}); err != nil {
		return fmt.Errorf("failed to render revoke script: %w", err)
	}

	if err := db.Exec(buf.String()).Error; err != nil {
		return fmt.Errorf("failed to revoke user: %w", err)
	}

	return nil
}

// replacePostgresCredentials replaces user/password in a postgres DSN.
// Handles both URL format (postgres://user:pass@host/db) and key=value format.
func replacePostgresCredentials(dsn, newUser, newPassword string) (string, error) {
	// Try URL format first
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", err
		}
		u.User = url.UserPassword(newUser, newPassword)
		return u.String(), nil
	}

	// Key=value format: user=x password=y host=z ...
	parts := strings.Fields(dsn)
	var result []string
	hasUser, hasPassword := false, false

	for _, part := range parts {
		if strings.HasPrefix(part, "user=") {
			result = append(result, fmt.Sprintf("user=%s", newUser))
			hasUser = true
		} else if strings.HasPrefix(part, "password=") {
			result = append(result, fmt.Sprintf("password=%s", newPassword))
			hasPassword = true
		} else {
			result = append(result, part)
		}
	}

	if !hasUser {
		result = append(result, fmt.Sprintf("user=%s", newUser))
	}
	if !hasPassword {
		result = append(result, fmt.Sprintf("password=%s", newPassword))
	}

	return strings.Join(result, " "), nil
}
