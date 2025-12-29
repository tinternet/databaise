package provision

import (
	"bytes"
	_ "embed"
	"fmt"
	"regexp"
	"strings"
	"text/template"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

//go:embed mysql_provision.sql
var mysqlProvisionSQL string

//go:embed mysql_revoke.sql
var mysqlRevokeSQL string

var mysqlProvisionTmpl = template.Must(template.New("mysql_provision").Parse(mysqlProvisionSQL))
var mysqlRevokeTmpl = template.Must(template.New("mysql_revoke").Parse(mysqlRevokeSQL))

type mysqlProvisioner struct{}

func init() {
	Register("mysql", &mysqlProvisioner{})
}

// enableMultiStatements adds multiStatements=true to a MySQL DSN if not present.
func enableMultiStatements(dsn string) string {
	if strings.Contains(dsn, "multiStatements=true") {
		return dsn
	}
	if strings.Contains(dsn, "?") {
		return dsn + "&multiStatements=true"
	}
	return dsn + "?multiStatements=true"
}

func (p *mysqlProvisioner) Provision(adminDSN string, opts Options) (*Result, error) {
	if len(opts.Schemas) == 0 {
		return nil, fmt.Errorf("at least one schema (database) must be specified")
	}

	// Enable multi-statement mode for executing the template
	db, err := gorm.Open(mysql.Open(enableMultiStatements(adminDSN)), &gorm.Config{})
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
	if err := mysqlProvisionTmpl.Execute(&buf, map[string]any{
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

	readonlyDSN, err := replaceMySQLCredentials(adminDSN, username, password)
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

func (p *mysqlProvisioner) Revoke(adminDSN string, username string) error {
	db, err := gorm.Open(mysql.Open(adminDSN), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	sqlDB, _ := db.DB()
	defer sqlDB.Close()

	var buf bytes.Buffer
	if err := mysqlRevokeTmpl.Execute(&buf, map[string]any{
		"Username": username,
	}); err != nil {
		return fmt.Errorf("failed to render revoke script: %w", err)
	}

	if err := db.Exec(buf.String()).Error; err != nil {
		return fmt.Errorf("failed to revoke user: %w", err)
	}

	return nil
}

// replaceMySQLCredentials replaces user/password in a MySQL DSN.
// MySQL DSN format: user:password@tcp(host:port)/dbname?params
func replaceMySQLCredentials(dsn, newUser, newPassword string) (string, error) {
	// Pattern: user:password@protocol(host)/db
	re := regexp.MustCompile(`^([^:]+):([^@]+)@`)
	if re.MatchString(dsn) {
		return re.ReplaceAllString(dsn, fmt.Sprintf("%s:%s@", newUser, newPassword)), nil
	}

	// Pattern without password: user@protocol(host)/db
	re = regexp.MustCompile(`^([^@]+)@`)
	if re.MatchString(dsn) {
		return re.ReplaceAllString(dsn, fmt.Sprintf("%s:%s@", newUser, newPassword)), nil
	}

	return "", fmt.Errorf("unrecognized DSN format")
}
