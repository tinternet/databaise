package provision

import (
	"context"
	"fmt"
	"strings"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type PostgresProvisioner struct {
	db *gorm.DB
}

func (p *PostgresProvisioner) Connect(dsn string) error {
	db, err := gorm.Open(postgres.Open(dsn))
	if err != nil {
		return err
	}
	p.db = db
	return nil
}

func (p *PostgresProvisioner) Close() error {
	db, err := p.db.DB()
	if err != nil {
		return err
	}
	return db.Close()
}

func (p *PostgresProvisioner) DropUser(ctx context.Context, user string) error {
	err := p.db.WithContext(ctx).Exec(fmt.Sprintf("DROP OWNED BY %s", user)).Error
	if err != nil {
		return err
	}
	return p.db.WithContext(ctx).Exec(fmt.Sprintf("DROP USER IF EXISTS %s", user)).Error
}

func (p *PostgresProvisioner) UserExists(ctx context.Context, user string) (*bool, error) {
	var exists bool
	err := p.db.WithContext(ctx).Raw("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = ?);", user).First(&exists).Error
	if err != nil {
		return nil, err
	}
	return &exists, nil
}

func (p *PostgresProvisioner) CreateUser(ctx context.Context, user, pass string) error {
	err := p.db.WithContext(ctx).Exec(fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s'", user, pass)).Error
	if err != nil {
		return err
	}
	return p.db.WithContext(ctx).Exec(fmt.Sprintf("ALTER USER %s SET default_transaction_read_only = on;", user)).Error
}

func (p *PostgresProvisioner) GrantReadOnly(ctx context.Context, user string, scope AccessScope) error {
	if len(scope.Groups) > 0 {
		if err := p.grantSchemas(ctx, user, scope.Groups); err != nil {
			return err
		}
	}
	if len(scope.Resources) > 0 {
		if err := p.grantTables(ctx, user, scope.Resources); err != nil {
			return err
		}
	}
	return nil
}

func (p *PostgresProvisioner) grantTables(ctx context.Context, user string, tables []string) error {
	var query strings.Builder
	for _, table := range tables {
		fmt.Fprintf(&query, "GRANT SELECT ON %s TO %s;\n", table, user)
	}
	return p.db.WithContext(ctx).Exec(query.String()).Error
}

func (p *PostgresProvisioner) grantSchemas(ctx context.Context, user string, schemas []string) error {
	var query strings.Builder
	for _, schema := range schemas {
		fmt.Fprintf(&query, "GRANT USAGE ON SCHEMA %s TO %s;\n", schema, user)
		fmt.Fprintf(&query, "GRANT SELECT ON ALL TABLES IN SCHEMA %s TO %s;\n", schema, user)
		fmt.Fprintf(&query, "ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT SELECT ON TABLES TO %s;\n", schema, user)
	}
	return p.db.WithContext(ctx).Exec(query.String()).Error
}
