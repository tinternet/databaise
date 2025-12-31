package provision

import (
	"context"
	"fmt"
	"strings"

	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
)

type SqlServerProvisioner struct {
	db *gorm.DB
}

func (p *SqlServerProvisioner) Connect(dsn string) error {
	db, err := gorm.Open(sqlserver.Open(dsn))
	if err != nil {
		return err
	}
	p.db = db
	return nil
}

func (p *SqlServerProvisioner) Close() error {
	db, err := p.db.DB()
	if err != nil {
		return err
	}
	return db.Close()
}

func (p *SqlServerProvisioner) DropUser(ctx context.Context, user string) error {
	err := p.db.WithContext(ctx).Exec(fmt.Sprintf("IF USER_ID('%s') IS NOT NULL DROP USER [%s]", user, user)).Error
	if err != nil {
		return err
	}
	return p.db.WithContext(ctx).Exec(fmt.Sprintf("IF EXISTS (SELECT * FROM sys.server_principals WHERE name = '%s') DROP LOGIN [%s]", user, user)).Error
}

func (p *SqlServerProvisioner) UserExists(ctx context.Context, user string) (*bool, error) {
	var exists bool
	err := p.db.WithContext(ctx).Raw("SELECT CASE WHEN EXISTS(SELECT 1 FROM sys.server_principals WHERE name = ?) THEN 1 ELSE 0 END", user).Find(&exists).Error
	if err != nil {
		return nil, err
	}
	return &exists, nil
}

func (p *SqlServerProvisioner) CreateUser(ctx context.Context, user, pass string) error {
	err := p.db.WithContext(ctx).Exec(fmt.Sprintf("CREATE LOGIN [%s] WITH PASSWORD = N'%s'", user, pass)).Error
	if err != nil {
		return err
	}
	return p.db.WithContext(ctx).Exec(fmt.Sprintf("CREATE USER [%s] FOR LOGIN [%s];", user, user)).Error
}

func (p *SqlServerProvisioner) GrantReadOnly(ctx context.Context, user string, scope AccessScope) error {
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

func (p *SqlServerProvisioner) grantTables(ctx context.Context, user string, tables []string) error {
	var query strings.Builder
	for _, table := range tables {
		fmt.Fprintf(&query, "GRANT SELECT ON [%s] TO [%s];\n", table, user)
	}
	return p.db.WithContext(ctx).Exec(query.String()).Error
}

func (p *SqlServerProvisioner) grantSchemas(ctx context.Context, user string, schemas []string) error {
	var query strings.Builder
	for _, schema := range schemas {
		fmt.Fprintf(&query, "GRANT SELECT ON SCHEMA::[%s] TO [%s];\n", schema, user)
	}
	return p.db.WithContext(ctx).Exec(query.String()).Error
}
