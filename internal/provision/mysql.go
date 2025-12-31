package provision

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type MySqlProvisioner struct {
	db *gorm.DB
}

func (p *MySqlProvisioner) Connect(dsn string) error {
	db, err := gorm.Open(mysql.Open(dsn))
	if err != nil {
		return err
	}
	p.db = db
	return nil
}

func (p *MySqlProvisioner) Close() error {
	db, err := p.db.DB()
	if err != nil {
		return err
	}
	return db.Close()
}

func (p *MySqlProvisioner) DropUser(ctx context.Context, user string) error {
	return p.db.WithContext(ctx).Exec(fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%';", user)).Error
}

func (p *MySqlProvisioner) UserExists(ctx context.Context, user string) (*bool, error) {
	var exists bool
	err := p.db.WithContext(ctx).Raw("SELECT EXISTS (SELECT 1 FROM mysql.user WHERE user = ? AND host = '%');", user).Find(&exists).Error
	if err != nil {
		return nil, err
	}
	return &exists, nil
}

func (p *MySqlProvisioner) CreateUser(ctx context.Context, user, pass string) error {
	if user == "" || pass == "" {
		return errors.New("user and password are required")
	}
	return p.db.WithContext(ctx).Exec(fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s'", user, pass)).Error
}

func (p *MySqlProvisioner) GrantReadOnly(ctx context.Context, user string, scope AccessScope) error {
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

func (p *MySqlProvisioner) grantTables(ctx context.Context, user string, tables []string) error {
	var query strings.Builder
	for _, table := range tables {
		fmt.Fprintf(&query, "GRANT SELECT ON `%s` TO '%s'@'%%';\n", table, user)
	}
	fmt.Fprintf(&query, "FLUSH PRIVILEGES;")
	return p.db.WithContext(ctx).Exec(query.String()).Error
}

func (p *MySqlProvisioner) grantSchemas(ctx context.Context, user string, schemas []string) error {
	var query strings.Builder
	for _, schema := range schemas {
		fmt.Fprintf(&query, "GRANT SELECT ON `%s`.* TO '%s'@'%%';\n", schema, user)
	}
	fmt.Fprintf(&query, "FLUSH PRIVILEGES;")
	return p.db.WithContext(ctx).Exec(query.String()).Error
}
