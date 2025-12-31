//go:build integration

package provision

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type TestData struct {
	ID   uint `gorm:"primaryKey"`
	Text string
}

type TestDataSecret struct {
	ID   uint `gorm:"primaryKey"`
	Text string
}

func migrateGormDatabase(t *testing.T, db *gorm.DB) {
	t.Helper()

	err := db.Migrator().AutoMigrate(TestData{}, TestDataSecret{})
	require.NoError(t, err)

	err = db.Create([]*TestData{{ID: 1}, {ID: 2}}).Error
	require.NoError(t, err)

	err = db.Create([]*TestDataSecret{{ID: 1}, {ID: 2}}).Error
	require.NoError(t, err)
}

func testReadonlySchemaScope(t *testing.T, db *gorm.DB) {
	t.Helper()

	// Test select
	count, err := gorm.G[TestData](db).Count(t.Context(), "id")
	require.NoError(t, err)
	require.EqualValues(t, 2, count)

	count, err = gorm.G[TestDataSecret](db).Count(t.Context(), "id")
	require.NoError(t, err)
	require.EqualValues(t, 2, count)

	// Test update
	_, err = gorm.G[TestData](db).Where("1 = 1").Update(t.Context(), "text", "text")
	require.Error(t, err)
	_, err = gorm.G[TestDataSecret](db).Where("1 = 1").Update(t.Context(), "text", "text")
	require.Error(t, err)

	// Test insert
	err = db.Create([]*TestData{{ID: 4}, {ID: 5}}).Error
	require.Error(t, err)

	err = db.Create([]*TestDataSecret{{ID: 4}, {ID: 5}}).Error
	require.Error(t, err)

	// Test delete
	_, err = gorm.G[TestData](db).Delete(t.Context())
	require.Error(t, err)
	_, err = gorm.G[TestDataSecret](db).Delete(t.Context())
	require.Error(t, err)

	// Test DDL
	type NewTable struct {
		ID uint `gorm:"primaryKey"`
	}
	err = db.Migrator().AutoMigrate(&NewTable{})
	require.Error(t, err)

	err = db.Migrator().DropTable(&TestData{})
	require.Error(t, err)

	err = db.Migrator().AddColumn(&TestData{}, "new_col")
	require.Error(t, err)

	err = db.Migrator().DropColumn(&TestData{}, "text")
	require.Error(t, err)

	err = db.Migrator().CreateIndex(&TestData{}, "idx_test_data_text")
	require.Error(t, err)
}

func testBadInputs(t *testing.T, provisioner Provisioner, dsn string) {
	t.Helper()
	require.NoError(t, provisioner.Connect(dsn))

	// Bad credentials
	require.Error(t, provisioner.CreateUser(t.Context(), "", ""))
	require.Error(t, provisioner.CreateUser(t.Context(), "user", ""))
	require.Error(t, provisioner.CreateUser(t.Context(), "", "pass"))

	// Bad access groups
	password, err := GeneratePassword()
	require.NoError(t, err)
	require.NoError(t, provisioner.CreateUser(t.Context(), "testuser", password))
	require.Error(t, provisioner.GrantReadOnly(t.Context(), "testuser", AccessScope{
		Groups:    []string{"'"},
		Resources: []string{"'"},
	}))
	require.Error(t, provisioner.GrantReadOnly(t.Context(), "testuser", AccessScope{
		Groups:    []string{"public"},
		Resources: []string{"'"},
	}))

	// For coverage
	require.NoError(t, provisioner.Close())
	require.NoError(t, provisioner.Close())

	_, err = provisioner.UserExists(t.Context(), "'")
	require.Error(t, err)

	require.Error(t, provisioner.DropUser(t.Context(), "user"))
	require.Error(t, provisioner.CreateUser(t.Context(), "user", "qweewqe"))
	require.Error(t, provisioner.Connect("fjdhfjdshfj"))
}

func testDropUser(t *testing.T, provisioner Provisioner, dsn string) {
	require.NoError(t, provisioner.Connect(dsn))
	password, err := GeneratePassword()
	require.NoError(t, err)
	require.NoError(t, provisioner.CreateUser(t.Context(), "testuser", password))
	require.NoError(t, provisioner.GrantReadOnly(t.Context(), "testuser", AccessScope{
		Groups:    []string{},
		Resources: []string{},
	}))

	exists, err := provisioner.UserExists(t.Context(), "testuser")
	require.NoError(t, err)
	require.True(t, *exists)

	err = provisioner.DropUser(t.Context(), "testuser")
	require.NoError(t, err)

	exists, err = provisioner.UserExists(t.Context(), "testuser")
	require.NoError(t, err)
	require.False(t, *exists)
}
