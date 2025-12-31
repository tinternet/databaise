package sqltest

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func ReplaceURLCredentials(t *testing.T, dsn, user, pass string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	u.User = url.UserPassword(user, pass)
	return u.String()
}

type User struct {
	gorm.Model
	Username string  `gorm:"size:50;not null;uniqueIndex:idx_username"`
	Email    string  `gorm:"size:100;not null;unique"`
	Age      uint8   `gorm:"check:age >= 18"`
	Role     string  `gorm:"size:20;default:'guest';not null"`
	Active   bool    `gorm:"default:true"`
	Salary   float64 `gorm:"precision:15;scale:2"`
	Bio      string  `gorm:"type:text"`
	Orders   []Order `gorm:"foreignKey:UserID"`
}

type Order struct {
	gorm.Model
	OrderCode string     `gorm:"size:20;uniqueIndex;not null"`
	Amount    float64    `gorm:"not null"`
	UserID    uint       `gorm:"not null;index"`
	User      User       `gorm:"constraint:OnUpdate:CASCADE,OnDelete:RESTRICT;"`
	ShippedAt *time.Time // Nullable field
}

func Seed(t *testing.T, db *gorm.DB) {
	require.NoError(t, db.AutoMigrate(&User{}, &Order{}))

	users := []User{
		{Username: "admin_user", Email: "admin@example.com", Age: 30, Role: "admin", Salary: 100000},
		{Username: "standard_user", Email: "user@example.com", Age: 25, Role: "user", Salary: 50000},
		{Username: "guest_user", Email: "guest@example.com", Age: 20, Role: "guest", Salary: 0},
	}
	require.NoError(t, db.Create(&users).Error)

	orders := []Order{
		{OrderCode: "ORD-001", Amount: 99.99, UserID: users[0].ID},
		{OrderCode: "ORD-002", Amount: 150.50, UserID: users[1].ID},
	}
	require.NoError(t, db.Create(&orders).Error)

	t.Logf("Seeded %d users and %d orders", len(users), len(orders))
}
