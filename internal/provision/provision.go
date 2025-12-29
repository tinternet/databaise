package provision

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// Options configures readonly user provisioning.
type Options struct {
	// Username for the readonly user; generated automatically if empty.
	Username string
	// Password for the readonly user; generated automatically if empty.
	Password string

	// Schemas defines which schemas/databases to grant access to.
	// Key: schema name (or database name for MySQL)
	// Value: list of tables/views to grant access to.
	//        Empty slice = all objects in the schema.
	//        Non-empty = only the specified objects.
	//
	// Examples:
	//   map[string][]string{"dbo": {}}                    // all objects in dbo
	//   map[string][]string{"dbo": {"orders", "users"}}   // only orders and users tables
	//   map[string][]string{"public": {}, "audit": {}}    // all objects in public and audit
	Schemas map[string][]string

	// Update allows updating an existing user's permissions.
	// If false (default), provisioning fails if the user already exists.
	// If true, skips user creation and only updates grants.
	Update bool
}

// Result contains the created user credentials.
type Result struct {
	User     string
	Password string
	DSN      string // Full connection string for the readonly user
	Grants   []string
}

// Provisioner creates readonly database users.
type Provisioner interface {
	// Provision creates a readonly user with the specified options.
	// adminDSN is used to connect and create the user.
	Provision(adminDSN string, opts Options) (*Result, error)

	// Revoke removes a previously provisioned user.
	Revoke(adminDSN string, username string) error
}

var registry = make(map[string]Provisioner)

// Register adds a provisioner for a backend.
func Register(name string, p Provisioner) {
	registry[name] = p
}

// Get returns the provisioner for a backend.
func Get(name string) (Provisioner, bool) {
	p, ok := registry[name]
	return p, ok
}

// List returns all registered backend names.
func List() []string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	return names
}

// GenerateUsername creates a unique username for provisioned users.
func GenerateUsername() string {
	b := make([]byte, 4)
	rand.Read(b)
	return fmt.Sprintf("databaise_ro_%s", hex.EncodeToString(b))
}

// GeneratePassword creates a secure random password.
func GeneratePassword() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
