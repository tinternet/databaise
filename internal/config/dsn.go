package config

// ReadConfig holds configuration for read operations on DSN-based drivers.
type ReadConfig struct {
	// DSN is the connection string for read operations.
	DSN string `json:"dsn"`
	// EnforceReadonly verifies the DSN user has no write permissions at startup.
	// Defaults to true if omitted.
	EnforceReadonly *bool `json:"enforce_readonly,omitempty"`
}

// ShouldEnforceReadonly returns whether readonly enforcement is enabled.
// Defaults to true if not explicitly set.
func (c ReadConfig) ShouldEnforceReadonly() bool {
	if c.EnforceReadonly == nil {
		return true
	}
	return *c.EnforceReadonly
}

// WriteConfig holds configuration for write operations on DSN-based drivers.
type WriteConfig struct {
	// DSN is the connection string for write operations.
	DSN string `json:"dsn"`
}

// AdminConfig holds configuration for admin operations on DSN-based drivers.
type AdminConfig struct {
	// DSN is the connection string for admin operations.
	DSN string `json:"dsn"`
}

type DSNConfig interface {
	GetDSN() string
}

func (c ReadConfig) GetDSN() string  { return c.DSN }
func (c WriteConfig) GetDSN() string { return c.DSN }
func (c AdminConfig) GetDSN() string { return c.DSN }
