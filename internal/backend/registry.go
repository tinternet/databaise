package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/tinternet/databaise/internal/config"
	"github.com/tinternet/databaise/internal/logging"
)

var log = logging.New("backend")

// Instance represents a configured database instance.
type Instance struct {
	Name        string
	Description string
	Dialect     string
	HasAdmin    bool

	// Read returns an SQLBackend using the read connection.
	Read func() SQLBackend

	// Admin returns an SQLBackend using the admin connection, or nil if not configured.
	Admin func() SQLBackend
}

// registry holds all registered database instances.
var (
	instances   = make(map[string]*Instance)
	instancesMu sync.RWMutex
)

// factoryEntry stores a registered backend factory with its initializer.
type factoryEntry struct {
	dialect string
	init    func(name string, cfg config.Database) error
}

// factories maps backend type names to their factory entries.
var (
	factories   = make(map[string]factoryEntry)
	factoriesMu sync.RWMutex
)

// RegisterFactory registers a backend factory for a given backend type.
// This should be called in init() by each backend package.
func RegisterFactory[R, A, DB any](backendType string, factory BackendFactory[DB], connect Connector[R, A, DB]) {
	factoriesMu.Lock()
	defer factoriesMu.Unlock()

	factories[backendType] = factoryEntry{
		dialect: factory.Dialect(),
		init: func(name string, cfg config.Database) error {
			return initInstance(name, cfg, factory, connect)
		},
	}
	log.Printf("Registered backend factory: %s (%s)", backendType, factory.Dialect())
}

// Connector handles database connections for a backend type.
type Connector[R, A, DB any] interface {
	// ConnectRead establishes a read-only connection.
	ConnectRead(cfg R) (DB, error)

	// ConnectAdmin establishes an admin connection.
	ConnectAdmin(cfg A) (DB, error)
}

// initInstance initializes a database instance using its factory.
func initInstance[R, A, DB any](name string, cfg config.Database, factory BackendFactory[DB], connect Connector[R, A, DB]) error {
	if !cfg.HasRead() {
		return fmt.Errorf("database %q must have read configuration", name)
	}

	var rCfg R
	if err := json.Unmarshal(cfg.Read, &rCfg); err != nil {
		return fmt.Errorf("failed to parse read config for %q: %w", name, err)
	}

	// Connect read
	readDB, err := connect.ConnectRead(rCfg)
	if err != nil {
		return fmt.Errorf("failed to connect read for %q: %w", name, err)
	}

	inst := &Instance{
		Name:        name,
		Description: cfg.Description,
		Dialect:     factory.Dialect(),
		HasAdmin:    cfg.HasAdmin(),
		Read:        func() SQLBackend { return factory.New(readDB) },
	}

	// Connect admin if configured
	if cfg.HasAdmin() {
		var aCfg A
		if err := json.Unmarshal(cfg.Read, &aCfg); err != nil {
			return fmt.Errorf("failed to parse read config for %q: %w", name, err)
		}

		adminDB, err := connect.ConnectAdmin(aCfg)
		if err != nil {
			return fmt.Errorf("failed to connect admin for %q: %w", name, err)
		}
		inst.Admin = func() SQLBackend { return factory.New(adminDB) }
	}

	instancesMu.Lock()
	instances[name] = inst
	instancesMu.Unlock()

	log.Printf("Initialized database: %s (%s)", name, factory.Dialect())
	return nil
}

// Init initializes a database instance from config.
func Init(name string, cfg config.Database) error {
	factoriesMu.RLock()
	entry, ok := factories[cfg.Backend]
	factoriesMu.RUnlock()

	if !ok {
		return fmt.Errorf("unknown backend type: %s", cfg.Backend)
	}

	return entry.init(name, cfg)
}

// Has returns true if a backend type is registered.
func Has(backendType string) bool {
	factoriesMu.RLock()
	defer factoriesMu.RUnlock()
	_, ok := factories[backendType]
	return ok
}

// GetInstance returns a database instance by name.
func GetInstance(name string) (*Instance, error) {
	instancesMu.RLock()
	defer instancesMu.RUnlock()

	inst, ok := instances[name]
	if !ok {
		return nil, fmt.Errorf("database %q not found", name)
	}
	return inst, nil
}

// GetReadBackend returns an SQLBackend for read operations.
func GetReadBackend(databaseName string) (SQLBackend, error) {
	inst, err := GetInstance(databaseName)
	if err != nil {
		return nil, err
	}
	return inst.Read(), nil
}

// GetAdminBackend returns an SQLBackend for admin operations.
func GetAdminBackend(databaseName string) (SQLBackend, error) {
	inst, err := GetInstance(databaseName)
	if err != nil {
		return nil, err
	}
	if inst.Admin == nil {
		return nil, fmt.Errorf("admin not configured for database %q", databaseName)
	}
	return inst.Admin(), nil
}

// Handle wraps a backend method call with database routing.
// getBackend should be either GetReadBackend or GetAdminBackend.
func Handle[In any, Out any](
	ctx context.Context,
	databaseName string,
	in In,
	getBackend func(string) (SQLBackend, error),
	fn func(SQLBackend, context.Context, In) (Out, error),
) (Out, error) {
	var zero Out
	backend, err := getBackend(databaseName)
	if err != nil {
		return zero, err
	}
	return fn(backend, ctx, in)
}
