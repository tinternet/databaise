package backend

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/tinternet/databaise/internal/config"
	"github.com/tinternet/databaise/internal/logging"
	"github.com/tinternet/databaise/internal/server"
)

var log = logging.New("backend")

// ToolLevel represents the permission level of a tool
type ToolLevel int

const (
	LevelRead ToolLevel = iota
	LevelWrite
	LevelAdmin
)

// toolDef stores a tool definition before registration
type toolDef struct {
	name        string
	description string
	level       ToolLevel
	register    func(instances map[string]*instance)
}

// instance holds connections for a single database
type instance struct {
	name        string
	description string
	readDB      any
	writeDB     any
	adminDB     any
}

// instanceInfo holds metadata for list_databases
type instanceInfo struct {
	name        string
	description string
	backendName string
	tools       []toolDef
	hasRead     bool
	hasWrite    bool
	hasAdmin    bool
}

// Backend holds runtime state for a backend type.
// This state is populated during initialization and becomes immutable once the server starts.
type Backend[DB any] struct {
	name            string
	tools           []toolDef
	instances       map[string]*instance
	toolsRegistered bool
	initRead        func(cfg config.Database) (DB, error)
	initWrite       func(cfg config.Database) (DB, error)
	initAdmin       func(cfg config.Database) (DB, error)
	validate        func(cfg config.Database) error
}

// DatabaseIdentifier is implemented by config types to provide a unique identifier
// for validation that read, write, and admin configs point to the same database.
type DatabaseIdentifier interface {
	GetDatabaseIdentifier() (string, error)
}

type Connector[R, W, A DatabaseIdentifier, DB any] interface {
	ConnectRead(cfg R) (DB, error)
	ConnectWrite(cfg W) (DB, error)
	ConnectAdmin(cfg A) (DB, error)
}

// validateSameDatabase ensures all configs point to the same database.
// This is a core requirement of the backend system - read, write, and admin
// connections must operate on the same database.
func validateSameDatabase(configs ...DatabaseIdentifier) error {
	m := make(map[string]bool)
	for _, cfg := range configs {
		if cfg == nil {
			continue
		}
		id, err := cfg.GetDatabaseIdentifier()
		if err != nil {
			return err
		}
		m[id] = true
	}
	if len(m) > 1 {
		return fmt.Errorf("read, write, admin configs must point to the same database")
	}
	return nil
}

func NewBackend[R, W, A DatabaseIdentifier, DB any](name string, c Connector[R, W, A, DB]) Backend[DB] {
	return Backend[DB]{
		name:      name,
		tools:     make([]toolDef, 0),
		instances: make(map[string]*instance),
		initRead:  makeConnector(c.ConnectRead, config.Database.ParseReadConfig, "read"),
		initWrite: makeConnector(c.ConnectWrite, config.Database.ParseWriteConfig, "write"),
		initAdmin: makeConnector(c.ConnectAdmin, config.Database.ParseAdminConfig, "admin"),
		validate:  makeValidator[R, W, A](),
	}
}

func makeValidator[R, W, A DatabaseIdentifier]() func(config.Database) error {
	return func(cfg config.Database) error {
		var identifiers []DatabaseIdentifier

		if cfg.HasRead() {
			var rCfg R
			if err := cfg.ParseReadConfig(&rCfg); err != nil {
				return fmt.Errorf("failed to parse read config: %w", err)
			}
			identifiers = append(identifiers, rCfg)
		}

		if cfg.HasWrite() {
			var wCfg W
			if err := cfg.ParseWriteConfig(&wCfg); err != nil {
				return fmt.Errorf("failed to parse write config: %w", err)
			}
			identifiers = append(identifiers, wCfg)
		}

		if cfg.HasAdmin() {
			var aCfg A
			if err := cfg.ParseAdminConfig(&aCfg); err != nil {
				return fmt.Errorf("failed to parse admin config: %w", err)
			}
			identifiers = append(identifiers, aCfg)
		}

		return validateSameDatabase(identifiers...)
	}
}

func makeConnector[C, DB any](connect func(C) (DB, error), parse func(config.Database, any) error, level string) func(config.Database) (DB, error) {
	return func(cfg config.Database) (DB, error) {
		var connCfg C
		var zero DB
		if err := parse(cfg, &connCfg); err != nil {
			return zero, fmt.Errorf("failed to parse %s config: %w", level, err)
		}
		db, err := connect(connCfg)
		if err != nil {
			return zero, fmt.Errorf("failed to connect %s: %w", level, err)
		}
		return db, nil
	}
}

type Handler[In, Out, DB any] func(context.Context, In, DB) (Out, error)

func AddReadTool[In, Out, DB any](b *Backend[DB], name, description string, h Handler[In, Out, DB]) {
	addTool(b, name, description, LevelRead, h, func(inst *instance) any { return inst.readDB })
}

func AddWriteTool[In, Out, DB any](b *Backend[DB], name, description string, h Handler[In, Out, DB]) {
	addTool(b, name, description, LevelWrite, h, func(inst *instance) any { return inst.writeDB })
}

func AddAdminTool[In, Out, DB any](b *Backend[DB], name, description string, h Handler[In, Out, DB]) {
	addTool(b, name, description, LevelAdmin, h, func(inst *instance) any { return inst.adminDB })
}

func addTool[In, Out, DB any](b *Backend[DB], name, description string, level ToolLevel, h Handler[In, Out, DB], getDB func(*instance) any) {
	b.tools = append(b.tools, toolDef{
		name:        name,
		description: description,
		level:       level,
		register: func(instances map[string]*instance) {
			toolName := b.name + "_" + name
			log.Printf("Registering tool: %s", toolName)
			server.AddToolWithDatabaseName[In](func(ctx context.Context, req Request[In]) (Out, error) {
				var zero Out

				inst, ok := instances[req.DatabaseName]
				if !ok {
					return zero, fmt.Errorf("database %q not found", req.DatabaseName)
				}

				db := getDB(inst)
				if db == nil {
					return zero, fmt.Errorf("database %q does not support this operation", req.DatabaseName)
				}

				return h(ctx, req.Payload, db.(DB))
			}, server.Tool{Name: toolName, Description: description})
		},
	})
}

var registry = make(map[string]func(dbName string, cfg config.Database) error)

func Register[DB any](b *Backend[DB]) {
	registry[b.name] = func(dbName string, cfg config.Database) error {
		if !cfg.HasAdmin() && !cfg.HasRead() && !cfg.HasWrite() {
			return fmt.Errorf("database %q has no read, write, or admin configuration", dbName)
		}

		// Validate that all configs point to the same database
		if err := b.validate(cfg); err != nil {
			return fmt.Errorf("database %q validation failed: %w", dbName, err)
		}

		inst := &instance{
			name:        dbName,
			description: cfg.Description,
		}

		// Connect based on what's configured
		if cfg.HasRead() {
			db, err := b.initRead(cfg)
			if err != nil {
				return err
			}
			inst.readDB = db
		}
		if cfg.HasWrite() {
			db, err := b.initWrite(cfg)
			if err != nil {
				return err
			}
			inst.writeDB = db
		}
		if cfg.HasAdmin() {
			db, err := b.initAdmin(cfg)
			if err != nil {
				return err
			}
			inst.adminDB = db
		}

		b.instances[dbName] = inst

		// Register all tools once (on first database init for this backend)
		if !b.toolsRegistered {
			for _, tool := range b.tools {
				tool.register(b.instances)
			}
			b.toolsRegistered = true
		}

		// Track instance globally for list_databases
		allInstances[dbName] = instanceInfo{
			name:        dbName,
			description: cfg.Description,
			backendName: b.name,
			tools:       b.tools,
			hasRead:     inst.readDB != nil,
			hasWrite:    inst.writeDB != nil,
			hasAdmin:    inst.adminDB != nil,
		}

		return nil
	}
}

// Has returns true if a backend is registered with the given name.
func Has(name string) bool {
	_, ok := registry[name]
	return ok
}

// Init initializes a database using its registered backend.
func Init(dbName string, cfg config.Database) error {
	init, ok := registry[cfg.Backend]
	if !ok {
		return fmt.Errorf("unknown backend: %s", cfg.Backend)
	}
	return init(dbName, cfg)
}

// Global instance tracking for list_databases.
// Populated during initialization, immutable after server starts.
var allInstances = make(map[string]instanceInfo)

// DatabaseInfo represents info about a database for list_databases
type DatabaseInfo struct {
	Name        string   `json:"name" jsonschema:"The unique identifier for this database."`
	Backend     string   `json:"backend" jsonschema:"The database backend type (e.g. postgres, sqlite, mssql)."`
	Description string   `json:"description,omitempty" jsonschema:"Human-readable description of the database contents."`
	Tools       []string `json:"tools" jsonschema:"List of available tool suffixes for this database (e.g. read_query, write_query)."`
}

// ListDatabasesOut is the output for the list_databases tool
type ListDatabasesOut struct {
	Databases []DatabaseInfo `json:"databases" jsonschema:"List of all available databases."`
}

// ListDatabases returns info about all initialized databases
func ListDatabases() ListDatabasesOut {
	result := make([]DatabaseInfo, 0, len(allInstances))
	for _, inst := range allInstances {
		info := DatabaseInfo{
			Name:        inst.name,
			Backend:     inst.backendName,
			Description: inst.description,
			Tools:       make([]string, 0),
		}

		// Derive available tools from the backend's tool definitions
		for _, tool := range inst.tools {
			switch tool.level {
			case LevelRead:
				if inst.hasRead {
					info.Tools = append(info.Tools, tool.name)
				}
			case LevelWrite:
				if inst.hasWrite {
					info.Tools = append(info.Tools, tool.name)
				}
			case LevelAdmin:
				if inst.hasAdmin {
					info.Tools = append(info.Tools, tool.name)
				}
			}
		}

		result = append(result, info)
	}

	return ListDatabasesOut{Databases: result}
}

// Request wraps tool input with database routing
type Request[In any] struct {
	DatabaseName string `jsonschema:"required,The name of the database to operate on."`
	Payload      In
}

// UnmarshalJSON implements custom json unmarshaling to support the inline payload
func (r *Request[In]) UnmarshalJSON(data []byte) error {
	var db struct {
		DatabaseName string `json:"database_name"`
	}
	var payload In
	if err := json.Unmarshal(data, &db); err != nil {
		return err
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}
	r.DatabaseName = db.DatabaseName
	r.Payload = payload
	return nil
}

func init() {
	// Register the list_databases tool
	server.AddTool(func(ctx context.Context, in struct{}) (ListDatabasesOut, error) {
		return ListDatabases(), nil
	}, server.Tool{
		Name:        "list_databases",
		Description: "List all available databases with their backend type and supported operations.",
	})
}
