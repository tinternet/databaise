package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// Server holds the list of databases in a map.
type Server map[string]Database

// Database is the top-level config for each database connection.
// The presence of read/write/admin keys determines which tools are registered.
type Database struct {
	// Backend is the database type: "postgres", "sqlite", "sqlserver", "mongodb"
	Backend string `json:"type"`
	// Description is a human-readable description for LLM context
	Description string `json:"description,omitempty"`
	// Read config - presence enables *_list_tables, *_describe_table, *_execute_query tools
	Read json.RawMessage `json:"read,omitempty"`
	// Write config - presence enables *_write_query tool
	Write json.RawMessage `json:"write,omitempty"`
	// Admin config - presence enables *_create_index and other admin tools
	Admin json.RawMessage `json:"admin,omitempty"`
}

// HasRead returns true if read operations are configured.
func (d Database) HasRead() bool {
	return len(d.Read) > 0
}

// HasWrite returns true if write operations are configured.
func (d Database) HasWrite() bool {
	return len(d.Write) > 0
}

// HasAdmin returns true if admin operations are configured.
func (d Database) HasAdmin() bool {
	return len(d.Admin) > 0
}

// ParseReadConfig unmarshals the read config into the provided struct.
func (d Database) ParseReadConfig(v any) error {
	if len(d.Read) == 0 {
		return fmt.Errorf("read config not provided")
	}
	return json.Unmarshal(d.Read, v)
}

// ParseWriteConfig unmarshals the write config into the provided struct.
func (d Database) ParseWriteConfig(v any) error {
	if len(d.Write) == 0 {
		return fmt.Errorf("write config not provided")
	}
	return json.Unmarshal(d.Write, v)
}

// ParseAdminConfig unmarshals the admin config into the provided struct.
func (d Database) ParseAdminConfig(v any) error {
	if len(d.Admin) == 0 {
		return fmt.Errorf("admin config not provided")
	}
	return json.Unmarshal(d.Admin, v)
}

func LoadFromFile(filename string) (Server, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Server
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return config, nil
}
