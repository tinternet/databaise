package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/tinternet/databaise/internal/provision"
)

func main() {
	// Common flags
	backend := flag.String("backend", "", "Database backend: postgres, mysql, sqlserver")
	adminDSN := flag.String("admin-dsn", "", "Admin DSN for connecting to create the user")
	schemas := flag.String("schemas", "", "Comma-separated schemas to grant access to (empty = all)")
	tables := flag.String("tables", "", "Comma-separated tables as schema.table or schema:table1,table2")
	outputJSON := flag.Bool("json", false, "Output result as JSON")
	outputConfig := flag.Bool("config", false, "Output as databaise config snippet")
	update := flag.Bool("update", false, "Update existing user's permissions instead of failing")

	// Revoke mode
	revoke := flag.String("revoke", "", "Username to revoke (instead of provisioning)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `databaise-provision: Create readonly database users for databaise

Usage:
  databaise-provision -backend postgres -admin-dsn "postgres://admin:pass@host/db" [options]
  databaise-provision -backend mysql -admin-dsn "user:pass@tcp(host)/db" [options]
  databaise-provision -backend sqlserver -admin-dsn "sqlserver://user:pass@host?database=db" [options]

Provision Options:
  -backend      Database backend: postgres, mysql, sqlserver (required)
  -admin-dsn    Admin connection string (required)
  -schemas      Comma-separated schemas to grant access to (empty = all objects)
  -tables       Specific tables in format: schema:table1,table2 or schema.table
  -update       Update existing user's permissions (default: fail if exists)
  -json         Output result as JSON
  -config       Output as databaise config snippet

Revoke Options:
  -revoke       Username to revoke (drops the user)

Examples:
  # Create readonly user with access to entire database
  databaise-provision -backend postgres -admin-dsn "postgres://admin:pass@localhost/mydb"

  # Create readonly user with access to specific schemas
  databaise-provision -backend postgres -admin-dsn "..." -schemas "public,analytics"

  # Create readonly user with access to specific tables
  databaise-provision -backend postgres -admin-dsn "..." -tables "public:users,orders"

  # Revoke a previously created user
  databaise-provision -backend postgres -admin-dsn "..." -revoke databaise_ro_abc123

`)
	}

	flag.Parse()

	if *backend == "" {
		fmt.Fprintln(os.Stderr, "Error: -backend is required")
		flag.Usage()
		os.Exit(1)
	}

	if *adminDSN == "" {
		fmt.Fprintln(os.Stderr, "Error: -admin-dsn is required")
		flag.Usage()
		os.Exit(1)
	}

	provisioner, ok := provision.Get(*backend)
	if !ok {
		fmt.Fprintf(os.Stderr, "Error: unsupported backend %q\n", *backend)
		fmt.Fprintf(os.Stderr, "Supported backends: %s\n", strings.Join(provision.List(), ", "))
		os.Exit(1)
	}

	// Revoke mode
	if *revoke != "" {
		if err := provisioner.Revoke(*adminDSN, *revoke); err != nil {
			fmt.Fprintf(os.Stderr, "Error revoking user: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Successfully revoked user: %s\n", *revoke)
		return
	}

	// Provision mode
	opts := provision.Options{
		Update: *update,
	}

	// Parse schemas and tables into the Schemas map
	if *schemas != "" || *tables != "" {
		opts.Schemas = make(map[string][]string)

		// Add schemas with full access
		if *schemas != "" {
			for schema := range strings.SplitSeq(*schemas, ",") {
				schema = strings.TrimSpace(schema)
				if schema != "" {
					opts.Schemas[schema] = []string{} // empty = all objects
				}
			}
		}

		// Add specific tables
		if *tables != "" {
			// Support formats:
			// - "schema:table1,table2" -> schema with specific tables
			// - "schema.table" -> single table
			for entry := range strings.SplitSeq(*tables, " ") {
				entry = strings.TrimSpace(entry)
				if entry == "" {
					continue
				}

				if strings.Contains(entry, ":") {
					// Format: schema:table1,table2
					parts := strings.SplitN(entry, ":", 2)
					schema := strings.TrimSpace(parts[0])
					tableList := strings.Split(parts[1], ",")
					for i, t := range tableList {
						tableList[i] = strings.TrimSpace(t)
					}
					opts.Schemas[schema] = tableList
				} else if strings.Contains(entry, ".") {
					// Format: schema.table
					parts := strings.SplitN(entry, ".", 2)
					schema := strings.TrimSpace(parts[0])
					table := strings.TrimSpace(parts[1])
					if existing, ok := opts.Schemas[schema]; ok {
						opts.Schemas[schema] = append(existing, table)
					} else {
						opts.Schemas[schema] = []string{table}
					}
				}
			}
		}
	}

	result, err := provisioner.Provision(*adminDSN, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error provisioning user: %v\n", err)
		os.Exit(1)
	}

	if *outputJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(result)
		return
	}

	if *outputConfig {
		config := map[string]any{
			"type": *backend,
			"read": map[string]string{
				"dsn": result.DSN,
			},
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(config)
		return
	}

	// Human-readable output
	fmt.Println("Readonly user created successfully!")
	fmt.Println()
	fmt.Printf("  Username: %s\n", result.User)
	fmt.Printf("  Password: %s\n", result.Password)
	fmt.Println()
	fmt.Println("  DSN (for databaise config):")
	fmt.Printf("    %s\n", result.DSN)
	fmt.Println()
	fmt.Println("  Grants applied:")
	for _, grant := range result.Grants {
		fmt.Printf("    %s\n", grant)
	}
	fmt.Println()
	fmt.Printf("  To revoke: databaise-provision -backend %s -admin-dsn \"...\" -revoke %s\n", *backend, result.User)
}
