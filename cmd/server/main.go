package main

import (
	"flag"
	"maps"
	"os"
	"slices"

	"github.com/tinternet/databaise/internal/backend"
	"github.com/tinternet/databaise/internal/config"
	"github.com/tinternet/databaise/internal/logging"
	"github.com/tinternet/databaise/internal/server"

	_ "github.com/tinternet/databaise/internal/postgres"
	_ "github.com/tinternet/databaise/internal/sqlite"
	_ "github.com/tinternet/databaise/internal/sqlserver"
)

func main() {
	transportMode := flag.String("transport", "http", "Transport mode: http or stdio")
	configPath := flag.String("config", "config.json", "Path to configuration file")
	httpAddress := flag.String("address", "0.0.0.0:8888", "HTTP server address (only used in http mode)")
	flag.Parse()

	if *transportMode == "stdio" {
		logging.SetOutput(os.Stderr)
	}

	cfg, err := config.LoadFromFile(*configPath)
	if err != nil {
		logging.Fatal("Failed to load config: %v", err)
	}

	// Sorted for consistent log order
	dbNames := slices.Sorted(maps.Keys(cfg))

	for _, dbName := range dbNames {
		dbCfg := cfg[dbName]
		if !backend.Has(dbCfg.Backend) {
			logging.Warn("unsupported backend %q for %s, skipping", dbCfg.Backend, dbName)
			continue
		}
		if err := backend.Init(dbName, dbCfg); err != nil {
			logging.Fatal("Failed to initialize db %q: %v", dbName, err)
		}
		logging.Info("Registered database: %s (%s)", dbName, dbCfg.Backend)
	}

	// Start server based on transport mode
	switch *transportMode {
	case "http":
		server.StartHTTP(*httpAddress)
	case "stdio":
		server.StartSTDIO()
	default:
		logging.Fatal("Unknown transport mode: %s (valid options: stdio, http)", *transportMode)
	}
}
