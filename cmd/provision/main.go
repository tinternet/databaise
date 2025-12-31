package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/tinternet/databaise/internal/provision"
)

func main() {
	backend := flag.String("backend", "", "postgres, mysql, sqlserver")
	dsn := flag.String("dsn", "", "Admin Connection String")
	scopeFlag := flag.String("scope", "", "Comma-separated Schemas/DBs (Grants ALL access)")
	resourceFlag := flag.String("resources", "", "Comma-separated FQNs (schema.table) (Grants Specific access)")
	user := flag.String("user", "", "Username to create/manage")
	revoke := flag.Bool("revoke", false, "Revoke and drop the user")

	flag.Parse()

	if *scopeFlag != "" && *resourceFlag != "" {
		log.Fatal("Error: You cannot use -scope and -resources together. Choose one.")
	}
	if *scopeFlag == "" && *resourceFlag == "" && !*revoke {
		log.Fatal("Error: You must provide either -scope or -resources (unless revoking).")
	}
	if *backend == "" || *dsn == "" || *user == "" {
		log.Fatal("Error: -backend, -dsn, and -user are required.")
	}

	scope := provision.AccessScope{}
	if *scopeFlag != "" {
		for v := range strings.SplitSeq(*scopeFlag, ",") {
			scope.Groups = append(scope.Groups, strings.TrimSpace(v))
		}
	} else if *resourceFlag != "" {
		for v := range strings.SplitSeq(*resourceFlag, ",") {
			if !strings.Contains(v, ".") {
				log.Fatalf("Error: Resource '%s' is not fully qualified. Use format 'schema.table' or 'db.collection'", v)
			}
			scope.Resources = append(scope.Resources, strings.TrimSpace(v))
		}
	}

	var p provision.Provisioner
	switch *backend {
	case "postgres":
		p = &provision.PostgresProvisioner{}
	case "mysql":
		p = &provision.MySqlProvisioner{}
	case "sqlserver":
		p = &provision.SqlServerProvisioner{}
	default:
		log.Fatal("Unknown backend type")
	}

	if err := p.Connect(*dsn); err != nil {
		log.Fatalf("Connection failed: %v", err)
	}
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if *revoke {
		if err := p.DropUser(ctx, *user); err != nil {
			log.Fatalf("Revoke failed: %v", err)
		}
		fmt.Printf("User %s revoked.\n", *user)
		return
	}

	exists, err := p.UserExists(ctx, *user)
	if err != nil {
		log.Fatal(err)
	}

	if !*exists {
		fmt.Println("Generating password...")
		password, err := provision.GeneratePassword()
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Creating user...")
		err = p.CreateUser(ctx, *user, password)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Success! User: %s Password: %s\n", *user, password)
	}

	fmt.Println("Granting permissions...")
	if err := p.GrantReadOnly(ctx, *user, scope); err != nil {
		log.Fatalf("Grant failed: %v", err)
	}
	fmt.Println("Success!")
}
