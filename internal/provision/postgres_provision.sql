-- PostgreSQL: Create readonly user with SELECT on specified schemas/tables.
-- Variables: {{.Username}}, {{.Password}}, {{.Schemas}}, {{.Update}}

{{ if not .Update }}
    -- Create user
    CREATE USER {{ .Username }} WITH PASSWORD '{{ .Password }}';

    -- Force read-only transactions (safety net)
    ALTER USER {{ .Username }} SET default_transaction_read_only = on;
{{ end }}

-- Grant SELECT on schemas/tables
{{ range $schema, $objects := .Schemas }}
    GRANT USAGE ON SCHEMA {{ $schema }} TO {{ $.Username }};

    {{ if not $objects }}
        GRANT SELECT ON ALL TABLES IN SCHEMA {{ $schema }} TO {{ $.Username }};
        ALTER DEFAULT PRIVILEGES IN SCHEMA {{ $schema }} GRANT SELECT ON TABLES TO {{ $.Username }};
    {{ else }}
        {{ range $objects }}
            GRANT SELECT ON {{ $schema }}.{{ . }} TO {{ $.Username }};
        {{ end }}
    {{ end }}
{{ end }}
