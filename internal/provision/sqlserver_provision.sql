-- SQL Server: Create readonly user with SELECT on specified schemas/tables.
-- Variables: {{.Username}}, {{.Password}}, {{.Schemas}}, {{.Update}}
{{ if not .Update }}
    -- 1. Create login (server-level)
    CREATE LOGIN [{{ .Username }}] WITH PASSWORD = N'{{.Password}}';

    -- 2. Create user (database-level)
    CREATE USER [{{ .Username }}] FOR LOGIN [{{ .Username }}];
{{ end }}
-- 3. Grant SELECT on schemas/tables
{{ range $schema, $objects := .Schemas }}
    {{ if not $objects }}
        GRANT SELECT ON SCHEMA::[{{ $schema }}] TO [{{ $.Username }}];
    {{ else }}
        {{ range $objects }}
            GRANT SELECT ON [{{ $schema }}].[{{ . }}] TO [{{ $.Username }}];
        {{ end }}
    {{ end }}
{{ end }}
