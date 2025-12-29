-- MySQL: Create readonly user with SELECT on specified databases/tables.
-- Variables: {{.Username}}, {{.Password}}, {{.Schemas}}, {{.Update}}
-- Note: MySQL uses "database" where others use "schema".
{{ if not .Update }}
    -- 1. Create user (% allows remote access)
    CREATE USER '{{.Username}}'@'%' IDENTIFIED BY '{{.Password}}';
{{ end }}
-- 2. Grant SELECT on databases/tables
{{ range $schema, $objects := .Schemas }}
    {{ if not $objects }}
        GRANT SELECT ON `{{$schema}}`.* TO '{{$.Username}}'@'%';
    {{ else }}
        {{ range $objects }}
            GRANT SELECT ON `{{$schema}}`.`{{.}}` TO '{{$.Username}}'@'%';
        {{ end }}
    {{ end }}
{{ end }}
-- 3. Apply changes
FLUSH PRIVILEGES;
