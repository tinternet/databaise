-- PostgreSQL: Revoke privileges and drop user.
-- Variables: {{.Username}}, {{.Schemas}}

-- Revoke all privileges (ignore errors - user might not have had these)
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM {{ .Username }};
REVOKE pg_read_all_data FROM {{ .Username }};

-- Remove default privileges that were granted to this user
{{ range .Schemas }}
    ALTER DEFAULT PRIVILEGES IN SCHEMA {{ . }} REVOKE SELECT ON TABLES FROM {{ $.Username }};
    ALTER DEFAULT PRIVILEGES IN SCHEMA {{ . }} REVOKE SELECT ON SEQUENCES FROM {{ $.Username }};
{{ end }}

-- Reassign owned objects to current user, then drop owned
REASSIGN OWNED BY {{ .Username }} TO CURRENT_USER;
DROP OWNED BY {{ .Username }};

-- Drop the user
DROP USER IF EXISTS {{ .Username }};
