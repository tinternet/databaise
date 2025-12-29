-- SQL Server: Revoke privileges and drop user/login.
-- Variables: {{.Username}}

-- Drop user from database
DROP USER IF EXISTS [{{ .Username }}];

-- Drop login from server
DROP LOGIN [{{ .Username }}];