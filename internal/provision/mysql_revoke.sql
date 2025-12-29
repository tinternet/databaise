-- MySQL: Revoke privileges and drop user.
-- Variables: {{.Username}}

-- Drop the user (this also revokes all privileges)
DROP USER IF EXISTS '{{.Username}}'@'%';
