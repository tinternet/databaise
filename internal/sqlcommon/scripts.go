package sqlcommon

import _ "embed"

//go:embed postgres_verify_readonly.sql
var PostgreSQLVerifyReadonlySQL string

//go:embed sqlserver_verify_readonly.sql
var SQLServerVerifyReadonlySQL string
