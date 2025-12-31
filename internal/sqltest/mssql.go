package sqltest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mssql"
	"github.com/testcontainers/testcontainers-go/wait"
)

func SetupSqlServerContainer(t *testing.T) string {
	t.Helper()
	mssqlContainer, err := mssql.Run(t.Context(),
		"mcr.microsoft.com/mssql/server:2019-latest",
		mssql.WithAcceptEULA(),
		mssql.WithPassword("MyStr0ng(!)Password"),
		// testcontainers.WithLogConsumers(&testcontainers.StdoutLogConsumer{}),
		testcontainers.WithWaitStrategy(
			wait.ForLog("SQL Server is now ready for client connections").WithStartupTimeout(60*time.Second),
			wait.ForLog("Common language runtime").WithStartupTimeout(60*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, mssqlContainer)
	require.NoError(t, err)
	sqlserverTestDSN, err := mssqlContainer.ConnectionString(t.Context())
	require.NoError(t, err)
	return sqlserverTestDSN + "encrypt=false&trustservercertificate=true"
}
