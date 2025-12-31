package sqltest

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
)

func SetupMySqlContainer(t *testing.T) string {
	t.Helper()
	mysqlContainer, err := mysql.Run(context.Background(),
		"mysql:9.5",
		testcontainers.WithEnv(map[string]string{"MYSQL_ROOT_PASSWORD": "test"}),
	)
	testcontainers.CleanupContainer(t, mysqlContainer)
	require.NoError(t, err)
	dsn, err := mysqlContainer.ConnectionString(t.Context())
	require.NoError(t, err)
	dsn = strings.Replace(dsn, "test", "root", 1)
	return dsn + "?multiStatements=true&parseTime=true"
}
