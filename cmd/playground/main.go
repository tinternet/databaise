package main

import (
	"database/sql"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func main() {
	// db, err := gorm.Open(postgres.Open("postgres://postgres:mysecretpassword@postgres:5432/postgres?sslmode=disable&TimeZone=Asia/Shanghai"), &gorm.Config{
	// 	SkipDefaultTransaction:   false,
	// 	PrepareStmt:              true,
	// 	DisableNestedTransaction: true,
	// 	PrepareStmtTTL:           time.Second * 2,
	// })

	db, err := gorm.Open(mysql.Open("root:my-secret-pw@tcp(mysql:3306)/test?charset=utf8mb4&parseTime=True&loc=Local&multiStatements=false"), &gorm.Config{
		SkipDefaultTransaction:   true,
		PrepareStmt:              true,
		DisableNestedTransaction: true,
		PrepareStmtTTL:           time.Second * 2,
		Logger:                   logger.Default.LogMode(logger.Info),
	})

	// db, err := gorm.Open(sqlserver.Open("sqlserver://sa:yourStrong(password@mssql:1433/TestMCP"), &gorm.Config{
	// 	SkipDefaultTransaction:   false,
	// 	PrepareStmt:              true,
	// 	DisableNestedTransaction: true,
	// 	PrepareStmtTTL:           time.Second * 2,
	// })

	// sqlserver: //sa:yourStrong(password@mssql:1433/TestMCP

	panicOnErr(err)

	panicOnErr(db.Exec(`CREATE TABLE IF NOT EXISTS test_ro (id INT, name VARCHAR(50))`).Error)
	db.Exec(`DELETE FROM test_ro`) // clean slate

	tx := db.Begin(&sql.TxOptions{ReadOnly: true})

	err = tx.Exec(`INSERT INTO test_ro (id, name) VALUES (1, 'test')`).Error
	if err != nil {
		println("INSERT blocked:", err.Error())
	} else {
		println("INSERT succeeded (read-only NOT enforced)")
	}

	err = tx.Exec(`CREATE TABLE test_ddl_ro (id INT)`).Error
	if err != nil {
		println("DDL blocked:", err.Error())
	} else {
		println("DDL succeeded (implicit commit bypassed read-only?)")
	}

	tx.Rollback()

	var tableCount int64
	db.Raw(`SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'test_ddl_ro'`).Scan(&tableCount)
	println("DDL table exists:", tableCount > 0)
}

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}
