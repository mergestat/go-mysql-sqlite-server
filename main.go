package main

import (
	"crawshaw.io/sqlite/sqlitex"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"github.com/mergestat/go-mysql-sqlite-server/sqlitedb"
)

const (
	dbName = "testdata/Chinook_Sqlite.sqlite"
)

func main() {
	pool, err := sqlitex.Open(dbName, 0, 10)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := pool.Close(); err != nil {
			panic(err)
		}
	}()

	db := sqlitedb.NewDatabase(dbName, pool, nil)
	engine := sqle.NewDefault(
		sql.NewDatabaseProvider(
			db,
			information_schema.NewInformationSchemaDatabase(),
		))

	config := server.Config{
		Protocol: "tcp",
		Address:  "localhost:3306",
		Auth:     auth.NewNativeSingle("root", "", auth.AllPermissions),
	}

	s, err := server.NewDefaultServer(config, engine)
	if err != nil {
		panic(err)
	}

	s.Start()
}
