package sqlitedb_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"crawshaw.io/sqlite/sqlitex"
	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/mergestat/go-mysql-sqlite-server/sqlitedb"
)

type SQLiteDBHarness struct {
	*testing.T
	parallelism int
}

func NewSQLiteDBHarness(t *testing.T, parallelism int) *SQLiteDBHarness {
	return &SQLiteDBHarness{T: t, parallelism: parallelism}
}

func (harness *SQLiteDBHarness) Parallelism() int { return harness.parallelism }

func (harness *SQLiteDBHarness) NewDatabase(name string) sql.Database {
	name = strings.ToLower(name)
	pool, err := sqlitex.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", name), 0, 10)
	if err != nil {
		harness.Fatal(err)
	}
	return sqlitedb.NewDatabase(name, pool, nil)
}

func (harness *SQLiteDBHarness) NewDatabases(names ...string) []sql.Database {
	dbs := make([]sql.Database, len(names))
	for i, name := range names {
		name = strings.ToLower(name)
		dbs[i] = harness.NewDatabase(name)
	}
	return dbs
}

func (harness *SQLiteDBHarness) NewDatabaseProvider(dbs ...sql.Database) sql.MutableDatabaseProvider {
	return sqlitedb.NewProvider(dbs...)
}

func (harness *SQLiteDBHarness) NewTable(db sql.Database, name string, schema sql.Schema) (sql.Table, error) {
	name = strings.ToLower(name)

	sqliteBackedDB, ok := db.(*sqlitedb.Database)
	if !ok {
		return nil, errors.New("provided sql.Database not a *sqlitedb.Database")
	}

	if err := sqliteBackedDB.CreateTable(sql.NewEmptyContext(), name, schema); err != nil {
		return nil, err
	}

	return sqlitedb.NewTable(name, sqliteBackedDB), nil
}

func (harness *SQLiteDBHarness) NewContext() *sql.Context {
	return sql.NewEmptyContext()
}

func TestQueriesSimple(t *testing.T) {
	harness := NewSQLiteDBHarness(t, 1)
	// enginetest.TestCreateTable(t, harness)
	enginetest.TestQueries(t, harness)
}
