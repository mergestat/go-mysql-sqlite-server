package sqlitedb

import "errors"

var (
	ErrNoSQLiteConn         = errors.New("could not retrieve SQLite connection")
	ErrNoInsertsAllowed     = errors.New("table does not permit INSERTs")
	ErrNoCreateTableAllowed = errors.New("database does not permit creating tables")
	ErrCouldNotFindDatabase = errors.New("could not find database")
)
