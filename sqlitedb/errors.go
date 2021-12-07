package sqlitedb

import "errors"

var (
	ErrNoSQLiteConn         = errors.New("could not retrieve SQLite connection")
	ErrNoInsertsAllowed     = errors.New("table does not permit INSERTs")
	ErrCouldNotFindDatabase = errors.New("could not find database")
)
