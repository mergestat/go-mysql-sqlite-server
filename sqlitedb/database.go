package sqlitedb

import (
	"crawshaw.io/sqlite/sqlitex"
	"github.com/dolthub/go-mysql-server/sql"
)

// Database is an implementation of a go-mysql-server database
// backed by a SQLite database.
type Database struct {
	name    string
	pool    *sqlitex.Pool
	options *DatabaseOptions
}

// DatabaseOptions are options for managing the SQLite backend
type DatabaseOptions struct {
	// PreventInserts will block table insertions
	PreventInserts bool
	// PreventCreateTable will block table creation
	PreventCreateTable bool
}

func NewDatabase(name string, pool *sqlitex.Pool, options *DatabaseOptions) *Database {
	if options == nil {
		options = &DatabaseOptions{}
	}
	return &Database{name: name, pool: pool, options: options}
}

func (db *Database) Name() string {
	return db.name
}

func (db *Database) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	conn := db.pool.Get(ctx)
	if conn == nil {
		return nil, false, ErrNoSQLiteConn
	}
	defer db.pool.Put(conn)

	stmt := conn.Prep("SELECT name FROM sqlite_master WHERE type='table' AND name=$name COLLATE NOCASE;")
	stmt.SetText("$name", tblName)

	if hasRow, err := stmt.Step(); err != nil {
		return nil, false, err
	} else if !hasRow {
		return nil, false, nil
	}

	if err := stmt.Reset(); err != nil {
		return nil, false, err
	}

	return NewTable(tblName, db), true, nil
}

func (db *Database) GetTableNames(ctx *sql.Context) ([]string, error) {
	conn := db.pool.Get(ctx)
	if conn == nil {
		return nil, ErrNoSQLiteConn
	}
	defer db.pool.Put(conn)

	tables := make([]string, 0)
	stmt := conn.Prep("SELECT name FROM sqlite_master WHERE type='table'")
	for {
		if hasRow, err := stmt.Step(); err != nil {
			return nil, err
		} else if !hasRow {
			break
		}
		tables = append(tables, stmt.GetText("name"))
	}

	return tables, nil
}

func (db *Database) CreateTable(ctx *sql.Context, name string, schema sql.Schema) error {
	if db.options.PreventCreateTable {
		return ErrNoCreateTableAllowed
	}

	conn := db.pool.Get(ctx)
	if conn == nil {
		return ErrNoSQLiteConn
	}
	defer db.pool.Put(conn)

	sql, err := createTableSQL(name, schema)
	if err != nil {
		return err
	}

	if err := sqlitex.Exec(conn, sql, nil); err != nil {
		return err
	}

	return nil
}
