package sqlitedb

import (
	"fmt"

	"crawshaw.io/sqlite/sqlitex"
	sq "github.com/Masterminds/squirrel"
	"github.com/dolthub/go-mysql-server/sql"
)

var _ sql.InsertableTable = (*Table)(nil)
var _ sql.RowInserter = (*rowInserter)(nil)

type rowInserter struct {
	*tableEditor
	table *Table
}

func newRowInserter(table *Table) *rowInserter {
	return &rowInserter{
		tableEditor: newTableEditor(),
		table:       table,
	}
}

func (t *Table) Inserter(*sql.Context) sql.RowInserter {
	return newRowInserter(t)
}

func (inserter *rowInserter) Insert(ctx *sql.Context, row sql.Row) error {
	if inserter.table.db.options.PreventInserts {
		return ErrNoInsertsAllowed
	}
	conn := inserter.table.db.pool.Get(ctx)
	if conn == nil {
		return ErrNoSQLiteConn
	}
	defer inserter.table.db.pool.Put(conn)

	schema := inserter.table.Schema()
	colNames := make([]string, len(schema))
	for c, col := range schema {
		colNames[c] = fmt.Sprintf("'%s'", col.Name)
	}

	sql, args, err := sq.Insert(inserter.table.name).
		Columns(colNames...).
		Values(row...).
		ToSql()
	if err != nil {
		return err
	}

	err = sqlitex.Exec(conn, sql, nil, args...)
	if err != nil {
		return err
	}

	return nil
}

func (inserter *rowInserter) Close(*sql.Context) error { return nil }
