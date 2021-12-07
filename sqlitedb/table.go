package sqlitedb

import (
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

type Table struct {
	name string
	db   *Database
}

func NewTable(name string, db *Database) *Table {
	return &Table{
		name: name,
		db:   db,
	}
}

func (t *Table) Name() string   { return t.name }
func (t *Table) String() string { return t.name }

func (t *Table) Schema() sql.Schema {
	conn := t.db.pool.Get(context.TODO())
	if conn == nil {
		//TODO(patrickdevivo) how should we handle error here?
		return nil
	}
	defer t.db.pool.Put(conn)

	schema := make([]*sql.Column, 0)

	// TODO(patrickdevivo) not sure if this is okay to do, the Sprintf
	// to use the table name in the SQL query. SQLite won't allow param binding in PRAGMA args
	// https://stackoverflow.com/questions/39985599/parameter-binding-not-working-for-sqlite-pragma-table-info
	// Maybe the table name string should be checked/escaped before use?
	stmt := conn.Prep(fmt.Sprintf("SELECT * FROM PRAGMA_TABLE_INFO('%s')", t.name))
	for {
		if hasRow, err := stmt.Step(); err != nil {
			// TODO(patrickdevivo) how do we handle the error here?
			return nil
		} else if !hasRow {
			break
		}

		col := &sql.Column{
			Name:          stmt.GetText("name"),
			Type:          mustInferType(stmt.GetText("type")),
			Default:       nil,
			AutoIncrement: false,
			Nullable:      stmt.GetInt64("notnull") == 0,
			Source:        t.Name(),
			PrimaryKey:    stmt.GetInt64("pk") == 1,
		}
		schema = append(schema, col)
	}

	return schema
}

func (t *Table) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return &singlePartitionIter{}, nil
}

func (t *Table) PartitionRows(ctx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	conn := t.db.pool.Get(ctx)
	if conn == nil {
		return nil, ErrNoSQLiteConn
	}

	stmt := conn.Prep(fmt.Sprintf("SELECT * FROM %s", t.Name()))
	closeFunc := func() error {
		t.db.pool.Put(conn)
		return nil
	}

	return newQueryResultRowIter(stmt, closeFunc), nil
}
