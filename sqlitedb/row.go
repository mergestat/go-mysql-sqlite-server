package sqlitedb

import (
	"io"

	"crawshaw.io/sqlite"
	"github.com/dolthub/go-mysql-server/sql"
)

type queryResultRowIter struct {
	stmt      *sqlite.Stmt
	closeFunc func() error
}

func newQueryResultRowIter(stmt *sqlite.Stmt, closeFunc func() error) *queryResultRowIter {
	return &queryResultRowIter{stmt: stmt, closeFunc: closeFunc}
}

func (iter *queryResultRowIter) Next() (sql.Row, error) {
	if hasNext, err := iter.stmt.Step(); err != nil {
		return nil, err
	} else if !hasNext {
		return nil, io.EOF
	} else {
		colCount := iter.stmt.ColumnCount()
		row := make([]interface{}, colCount)
		for c := 0; c < colCount; c++ {
			var tmp interface{}
			switch iter.stmt.ColumnType(c) {
			case sqlite.SQLITE_INTEGER:
				tmp = iter.stmt.ColumnInt64(c)
			case sqlite.SQLITE_FLOAT:
				tmp = iter.stmt.ColumnFloat(c)
			case sqlite.SQLITE_TEXT:
				tmp = iter.stmt.ColumnText(c)
			case sqlite.SQLITE_BLOB:
				buf := make([]byte, 1024*1024) // TODO(patrickdevivo) figure out a better value here?
				_ = iter.stmt.ColumnBytes(c, buf)
			case sqlite.SQLITE_NULL:
				tmp = nil
			}
			row[c] = tmp
		}
		return row, nil
	}
}

func (iter *queryResultRowIter) Close(*sql.Context) error {
	if err := iter.stmt.Reset(); err != nil {
		return err
	}
	if iter.closeFunc != nil {
		return iter.closeFunc()
	} else {
		return nil
	}
}
