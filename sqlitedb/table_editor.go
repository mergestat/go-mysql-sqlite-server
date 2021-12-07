package sqlitedb

import "github.com/dolthub/go-mysql-server/sql"

type tableEditor struct{}

func newTableEditor() *tableEditor {
	return &tableEditor{}
}

func (edit *tableEditor) StatementBegin(ctx *sql.Context) {
	// TODO(patrickdevivo)
}

func (edit *tableEditor) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	// TODO(patrickdevivo)
	return nil
}

func (edit *tableEditor) StatementComplete(ctx *sql.Context) error {
	// TODO(patrickdevivo)
	return nil
}
