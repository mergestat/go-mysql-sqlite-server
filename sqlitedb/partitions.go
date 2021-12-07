package sqlitedb

import (
	"io"

	"github.com/dolthub/go-mysql-server/sql"
)

type singlePartitionIter struct {
	called bool
}

func (iter *singlePartitionIter) Next() (sql.Partition, error) {
	if iter.called {
		return nil, io.EOF
	} else {
		iter.called = true
		return &partition{}, nil
	}
}

func (iter *singlePartitionIter) Close(ctx *sql.Context) error { return nil }

type partition struct{}

func (p *partition) Key() []byte { return []byte("single-partition-key") }
