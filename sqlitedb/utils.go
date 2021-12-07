package sqlitedb

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"text/template"

	"crawshaw.io/sqlite"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

func inferType(sqliteType string) sql.Type {
	t := strings.ToLower(sqliteType)
	switch {
	case strings.Contains(t, "int"):
		return sql.Int64
	case strings.Contains(t, "varchar"), strings.Contains(t, "text"):
		return sql.Text
	default:
		return sql.Text
	}
}

func toSQLiteType(t query.Type) sqlite.ColumnType {
	switch t {
	case query.Type_NULL_TYPE:
		return sqlite.SQLITE_NULL
	case query.Type_INT8, query.Type_UINT8,
		query.Type_INT16, query.Type_UINT16,
		query.Type_INT24, query.Type_UINT24,
		query.Type_INT32, query.Type_UINT32,
		query.Type_INT64, query.Type_UINT64:
		return sqlite.SQLITE_INTEGER
	case query.Type_FLOAT32, query.Type_FLOAT64, query.Type_DECIMAL:
		return sqlite.SQLITE_FLOAT
	case query.Type_BINARY, query.Type_BIT:
		return sqlite.SQLITE_BLOB
	default:
		return sqlite.SQLITE_TEXT
	}
}

func createTableSQL(tableName string, schema sql.Schema) (string, error) {
	const declare = `CREATE TABLE {{ .TableName }} (
		{{- range $c, $col := .Columns }}
			{{ quote .Name }} {{ colType $c }}{{ if columnComma $c }},{{ end }}
		{{- end }}
	  )`

	// helper to determine whether we're on the last column (and therefore should avoid a comma ",") in the range
	fns := template.FuncMap{
		"columnComma": func(c int) bool {
			return c < len(schema)-1
		},
		"colType": func(colIndex int) string {
			return toSQLiteType(schema[colIndex].Type.Type()).String()
		},
		"quote": strconv.Quote,
	}

	tmpl, err := template.New(fmt.Sprintf("declare_table_func_%s", tableName)).Funcs(fns).Parse(declare)
	if err != nil {
		return "", err
	}

	buf := new(bytes.Buffer)
	err = tmpl.Execute(buf, struct {
		TableName string
		Columns   sql.Schema
	}{
		tableName,
		schema,
	})
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}
