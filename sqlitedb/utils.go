package sqlitedb

import (
	"bytes"
	"fmt"
	"strconv"
	"text/template"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
)

// mustInferType receives a MySQL type name, and resolves it to a sql.Type, or panics if it cannot.
// It's a little odd how it does it - it creates a dummy `CREATE TABLE` statement with a single column
// of the supplied type. It uses the vitess sql parser to turn the statement into an AST
// (which will be of a known structure) and type-asserts it into the DDL node to retrieve the sqlparser.ColumnType.
// This is allso so that we can use sql.ColumnTypeToType to resolve to a sql.Type
func mustInferType(typeName string) sql.Type {
	stmt, err := sqlparser.Parse(fmt.Sprintf("CREATE TABLE t (c %s);", typeName))
	if err != nil {
		panic(err)
	}

	parsed := stmt.(*sqlparser.DDL)
	if t, err := sql.ColumnTypeToType(&parsed.TableSpec.Columns[0].Type); err != nil {
		panic(err)
	} else {
		return t
	}
}

func createTableSQL(tableName string, schema sql.Schema) (string, error) {
	const declare = `CREATE TABLE {{ .TableName }} (
		{{- range $c, $col := .Columns }}
			{{ quote .Name }} {{ colType $c }}{{ if notNull $c }} NOT NULL{{ end }}{{ if columnComma $c }},{{ end }}
		{{- end }}
	  )`

	// helper to determine whether we're on the last column (and therefore should avoid a comma ",") in the range
	fns := template.FuncMap{
		"columnComma": func(c int) bool {
			return c < len(schema)-1
		},
		"colType": func(colIndex int) string {
			return schema[colIndex].Type.String()
		},
		"notNull": func(colIndex int) bool {
			return !schema[colIndex].Nullable
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
