package datatypes

// Taken and adapted from https://github.com/go-gorm/datatypes/blob/986ef5926aae40c832854e2a8fe158fd641bfff4/json.go

import (
	"encoding/json"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// JSONQueryExpression json query expression, implements clause.Expression interface to use as querier
type JSONQueryExpression struct {
	column string
	path   []string
	exists bool
}

// JSONQuery query column as json
func JSONQuery(column string) JSONQueryExpression {
	return JSONQueryExpression{column: column}
}

func (jsonQuery JSONQueryExpression) Value(path ...string) JSONQueryExpression {
	jsonQuery.path = append(jsonQuery.path, path...)
	return jsonQuery
}

func (jsonQuery JSONQueryExpression) Exists() JSONQueryExpression {
	jsonQuery.exists = true
	return jsonQuery
}

// Build implements clause.Expression
func (jsonQuery JSONQueryExpression) Build(builder clause.Builder) {
	stmt, ok := builder.(*gorm.Statement)
	if !ok {
		return
	}
	switch stmt.Dialector.Name() {
	case "mysql", "sqlite":
		if jsonQuery.exists {
			builder.WriteString("JSON_EXISTS(")
		} else {
			builder.WriteString("JSON_EXTRACT(")
		}
		builder.WriteQuoted(jsonQuery.column)
		builder.WriteByte(',')
		builder.AddVar(stmt, jsonQueryJoin(jsonQuery.path))
		builder.WriteString(")")
	case "postgres":
		if jsonQuery.exists {
			builder.WriteString("jsonb_exists(")
		} else {
			builder.WriteString("jsonb_extract_path_text(")
		}
		builder.WriteQuoted(jsonQuery.column)
		for _, p := range jsonQuery.path {
			builder.WriteByte(',')
			builder.AddVar(stmt, p)
		}
		builder.WriteString(")")
	}
}

const prefix = "$."

func jsonQueryJoin(keys []string) string {
	if len(keys) == 1 {
		return prefix + keys[0]
	}

	n := len(prefix)
	n += len(keys) - 1
	for i := 0; i < len(keys); i++ {
		n += len(keys[i])
	}

	var b strings.Builder
	b.Grow(n)
	b.WriteString(prefix)
	b.WriteString(encode(keys[0]))
	for _, key := range keys[1:] {
		b.WriteString(".")
		b.WriteString(encode(key))
	}
	return b.String()
}

func encode(str string) string {
	d, _ := json.Marshal(str)
	return string(d)
}
