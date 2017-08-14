package util

import (
	. "k2dbAccess/def"
	"reflect"
)

func Sql(sql string) (*[]P, error) {
	var err error
	rows, err := Citus.Raw(sql).Rows()
	results := []P{}
	if err != nil {
		Error(err)
		return &results, err
	}
	cols, err := rows.Columns()
	for rows.Next() {
		var row = make([]interface{}, len(cols))
		var rowp = make([]interface{}, len(cols))
		for i := 0; i < len(cols); i++ {
			rowp[i] = &row[i]
		}

		rows.Scan(rowp...)

		rowMap := P{}
		for i, col := range cols {
			switch reflect.TypeOf(row[i]).Kind() {
			case reflect.Slice:
				row[i] = string(row[i].([]byte))
			}
			rowMap[col] = row[i]
		}

		results = append(results, rowMap)
	}
	return &results, err
}
