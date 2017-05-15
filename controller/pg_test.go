package controller

import (
	. "k2db/util"
	//. "github.com/Shopify/sarama"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"reflect"
	"testing"
	"time"
)

type User struct {
	Id    int
	Name  string
	Birth time.Time
	Price float32
}

func Test_Pg(t *testing.T) {
	db, err := gorm.Open("postgres", "host=localhost user=dh dbname=dh sslmode=disable password=")
	db.SingularTable(true)
	defer db.Close()
	Error(err)

	db.Exec(`drop stream s_demo`)
	db.Exec(`create stream s_demo (id int, name text, birth timestamp, price float)`)
	db.Exec(`create continuous view v_demo as select count(*),birth from s_demo group by birth`)

	for i := 0; i < 10; i++ {
		name := JoinStr("name", i)
		db.Exec(`insert into s_demo values (?,?,?,?)`, i, name, time.Now(), float32(i))
	}

}

func Test_Pg2(t *testing.T) {
	db, err := gorm.Open("postgres", "host=localhost user=dh dbname=dh sslmode=disable password=")
	defer db.Close()
	Error(err)

	db.Exec(`drop table t_test`)
	db.Exec(`create table t_test (id int, name text, birth timestamp, price float)`)

	for i := 0; i < 3; i++ {
		name := JoinStr("name", i)
		db.Exec(`insert into t_test values (?,?,?,?)`, i, name, time.Now(), float32(i))
	}

	//p := P{}
	//db.Raw(`select * from t_test`).Scan(&p)
	rows, e := db.Raw(`select * from t_test`).Rows()
	if e != nil {
		Error(e)
		return
	}
	results := []P{}
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
	Debug(JsonEncode(results))

}
