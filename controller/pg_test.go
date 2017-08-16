package controller

import (
	. "k2db/util"
	//. "github.com/Shopify/sarama"
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	. "k2db/def"
	"k2db/task"
	"testing"
	"time"
)

type User struct {
	Id    int
	Name  string
	Birth time.Time
	Price float32
}

func init() {
	var err error
	Stream, err = gorm.Open("postgres", "host=localhost user=altman dbname=postgres sslmode=disable password=")
	//defer Stream.Close()
	Error(err)
}

func Test_Pg(t *testing.T) {
	db, err := gorm.Open("postgres", "host=localhost user=dh dbname=dh sslmode=disable password=")
	db.SingularTable(true)
	defer db.Close()
	Error(err)

	db.Exec(`drop stream s_demo`)
	db.Exec(`drop continous view v_demo`)
	db.Exec(`create stream s_demo (id int, name text, birth timestamp, price float)`)
	db.Exec(`create continuous view v_demo as select count(*),birth from s_demo group by birth`)

	for i := 0; i < 10; i++ {
		name := JoinStr("name", i)
		db.Exec(`insert into s_demo values (?,?,?,?)`, i, name, time.Now(), float32(i))
	}

}

func BenchmarkPipeline(b *testing.B) {
	for i := 0; i < b.N; i++ {
		name := JoinStr("name", i)
		task.Dhq <- func() {
			Stream.Exec(`insert into s_demo values (?,?,?,?)`, i, name, time.Now(), float32(i))
		}
	}
}

func Benchmark_Insert(b *testing.B) {
	for i := 0; i < b.N; i++ {
		v := ""
		name := JoinStr("name", i)
		for j := 0; j < 10; j++ {
			v = JoinStr(v, fmt.Sprintf("('%v','%v','%v','%v'),", i, name, DateTimeStr(), i))
			Debug(v)
		}
		v = v[0 : len(v)-1]
		sql := fmt.Sprintf("insert into t_1 values %v", v)
		//Debug(sql)
		Stream.Exec(sql)
	}
}

func Benchmark_Insert2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tx := Stream.Begin()
		for j := 0; j < 100; j++ {
			name := JoinStr("name", i)
			tx.Exec(`insert into t_1 values (?,?,?,?)`, i, name, time.Now(), float32(i))
		}
		tx.Commit()
	}
}

func Test_Insert(t *testing.T) {
	i := 0
	name := JoinStr("name", i)
	Stream.Begin()
	Stream.Exec(`insert into t_1 values (?,?,?,?)`, i, name, time.Now(), float32(i))
	Stream.Commit()
}

func Test_Pg2(t *testing.T) {
	var err error
	Citus, err = gorm.Open("postgres", "host=citus user=postgres dbname=postgres sslmode=disable password=")
	defer Citus.Close()
	Error(err)

	//Citus.Exec(`drop table t_test`)
	//Citus.Exec(`create table t_test (id int, name text, birth timestamp, price float)`)

	for i := 0; i < 3; i++ {
		name := JoinStr("name", i)
		Citus.Exec(`insert into t_test values (?,?)`, i, name)
	}

	results, err := Sql("select * from t_test")
	Debug(JsonEncode(results))

}
