package controller

import (
	. "k2db/util"
	//. "github.com/Shopify/sarama"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	. "k2db/def"
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
	Stream, err = gorm.Open("postgres", "host=localhost user=dh dbname=dh sslmode=disable password=")
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
		Stream.Exec(`insert into s_demo values (?,?,?,?)`, i, name, time.Now(), float32(i))
	}
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
