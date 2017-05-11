package controller

import (
	. "k2db/util"
	//. "github.com/Shopify/sarama"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"testing"
	"time"
)

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
