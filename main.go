package main

import (
	"github.com/astaxie/beego"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	. "k2db/controller"
	. "k2db/def"
	. "k2db/util"
	"log"

	"github.com/nats-io/go-nats"
	"os"
	"runtime"
	"sync/atomic"
	"time"
)

var count = int64(0)

func main() {
	initConf()
	beego.BConfig.Listen.HTTPPort = ToInt(Trim(os.Getenv("port")), 7877)
	beego.BConfig.RecoverPanic = true
	beego.BConfig.EnableErrorsShow = true
	beego.AutoRouter(&ApiController{})
	beego.AutoRouter(&WsController{})
	go Natscn()
	go AutoCsv2Db()
	beego.Run()
}
func initConf() {
	conf := new(Conf)
	confMap := conf.InitConfig("./", "k2db.ini", "k2db")
	NATS_PARA = confMap["nats_para"]
	CITUS_PARA = confMap["citus_para"]
	SUBJ = confMap["subj"]
	CSVPATH = confMap["csvpath"]
	TABLE = confMap["table"]

}
func Natscn() {
	//server的连接
	nc, err1 := nats.Connect(NATS_PARA)
	if err1 != nil {
		log.Fatalf("Can't connect: %v\n", err1)
	}
	// 订阅的subject
	_, err := nc.Subscribe(SUBJ, func(msg *nats.Msg) {
		Debug("Received a message: %s\n", string(msg.Data))
		Consume(msg)
	})

	if err != nil {
		nc.Close()
		log.Fatal(err)
	}
	nc.Flush()
	Debug("Subscribe", SUBJ)
	//保持连接
	runtime.Goexit()
}

func Consume(msg *nats.Msg) {
	atomic.AddInt64(&count, 1)
	line := string(msg.Data)
	line = Replace(line, []string{"|"}, ",")
	Aggr.Add(line)
}

func AutoCsv2Db() {
	for {
		data := Aggr.Dump()
		if !IsEmpty(data) {
			WriteFile(CsvFile, []byte(data))
			err := Csv2Db(CsvFile)
			if err != nil {
				Error(err)
			}
		}
		Debug("count", count)
		time.Sleep(10 * time.Second)
	}
}

func Csv2Db(file string) error {
	Debug("Csv2Db", file)
	pg := Postgre{}
	_, e := pg.LoadCsv(file, "flow", ",")
	return e
}
