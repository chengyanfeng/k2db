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
		go dump2db(data)
		Debug("count", count)
		time.Sleep(1 * time.Second)
	}
}

func dump2db(data []string) {
	if !IsEmpty(data) {
		file := JoinStr("flow", Timestamp(), ".csv")
		for _, v := range data {
			Msg2Csv(file, v)
		}
		err := Csv2Db(file)
		if err != nil {
			Error(err)
		} else {
			DeleteFile(file)
		}
	}
}

func Msg2Csv(file string, msg string) {
	if !EndsWith(msg, "\n") {
		msg = msg + "\n"
	}
	AppendFile(file, msg)
}

func Csv2Db(file string) error {
	Debug("Csv2Db", file)
	pg := Postgre{}
	_, e := pg.LoadCsv(file, "flow", ",")
	return e
}
