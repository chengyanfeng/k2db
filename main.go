package main

import (
	. "datahunter.cn/util"
	. "k2db/controller"
	//. "k2db/def"
	//"encoding/json"
	. "github.com/Shopify/sarama"
	"github.com/astaxie/beego"
	//"github.com/nanobox-io/golang-scribble"
	//"gopkg.in/robfig/cron.v2"
	"log"
	"os"
	"os/signal"
)

func main() {
	beego.BConfig.Listen.HTTPPort = ToInt(Trim(os.Getenv("port")), 7878)
	beego.BConfig.RecoverPanic = true
	beego.BConfig.EnableErrorsShow = true
	beego.AutoRouter(&ApiController{})
	//beego.SetLogger("file", `{"filename":"logs/run.log"}`)
	//beego.BeeLogger.SetLogFuncCallDepth(4)
	//Db, _ = scribble.New("cron", nil)
	go loadJob()
	beego.Run()
}

func loadJob() {
	consumer, err := NewConsumer([]string{"kafka:9092"}, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()
	offset := LoadOffset()
	if offset < 1 {
		offset = OffsetNewest
	}
	logConsumer, err := consumer.ConsumePartition("log_topic", 0, offset)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := logConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumed := 0
	Debug("Consuming...")
ConsumerLoop:
	for {
		select {
		case msg := <-logConsumer.Messages():
			ProcMsg(msg)
			consumed++
		case <-signals:
			break ConsumerLoop
		}
	}

	Debug("Stop consume: %d\n", consumed)
}

func LoadOffset() int64 {
	// todo: load offset from local db
	return 0
}

func ProcMsg(msg *ConsumerMessage) {
	Debug("Consumed ", msg.Offset, string(msg.Value))
}
