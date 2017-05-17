package main

import (
	. "github.com/Shopify/sarama"
	"github.com/astaxie/beego"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/nanobox-io/golang-scribble"
	. "k2db/controller"
	. "k2db/def"
	. "k2db/util"
	"log"
	"os"
	"os/signal"
	"time"
)

func main() {
	beego.BConfig.Listen.HTTPPort = ToInt(Trim(os.Getenv("port")), 7878)
	beego.BConfig.RecoverPanic = true
	beego.BConfig.EnableErrorsShow = true
	beego.AutoRouter(&ApiController{})
	beego.AutoRouter(&WsController{})
	//beego.SetLogger("file", `{"filename":"logs/run.log"}`)
	//beego.BeeLogger.SetLogFuncCallDepth(4)
	LocalDb, _ = scribble.New("log", nil)
	var err error
	// todo 配置通过文件读取
	Stream, err = gorm.Open("postgres", "host=pipeline user=dh dbname=dh sslmode=disable password=")
	Citus, err = gorm.Open("postgres", "host=citus user=postgres dbname=postgres sslmode=disable password=")
	defer Stream.Close()
	if err != nil {
		panic(err)
	}
	initConsumer()
	defer func() {
		if err := KafkaConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()
	go consume("log_topic", ProcLog)
	go consume("ws_topic", ProcWs)
	beego.Run()
}

func consume(topic string, f func(msg *ConsumerMessage)) {
	go AutoSaveOffset(topic)
	offset := LoadOffset(topic) + 1
	if offset < 2 {
		offset = OffsetNewest
	}
	// todo 要考虑分区消费，便于并行处理
	logConsumer, err := KafkaConsumer.ConsumePartition(topic, 0, offset)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := logConsumer.Close(); err != nil {
			Error(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	Debug("Consuming", topic)
ConsumerLoop:
	for {
		select {
		case msg := <-logConsumer.Messages():
			// todo 考虑增加任务队列，提高消费速度
			f(msg)
		case <-signals:
			break ConsumerLoop
		}
	}
}

func initConsumer() {
	var err error
	KafkaConsumer, err = NewConsumer([]string{"kafka:9092"}, nil)
	if err != nil {
		panic(err)
	}
}

func ProcLog(msg *ConsumerMessage) {
	parser := LogParser{}
	t := string(msg.Value)
	p := parser.Parse(t)
	//Debug("Consumed ", msg.Offset, string(msg.Value))
	err := InsertDb(p)
	if err != nil {
		Error(err)
	} else {
		Cmap.Set(msg.Topic, msg.Offset)
	}
}

func InsertDb(p *P) error {
	v := *p
	return Stream.Exec(`insert into s_log (msg) values (?)`,
		v["msg"]).Error
}

func LoadOffset(topic string) int64 {
	i := int64(0)
	LocalDb.Read(topic, "offset", &i)
	//Debug("LoadOffset", i)
	return i
}

func SaveOffset(topic string, offset int64) {
	LocalDb.Write(topic, "offset", offset)
	Debug("SaveOffset", offset)
}

func AutoSaveOffset(topic string) {
	for {
		time.Sleep(time.Duration(1 * time.Second))
		old := LoadOffset(topic)
		tmp, _ := Cmap.Get(topic)
		offset := ToInt64(tmp)
		if offset > old {
			SaveOffset(topic, offset)
		}
	}
}
