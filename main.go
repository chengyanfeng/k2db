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
)

func main() {
	beego.BConfig.Listen.HTTPPort = ToInt(Trim(os.Getenv("port")), 7878)
	beego.BConfig.RecoverPanic = true
	beego.BConfig.EnableErrorsShow = true
	beego.AutoRouter(&ApiController{})
	//beego.SetLogger("file", `{"filename":"logs/run.log"}`)
	//beego.BeeLogger.SetLogFuncCallDepth(4)
	LocalDb, _ = scribble.New("log", nil)
	var err error
	// todo 配置通过文件读取
	Stream, err = gorm.Open("postgres", "host=localhost user=dh dbname=dh sslmode=disable password=")
	defer Stream.Close()
	if err != nil {
		panic(err)
	}
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
	topic := "log_topic"
	offset := LoadOffset(topic) + 1
	if offset < 2 {
		offset = OffsetNewest
	}
	// todo 要考虑分区消费，便于并行处理
	logConsumer, err := consumer.ConsumePartition(topic, 0, offset)
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
			// todo 考虑增加任务队列，提高消费速度
			ProcMsg(msg)
			consumed++
		case <-signals:
			break ConsumerLoop
		}
	}

	Debug("Stop consume: %d\n", consumed)
}

func ProcMsg(msg *ConsumerMessage) {
	parser := LogParser{}
	t := string(msg.Value)
	p := parser.Parse(t)
	Debug("Consumed ", msg.Offset, string(msg.Value))
	err := InsertDb(p)
	if err != nil {
		Error(err)
	} else {
		SaveOffset(msg.Topic, msg.Offset)
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
	Debug("LoadOffset", i)
	return i
}

func SaveOffset(topic string, offset int64) {
	// todo 优化存储方式，按照每隔1s定期记录
	if offset%10000 == 0 {
		LocalDb.Write(topic, "offset", offset)
		Debug("SaveOffset", offset)
	}
}
