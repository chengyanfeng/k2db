package main

import (
	. "github.com/Shopify/sarama"
	"github.com/astaxie/beego"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/nanobox-io/golang-scribble"
	. "k2db/controller"
	. "k2db/def"
	. "k2db/task"
	. "k2db/util"
	"log"
	"os"
	"os/signal"
	"time"
	"encoding/json"
	"runtime"
	"fmt"
	"github.com/nats-io/go-nats"
	"github.com/jinzhu/gorm"
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
	//// todo 配置通过文件读取
	Stream, err = gorm.Open("postgres", "host=pipeline1 user=haproxy dbname=haproxy sslmode=disable password=haproxy123456")
	defer Stream.Close()
	if err != nil {
		panic(err)
	}
	Stream1, err = gorm.Open("postgres", "host=pipeline2 user=haproxy dbname=haproxy sslmode=disable password=haproxy123456")
	defer Stream1.Close()
	if err != nil {
		panic(err)
	}
	Citus, err = gorm.Open("postgres", "host=citus1 user=postgres dbname=postgres sslmode=disable password=")
	defer Citus.Close()
	if err != nil {
		panic(err)
	}
	//initConsumer()
	//defer func() {
	//	if err := KafkaConsumer.Close(); err != nil {
	//		log.Fatalln(err)
	//	}
	//}()
	//go consume("log_topic", ProcLog)
	go Natscn()
	//go consume("ws_topic", ProcWs)
	beego.Run()
}

func Natscn(){
	//server的地址
	//var urls = flag.String("s", "nats://111.206.135.105:9092,nats://111.206.135.106:9092,nats://111.206.135.107:9092", "The nats server URLs (separated by comma)")
	//nc, err := nats.Connect(*urls)

	//stan.Connect(clusterID, clientID, ops ...Option)
	//ns, _ := stan.Connect("my_cluster", "stan1", stan.NatsURL("nats://111.206.135.107:9092"))

	nc, err := nats.Connect("nats://172.16.102.133:9092,nats://172.16.102.134:9092,nats://172.16.102.135:9092")
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}
	// 订阅的subject
	subj := "log"

	// 订阅主题, 当收到subject时候执行后面的func函数
	// 返回值sub是subscription的实例
	// Simple Async Subscriber
	nc.Subscribe(subj, func(msg *nats.Msg){
		fmt.Printf("Received a message: %s\n", string(msg.Data))
		parser := LogParser{}
		fmt.Println(string(msg.Data))
		p := parser.Parse(string(msg.Data))
		fmt.Println(p)
		err1 := InsertDb(p)
		if err1 != nil {
			Error(err1)
		}
	})

	log.Printf("Listening on [%s]\n", subj)
	//保持连接
	runtime.Goexit()
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
		Error(err)
		logConsumer, err = KafkaConsumer.ConsumePartition(topic, 0, OffsetNewest)
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
			Dhq <- func() {
				f(msg)
			}
		case <-signals:
			break ConsumerLoop
		}
	}
}

func initConsumer() {
	var err error
	KafkaConsumer, err = NewConsumer([]string{"kafka1:9092","kafka2:9092","kafka3:9092"}, nil)
	if err != nil {
		panic(err)
	}
}

func ProcLog(msg *ConsumerMessage) {
	parser := LogParser{}
	t := string(msg.Value)
	//由于t为json格式，下面处理json转换成map，取出message对应的值，然后进行解析
	msg1 := []byte(t)
	dat := make(map[string]interface{})
	if err := json.Unmarshal(msg1, &dat); err != nil {
		panic(err)
	}
	//filebeat产生的信息
	beat :=dat["beat"].(map[string]interface {})
	//将日志和filebeat所在机器的主机名拼接
	msg2 := dat["message"].(string) + " " + beat["hostname"].(string)

	p := parser.Parse(msg2)

	//Debug("Consumed ", msg.Offset, string(msg.Value))
	err := InsertDb(p)
	if err != nil {
		Error(err)
	} else {
		Cmap.Set(msg.Topic, msg.Offset)
	}
}

func InsertDb(p *P) (e error) {
	v := *p

	//todo
	//Debug(v)
	defer func() {
		if r := recover(); r != nil {
			//log.Println("pipelinedb1", r)
			return
		}
	}()
	//往pipeline1中插数据
	Stream.Exec(`insert into s_log (time_local,request_time,remote_addr,status,err_code,request_length,bytes_sent,request_method,http_referer,http_user_agent,cache_status,dhbeat_hostname,userip,spid,pid,spport,userid,portalid,spip,st,bw) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		v["time_local"],v["request_time"],v["remote_addr"],v["status"],v["err_code"],v["request_length"],v["bytes_sent"],v["request_method"],v["http_referer"],v["http_user_agent"],v["cache_status"],v["dhbeat_hostname"],v["userip"],v["spid"],v["pid"],v["spport"],v["userid"],v["portalid"],v["spip"],v["st"],v["bw"])

	defer func() {
		if r := recover(); r != nil {
			//log.Println("pipelinedb2", r)
			return
		}
	}()
	//往pipeline2中插数据
	Stream1.Exec(`insert into s_log (time_local,request_time,remote_addr,status,err_code,request_length,bytes_sent,request_method,http_referer,http_user_agent,cache_status,dhbeat_hostname,userip,spid,pid,spport,userid,portalid,spip,st,bw) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		v["time_local"],v["request_time"],v["remote_addr"],v["status"],v["err_code"],v["request_length"],v["bytes_sent"],v["request_method"],v["http_referer"],v["http_user_agent"],v["cache_status"],v["dhbeat_hostname"],v["userip"],v["spid"],v["pid"],v["spport"],v["userid"],v["portalid"],v["spip"],v["st"],v["bw"])

	defer func() {
		if r := recover(); r != nil {
			//log.Println("citus", r)
			return
		}
	}()
	e = Citus.Exec(`insert into u_log (time_local,http_user_agent,cache_status,dhbeat_hostname,userip,spid,pid,userid,spip,bytes_sent) values (?,?,?,?,?,?,?,?,?,?) on conflict(time_local,http_user_agent,cache_status,dhbeat_hostname,userip,spid,pid,userid,spip) do update set bytes_sent = u_log.bytes_sent + EXCLUDED.bytes_sent`,
		v["time_local"], v["http_user_agent"], v["cache_status"], v["dhbeat_hostname"], v["userip"], v["spid"], v["pid"], v["userid"], v["spip"], v["bytes_sent"]).Error
	return
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
		if offset != old {
			SaveOffset(topic, offset)
		}
	}
}
