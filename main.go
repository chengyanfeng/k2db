package main

import (
	. "github.com/Shopify/sarama"
	"github.com/astaxie/beego"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/nanobox-io/golang-scribble"
	. "k2dbAccess/controller"
	. "k2dbAccess/def"
	. "k2dbAccess/task"
	. "k2dbAccess/util"
	"log"
	"os"
	"os/signal"
	"time"
	"encoding/json"
	"runtime"
	"github.com/nats-io/go-nats"
	"fmt"
	"github.com/jinzhu/gorm"
)

func main() {
	beego.BConfig.Listen.HTTPPort = ToInt(Trim(os.Getenv("port")), 7876)
	beego.BConfig.RecoverPanic = true
	beego.BConfig.EnableErrorsShow = true
	beego.AutoRouter(&ApiController{})
	beego.AutoRouter(&WsController{})
	//beego.SetLogger("file", `{"filename":"logs/run.log"}`)
	//beego.BeeLogger.SetLogFuncCallDepth(4)
	LocalDb, _ = scribble.New("log", nil)
	var err error
	// todo 配置通过文件读取
	Citus, err = gorm.Open("postgres", "host=citus1 user=postgres dbname=postgres sslmode=disable password=")
	defer Citus.Close()
	if err != nil {
		panic(err)
	}
	go Natscn()
	beego.Run()
}

func Natscn(){
	//server的连接
	//nc, err1 := nats.Connect("nats://111.206.135.105:9092,nats://111.206.135.106:9092,nats://111.206.135.107:9092")

	//stan.Connect(clusterID, clientID, ops ...Option)
	//ns, err1 := stan.Connect("my_cluster", "myid", stan.NatsURL("nats://172.16.102.133:9092,nats://172.16.102.134:9092,nats://172.16.102.135:9092"))
	//ns, err1 := stan.Connect("my_cluster", "myiddd", stan.NatsURL("nats://111.206.135.107:9092"))

	//ns.Publish("log", []byte("Hello World!1"))
	nc, err1 := nats.Connect("nats://172.16.102.133:9092,nats://172.16.102.134:9092,nats://172.16.102.135:9092")
	if err1 != nil {
		log.Fatalf("Can't connect: %v\n", err1)
	}
	// 订阅的subject
	subj := "access"
	//subj, i := "access", 0
	//v1 :=""
	// 订阅主题, 当收到subject时候执行后面的func函数
	// 返回值sub是subscription的实例
	// Async Subscriber
	//_, err :=ns.QueueSubscribe(subj, "bar1",func(msg *stan.Msg){
	//	//fmt.Printf("Received a message: %s\n", string(msg.Data))
	//	parser := LogParser{}
	//	p := parser.Parse(string(msg.Data))
	//	Debug(p)
	//	//Stream.Exec(`insert into s_test (msg) values (?)`,
	//
	//		v := *p
	//		i++
	//		v1 = JoinStr(v1, fmt.Sprintf("('%v','%v','%v','%v','%v','%v','%v','%v','%v','%v','%v','%v','%v','%v','%v','%v','%v','%v','%v','%v','%v'),", v["time1"],v["request_time"],v["remote_addr"],v["status"],v["err_code"],v["request_length"],v["bytes_sent"],v["request_method"],v["http_referer"],v["http_user_agent"],v["cache_status"],v["dhbeat_hostname"],v["userip"],v["spid"],v["pid"],v["spport"],v["userid"],v["portalid"],v["spip"],v["st"],v["bw"]))
	//		//Debug(i)
	//		if i%100 == 0 {
	//			v1 = v1[0 : len(v1)-1]
	//			sql := fmt.Sprintf("insert into s_log (time_local,request_time,remote_addr,status,err_code,request_length,bytes_sent,request_method,http_referer,http_user_agent,cache_status,dhbeat_hostname,userip,spid,pid,spport,userid,portalid,spip,st,bw) values %v", v1)
	//			//Debug(sql)
	//			InsertDb(sql)
	//			v1 =""
	//		}
	//}, stan.DurableName("cdn1"))

	_, err :=nc.Subscribe(subj, func(msg *nats.Msg){
		fmt.Printf("Received a message: %s\n", string(msg.Data))
		parser := LogParser1{}
		p := parser.Parse1(string(msg.Data))

		//Debug(p)
		//fmt.Println(p)
		//v := *p
		//i++
		//v1 = JoinStr(v1, fmt.Sprintf("('%v','%v','%v','%v','%v','%v'),", v["userid"],v["tohost"],v["spid"],v["start_time"],v["err_code"],v["times"]))
		//Debug(i)
		//ss = append(ss, v1)
	//	if i%10 == 0 {
	//		v1 = v1[0 : len(v1)-1]
		//	v3 :=""
			//	v3 = ss[len(ss)-1]
		//	v3 = v3[0 : len(v3)-1]
	//		Debug(v1)
	//		sql := fmt.Sprintf("insert into access (userid,tohost,spid,start_time,err_code,times) values %v", v1)
	//		Debug(sql)
			InsertDb(*p)
			//ss = append(ss[:0], ss[len(ss):]...)
		//	v1 =""
	//	}
		//go InsertDb(p)
		//Dhq <- func() {
		//	 InsertDb(p)
		//}
	})

	if err != nil {
		nc.Close()
		log.Fatal(err)
	}
	nc.Flush()
	log.Printf("Listening on [%s]\n", subj)
	//保持连接
	runtime.Goexit()
}

func InsertDb(v P)  {
	//todo
	defer func() {
	if r := recover(); r != nil {
		log.Println("Citus", r)
		}
	}()
	Citus.Exec(`insert into access (userid,tohost,spid,start_time,err_code,times) values (?,?,?,?,?,?) `,
		v["userid"], v["tohost"], v["spid"], v["start_time"], v["err_code"], v["times"])

	//Citus.Exec(`insert into u_log (time_local,spid,pid,userid,bytes_sent) values (?,?,?,?,?) on conflict(time_local,spid,pid,userid) do update set bytes_sent = u_log.bytes_sent + EXCLUDED.bytes_sent`,
	//	v["time_local"], v["spid"], v["pid"], v["userid"], v["bytes_sent"])

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
	Debug(p)
	//Debug("Consumed ", msg.Offset, string(msg.Value))
	//err := InsertDb(p)
	//if err != nil {
	//	Error(err)
	//} else {
	//	Cmap.Set(msg.Topic, msg.Offset)
	//}
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
