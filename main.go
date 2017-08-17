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
	"github.com/nats-io/go-nats"
	"github.com/jinzhu/gorm"
	"io/ioutil"
	"fmt"
)

func main() {
	initConf()
	beego.BConfig.Listen.HTTPPort = ToInt(Trim(os.Getenv("port")), 7877)
	beego.BConfig.RecoverPanic = true
	beego.BConfig.EnableErrorsShow = true
	beego.AutoRouter(&ApiController{})
	beego.AutoRouter(&WsController{})
	//beego.SetLogger("file", `{"filename":"logs/run.log"}`)
	//beego.BeeLogger.SetLogFuncCallDepth(4)
	LocalDb, _ = scribble.New("log", nil)
	var err error
	// todo 配置通过文件读取
	Citus, err = gorm.Open("postgres", CITUS_PARA)
	defer Citus.Close()
	if err != nil {
		panic(err)
	}
	go Natscn()
	beego.Run()
}
func initConf(){
	conf := new(Conf)
	confMap := conf.InitConfig("./","k2db.ini","k2db")
	NATS_PARA = confMap["nats_para"]
	CITUS_PARA = confMap["citus_para"]
	SUBJ = confMap["subj"]
	CSVPATH =confMap["csvpath"]
	TABLE=confMap["table"]

}
func Natscn(){
	//server的连接
	nc, err1 := nats.Connect(NATS_PARA)
	if err1 != nil {
		log.Fatalf("Can't connect: %v\n", err1)
	}
	// 订阅的subject
	_, err :=nc.Subscribe(SUBJ, func(msg *nats.Msg){
		Debug("Received a message: %s\n", string(msg.Data))
		parser := LogParser1{}
		p := parser.Parse1(string(msg.Data))

	//	InsertDb(*p)
	//	go InsertDb(p)
		Dhq <- func() {
			 tocsv(p)
		}
	})

	if err != nil {
		nc.Close()
		log.Fatal(err)
	}
	nc.Flush()
	log.Printf("Listening on [%s]\n", SUBJ)
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
	Citus.Exec(`insert into flow (time_inter,userid,spid,flow_rate) values (?,?,?,?) `,
		v["time_inter"], v["userid"], v["spid"], v["flow_rate"])

}


func tocsv(v P){
	var wireteString string
	wireteString=v["time_inter"]+","+ v["userid"]+","+ v["spid"]+","+v["flow_rate"]+"\n"
	if len(wireteString)>1000000{
		var d1 = []byte(wireteString);
		err2 := ioutil.WriteFile(CSVPATH, d1, 0644)
		if err2==nil{
			fmt.Println("创建并文件输入内容")
			Csv2Db(CSVPATH)
		}
		err:= ioutil.WriteFile(CSVPATH,[]byte(""),0644)
		if err==nil{
			fmt.Println(wireteString)
			wireteString=""
			fmt.Println("清空文件")
		}
	}


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
func Csv2Db(file string) error {
	Debug("Csv2Db", file)
	pg := Postgre{}
	_, e := pg.LoadCsv(file, "access", ",")
	return e
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
