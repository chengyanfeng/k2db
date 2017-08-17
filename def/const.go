package def

import (
	"github.com/Shopify/sarama"
	"github.com/jinzhu/gorm"
	"github.com/nanobox-io/golang-scribble"
	"github.com/orcaman/concurrent-map"
	"gopkg.in/robfig/cron.v2"
	"time"
)

var Cron *cron.Cron
var Cmap cmap.ConcurrentMap = cmap.New()
var LocalDb *scribble.Driver
var KafkaConsumer sarama.Consumer
var Stream *gorm.DB
var Stream1  *gorm.DB
var Citus *gorm.DB
var UPTIME = time.Now().UnixNano() / int64(time.Millisecond)
var URL string
var NATS_PARA string
var CITUS_PARA string
var SUBJ string
var CSVPATH string
var TABLE string
const (
	Cname string = "job"
)

const (
	GENERAL_ERR int = 400
)
