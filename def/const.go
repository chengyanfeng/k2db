package def

import (
	"github.com/jinzhu/gorm"
	"github.com/nanobox-io/golang-scribble"
	"github.com/orcaman/concurrent-map"
	"gopkg.in/robfig/cron.v2"
	"time"
)

var Cron *cron.Cron
var Cmap cmap.ConcurrentMap = cmap.New()
var LocalDb *scribble.Driver

var Stream *gorm.DB
var UPTIME = time.Now().UnixNano() / int64(time.Millisecond)

const (
	Cname string = "job"
)

const (
	GENERAL_ERR int = 400
)
