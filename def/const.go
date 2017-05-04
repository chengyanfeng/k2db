package def

import (
	"github.com/nanobox-io/golang-scribble"
	"github.com/orcaman/concurrent-map"
	"gopkg.in/robfig/cron.v2"
	"time"
)

var Cron *cron.Cron
var Cmap cmap.ConcurrentMap
var Db *scribble.Driver
var UPTIME = time.Now().UnixNano() / int64(time.Millisecond)

const (
	Cname string = "job"
)

const (
	GENERAL_ERR int = 400
)
