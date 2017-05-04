package main

import (
	. "datahunter.cn/util"
	. "k2db/controller"
	. "k2db/def"
	//"encoding/json"
	"github.com/astaxie/beego"
	//"github.com/nanobox-io/golang-scribble"
	"github.com/orcaman/concurrent-map"
	"gopkg.in/robfig/cron.v2"
	"os"
)

func main() {
	beego.BConfig.Listen.HTTPPort = ToInt(Trim(os.Getenv("port")), 7878)
	beego.BConfig.RecoverPanic = true
	beego.BConfig.EnableErrorsShow = true
	beego.AutoRouter(&ApiController{})
	//beego.SetLogger("file", `{"filename":"logs/run.log"}`)
	//beego.BeeLogger.SetLogFuncCallDepth(4)
	Cmap = cmap.New()
	//Db, _ = scribble.New("cron", nil)
	Cron = cron.New()
	Cron.Start()
	defer Cron.Stop()
	go loadJob()
	beego.Run()
}

func loadJob() {
}
