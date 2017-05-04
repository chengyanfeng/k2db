package controller

import (
	. "datahunter.cn/util"
	"github.com/astaxie/beego"
	. "k2db/def"
	"os"
	"runtime"
)

type ApiController struct {
	BaseController
}

// localhost:8080/api/ok
func (this *ApiController) Ok() {
	args := os.Args
	file := args[0]
	info, _ := os.Stat(file)
	p := P{"port": beego.BConfig.Listen.HTTPPort,
		"form":   this.FormToP(),
		"args":   args,
		"yourip": this.Ctx.Input.IP(),
		"cwd":    Cwd(),
		"uptime": GetCronStr(int((Timestamp() - UPTIME) / 1000)),
		"os":     runtime.GOOS,
		"arch":   runtime.GOARCH,
		"host":   this.Hostname(),
		"ct":     info.ModTime().Format("2006/01/02 15:04:05"),
		"ts":     Timestamp()}
	this.EchoJsonMsg(p)
}
