package controller

import (
	"fmt"
	"github.com/astaxie/beego"
	"gopkg.in/mgo.v2/bson"
	. "k2dbAccess/def"
	. "k2dbAccess/util"
)

type BaseController struct {
	beego.Controller
}

func (this *BaseController) Echo(msg ...interface{}) {
	var out string = ""
	for _, v := range msg {
		out += fmt.Sprintf("%v", v)
	}
	this.Ctx.WriteString(out)
}

func (this *BaseController) EchoJson(m interface{}) {
	this.Data["json"] = m
	this.ServeJSON()
}

func (this *BaseController) EchoJsonMsg(msg interface{}) {
	this.Data["json"] = P{"code": 200, "msg": msg}
	this.ServeJSON()
}

func (this *BaseController) EchoJsonOk(msg ...interface{}) {
	if msg == nil {
		msg = []interface{}{"ok"}
	}
	this.Data["json"] = P{"code": 200, "msg": msg[0]}
	this.ServeJSON()
}

func (this *BaseController) EchoJsonErr(msg ...interface{}) {
	out := ""
	if msg != nil {
		for _, v := range msg {
			out = JoinStr(out, v)
		}
	}
	this.Data["json"] = P{"code": GENERAL_ERR, "msg": out}
	this.ServeJSON()
}

// 把form表单内容赋予P结构体
func (this *BaseController) FormToP(keys ...string) (p P) {
	p = P{}
	r := this.Ctx.Request
	r.ParseForm()
	for k, v := range r.Form {
		if len(keys) > 0 {
			if InArray(k, keys) {
				setKv(p, k, v)
			}
		} else {
			setKv(p, k, v)
		}
	}
	delete(p, "auth")
	return
}

func setKv(p P, k string, v []string) {
	if len(v) == 1 {
		if len(v[0]) > 0 {
			p[k] = v[0]
		}
	} else {
		p[k] = v
	}
}

func (this *BaseController) QueryParam(args ...interface{}) *P {
	p := P{}
	for _, v := range args {
		switch v.(type) {
		case string:
			k := v.(string)
			if this.GetString(k) != "" {
				p[k] = this.GetString(k)
			}
		case P:
			// todo
		}
	}
	return &p
}

func (this *BaseController) PageParam() (start int, rows int) {
	page, _ := this.GetInt("page", 1)
	if page < 1 {
		page = 1
	}
	rows, _ = this.GetInt("rows", 10)
	start = (page - 1) * rows
	return
}

func (this *BaseController) GetOid(str string) (oid bson.ObjectId) {
	oid = ToOid(this.GetString(str))
	return
}

func (this *BaseController) Hostname() string {
	return this.Ctx.Request.Host
}
