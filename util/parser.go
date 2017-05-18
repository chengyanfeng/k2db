package util

import (
	"net/url"
	"strings"
	"time"
)

type LogParser struct {
}

func (this *LogParser) Parse(msg string) *P {
	// todo: 解析msg并入库，需要考虑数据计算，如带宽、开始时间，需要考虑多值返回，用于多stream入库
	tmp := strings.Split(msg, " ")
	seg := []interface{}{}
	token := ""
	wait := false
	for _, v := range tmp {
		if StartsWith(v, `"`) || StartsWith(v, `[`) {
			if EndsWith(v, `"`) {
				token = Replace(v, []string{`"`}, "")
				seg = append(seg, token)
			} else {
				wait = true
				token = v[1:]
			}

		} else {
			if wait {
				if EndsWith(v, `"`) || EndsWith(v, `]`) {
					token = JoinStr(token, " ", v)
					wait = false
					token = token[0 : len(token)-1]
					seg = append(seg, token)
					token = ""
				} else {
					token = JoinStr(token, " ", v)
				}
			} else {
				seg = append(seg, v)
			}
		}

	}
	//Debug(len(seg), JsonEncode(seg))
	p := P{}
	if len(seg) != 14 {
		Error("Invalid msg", msg)
		return &p
	}
	p["ct"], _ = ToTime(ToString(seg[0])) // 服务器时间戳
	p["dur"] = ToFloat(seg[1])            // 会话持续时间
	p["client"] = seg[2]                  // 客户端IP
	p["code"] = seg[3]                    // HTTP返回值
	p["auth"] = seg[4]                    // 鉴权成功失败
	p["up"] = seg[5]                      // 上行字节
	p["down"] = seg[6]                    // 下行字节
	p["method"] = seg[7]                  // 请求方法（GET POST）
	p["url"] = seg[8]                     // URL
	p["refer"] = seg[9]                   // httpreferer
	p["ua"] = seg[10]                     // useragent
	p["hit"] = seg[11]                    // 命中
	p["rrange"] = seg[12]                 // 请求range
	p["frange"] = seg[13]                 // 返回range
	this.ParseUrl(p)
	this.ParseBandwidth(p)
	return &p
}

// 解析url
func (this *LogParser) ParseUrl(p P) {
	u, err := url.Parse(ToString(p["url"]))
	if err != nil {
		Error(err)
		return
	}
	m, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		Error(err)
		return
	}
	defer func() {
		if err := recover(); err != nil {
			Error("ParseUrl", err, m)
		}
	}()
	p["uri"] = u.Path                //
	p["vkey"] = m["vkey"][0]         //
	p["locid"] = m["locid"][0]       //
	p["size"] = m["size"][0]         //
	p["userid"] = m["userid"][0]     //
	p["ocid"] = m["ocid"][0]         //
	p["userip"] = m["userip"][0]     //
	p["spid"] = m["spid"][0]         //
	p["pid"] = m["pid"][0]           //
	p["portalid"] = m["portalid"][0] //
	p["spip"] = m["spip"][0]         //
	p["spport"] = m["spport"][0]     //
	p["sdtfrom"] = m["sdtfrom"][0]   //
	p["tradeid"] = m["tradeid"][0]   //
	p["lsttm"] = m["lsttm"][0]       //
	delete(p, "url")
}

func (this *LogParser) ParseBandwidth(p P) {
	// todo
	ct := p["ct"].(time.Time)
	dur := p["dur"].(float64)
	st := ct.Add(time.Duration(1 * time.Second))
	p["st"] = st
	bw := ToFloat(p["down"]) / dur
	p["bw"] = bw
}
