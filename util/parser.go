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
	p["time_local"], _ = ToTime(ToString(seg[0])) // 本地时间
	p["request_time"] = ToFloat(seg[1])            // 服务时间
	p["remote_addr"] = seg[2]                  // 远端地址（客户端地址）
	p["status"] = seg[3]                    // HTTP回复状态码
	p["err_code"] = seg[4]                    // 错误码
	p["request_length"] = seg[5]                      // 请求长度
	p["bytes_sent"] = seg[6]                    // 发送长度
	p["request_method"] = seg[7]                  // 请求方式
	p["url"] = seg[8]                     // 完整请求链接
	p["http_referer"] = seg[9]                   // HTTP_REFERER
	p["http_user_agent"] = seg[10]                     // USERAGENT
	p["cache_status"] = seg[11]                    // 缓存状态（MISS HIT IOTHROUGH）
	p["http_range"] = seg[12]                 // 请求range
	p["sent_http_content_range"] = seg[13]                 // 发送range
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
	p["userip"] = m["userip"][0]         //
	p["spid"] = m["spid"][0]       //
	p["pid"] = m["pid"][0]         //
	p["spport"] = m["spport"][0]     //
	p["lsttm"] = m["lsttm"][0]         //
	p["vkey"] = m["vkey"][0]     //
	p["userid"] = m["userid"][0]         //
	p["portalid"] = m["portalid"][0]           //
	p["spip"] = m["spip"][0] //
	p["sdtfrom"] = m["sdtfrom"][0]         //
	p["tradeid"] = m["tradeid"][0]     //
	p["enkey"] = m["enkey"][0]   //
	delete(p, "url")
}

func (this *LogParser) ParseBandwidth(p P) {
	// todo
	ct := p["time_local"].(time.Time)
	dur := p["request_time"].(float64)
	st := ct.Add(time.Duration(dur) * time.Second)
	p["st"] = st
	bw := ToFloat(p["bytes_sent"]) * 8 / dur
	p["bw"] = bw
}
