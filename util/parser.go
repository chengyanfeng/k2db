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
	if len(seg) != 15 {
		Error("Invalid msg", msg)
		return &p
	}
	p["time_local_origin"], _ = ToTime(ToString(seg[0])) // 本地时间
	//s, _ := ToTime(ToString(seg[0]))
	p["time_local"] = p["time_local_origin"].(time.Time).Format("2006-01-02") //只要年月日
	//fmt.Println(strings.Split(s.Format("2006-01-02"),"-")[0])
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
	p["filebeat_hostname"] = seg[14]
	this.ParseUrl(p)
	this.ParseBandwidth(p)
	return &p
}

// 解析url
func (this *LogParser) ParseUrl(p P) {
	aes :=Aes{}
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
	p["uri"] = u.Path                //文件地址
	p["userip"] = m["userip"][0]         //客户ip
	p["spid"] = m["spid"][0]       //客户标识（产品）可暂时作为产品id
	p["pid"] = m["pid"][0]         //产品id
	p["spport"] = m["spport"][0]     //sp端口
	if strings.Count(string(m["userid"][0]),"")-1==32 {
		key :=[]byte("ac22273abb2f4960")
		userid,_ :=aes.CBCDecrypter(key, m["userid"][0])	//解密userid
		p["userid"] = strings.Split(userid, "\u0005")[0]       //用户id
	}else {
		p["userid"] = m["userid"][0]			//用户id
	}
	p["portalid"] = m["portalid"][0]           //门户id，用以归类客户
	p["spip"] = m["spip"][0] //服务商ip
	delete(p, "url")
}

func (this *LogParser) ParseBandwidth(p P) {
	// todo
	ct := p["time_local_origin"].(time.Time)
	dur := p["request_time"].(float64)
	st := ct.Add(-time.Duration(dur) * time.Second)
	p["st"] = st
	bw := ToFloat(p["bytes_sent"]) * 8 / dur
	p["bw"] = bw
	delete(p, "time_local_origin")
}
