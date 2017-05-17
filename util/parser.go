package util

import (
	"github.com/chanxuehong/time"
	"strings"
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
			wait = true
			token = v[1:]
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
	//Debug(JsonEncode(seg))
	p := P{}
	p["ct"], _ = ToTime(ToString(seg[0]))
	p["time"] = ToFloat(seg[1])
	p["client"] = seg[2]
	p["code"] = seg[3]
	return &p
}

func (this *LogParser) ParseTime(str string) time.Time {
	// todo
	return time.Now()
}
