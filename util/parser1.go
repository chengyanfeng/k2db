package util

import (
	"strings"
)

type LogParser1 struct {
}

func (this *LogParser1) Parse1 (msg string) *P {
	tmp := strings.Split(msg, "|")
	seg := []interface{}{}
	for _, v := range tmp {
		seg = append(seg, v)
	}
	p := P{}
	if len(seg) != 5 {
		Error("Invalid msg", msg)
		return &p
	}
	if len(seg) == 5 {
		p["time_local"], _ = ToTime(ToString(seg[0]))
		p["spid"] = seg[1]
		p["pid"] = seg[2]
		p["dhbeat_hostname"] = seg[3]
		p["bw"] = seg[4]
	}
	return &p
}