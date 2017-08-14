package util

import (
	"strings"
)

type LogParser1 struct {
}

func (this *LogParser1) Parse1 (msg string) *P {
	//2017-08-11 13:30:00|17631322508|2017-08-11 13:30:22|21130|111.202.83.63:809|0|328
	tmp := strings.Split(msg, "|")
	seg := []interface{}{}
	for _, v := range tmp {
		seg = append(seg, v)
	}
	p := P{}
	if len(seg) != 7 {
		Error("Invalid msg", msg)
		return &p
	}
	if len(seg) == 7 {
	//	str :=strings.Split(ToString(seg[0]),"+")
	//	t0 := str[0]
		p["time_local"] = seg[0]
		p["userid"] = seg[1]
		p["start_time"] = seg[2]
		p["spid"] = seg[3]
		p["tohost"] = seg[4]
		p["err_code"] = seg[5]
		p["times"] = seg[6]
	}
	return &p
}