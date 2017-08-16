package util

import (
	"strings"
)

type LogParser1 struct {
}

func (this *LogParser1) Parse1 (msg string) *P {
	//17665451650|21130|31.698|2017-07-29 23:00:00|15|157.255.41.144|null|null
	tmp := strings.Split(msg, "|")
	seg := []interface{}{}
	for _, v := range tmp {
		seg = append(seg, v)
	}
	p := P{}
	if len(seg) != 8 {
		Error("Invalid msg", msg)
		return &p
	}
	if len(seg) == 8 {
	//	str :=strings.Split(ToString(seg[0]),"+")
	//	t0 := str[0]
		p["userid"] = seg[0]
		p["spid"] = seg[1]
		p["flow_rate"] = seg[2]
		p["time_inter"] = seg[3]
	//	p["total"] = seg[4]
	//	p["host_ip"] = seg[5]
	//	p["video_name"] = seg[6]
	//	p["wifi"] = seg[7]
	}
	return &p
}