package util

import . "datahunter.cn/util"

type LogParser struct {
}

func (this *LogParser) Parse(msg string) *P {
	// todo: parse msg and insert into pg
	p := P{}
	p["msg"] = msg
	return &p
}
