package util

type LogParser struct {
}

func (this *LogParser) Parse(msg string) *P {
	// todo: 解析msg并入库，需要考虑数据计算，如带宽、开始时间，需要考虑多值返回，用于多stream入库
	p := P{}
	p["msg"] = msg
	return &p
}
