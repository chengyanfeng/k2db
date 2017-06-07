package util

import (
	"testing"
	"encoding/json"
)

func TestLogParser_Parse(t *testing.T) {
	parser := LogParser{}
	msg := ReadFile("sample.txt")

	//msg := ""
	p := parser.Parse(msg)
	Debug(JsonEncode(p))
}

// 测试filebeat包装过的数据
func TestLogParser_Json(t *testing.T) {
	parser := LogParser{}
	msg := ReadFile("sample1.txt")
	//p := parser.Parser(msg)
	//Debug(JsonEncode(p))
	msg1 := []byte(msg)
	dat := make(map[string]interface{})
	if err := json.Unmarshal(msg1, &dat); err != nil {
		panic(err)
	}
	p := parser.Parse(dat["message"].(string))

	Debug(JsonEncode(p))
}

func BenchmarkLogParser_Parse(b *testing.B) {
	msg := ReadFile("sample.txt")
	for i := 0; i < b.N; i++ {
		parser := LogParser{}
		parser.Parse(msg)
	}
}
