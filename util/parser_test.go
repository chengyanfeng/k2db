package util

import "testing"

func TestLogParser_Parse(t *testing.T) {
	parser := LogParser{}
	msg := ReadFile("sample.txt")
	//msg := ""
	p := parser.Parse(msg)
	Debug(JsonEncode(p))
}
