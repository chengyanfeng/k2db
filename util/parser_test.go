package util

import "testing"

func TestLogParser_Parse(t *testing.T) {
	parser := LogParser{}
	msg := ReadFile("sample.txt")
	p := parser.Parse(msg)
	Debug(JsonEncode(p))
}
