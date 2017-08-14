package util

import (
	"encoding/json"
	"testing"
	"strings"
	"fmt"
)

func TestLogParser_Parse(t *testing.T) {
	parser := LogParser{}
	msg := ReadFile("sample3.txt")

	//msg := ""
	p := parser.Parse(msg)
	Debug(JsonEncode(p))
}

func TestLogParser_Parse1(t *testing.T) {
	msg :="2017-08-11 13:30:00|17631322508|2017-08-11 13:30:22|21130|111.202.83.63:809|0|328"
	tmp := strings.Split(msg, "|")
	seg := []interface{}{}
	fmt.Println(len(tmp))
	for _, v := range tmp {
		seg = append(seg, v)
	}
	fmt.Println(seg, seg[1],len(seg))
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
	//filebeat产生的信息
	beat :=dat["beat"].(map[string]interface {})
	//将日志和filebeat所在机器的主机名拼接
	msg2 := dat["message"].(string) + " " + beat["hostname"].(string)
	p := parser.Parse(msg2)

	Debug(msg2)
	Debug(JsonEncode(p))
}

func BenchmarkLogParser_Parse(b *testing.B) {
	msg := ReadFile("sample.txt")
	for i := 0; i < b.N; i++ {
		parser := LogParser{}
		parser.Parse(msg)
	}
}

func BenchmarkLogParser_ParseJson(b *testing.B) {
	msg := ReadFile("sample1.txt")
	for i := 0; i < b.N; i++ {
		parser := LogParser{}
		msg1 := []byte(msg)
		dat := make(map[string]interface{})
		if err := json.Unmarshal(msg1, &dat); err != nil {
			panic(err)
		}
		parser.Parse(dat["message"].(string))
	}
}

func TestAesDecrypt(t *testing.T) {
	aes :=Aes{}
	key :=[]byte("ac22273abb2f4960")
	userids_str :="547409b7086f8be77774f3db148d0451"
	aes.CBCDecrypter(key, userids_str)
}