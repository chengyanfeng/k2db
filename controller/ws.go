package controller

import (
	//. "k2db/def"
	"errors"
	. "k2dbAccess/util"
	//"fmt"
	. "github.com/Shopify/sarama"
	"github.com/gorilla/websocket"
	"github.com/orcaman/concurrent-map"
	"net/http"
)

var onlineUser cmap.ConcurrentMap = cmap.New()

type WsController struct {
	BaseController
}

func ProcWs(msg *ConsumerMessage) {
	for t := range onlineUser.IterBuffered() {
		ws, _ := t.Val.(*websocket.Conn)
		err := ws.WriteMessage(websocket.TextMessage, msg.Value)
		if err != nil {
			ws.Close()
			onlineUser.Remove(t.Key)
			return
		}
	}
}

func (this *WsController) Join() {
	ws, err := websocket.Upgrade(this.Ctx.ResponseWriter, this.Ctx.Request, nil, 1024, 1024)
	if _, ok := err.(websocket.HandshakeError); ok {
		http.Error(this.Ctx.ResponseWriter, "Not a websocket handshake", 400)
		return
	} else if err != nil {
		Error("Cannot setup WebSocket connection:", err)
		return
	}

	// Message receive loop.
	for {
		_, payload, err := ws.ReadMessage()
		if err != nil {
			ws.Close()
			return
		}
		p := *JsonDecode(payload)
		if IsEmpty(p) {
			err = errors.New("无效的请求参数格式")
		} else {
			uid := p["uid"]
			onlineUser.Set(ToString(uid), ws)
		}
	}
}
