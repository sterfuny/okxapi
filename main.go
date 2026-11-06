package main

import (
	// "bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"
	"io"

	"github.com/gorilla/websocket"
)

const (
	writeWait = 1 * time.Second
	holdWait  = 30 * time.Second //Im not server
	pingSpit  = (holdWait * 9) / 10
)

var upgrader = websocket.Upgrader{
	CheckOrigin:     func(r *http.Request) bool { return true },
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

var runconn *websocket.Conn
type flag struct {
	para string
	value string
}
type URlInfo struct {
	base string
	path string
	flags []flag
	*url.URL
	reqJSON string
}

func create_URLInfo(base string, path string, flags []flag) URlInfo {
	urlTmp := URlInfo{base:base, path:path, flags:flags}
	if u, err := urlTmp.generate(); err != nil {
		log.Panic(err)
	} else {
		urlTmp.URL = u
	}
	fmt.Println("create")
	return urlTmp
}

func (U URlInfo) generate() (*url.URL, error) {
	str := fmt.Sprint(U.base, U.path)
	for i, v := range U.flags {
		if i == 0 {
			str = fmt.Sprint(str, "?", v.para, "=", v.value)
		} else {
			str = fmt.Sprint(str, "&", v.para, "=", v.value)
		}
	}

	u, err := url.Parse(str)
	if err != nil {
		return nil,err
	}
	return u,nil
}

func (U URlInfo) restCall() error {
	// okxURL := "https://ws.okx.com:443/api/v5/market"
	// path := "candles"
	// flag := []urlFlag{{"instId","ETH-USDT-SWAP"}}
	reqNow := U.reqJSON

	var data any
	json.Unmarshal([]byte(reqNow), &data)
	// out, _ := json.MarshalIndent(data, "", "  ")
	//
	// log.Println(string(out))

	// req,err := http.NewRequest(http.MethodPost, U.URL.String(), bytes.NewBuffer(out))
	// if err != nil {
	// 	panic(err)
	// }

	resp, err := http.Get(U.URL.String())
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	fmt.Println("return:",string(body))

	return nil
}

func (U URlInfo) wsConnect() error {
	// okxURL := "wss://ws.okx.com:443/ws/v5/business"
	// okxURL := "wss://ws.okx.com:443/ws/v5/public"
	// okxURL := "wss://[2a06:98c1:3103::ac40:9052]:8443/ws/v5/public"
	log.Println("conning")

	conn, _, err := websocket.DefaultDialer.Dial(U.URL.String(), nil)
	if err != nil {
		// log.Println("can not ws")
		return err
	}

	if runconn != nil {
		log.Println("close old conn")
		runconn.Close()
	}
	runconn = conn

	// // 放入计时
	// timer := time.NewTimer(pingSpit)
	// defer timer.Stop()

	// // 初次死线
	// conn.SetReadDeadline(time.Now().Add(holdWait))
	// // 设置收pong触发器
	// conn.SetPongHandler(func(string) error {
	// 	conn.SetReadDeadline(time.Now().Add(holdWait))
	// 	timer.Reset(pingSpit)
	// 	return nil
	// })

	// // 初次死线
	// conn.SetReadDeadline(time.Now().Add(holdWait))
	// // 设置收pong触发器
	// conn.SetPingHandler(func(appData string) error {
	// 	conn.SetReadDeadline(time.Now().Add(holdWait))
	// 	return conn.WriteControl(
	// 		websocket.PongMessage,
	// 		nil,
	// 		time.Now().Add(writeWait),
	// 	)
	// })

	log.Printf("已建立连接:%s", U.URL.String())

	go func(ws *websocket.Conn) {
		//订阅
		reqNow := U.reqJSON
		// `
		// {
		//     "op": "subscribe",
		// 	"args":[{
		//            "channel":"candle1m",
		//           	"instId":"ETH-USDT-SWAP"
		//       	}]
		// }
		// `

		// if err := json.Unmarshal(data, &m); err != nil {}
		var data any
		json.Unmarshal([]byte(reqNow), &data)
		out, _ := json.MarshalIndent(data, "", "  ")

		log.Println(string(out))
		ws.WriteMessage(websocket.TextMessage, out)
	}(runconn)

	for {
		iType, msg, err := conn.ReadMessage()
		if err != nil {
			return err
		}

		var msgType string = "Unkown"
		switch iType {
		case websocket.TextMessage:
			msgType = "TextMessage"
		case websocket.BinaryMessage:
			msgType = "BinaryMessage"
		case websocket.CloseMessage:
			msgType = "CloseMessage"
		case websocket.PongMessage:
			msgType = "PongMessage"
		}
		log.Printf("%s\n%s", msgType,msg)

		conn.SetReadDeadline(time.Now().Add(holdWait))
	}
}
func curlCandle() {

}

func main() {
	// path := "candles"
	// flag := []urlFlag{{"instId","ETH-USDT-SWAP"}}
	okx_candle:=create_URLInfo(
		"https://www.okx.com:443/api/v5","/market/candles",
		[]flag{
			{"instId","ETH-USDT-SWAP"},
			{"bar","1m"},
			{"after", fmt.Sprint(time.Now().Unix())},
		},
	)

	for {
		if err := okx_candle.restCall(); err != nil {
			fmt.Println(err)
			time.Sleep(1*time.Second)
			continue
		}
		time.Sleep(1*time.Second)
	}
	// for {
	// 	if err := okx_candle.wsConnect(); err != nil {
	// 		fmt.Println(err)
	// 		time.Sleep(10 * time.Second)
	// 	}
	// }
}
