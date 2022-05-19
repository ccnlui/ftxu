package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"nhooyr.io/websocket"
)

type BidOrOffer [2]float64

type OrderbookData struct {
	Action   string       `json:"action"`
	Bids     []BidOrOffer `json:"bids"`
	Asks     []BidOrOffer `json:"asks"`
	Checksum uint32       `json:"checksum"`
}

type Resp struct {
	Channel string          `json:"channel"`
	Market  string          `json:"market"`
	Type    string          `json:"type"`
	Data    json.RawMessage `json:"data"`
}

const url = "wss://ftx.com/ws/"

var PriceSize map[float64]float64

func main() {
	ctx := context.Background()

	opts := &websocket.DialOptions{
		CompressionMode: websocket.CompressionContextTakeover,
	}
	conn, _, err := websocket.Dial(ctx, url, opts)
	if err != nil {
		log.Fatal("[fatal] websocket dial:", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "normal closure")

	PriceSize = make(map[float64]float64, 5000)

	go reader(ctx, conn)

	subscribe := `{"op":"subscribe", "channel":"orderbook", "market":"BTC/USD"}`
	conn.Write(ctx, websocket.MessageText, []byte(subscribe))

	select {}
}

func reader(ctx context.Context, conn *websocket.Conn) error {

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			log.Fatal("[fatal] websocket read:", err)
		}

		var resp Resp
		err = json.Unmarshal(data, &resp)
		if err != nil {
			log.Fatal("[fatal] json unmarshal:", err)
		}
		// fmt.Println("resp:", resp)

		var ob OrderbookData
		switch resp.Type {
		case "partial", "update":
			err = json.Unmarshal(resp.Data, &ob)
			if err != nil {
				log.Fatal("[fatal] json unmarshal:", err)
			}
			handle(ob)
		default:
			fmt.Println(resp)
		}
	}
}

func handle(ob OrderbookData) {
	for _, b := range ob.Bids {
		fmt.Println(b)
	}
}
