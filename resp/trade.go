package resp

import (
	"strconv"
	"time"
)

var TradeHeader = []string{
	"id",
	"price",
	"side",
	"size",
	"time",
}

type TradeResponse struct {
	Success bool    `json:"success"`
	Result  []Trade `json:"result"`
}

type Trade struct {
	ID    uint64    `json:"id"`
	Price float64   `json:"price"`
	Side  string    `json:"side"`
	Size  float64   `json:"size"`
	Time  time.Time `json:"time"`
}

func (t *Trade) Strings() []string {
	return []string{
		strconv.FormatUint(t.ID, 10),
		strconv.FormatFloat(t.Price, 'f', -1, 64),
		t.Side,
		strconv.FormatFloat(t.Size, 'f', -1, 64),
		t.Time.Format(time.RFC3339Nano),
	}
}
