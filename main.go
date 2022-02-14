package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"ftxu/resp"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

func main() {
	log.Println("FTXU!")

	r, err := GetTrades()
	if err != nil {
		log.Fatal("[fatal] cannot get trades", err)
	}

	trades := arrangeByDate(r)
	fnameFmt := "FTXU-BTCUSD-%s.csv"

	for date, t := range trades {
		fname := fmt.Sprintf(fnameFmt, date)
		err = saveCSV(t, fname)
		if err != nil {
			log.Fatal("[fatal] save csv file", err, fname)
		}
	}
}

func arrangeByDate(r resp.TradeResponse) map[string][]resp.Trade {
	res := make(map[string][]resp.Trade)

	// TODO: option to disable sort?
	sort.Slice(r.Result, func(i, j int) bool {
		return r.Result[i].Time.Before(r.Result[j].Time)
	})

	dateFmt := "20060102"

	for _, t := range r.Result {
		date := t.Time.Format(dateFmt)
		res[date] = append(res[date], t)
	}

	return res
}

func GetTrades() (resp.TradeResponse, error) {
	base := "https://ftx.us/api"
	marketName := "BTC/USD"
	startTime := "2022-02-01T00:00:00Z"
	endTime := "2022-02-01T23:59:59Z"

	st, _ := time.Parse(time.RFC3339, startTime)
	et, _ := time.Parse(time.RFC3339, endTime)

	dateFmt := "20060102"
	fname := fmt.Sprintf("FTXU-%s-%s-%s.json",
		strings.ReplaceAll(marketName, "/", ""),
		st.Format(dateFmt),
		et.Format(dateFmt),
	)

	url := fmt.Sprintf("%s/markets/%s/trades?start_time=%d&end_time=%d",
		base,
		marketName,
		st.Unix(),
		et.Unix(),
	)

	log.Println("url:", url)

	resp, err := http.Get(url)
	if err != nil {
		log.Println("[error] http GET error", err)
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println("[error] io ReadAll error", err)
	}
	log.Println("[info] resp code", resp.StatusCode)
	err = saveResponse(b, fname)
	if err != nil {
		log.Println("[error] save response error", err)
	}

	return parseResponse(b)
}

func saveCSV(trades []resp.Trade, fname string) error {
	f, err := os.Create(fname)
	if err != nil {
		log.Println("[error] create file", err)
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	err = w.Write(resp.TradeHeader)
	if err != nil {
		log.Println("[error] write", err)
		return err
	}

	for _, t := range trades {
		err = w.Write(t.Strings())
		if err != nil {
			log.Println("[error] write", err)
			return err
		}
	}
	return nil
}

func saveResponse(b []byte, fname string) error {
	// permission: -rw-r--r--
	err := os.WriteFile(fname, b, 0644)
	return err
}

func parseResponse(b []byte) (resp.TradeResponse, error) {
	var r resp.TradeResponse
	if err := json.Unmarshal(b, &r); err != nil {
		log.Println("[error] json unmarshal", err)
		return r, err
	}
	return r, nil
}
