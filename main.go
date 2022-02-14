package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	log.Println("FTXU!")

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
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println("[error] io ReadAll error", err)
	}
	log.Println("[info] resp code", resp.StatusCode)
	err = saveResponse(b, fname)
	if err != nil {
		log.Println("[error] save response error", err)
	}
}

func saveResponse(b []byte, fname string) error {
	// permission: -rw-r--r--
	err := os.WriteFile(fname, b, 0644)
	return err
}
