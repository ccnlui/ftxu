package download

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

	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "download",
	Short: "Download FTXU market data",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := checkFlags(); err != nil {
			return err
		}
		return download()
	},
}

// flags
var (
	market    string
	startDate string
	endDate   string
	outDir    string
	sortAsc   bool
)

var (
	base = "https://ftx.us/api"
	// marketName = "BTC/USD"
	startTime time.Time
	endTime   time.Time
)

func init() {
	Cmd.Flags().StringVarP(&market, "market", "m", "", "Name of market (e.g. BTC/USD)")
	Cmd.Flags().StringVar(&startDate, "start", "", "Start date in YYYY-MM-DD format")
	Cmd.Flags().StringVar(&endDate, "end", "", "End date (inclusive) in YYYY-MM-DD format")
	Cmd.Flags().StringVar(&outDir, "out-dir", "", "Output directory to save data")
	Cmd.Flags().BoolVarP(&sortAsc, "sort", "", true, "Sort output data in ascending order by time")

	Cmd.MarkFlagRequired("market")
	Cmd.MarkFlagRequired("start")
	Cmd.MarkFlagRequired("end")
	Cmd.MarkFlagRequired("out-dir")
}

func checkFlags() error {
	// TODO: check market.
	st, err := time.Parse("2006-01-02", startDate)
	if err != nil {
		return fmt.Errorf("invalid start: %w", err)
	}
	startTime = st
	et, err := time.Parse("2006-01-02", endDate)
	if err != nil {
		return fmt.Errorf("invalid end: %w", err)
	}
	// Last second of end date.
	endTime = et.Add(24 * time.Hour).Add(-time.Second)
	if _, err := os.Stat(outDir); err != nil {
		return fmt.Errorf("invalid output directory: %v", outDir)
	}
	return nil
}

func download() error {
	r, err := GetTrades(market, startTime, endTime)
	if err != nil {
		log.Fatal("[fatal] cannot get trades", err)
	}

	trades := arrangeByDate(r, sortAsc)
	fnameFmt := "%s/FTXU-BTCUSD-%s.csv"

	for date, t := range trades {
		fname := fmt.Sprintf(fnameFmt, outDir, date)
		err = saveCSV(t, fname)
		if err != nil {
			log.Fatal("[fatal] save csv file", err, fname)
		}
	}
	return nil
}

func arrangeByDate(r resp.TradeResponse, sortAsc bool) map[string][]resp.Trade {
	res := make(map[string][]resp.Trade)

	dateFmt := "20060102"

	for _, t := range r.Result {
		date := t.Time.Format(dateFmt)
		res[date] = append(res[date], t)
	}

	// TODO: sort in parallel
	if sortAsc {
		for _, t := range res {
			sort.Slice(t, func(i, j int) bool {
				return t[i].Time.Before(t[j].Time)
			})
		}
	}
	return res
}

func GetTrades(market string, startTime, endTime time.Time) (resp.TradeResponse, error) {
	dateFmt := "20060102"
	fname := fmt.Sprintf("%s/FTXU-%s-%s-%s.json",
		outDir,
		strings.ReplaceAll(market, "/", ""),
		startTime.Format(dateFmt),
		endTime.Format(dateFmt),
	)

	url := fmt.Sprintf("%s/markets/%s/trades?start_time=%d&end_time=%d",
		base,
		market,
		startTime.Unix(),
		endTime.Unix(),
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
