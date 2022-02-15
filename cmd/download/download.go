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
	raw       bool
)

var (
	base      = "https://ftx.us/api"
	startTime time.Time
	endTime   time.Time
	symbol    string
	exchange  = "FTXU"
)

func init() {
	Cmd.Flags().StringVarP(&market, "market", "m", "", "Name of market (e.g. BTC/USD)")
	Cmd.Flags().StringVar(&startDate, "start", "", "Start date in YYYY-MM-DD format")
	Cmd.Flags().StringVar(&endDate, "end", "", "End date (inclusive) in YYYY-MM-DD format")
	Cmd.Flags().StringVar(&outDir, "out-dir", "", "Output directory to save data")
	Cmd.Flags().BoolVarP(&sortAsc, "sort", "", true, "Sort output data in ascending order by time")
	Cmd.Flags().BoolVarP(&raw, "raw", "", false, "If false, store data in btloader compatible format, otherwise, store in raw FTXU format")

	Cmd.MarkFlagRequired("market")
	Cmd.MarkFlagRequired("start")
	Cmd.MarkFlagRequired("end")
	Cmd.MarkFlagRequired("out-dir")
}

func checkFlags() error {
	// TODO: check if valid ftxu market.
	symbol = strings.ReplaceAll(market, "/", "")
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
	trades := make(map[int64]map[uint64]resp.Trade)
	defer FlushTrades(trades, 0)

	lastEndTime := endTime

	for {
		r, err := GetTrades(market, startTime, lastEndTime)
		if err != nil || !r.Success {
			return fmt.Errorf("cannot get trades, err %v, success %v", err, r.Success)
		}
		if len(r.Result) == 0 {
			break
		}

		for _, t := range r.Result {
			date := time.Date(t.Time.Year(), t.Time.Month(), t.Time.Day(), 0, 0, 0, 0, time.UTC)
			dsec := date.Unix()
			// New date.
			if _, ok := trades[dsec]; !ok {
				trades[dsec] = make(map[uint64]resp.Trade)
			}
			// Duplication check.
			if _, ok := trades[dsec][t.ID]; ok {
				log.Printf("[warn] duplicated trade ID %v on %v\n", t.ID, date)
			}
			trades[dsec][t.ID] = t
			if t.Time.Before(lastEndTime) {
				lastEndTime = t.Time
			}
		}
		FlushTrades(trades, lastEndTime.Unix())
	}
	log.Println("[info] received all trades")
	return nil
}

// FlushTrades flushes trades from dates with unix second greater than second.
func FlushTrades(trades map[int64]map[uint64]resp.Trade, second int64) {
	dateFmt := "20060102"
	fnameFmt := "%s/FTXU-%s-%s.csv"
	for dsec, tradeMap := range trades {
		// Already received all trades for this date.
		if dsec > second {
			// Convert to slice.
			tradeList := make([]resp.Trade, len(tradeMap))
			i := 0
			for _, t := range tradeMap {
				tradeList[i] = t
				i += 1
			}
			if sortAsc {
				sort.Slice(tradeList, func(i, j int) bool {
					return tradeList[i].Time.Before(tradeList[j].Time)
				})
			}
			date := time.Unix(dsec, 0).UTC().Format(dateFmt)
			fname := fmt.Sprintf(fnameFmt, outDir, symbol, date)
			err := saveCSV(tradeList, fname, raw)
			if err != nil {
				log.Fatal("[fatal] save csv file", err, fname)
			}
			delete(trades, dsec)
		}
	}
}

func GetTrades(market string, startTime, endTime time.Time) (resp.TradeResponse, error) {
	url := fmt.Sprintf("%s/markets/%s/trades?start_time=%d&end_time=%d",
		base,
		market,
		startTime.Unix(),
		endTime.Unix(),
	)

	log.Println("[info] url:", url)

	r, err := http.Get(url)
	if err != nil {
		log.Println("[error] http GET error", err)
		return resp.TradeResponse{}, err
	}
	defer r.Body.Close()

	b, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println("[error] io ReadAll error", err)
		return resp.TradeResponse{}, err
	}
	if r.StatusCode != http.StatusOK {
		log.Println("[error] resp code", r.StatusCode)
		return resp.TradeResponse{}, err
	}
	// Debug only.
	// fname := fmt.Sprintf("%s/FTXU-%s-%d-%d.json",
	// 	outDir,
	// 	symbol,
	// 	startTime.Unix(),
	// 	endTime.Unix(),
	// )
	// err = saveResponse(b, fname)
	// if err != nil {
	// 	log.Println("[error] save response error", err)
	// 	return resp.TradeResponse{}, err
	// }

	return parseResponse(b)
}

func saveCSV(trades []resp.Trade, fname string, raw bool) error {
	f, err := os.Create(fname)
	if err != nil {
		log.Println("[error] create file", err)
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if !raw {
		w.Comma = '|'
	}
	defer w.Flush()

	var header []string
	if raw {
		header = resp.TradeHeader
	} else {
		header = resp.TradeHeaderForBtloader
	}
	err = w.Write(header)
	if err != nil {
		log.Println("[error] write", err)
		return err
	}

	for _, t := range trades {
		if raw {
			err = w.Write(t.Strings())
		} else {
			err = w.Write(t.StringsForBtloader(symbol, exchange))
		}
		if err != nil {
			log.Println("[error] write", err)
			return err
		}
	}
	log.Printf("[info] saved %v trades to %v", len(trades), fname)
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
