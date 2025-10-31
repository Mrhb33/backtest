package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Candle struct {
	OpenTimeMs uint64
	Open       float64
	High       float64
	Low        float64
	Close      float64
}

type ParityRow struct {
	OpenTimeMs uint64
	Open       float64
	High       float64
	Low        float64
	Close      float64
	EMA        float64
	ATR        float64
	RefEMA     *float64
	RefATR     *float64
	DiffEMA    *float64
	DiffATR    *float64
	MatchEMA   *bool
	MatchATR   *bool
}

type Config struct {
	ClickhouseURL string
	Symbol        string
	StartMs       int64
	EndMs         int64
	EMAPeriod     int
	ATRPeriod     int
	Seed          string // "tv" | "classic" (tv: EMA seeded with SMA, ATR with SMA→RMA)
	ReferenceCSV  string
	OutputCSV     string
	Tolerance     float64
}

type App struct {
	cfg  Config
	conn driver.Conn
}

func mustParseFloat(s string) float64 {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return math.NaN()
	}
	return v
}

func (a *App) connect() error {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{a.cfg.ClickhouseURL},
		Auth: clickhouse.Auth{Database: "backtest", Username: "backtest", Password: "backtest123"},
	})
	if err != nil {
		return err
	}
	a.conn = conn
	return nil
}

func (a *App) loadCandles() ([]Candle, error) {
	q := `
SELECT open_time_ms, open, high, low, close
FROM backtest.ohlcv_raw
WHERE symbol = ? AND interval = '1m' AND open_time_ms BETWEEN ? AND ?
ORDER BY open_time_ms`
	rows, err := a.conn.Query(a.conn.Context(), q, a.cfg.Symbol, a.cfg.StartMs, a.cfg.EndMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Candle
	for rows.Next() {
		var (
			ot uint64
			o, h, l, c string
		)
		if err := rows.Scan(&ot, &o, &h, &l, &c); err != nil {
			return nil, err
		}
		out = append(out, Candle{
			OpenTimeMs: ot,
			Open:       mustParseFloat(o),
			High:       mustParseFloat(h),
			Low:        mustParseFloat(l),
			Close:      mustParseFloat(c),
		})
	}
	return out, nil
}

// TradingView-style EMA: seed with SMA of first N closes
func computeEMA(closes []float64, period int) []float64 {
	result := make([]float64, len(closes))
	if period <= 0 || len(closes) == 0 {
		return result
	}
	k := 2.0 / float64(period+1)
	if len(closes) < period {
		return result
	}
	// seed
	sum := 0.0
	for i := 0; i < period; i++ {
		sum += closes[i]
	}
	ema := sum / float64(period)
	result[period-1] = ema
	for i := period; i < len(closes); i++ {
		ema = closes[i]*k + ema*(1-k)
		result[i] = ema
	}
	return result
}

// ATR with TV/Wilder: TR = max(high-low, abs(high-prevClose), abs(low-prevClose)); seed ATR as SMA of first N TRs, then RMA
func computeATR(candles []Candle, period int) []float64 {
	atr := make([]float64, len(candles))
	if period <= 0 || len(candles) == 0 {
		return atr
	}
	tr := make([]float64, len(candles))
	for i := range candles {
		hl := candles[i].High - candles[i].Low
		if i == 0 {
			if hl < 0 { hl = 0 }
			tr[i] = hl
			continue
		}
		up := math.Abs(candles[i].High - candles[i-1].Close)
		dn := math.Abs(candles[i].Low - candles[i-1].Close)
		v := hl
		if up > v { v = up }
		if dn > v { v = dn }
		tr[i] = v
	}
	if len(candles) < period {
		return atr
	}
	// seed ATR as SMA of first N TRs (starting from index 1 to period inclusive)
	sum := 0.0
	for i := 1; i <= period; i++ { // TV ignores TR[0] in many seeding variants; using 1..period
		sum += tr[i]
	}
	seed := sum / float64(period)
	atr[period] = seed
	alpha := 1.0 / float64(period)
	for i := period + 1; i < len(candles); i++ {
		atr[i] = (atr[i-1]*(1-alpha) + tr[i]*alpha)
	}
	return atr
}

func loadReferenceCSV(path string) (map[uint64]struct{ EMA, ATR *float64 }, error) {
	if path == "" {
		return map[uint64]struct{ EMA, ATR *float64 }{}, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := csv.NewReader(bufio.NewReader(f))
	r.FieldsPerRecord = -1
	header, err := r.Read()
	if err != nil {
		return nil, err
	}
	// Expect columns including time(open_time_ms or timestamp), ema, atr (case-insensitive search)
	idxTime, idxEMA, idxATR := -1, -1, -1
	for i, h := range header {
		hl := strings.ToLower(strings.TrimSpace(h))
		if hl == "open_time_ms" || hl == "time" || hl == "timestamp" {
			idxTime = i
		}
		if hl == "ema" || strings.Contains(hl, "ema") {
			idxEMA = i
		}
		if hl == "atr" || strings.Contains(hl, "atr") {
			idxATR = i
		}
	}
	ref := make(map[uint64]struct{ EMA, ATR *float64 })
	for {
		rec, err := r.Read()
		if err != nil {
			break
		}
		if idxTime < 0 || idxEMA < 0 && idxATR < 0 {
			continue
		}
		var ot uint64
		if idxTime >= 0 {
			v, _ := strconv.ParseUint(strings.TrimSpace(rec[idxTime]), 10, 64)
			ot = v
		}
		var emaPtr, atrPtr *float64
		if idxEMA >= 0 {
			v := mustParseFloat(strings.TrimSpace(rec[idxEMA]))
			emaPtr = &v
		}
		if idxATR >= 0 {
			v := mustParseFloat(strings.TrimSpace(rec[idxATR]))
			atrPtr = &v
		}
		ref[ot] = struct{ EMA, ATR *float64 }{EMA: emaPtr, ATR: atrPtr}
	}
	return ref, nil
}

func (a *App) run() error {
	if err := a.connect(); err != nil {
		return err
	}
	candles, err := a.loadCandles()
	if err != nil {
		return err
	}
	if len(candles) == 0 {
		return fmt.Errorf("no candles in range")
	}
	closes := make([]float64, len(candles))
	for i := range candles {
		closes[i] = candles[i].Close
	}
	ema := computeEMA(closes, a.cfg.EMAPeriod)
	atr := computeATR(candles, a.cfg.ATRPeriod)

	refMap, err := loadReferenceCSV(a.cfg.ReferenceCSV)
	if err != nil {
		log.Printf("warning: failed to load reference csv: %v", err)
		refMap = map[uint64]struct{ EMA, ATR *float64 }{}
	}

	rows := make([]ParityRow, len(candles))
	for i := range candles {
		row := ParityRow{
			OpenTimeMs: candles[i].OpenTimeMs,
			Open:       candles[i].Open,
			High:       candles[i].High,
			Low:        candles[i].Low,
			Close:      candles[i].Close,
			EMA:        ema[i],
			ATR:        atr[i],
		}
		if ref, ok := refMap[candles[i].OpenTimeMs]; ok {
			if ref.EMA != nil {
				d := math.Abs(row.EMA - *ref.EMA)
				row.DiffEMA = &d
				m := d <= a.cfg.Tolerance
				row.MatchEMA = &m
				row.RefEMA = ref.EMA
			}
			if ref.ATR != nil {
				d := math.Abs(row.ATR - *ref.ATR)
				row.DiffATR = &d
				m := d <= a.cfg.Tolerance
				row.MatchATR = &m
				row.RefATR = ref.ATR
			}
		}
		rows[i] = row
	}
	return writeCSV(a.cfg.OutputCSV, rows)
}

func writeCSV(path string, rows []ParityRow) error {
	if path == "" {
		path = fmt.Sprintf("indicator_parity_%d.csv", time.Now().Unix())
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	header := []string{"open_time_ms","open","high","low","close","ema","atr","ref_ema","ref_atr","diff_ema","diff_atr","match_ema","match_atr"}
	if err := w.Write(header); err != nil { return err }
	for _, r := range rows {
		refEMA := ""
		refATR := ""
		diffEMA := ""
		diffATR := ""
		matchEMA := ""
		matchATR := ""
		if r.RefEMA != nil { refEMA = fmt.Sprintf("%.10f", *r.RefEMA) }
		if r.RefATR != nil { refATR = fmt.Sprintf("%.10f", *r.RefATR) }
		if r.DiffEMA != nil { diffEMA = fmt.Sprintf("%.10f", *r.DiffEMA) }
		if r.DiffATR != nil { diffATR = fmt.Sprintf("%.10f", *r.DiffATR) }
		if r.MatchEMA != nil { matchEMA = fmt.Sprintf("%t", *r.MatchEMA) }
		if r.MatchATR != nil { matchATR = fmt.Sprintf("%t", *r.MatchATR) }
		rec := []string{
			fmt.Sprintf("%d", r.OpenTimeMs),
			fmt.Sprintf("%.8f", r.Open),
			fmt.Sprintf("%.8f", r.High),
			fmt.Sprintf("%.8f", r.Low),
			fmt.Sprintf("%.8f", r.Close),
			fmt.Sprintf("%.10f", r.EMA),
			fmt.Sprintf("%.10f", r.ATR),
			refEMA, refATR, diffEMA, diffATR, matchEMA, matchATR,
		}
		if err := w.Write(rec); err != nil { return err }
	}
	log.Printf("Indicator parity CSV written: %s", path)
	return nil
}

func parseTimeOrDate(s string) (int64, error) {
	// Accept ms epoch or RFC3339 date
	if ms, err := strconv.ParseInt(s, 10, 64); err == nil {
		return ms, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UnixMilli(), nil
	}
	// Accept YYYY-MM-DD
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t.UnixMilli(), nil
	}
	return 0, fmt.Errorf("invalid time: %s", s)
}

func main() {
	var cfg Config
	flag.StringVar(&cfg.ClickhouseURL, "clickhouse-url", "localhost:9000", "ClickHouse URL host:port")
	flag.StringVar(&cfg.Symbol, "symbol", "BTCUSDT", "Symbol")
	start := flag.String("start", "", "Start time (ms epoch or YYYY-MM-DD or RFC3339)")
	end := flag.String("end", "", "End time (ms epoch or YYYY-MM-DD or RFC3339)")
	flag.IntVar(&cfg.EMAPeriod, "ema", 20, "EMA period")
	flag.IntVar(&cfg.ATRPeriod, "atr", 14, "ATR period")
	flag.StringVar(&cfg.Seed, "seed", "tv", "Seeding mode: tv|classic")
	flag.StringVar(&cfg.ReferenceCSV, "reference-csv", "", "Optional reference CSV with time, ema, atr columns")
	flag.StringVar(&cfg.OutputCSV, "output", "", "Output CSV path")
	flag.Float64Var(&cfg.Tolerance, "tolerance", 1e-8, "Match tolerance")
	flag.Parse()

	if *start == "" || *end == "" {
		log.Fatal("start and end are required")
	}
	var err error
	cfg.StartMs, err = parseTimeOrDate(*start)
	if err != nil { log.Fatal(err) }
	cfg.EndMs, err = parseTimeOrDate(*end)
	if err != nil { log.Fatal(err) }

	app := &App{cfg: cfg}
	if err := app.run(); err != nil {
		log.Fatalf("error: %v", err)
	}
}
