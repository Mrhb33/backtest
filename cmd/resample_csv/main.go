package main

import (
    "bufio"
    "encoding/csv"
    "flag"
    "fmt"
    "io"
    "os"
    "sort"
    "strconv"
    "strings"
    "golang.org/x/text/encoding/unicode"
    "golang.org/x/text/transform"
)

type Bar struct {
    Ts     int64
    Open   float64
    High   float64
    Low    float64
    Close  float64
    Volume float64
}

func parseDurMin(s string) (int64, error) {
    s = strings.ToLower(strings.TrimSpace(s))
    if strings.HasSuffix(s, "m") {
        v := strings.TrimSuffix(s, "m")
        n, err := strconv.Atoi(v)
        if err != nil { return 0, err }
        return int64(n), nil
    }
    if strings.HasSuffix(s, "min") {
        v := strings.TrimSuffix(s, "min")
        n, err := strconv.Atoi(v)
        if err != nil { return 0, err }
        return int64(n), nil
    }
    // plain number means minutes
    n, err := strconv.Atoi(s)
    if err != nil { return 0, fmt.Errorf("unsupported duration: %s", s) }
    return int64(n), nil
}

func main() {
    in := flag.String("in", "", "Input CSV (timestamp,open,high,low,close,volume)")
    out := flag.String("out", "", "Output CSV path")
    src := flag.String("src", "5m", "Source cadence (e.g., 5m)")
    dst := flag.String("dst", "15m", "Target cadence (e.g., 15m)")
    flag.Parse()

    if *in == "" || *out == "" {
        panic("-in and -out are required")
    }

    srcMin, err := parseDurMin(*src)
    if err != nil { panic(err) }
    dstMin, err := parseDurMin(*dst)
    if err != nil { panic(err) }
    if dstMin%srcMin != 0 { panic("dst must be a multiple of src") }
    factor := int(dstMin / srcMin)
    _ = factor

    f, err := os.Open(*in)
    if err != nil { panic(err) }
    defer f.Close()
    br := bufio.NewReader(f)
    // detect UTF-16 BOM; if present, decode to UTF-8
    if b, _ := br.Peek(2); len(b) >= 2 && ((b[0] == 0xFF && b[1] == 0xFE) || (b[0] == 0xFE && b[1] == 0xFF)) {
        // reset to start
        if _, err := f.Seek(0, 0); err != nil { panic(err) }
        tr := transform.NewReader(f, unicode.UTF16(unicode.LittleEndian, unicode.ExpectBOM).NewDecoder())
        br = bufio.NewReader(tr)
    }
    r := csv.NewReader(br)
    r.FieldsPerRecord = -1
    r.ReuseRecord = false
    r.LazyQuotes = true

    // Read and skip header if present
    var rows [][]string
    for {
        rec, err := r.Read()
        if err == io.EOF { break }
        if err != nil { continue }
        if len(rec) < 5 { continue }
        rows = append(rows, rec)
    }

    bars := make([]Bar, 0, len(rows))
    for i, rec := range rows {
        // header detection
        if i == 0 && (strings.EqualFold(rec[0], "timestamp") || strings.EqualFold(rec[0], "timestamp_ms")) {
            continue
        }
        tsStr := strings.TrimSpace(strings.TrimPrefix(rec[0], "\ufeff"))
        ts, err := strconv.ParseInt(tsStr, 10, 64)
        if err != nil { continue }
        parse := func(s string) float64 { v, _ := strconv.ParseFloat(strings.TrimSpace(strings.Trim(s, `"`)), 64); return v }
        o := parse(rec[1])
        h := parse(rec[2])
        l := parse(rec[3])
        c := parse(rec[4])
        v := 0.0
        if len(rec) >= 6 { v = parse(rec[5]) }
        bars = append(bars, Bar{Ts: ts, Open: o, High: h, Low: l, Close: c, Volume: v})
    }

    if len(bars) == 0 { panic("no input bars parsed") }
    sort.Slice(bars, func(i, j int) bool { return bars[i].Ts < bars[j].Ts })

    // Aggregate into dst buckets aligned to epoch (UTC) by milliseconds
    srcMs := srcMin * 60 * 1000
    dstMs := dstMin * 60 * 1000
    _ = srcMs // unused but kept for clarity

    buckets := make(map[int64]*Bar)
    order := make([]int64, 0)

    for _, b := range bars {
        bucket := (b.Ts / dstMs) * dstMs
        agg, ok := buckets[bucket]
        if !ok {
            nb := b // copy
            buckets[bucket] = &nb
            order = append(order, bucket)
            continue
        }
        // open is first
        // high max, low min, close last, volume sum
        if b.High > agg.High { agg.High = b.High }
        if b.Low < agg.Low { agg.Low = b.Low }
        agg.Close = b.Close
        agg.Volume += b.Volume
    }

    sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })

    of, err := os.Create(*out)
    if err != nil { panic(err) }
    defer of.Close()
    w := bufio.NewWriter(of)
    // header
    _, _ = w.WriteString("timestamp,open,high,low,close,volume\n")
    for _, ts := range order {
        b := buckets[ts]
        line := fmt.Sprintf("%d,%.8f,%.8f,%.8f,%.8f,%.8f\n", ts, b.Open, b.High, b.Low, b.Close, b.Volume)
        _, _ = w.WriteString(line)
    }
    _ = w.Flush()
}


