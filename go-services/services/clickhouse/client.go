package clickhouse

import (
	"context"

	"github.com/shopspring/decimal"
)

type Client struct{}

func NewClient(cfg any) (*Client, error) { return &Client{}, nil }

type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
}

type Bar struct {
	Symbol     string
	Timestamp  uint64
	Open       decimal.Decimal
	High       decimal.Decimal
	Low        decimal.Decimal
	Close      decimal.Decimal
	Volume     decimal.Decimal
	TradeCount uint32
}

type MarketData struct {
	Symbol string
	Bars   []Bar
}

type rowsStub struct{}

func (r *rowsStub) Next() bool             { return false }
func (r *rowsStub) Scan(dest ...any) error { return nil }
func (r *rowsStub) Close() error           { return nil }

func (c *Client) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	// TODO: replace with real ClickHouse query implementation
	return &rowsStub{}, nil
}
