package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

type SaveToTimescaleDB struct {
	db         *sql.DB
	processors []processor.Processor
}

func NewSaveToTimescaleDB(config map[string]interface{}) (*SaveToTimescaleDB, error) {
	connStr, ok := config["connection_string"].(string)
	if !ok {
		return nil, fmt.Errorf("missing connection_string in configuration")
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("error connecting to TimescaleDB: %w", err)
	}

	// Initialize tables
	if err := initializeTimescaleTables(db); err != nil {
		return nil, err
	}

	return &SaveToTimescaleDB{
		db: db,
	}, nil
}

func initializeTimescaleTables(db *sql.DB) error {
	// Create and set up TimescaleDB tables
	queries := []string{
		`CREATE TABLE IF NOT EXISTS trades (
            time        TIMESTAMPTZ NOT NULL,
            seller_id   TEXT,
            buyer_id    TEXT,
            sell_asset  TEXT,
            buy_asset   TEXT,
            sell_amount NUMERIC,
            buy_amount  NUMERIC,
            price      NUMERIC,
            type       TEXT,
            orderbook_id TEXT,
            liquidity_pool TEXT
        )`,
		`SELECT create_hypertable('trades', 'time', if_not_exists => TRUE)`,
		`CREATE INDEX IF NOT EXISTS idx_trades_assets ON trades(sell_asset, buy_asset)`,
		`CREATE INDEX IF NOT EXISTS idx_trades_accounts ON trades(seller_id, buyer_id)`,
	}

	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			return fmt.Errorf("error executing setup query: %w", err)
		}
	}

	return nil
}

func (s *SaveToTimescaleDB) Subscribe(processor processor.Processor) {
	s.processors = append(s.processors, processor)
}

func (s *SaveToTimescaleDB) Process(ctx context.Context, msg processor.Message) error {
	payloadBytes, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte type for message.Payload, got %T", msg.Payload)
	}

	var trade AppTrade
	if err := json.Unmarshal(payloadBytes, &trade); err != nil {
		return fmt.Errorf("error unmarshaling trade: %w", err)
	}

	// Parse timestamp
	timestamp, err := strconv.ParseInt(trade.Timestamp, 10, 64)
	if err != nil {
		return fmt.Errorf("error parsing timestamp: %w", err)
	}

	// Insert trade data
	_, err = s.db.ExecContext(ctx, `
        INSERT INTO trades (
            time, seller_id, buyer_id, sell_asset, buy_asset,
            sell_amount, buy_amount, price, type, orderbook_id,
            liquidity_pool
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    `,
		time.Unix(timestamp, 0),
		trade.SellerId,
		trade.BuyerId,
		trade.SellAsset,
		trade.BuyAsset,
		trade.SellAmount,
		trade.BuyAmount,
		trade.Price,
		trade.Type,
		trade.OrderbookID,
		trade.LiquidityPool,
	)

	if err != nil {
		return fmt.Errorf("error inserting trade: %w", err)
	}

	log.Printf("Successfully stored trade data for %s/%s pair",
		trade.SellAsset, trade.BuyAsset)
	return nil
}

func (s *SaveToTimescaleDB) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

type AppTrade struct {
	Timestamp        string       `json:"timestamp"`
	SellerId         string       `json:"seller_id"`
	BuyerId          string       `json:"buyer_id"`
	SellAsset        string       `json:"sell_asset"`
	BuyAsset         string       `json:"buy_asset"`
	SellAmount       string       `json:"sell_amount"`
	BuyAmount        string       `json:"buy_amount"`
	Price            float64      `json:"price"`
	Type             string       `json:"type"`
	OrderbookID      string       `json:"orderbook_id"`
	LiquidityPool    string       `json:"liquidity_pool,omitempty"`
	SellAssetDetails AssetDetails `json:"sell_asset_details"`
	BuyAssetDetails  AssetDetails `json:"buy_asset_details"`
}

type AssetDetails struct {
	Code      string    `json:"code"`
	Issuer    string    `json:"issuer,omitempty"` // omitempty since native assets have no issuer
	Type      string    `json:"type"`             // native, credit_alphanum4, credit_alphanum12
	Timestamp time.Time `json:"timestamp"`
}
