package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type SaveToClickHouse struct {
	conn       driver.Conn
	processors []Processor
}

type ClickHouseConfig struct {
	Address      string
	Database     string
	Username     string
	Password     string
	MaxOpenConns int
	MaxIdleConns int
}

func NewSaveToClickHouse(config map[string]interface{}) (*SaveToClickHouse, error) {
	// Parse configuration
	chConfig, err := parseClickHouseConfig(config)
	if err != nil {
		return nil, fmt.Errorf("error parsing config: %w", err)
	}

	// Create ClickHouse connection
	clickhouseconn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{chConfig.Address},
		Auth: clickhouse.Auth{
			Database: chConfig.Database,
			Username: chConfig.Username,
			Password: chConfig.Password,
		},
		MaxOpenConns: chConfig.MaxOpenConns,
		MaxIdleConns: chConfig.MaxIdleConns,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("error connecting to ClickHouse: %w", err)
	}

	// Initialize tables
	if err := initializeClickHouseTables(clickhouseconn); err != nil {
		return nil, fmt.Errorf("error initializing tables: %w", err)
	}

	return &SaveToClickHouse{
		conn: clickhouseconn,
	}, nil
}

func parseClickHouseConfig(config map[string]interface{}) (ClickHouseConfig, error) {
	var chConfig ClickHouseConfig

	addr, ok := config["address"].(string)
	if !ok {
		return chConfig, fmt.Errorf("missing address in config")
	}
	chConfig.Address = addr

	dbname, ok := config["database"].(string)
	if !ok {
		return chConfig, fmt.Errorf("missing database in config")
	}
	chConfig.Database = dbname

	username, ok := config["username"].(string)
	if !ok {
		return chConfig, fmt.Errorf("missing username in config")
	}
	chConfig.Username = username

	password, ok := config["password"].(string)
	if !ok {
		return chConfig, fmt.Errorf("missing password in config")
	}
	chConfig.Password = password

	// Set defaults for connection pools
	chConfig.MaxOpenConns = 10
	chConfig.MaxIdleConns = 5

	if maxOpen, ok := config["max_open_conns"].(int); ok {
		chConfig.MaxOpenConns = maxOpen
	}
	if maxIdle, ok := config["max_idle_conns"].(int); ok {
		chConfig.MaxIdleConns = maxIdle
	}

	return chConfig, nil
}

func initializeClickHouseTables(conn driver.Conn) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS payments (
            timestamp DateTime,
            ledger_sequence UInt32,
            buyer_account_id String,
            seller_account_id String,
            asset_code LowCardinality(String),
            amount Decimal64(7),
            operation_type LowCardinality(String),
            date Date MATERIALIZED toDate(timestamp),
            created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(date)
        ORDER BY (timestamp, buyer_account_id, seller_account_id)`,

		`CREATE TABLE IF NOT EXISTS account_creations (
            timestamp DateTime,
            ledger_sequence UInt32,
            funder String,
            account String,
            starting_balance Decimal64(7),
            operation_type LowCardinality(String),
            date Date MATERIALIZED toDate(timestamp),
            created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(date)
        ORDER BY (timestamp, account, funder)`,

		// Create materialized views for real-time analytics
		`CREATE MATERIALIZED VIEW IF NOT EXISTS payment_hourly_stats
        ENGINE = SummingMergeTree()
        PARTITION BY toYYYYMM(date)
        ORDER BY (date, asset_code, hour)
        AS SELECT
            toDate(timestamp) as date,
            toHour(timestamp) as hour,
            asset_code,
            count() as tx_count,
            sum(amount) as total_amount,
            uniqExact(buyer_account_id) as unique_buyers,
            uniqExact(seller_account_id) as unique_sellers
        FROM payments
        GROUP BY date, hour, asset_code`,

		`CREATE MATERIALIZED VIEW IF NOT EXISTS account_creation_daily_stats
        ENGINE = SummingMergeTree()
        PARTITION BY toYYYYMM(date)
        ORDER BY (date)
        AS SELECT
            toDate(timestamp) as date,
            count() as accounts_created,
            sum(starting_balance) as total_initial_balance,
            uniqExact(funder) as unique_funders
        FROM account_creations
        GROUP BY date`,
	}

	for _, query := range queries {
		err := conn.Exec(context.Background(), query)
		if err != nil {
			return fmt.Errorf("error executing query: %s: %w", query, err)
		}
	}

	return nil
}

func (ch *SaveToClickHouse) Subscribe(processor Processor) {
	ch.processors = append(ch.processors, processor)
}

func (ch *SaveToClickHouse) Process(ctx context.Context, msg Message) error {
	payloadBytes, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected Message type, got %T", msg)
	}

	var data map[string]interface{}
	if err := json.Unmarshal(payloadBytes, &data); err != nil {
		return fmt.Errorf("error unmarshaling payload: %w", err)
	}

	operationType, ok := data["type"].(string)
	if !ok {
		return fmt.Errorf("missing operation type in payload")
	}

	switch operationType {
	case "payment":
		return ch.processPayment(ctx, data)
	case "create_account":
		return ch.processCreateAccount(ctx, data)
	default:
		return fmt.Errorf("unsupported operation type: %s", operationType)
	}
}

func (ch *SaveToClickHouse) processPayment(ctx context.Context, data map[string]interface{}) error {
	query := `
        INSERT INTO payments (
            timestamp,
            ledger_sequence,
            buyer_account_id,
            seller_account_id,
            asset_code,
            amount,
            operation_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    `

	timestamp, err := parseTimestamp(data["timestamp"].(string))
	if err != nil {
		return fmt.Errorf("error parsing timestamp: %w", err)
	}

	amount, err := parseAmount(data["amount"].(string))
	if err != nil {
		return fmt.Errorf("error parsing amount: %w", err)
	}

	err = ch.conn.Exec(ctx, query,
		timestamp,
		data["ledger_sequence"],
		data["buyer_account_id"],
		data["seller_account_id"],
		data["asset_code"],
		amount,
		"payment",
	)

	if err != nil {
		return fmt.Errorf("error inserting payment: %w", err)
	}

	return nil
}

func (ch *SaveToClickHouse) processCreateAccount(ctx context.Context, data map[string]interface{}) error {
	query := `
        INSERT INTO account_creations (
            timestamp,
            ledger_sequence,
            funder,
            account,
            starting_balance,
            operation_type
        ) VALUES (?, ?, ?, ?, ?, ?)
    `

	timestamp, err := parseTimestamp(data["timestamp"].(string))
	if err != nil {
		return fmt.Errorf("error parsing timestamp: %w", err)
	}

	startingBalance, err := parseAmount(data["starting_balance"].(string))
	if err != nil {
		return fmt.Errorf("error parsing starting_balance: %w", err)
	}

	err = ch.conn.Exec(ctx, query,
		timestamp,
		data["ledger_sequence"],
		data["funder"],
		data["account"],
		startingBalance,
		"create_account",
	)

	if err != nil {
		return fmt.Errorf("error inserting account creation: %w", err)
	}

	return nil
}

// Helper functions for parsing data
func parseTimestamp(ts string) (time.Time, error) {
	i, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(i, 0), nil
}

func parseAmount(amount string) (float64, error) {
	return strconv.ParseFloat(amount, 64)
}

func (ch *SaveToClickHouse) Close() error {
	return ch.conn.Close()
}

// Query helper methods
func (ch *SaveToClickHouse) GetPaymentStats(ctx context.Context, start, end time.Time) ([]map[string]interface{}, error) {
	query := `
        SELECT
            date,
            hour,
            asset_code,
            tx_count,
            total_amount,
            unique_buyers,
            unique_sellers
        FROM payment_hourly_stats
        WHERE date BETWEEN ? AND ?
        ORDER BY date, hour, asset_code
    `

	rows, err := ch.conn.Query(ctx, query, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var (
			date          time.Time
			hour          uint8
			assetCode     string
			txCount       uint64
			totalAmount   float64
			uniqueBuyers  uint64
			uniqueSellers uint64
		)

		if err := rows.Scan(
			&date,
			&hour,
			&assetCode,
			&txCount,
			&totalAmount,
			&uniqueBuyers,
			&uniqueSellers,
		); err != nil {
			return nil, err
		}

		results = append(results, map[string]interface{}{
			"date":           date,
			"hour":           hour,
			"asset_code":     assetCode,
			"tx_count":       txCount,
			"total_amount":   totalAmount,
			"unique_buyers":  uniqueBuyers,
			"unique_sellers": uniqueSellers,
		})
	}

	return results, nil
}
