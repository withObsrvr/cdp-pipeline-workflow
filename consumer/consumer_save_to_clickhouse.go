package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

type SaveToClickHouse struct {
	conn       driver.Conn
	processors []processor.Processor
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

		`CREATE TABLE IF NOT EXISTS trustlines (
		timestamp DateTime,
		account_id String,
		asset_code LowCardinality(String),
		asset_issuer String,
		limit Decimal64(7),
		action LowCardinality(String),
		type LowCardinality(String),
		ledger_seq UInt32,
		tx_hash String,
		auth_flags UInt32,
		auth_required Bool,
		auth_revocable Bool,
		auth_immutable Bool,
		date Date MATERIALIZED toDate(timestamp),
		created_at DateTime DEFAULT now()
	) ENGINE = MergeTree()
	PARTITION BY toYYYYMM(date)
	ORDER BY (timestamp, account_id, asset_code, asset_issuer);`,

		`-- Materialized view for trustline statistics
	CREATE MATERIALIZED VIEW IF NOT EXISTS trustline_daily_stats
	ENGINE = SummingMergeTree()
	PARTITION BY toYYYYMM(date)
	ORDER BY (date, asset_code, action)
	AS SELECT
		toDate(timestamp) as date,
		asset_code,
		action,
		count() as operation_count,
		uniqExact(account_id) as unique_accounts,
		uniqExact(asset_issuer) as unique_issuers
	FROM trustlines
	GROUP BY date, asset_code, action;`,

		`-- Materialized view for asset issuer statistics
	CREATE MATERIALIZED VIEW IF NOT EXISTS asset_issuer_stats
	ENGINE = SummingMergeTree()
	PARTITION BY toYYYYMM(date)
	ORDER BY (date, asset_issuer, asset_code)
	AS SELECT
		toDate(timestamp) as date,
		asset_issuer,
		asset_code,
		count() as trustline_count,
		uniqExact(account_id) as unique_trustors,
		countIf(action = 'created') as created_count,
		countIf(action = 'removed') as removed_count,
		countIf(action = 'updated') as updated_count
	FROM trustlines
	GROUP BY date, asset_issuer, asset_code;`,

		`CREATE TABLE IF NOT EXISTS token_prices (
            timestamp DateTime,
            base_asset LowCardinality(String),
            base_issuer String,
            quote_asset LowCardinality(String),
            quote_issuer String,
            price Float64,
            volume_24h Float64,
            num_trades_24h Int64,
            high_price_24h Float64,
            low_price_24h Float64,
            price_change_24h Float64,
			market_cap Float64,
			circulating_supply Float64,
			total_supply Float64,
			num_holders Int64,
			liquidity_depth Float64,
            bid_price Float64,
            ask_price Float64,
            date Date MATERIALIZED toDate(timestamp)
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(date)
        ORDER BY (timestamp, base_asset, quote_asset);`,
		`-- Add market cap analytics view
		CREATE MATERIALIZED VIEW IF NOT EXISTS market_cap_analytics
		ENGINE = SummingMergeTree()
		PARTITION BY toYYYYMM(date)
		ORDER BY (date, asset_code)
		AS SELECT
			toDate(timestamp) as date,
			base_asset as asset_code,
			argMax(market_cap, timestamp) as market_cap,
			argMax(circulating_supply, timestamp) as circulating_supply,
			argMax(num_holders, timestamp) as num_holders,
			argMax(price, timestamp) as price
		FROM token_prices
		GROUP BY date, base_asset;`,

		`-- Materialized view for price analytics
		CREATE MATERIALIZED VIEW IF NOT EXISTS token_price_analytics
		ENGINE = SummingMergeTree()
		PARTITION BY toYYYYMM(date)
		ORDER BY (date, hour, base_asset, quote_asset)
		AS SELECT
			toDate(timestamp) as date,
			toHour(timestamp) as hour,
			base_asset,
			quote_asset,
			avg(price) as avg_price,
			min(price) as low_price,
			max(price) as high_price,
			sum(volume_24h) as total_volume,
			sum(num_trades_24h) as total_trades
		FROM token_prices
		GROUP BY date, hour, base_asset, quote_asset;`,
	}

	for _, query := range queries {
		err := conn.Exec(context.Background(), query)
		if err != nil {
			return fmt.Errorf("error executing query: %s: %w", query, err)
		}
	}

	return nil
}

func (ch *SaveToClickHouse) Subscribe(processor processor.Processor) {
	ch.processors = append(ch.processors, processor)
}

func (ch *SaveToClickHouse) Process(ctx context.Context, msg processor.Message) error {
	payloadBytes, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte payload, got %T", msg.Payload)
	}

	log.Printf("ClickHouse consumer received payload: %s", string(payloadBytes))

	var data map[string]interface{}
	if err := json.Unmarshal(payloadBytes, &data); err != nil {
		return fmt.Errorf("error unmarshaling payload: %w", err)
	}

	switch data["type"] {
	case "token_price":
		return ch.processTokenPrice(ctx, data)
	case "payment":
		return ch.processPayment(ctx, data)
	case "create_account":
		return ch.processCreateAccount(ctx, data)
	default:
		return fmt.Errorf("unsupported operation type: %v", data["type"])
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

func (ch *SaveToClickHouse) GetPriceHistory(ctx context.Context, baseAsset, quoteAsset string, interval string, start, end time.Time) ([]map[string]interface{}, error) {
	query := `
        SELECT
            date,
            hour,
            avg_price,
            low_price,
            high_price,
            total_volume,
            total_trades
        FROM token_price_analytics
        WHERE base_asset = ? AND quote_asset = ?
            AND date BETWEEN ? AND ?
        ORDER BY date, hour
    `

	rows, err := ch.conn.Query(ctx, query, baseAsset, quoteAsset, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]interface{}
	// Process rows and return results
	return results, nil
}

func (ch *SaveToClickHouse) processTokenPrice(ctx context.Context, data map[string]interface{}) error {
	// Prepare the query
	query := `
        INSERT INTO token_prices (
            timestamp,
            base_asset,
            base_issuer,
            quote_asset,
            quote_issuer,
            price,
            volume_24h,
            num_trades_24h,
            high_price_24h,
            low_price_24h,
            price_change_24h
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `

	// Parse timestamp
	ts, err := time.Parse(time.RFC3339, data["timestamp"].(string))
	if err != nil {
		return fmt.Errorf("error parsing timestamp: %w", err)
	}

	// Extract values with safe type assertions and default values
	baseAsset := getClickhouseStringValue(data, "base_asset", "")
	baseIssuer := getClickhouseStringValue(data, "base_issuer", "")
	quoteAsset := getClickhouseStringValue(data, "quote_asset", "")
	quoteIssuer := getClickhouseStringValue(data, "quote_issuer", "")
	price := getFloat64Value(data, "price", 0)
	volume24h := getFloat64Value(data, "volume_24h", 0)
	numTrades24h := getInt64Value(data, "num_trades_24h", 0)
	highPrice24h := getFloat64Value(data, "high_price_24h", 0)
	lowPrice24h := getFloat64Value(data, "low_price_24h", 0)
	priceChange24h := getFloat64Value(data, "price_change_24h", 0)

	// Execute the insert
	if err := ch.conn.Exec(ctx, query,
		ts,
		baseAsset,
		baseIssuer,
		quoteAsset,
		quoteIssuer,
		price,
		volume24h,
		numTrades24h,
		highPrice24h,
		lowPrice24h,
		priceChange24h,
	); err != nil {
		return fmt.Errorf("error inserting token price: %w", err)
	}

	return nil
}

// Helper functions for safe type assertions
func getClickhouseStringValue(data map[string]interface{}, key, defaultValue string) string {
	if val, ok := data[key].(string); ok {
		return val
	}
	return defaultValue
}

func getFloat64Value(data map[string]interface{}, key string, defaultValue float64) float64 {
	if val, ok := data[key].(float64); ok {
		return val
	}
	return defaultValue
}

func getInt64Value(data map[string]interface{}, key string, defaultValue int64) int64 {
	switch v := data[key].(type) {
	case int64:
		return v
	case float64:
		return int64(v)
	case int:
		return int64(v)
	default:
		return defaultValue
	}
}
