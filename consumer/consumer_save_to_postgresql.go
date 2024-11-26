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

type SaveToPostgreSQL struct {
	db         *sql.DB
	processors []processor.Processor
}

type PostgresConfig struct {
	Host         string
	Port         int
	Database     string
	Username     string
	Password     string
	SSLMode      string
	MaxOpenConns int
	MaxIdleConns int
}

const initSchema = `
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'event_type') THEN
        CREATE TYPE event_type AS ENUM (
            'payment', 'create_account', 'trustline', 'trade'
        );
    END IF;
END $$;

-- Base events table
CREATE TABLE IF NOT EXISTS stellar_events (
    id                  BIGSERIAL PRIMARY KEY,
    event_type         event_type NOT NULL,
    ledger_sequence    BIGINT NOT NULL,
    timestamp          TIMESTAMPTZ NOT NULL,
    transaction_hash   VARCHAR(64),
    created_at        TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT check_timestamp_range 
        CHECK (timestamp >= '2015-01-01' AND timestamp <= '2100-01-01')
);

-- Payment events
CREATE TABLE IF NOT EXISTS payments (
    id                      BIGSERIAL PRIMARY KEY,
    event_id               BIGINT REFERENCES stellar_events(id),
    buyer_account_id       VARCHAR(56) NOT NULL,
    seller_account_id      VARCHAR(56) NOT NULL,
    asset_code            VARCHAR(12) NOT NULL,
    amount                NUMERIC(20,7) NOT NULL,
    memo                  TEXT,
    CONSTRAINT fk_event
        FOREIGN KEY (event_id) 
        REFERENCES stellar_events(id)
        ON DELETE CASCADE
);

-- Create account events
CREATE TABLE IF NOT EXISTS account_creations (
    id                      BIGSERIAL PRIMARY KEY,
    event_id               BIGINT REFERENCES stellar_events(id),
    funder                 VARCHAR(56) NOT NULL,
    account               VARCHAR(56) NOT NULL,
    starting_balance      NUMERIC(20,7) NOT NULL,
    CONSTRAINT fk_event
        FOREIGN KEY (event_id) 
        REFERENCES stellar_events(id)
        ON DELETE CASCADE
);

-- Trustline events
CREATE TABLE IF NOT EXISTS trustlines (
    id                      BIGSERIAL PRIMARY KEY,
    event_id               BIGINT REFERENCES stellar_events(id),
    account_id            VARCHAR(56) NOT NULL,
    asset_code            VARCHAR(12) NOT NULL,
    asset_issuer          VARCHAR(56) NOT NULL,
    limit_amount          NUMERIC(20,7),
    action                VARCHAR(20) NOT NULL,
    auth_flags            INTEGER,
    auth_required         BOOLEAN,
    auth_revocable        BOOLEAN,
    auth_immutable        BOOLEAN,
    CONSTRAINT fk_event
        FOREIGN KEY (event_id) 
        REFERENCES stellar_events(id)
        ON DELETE CASCADE
);

-- Trade events
CREATE TABLE IF NOT EXISTS trades (
    id                      BIGSERIAL PRIMARY KEY,
    event_id               BIGINT REFERENCES stellar_events(id),
    seller_id             VARCHAR(56) NOT NULL,
    buyer_id              VARCHAR(56) NOT NULL,
    sell_asset            VARCHAR(12) NOT NULL,
    buy_asset             VARCHAR(12) NOT NULL,
    sell_amount           NUMERIC(20,7) NOT NULL,
    buy_amount            NUMERIC(20,7) NOT NULL,
    price                 NUMERIC(20,7) NOT NULL,
    liquidity_pool        VARCHAR(56),
    CONSTRAINT fk_event
        FOREIGN KEY (event_id) 
        REFERENCES stellar_events(id)
        ON DELETE CASCADE
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON stellar_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_events_type ON stellar_events(event_type);
CREATE INDEX IF NOT EXISTS idx_payments_accounts ON payments(buyer_account_id, seller_account_id);
CREATE INDEX IF NOT EXISTS idx_payments_asset ON payments(asset_code);
CREATE INDEX IF NOT EXISTS idx_trustlines_account ON trustlines(account_id);
CREATE INDEX IF NOT EXISTS idx_trustlines_asset ON trustlines(asset_code, asset_issuer);
CREATE INDEX IF NOT EXISTS idx_trades_assets ON trades(sell_asset, buy_asset);
`

func NewSaveToPostgreSQL(config map[string]interface{}) (*SaveToPostgreSQL, error) {
	pgConfig, err := parsePostgresConfig(config)
	if err != nil {
		return nil, err
	}

	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		pgConfig.Host, pgConfig.Port, pgConfig.Username, pgConfig.Password,
		pgConfig.Database, pgConfig.SSLMode,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("error connecting to PostgreSQL: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(pgConfig.MaxOpenConns)
	db.SetMaxIdleConns(pgConfig.MaxIdleConns)
	db.SetConnMaxLifetime(time.Hour)

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("error pinging PostgreSQL: %w", err)
	}

	// Initialize schema
	if err := initializeSchema(db); err != nil {
		return nil, fmt.Errorf("error initializing schema: %w", err)
	}

	return &SaveToPostgreSQL{
		db: db,
	}, nil
}

func parsePostgresConfig(config map[string]interface{}) (PostgresConfig, error) {
	var pgConfig PostgresConfig

	host, ok := config["host"].(string)
	if !ok {
		return pgConfig, fmt.Errorf("missing host in config")
	}
	pgConfig.Host = host

	port, ok := config["port"].(float64)
	if !ok {
		pgConfig.Port = 5432 // Default PostgreSQL port
	} else {
		pgConfig.Port = int(port)
	}

	database, ok := config["database"].(string)
	if !ok {
		return pgConfig, fmt.Errorf("missing database in config")
	}
	pgConfig.Database = database

	username, ok := config["username"].(string)
	if !ok {
		return pgConfig, fmt.Errorf("missing username in config")
	}
	pgConfig.Username = username

	password, ok := config["password"].(string)
	if !ok {
		return pgConfig, fmt.Errorf("missing password in config")
	}
	pgConfig.Password = password

	sslMode, ok := config["ssl_mode"].(string)
	if !ok {
		pgConfig.SSLMode = "disable" // Default to disable
	} else {
		pgConfig.SSLMode = sslMode
	}

	// Set connection pool defaults
	pgConfig.MaxOpenConns = 25
	pgConfig.MaxIdleConns = 5

	if maxOpen, ok := config["max_open_conns"].(float64); ok {
		pgConfig.MaxOpenConns = int(maxOpen)
	}
	if maxIdle, ok := config["max_idle_conns"].(float64); ok {
		pgConfig.MaxIdleConns = int(maxIdle)
	}

	return pgConfig, nil
}

func initializeSchema(db *sql.DB) error {
	_, err := db.Exec(initSchema)
	return err
}

func (p *SaveToPostgreSQL) Subscribe(processor processor.Processor) {
	p.processors = append(p.processors, processor)
}

// Only showing the key changes needed to fix type assertions.
// The rest of the code remains the same as the previous version.

func (p *SaveToPostgreSQL) Process(ctx context.Context, msg processor.Message) error {
	payload, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte payload, got %T", msg.Payload)
	}

	// Start transaction
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}
	defer tx.Rollback() // Will be ignored if transaction is committed

	// Parse the event
	var data map[string]interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		return fmt.Errorf("error unmarshaling payload: %w", err)
	}

	eventType, ok := data["type"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid event type")
	}

	// Convert timestamp to proper format
	timestamp, err := parsePostgresTimestamp(data)
	if err != nil {
		return fmt.Errorf("error parsing timestamp: %w", err)
	}

	// Extract and validate ledger sequence
	ledgerSequence, err := extractLedgerSequence(data)
	if err != nil {
		return fmt.Errorf("error extracting ledger sequence: %w", err)
	}

	// Extract transaction hash, defaulting to empty string if not present
	txHash, _ := data["tx_hash"].(string)

	// Log the values we're about to insert for debugging
	log.Printf("Inserting event - Type: %s, Ledger: %d, Timestamp: %v, TxHash: %s",
		eventType, ledgerSequence, timestamp, txHash)

	var eventID int64
	err = tx.QueryRowContext(ctx,
		`INSERT INTO stellar_events (event_type, ledger_sequence, timestamp, transaction_hash)
		VALUES ($1, $2, $3, $4)
		RETURNING id`,
		eventType,
		ledgerSequence,
		timestamp,
		txHash).Scan(&eventID)
	if err != nil {
		return fmt.Errorf("error inserting event: %w", err)
	}

	// Rest of the processing remains the same...
	return nil
}

// Updated timestamp parsing function
func parsePostgresTimestamp(data map[string]interface{}) (time.Time, error) {
	raw, exists := data["timestamp"]
	if !exists {
		return time.Time{}, fmt.Errorf("timestamp field missing")
	}

	switch v := raw.(type) {
	case string:
		// Try parsing as Unix timestamp first
		if unixTime, err := strconv.ParseInt(v, 10, 64); err == nil {
			return time.Unix(unixTime, 0).UTC(), nil
		}

		// Try parsing as RFC3339
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t, nil
		}

		// Try parsing as RFC3339Nano
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return t, nil
		}

		return time.Time{}, fmt.Errorf("unable to parse timestamp string: %s", v)

	case float64:
		return time.Unix(int64(v), 0).UTC(), nil

	case int64:
		return time.Unix(v, 0).UTC(), nil

	case int:
		return time.Unix(int64(v), 0).UTC(), nil

	case json.Number:
		if i, err := v.Int64(); err == nil {
			return time.Unix(i, 0).UTC(), nil
		}
		return time.Time{}, fmt.Errorf("unable to parse json.Number timestamp: %v", v)

	default:
		return time.Time{}, fmt.Errorf("unsupported timestamp type: %T", raw)
	}
}

// Helper function to safely get a string value from a map
func getStringValue(data map[string]interface{}, key string) (string, bool) {
	if raw, exists := data[key]; exists {
		if str, ok := raw.(string); ok {
			return str, true
		}
	}
	return "", false
}

// Helper function to safely get a numeric value from a map
func getNumericValue(data map[string]interface{}, key string) (float64, bool) {
	if raw, exists := data[key]; exists {
		switch v := raw.(type) {
		case float64:
			return v, true
		case int64:
			return float64(v), true
		case int:
			return float64(v), true
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f, true
			}
		case json.Number:
			if f, err := v.Float64(); err == nil {
				return f, true
			}
		}
	}
	return 0, false
}

// Helper function to safely get a boolean value from a map
func getBoolValue(data map[string]interface{}, key string) (bool, bool) {
	if raw, exists := data[key]; exists {
		if b, ok := raw.(bool); ok {
			return b, true
		}
		if s, ok := raw.(string); ok {
			if b, err := strconv.ParseBool(s); err == nil {
				return b, true
			}
		}
	}
	return false, false
}

func (p *SaveToPostgreSQL) processPayment(ctx context.Context, tx *sql.Tx, eventID int64, data map[string]interface{}) error {
	// Extract required fields using helper functions
	buyerAccountID, ok := getStringValue(data, "buyer_account_id")
	if !ok || buyerAccountID == "" {
		return fmt.Errorf("missing or invalid buyer_account_id")
	}

	sellerAccountID, ok := getStringValue(data, "seller_account_id")
	if !ok || sellerAccountID == "" {
		return fmt.Errorf("missing or invalid seller_account_id")
	}

	assetCode, ok := getStringValue(data, "asset_code")
	if !ok || assetCode == "" {
		return fmt.Errorf("missing or invalid asset_code")
	}

	amount, ok := getNumericValue(data, "amount")
	if !ok {
		return fmt.Errorf("missing or invalid amount")
	}

	memo, _ := getStringValue(data, "memo")

	log.Printf("Processing payment - Buyer: %s, Seller: %s, Asset: %s, Amount: %f",
		buyerAccountID, sellerAccountID, assetCode, amount)

	_, err := tx.ExecContext(ctx,
		`INSERT INTO payments (event_id, buyer_account_id, seller_account_id, asset_code, amount, memo)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		eventID,
		buyerAccountID,
		sellerAccountID,
		assetCode,
		amount,
		memo,
	)

	if err != nil {
		return fmt.Errorf("error inserting payment: %w, values: %v", err,
			[]interface{}{eventID, buyerAccountID, sellerAccountID, assetCode, amount, memo})
	}

	return nil
}

// Helper function to extract and validate ledger sequence
func extractLedgerSequence(data map[string]interface{}) (int64, error) {
	// Check different possible field names for ledger sequence
	for _, field := range []string{"ledger", "ledger_sequence", "sequence", "ledger_seq"} {
		if val, ok := data[field]; ok {
			switch v := val.(type) {
			case float64:
				return int64(v), nil
			case int64:
				return v, nil
			case int:
				return int64(v), nil
			case string:
				if seq, err := strconv.ParseInt(v, 10, 64); err == nil {
					return seq, nil
				}
			case json.Number:
				if seq, err := v.Int64(); err == nil {
					return seq, nil
				}
			}
		}
	}

	// If no sequence found in fields, check if it's in a nested structure
	if ledger, ok := data["ledger"].(map[string]interface{}); ok {
		if seq, ok := ledger["sequence"]; ok {
			switch v := seq.(type) {
			case float64:
				return int64(v), nil
			case int64:
				return v, nil
			case int:
				return int64(v), nil
			case string:
				if seqNum, err := strconv.ParseInt(v, 10, 64); err == nil {
					return seqNum, nil
				}
			}
		}
	}

	return 0, fmt.Errorf("no valid ledger sequence found in event data: %v", data)
}

func (p *SaveToPostgreSQL) processCreateAccount(ctx context.Context, tx *sql.Tx, eventID int64, data map[string]interface{}) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO account_creations (event_id, funder, account, starting_balance)
		VALUES ($1, $2, $3, $4)`,
		eventID,
		data["funder"],
		data["account"],
		data["starting_balance"],
	)
	return err
}

func (p *SaveToPostgreSQL) processTrustline(ctx context.Context, tx *sql.Tx, eventID int64, data map[string]interface{}) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO trustlines (
			event_id, account_id, asset_code, asset_issuer, limit_amount,
			action, auth_flags, auth_required, auth_revocable, auth_immutable
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		eventID,
		data["account_id"],
		data["asset_code"],
		data["asset_issuer"],
		data["limit"],
		data["action"],
		data["auth_flags"],
		data["auth_required"],
		data["auth_revocable"],
		data["auth_immutable"],
	)
	return err
}

func (p *SaveToPostgreSQL) processTrade(ctx context.Context, tx *sql.Tx, eventID int64, data map[string]interface{}) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO trades (
			event_id, seller_id, buyer_id, sell_asset, buy_asset,
			sell_amount, buy_amount, price, liquidity_pool
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		eventID,
		data["seller_id"],
		data["buyer_id"],
		data["sell_asset"],
		data["buy_asset"],
		data["sell_amount"],
		data["buy_amount"],
		data["price"],
		data["liquidity_pool"],
	)
	return err
}

func (p *SaveToPostgreSQL) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}
