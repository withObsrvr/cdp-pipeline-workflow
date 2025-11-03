package consumer

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
	_ "github.com/lib/pq" // PostgreSQL driver
)

// SaveEventPaymentToPostgreSQL consumes EventPayment messages and saves them to PostgreSQL
// Creates accounts table and event_payments table with proper foreign key relationships
type SaveEventPaymentToPostgreSQL struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
	SSLMode  string

	MaxOpenConns int
	MaxIdleConns int

	db    *sql.DB
	stats SaveEventPaymentStats
	mu    sync.RWMutex
}

// SaveEventPaymentStats tracks consumer statistics
type SaveEventPaymentStats struct {
	TotalProcessed   uint64
	AccountsUpserted uint64
	PaymentsSaved    uint64
	Errors           uint64
}

// NewSaveEventPaymentToPostgreSQL creates a new PostgreSQL consumer for EventPayment
func NewSaveEventPaymentToPostgreSQL(config map[string]interface{}) (*SaveEventPaymentToPostgreSQL, error) {
	consumer := &SaveEventPaymentToPostgreSQL{
		Host:         getStringConfig(config, "host", "localhost"),
		Port:         getIntConfig(config, "port", 5432),
		Database:     getStringConfig(config, "database", "postgres"),
		Username:     getStringConfig(config, "username", "postgres"),
		Password:     getStringConfig(config, "password", ""),
		SSLMode:      getStringConfig(config, "sslmode", "disable"),
		MaxOpenConns: getIntConfig(config, "max_open_conns", 10),
		MaxIdleConns: getIntConfig(config, "max_idle_conns", 5),
	}

	// Support environment variables for sensitive data
	if envPassword := os.Getenv("DB_PASSWORD"); envPassword != "" {
		consumer.Password = envPassword
	}
	if envHost := os.Getenv("DB_HOST"); envHost != "" {
		consumer.Host = envHost
	}
	if envDatabase := os.Getenv("DB_DATABASE"); envDatabase != "" {
		consumer.Database = envDatabase
	}

	// Build connection string
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		consumer.Host, consumer.Port, consumer.Username, consumer.Password,
		consumer.Database, consumer.SSLMode)

	log.Printf("SaveEventPaymentToPostgreSQL: Connecting to PostgreSQL at %s:%d/%s",
		consumer.Host, consumer.Port, consumer.Database)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(consumer.MaxOpenConns)
	db.SetMaxIdleConns(consumer.MaxIdleConns)

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	consumer.db = db

	log.Printf("SaveEventPaymentToPostgreSQL: Connected to PostgreSQL successfully")

	// Initialize database schema
	if err := consumer.initSchema(); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return consumer, nil
}

// initSchema creates the necessary tables and indexes
func (c *SaveEventPaymentToPostgreSQL) initSchema() error {
	log.Printf("SaveEventPaymentToPostgreSQL: Initializing database schema")

	// Create accounts table
	accountsTableSQL := `
	CREATE TABLE IF NOT EXISTS accounts (
		id TEXT PRIMARY KEY
	);
	`

	if _, err := c.db.Exec(accountsTableSQL); err != nil {
		return fmt.Errorf("failed to create accounts table: %w", err)
	}

	// Create event_payments table
	eventPaymentsTableSQL := `
	CREATE TABLE IF NOT EXISTS event_payments (
		id TEXT PRIMARY KEY,
		payment_id TEXT NOT NULL,
		token_id TEXT NOT NULL REFERENCES accounts(id),
		amount BIGINT NOT NULL,
		from_id TEXT NOT NULL REFERENCES accounts(id),
		merchant_id TEXT NOT NULL REFERENCES accounts(id),
		royalty_amount BIGINT NOT NULL,
		tx_hash TEXT NOT NULL,
		block_height BIGINT NOT NULL,
		block_timestamp TIMESTAMP NOT NULL
	);
	`

	if _, err := c.db.Exec(eventPaymentsTableSQL); err != nil {
		return fmt.Errorf("failed to create event_payments table: %w", err)
	}

	// Create indexes for fast queries
	indexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_payment_id ON event_payments(payment_id);",
		"CREATE INDEX IF NOT EXISTS idx_token_id ON event_payments(token_id);",
		"CREATE INDEX IF NOT EXISTS idx_merchant_id ON event_payments(merchant_id);",
		"CREATE INDEX IF NOT EXISTS idx_tx_hash ON event_payments(tx_hash);",
		"CREATE INDEX IF NOT EXISTS idx_block_height ON event_payments(block_height);",
		"CREATE INDEX IF NOT EXISTS idx_block_timestamp ON event_payments(block_timestamp);",
	}

	for _, indexSQL := range indexes {
		if _, err := c.db.Exec(indexSQL); err != nil {
			log.Printf("Warning: Failed to create index: %v", err)
			// Don't fail if index creation fails (might already exist)
		}
	}

	log.Printf("SaveEventPaymentToPostgreSQL: Schema initialized successfully")

	return nil
}

// Process consumes EventPayment messages and saves them to PostgreSQL
func (c *SaveEventPaymentToPostgreSQL) Process(ctx context.Context, msg processor.Message) error {
	// Expect EventPayment payload
	eventPayment, ok := msg.Payload.(*processor.EventPayment)
	if !ok {
		return fmt.Errorf("expected *processor.EventPayment payload, got %T", msg.Payload)
	}

	c.mu.Lock()
	c.stats.TotalProcessed++
	c.mu.Unlock()

	// Start a transaction
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		c.mu.Lock()
		c.stats.Errors++
		c.mu.Unlock()
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Rollback if not committed

	// Upsert accounts (from, merchant, token)
	// Use ON CONFLICT DO NOTHING for idempotency
	accountsSQL := `
		INSERT INTO accounts (id) VALUES ($1), ($2), ($3)
		ON CONFLICT (id) DO NOTHING
	`

	_, err = tx.ExecContext(ctx, accountsSQL,
		eventPayment.FromID,
		eventPayment.MerchantID,
		eventPayment.TokenID,
	)
	if err != nil {
		c.mu.Lock()
		c.stats.Errors++
		c.mu.Unlock()
		return fmt.Errorf("failed to upsert accounts: %w", err)
	}

	c.mu.Lock()
	c.stats.AccountsUpserted += 3
	c.mu.Unlock()

	// Insert event_payment record
	// Use ON CONFLICT DO NOTHING for idempotency (in case of retries)
	paymentSQL := `
		INSERT INTO event_payments (
			id, payment_id, token_id, amount, from_id,
			merchant_id, royalty_amount, tx_hash,
			block_height, block_timestamp
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (id) DO NOTHING
	`

	_, err = tx.ExecContext(ctx, paymentSQL,
		eventPayment.ID,
		eventPayment.PaymentID,
		eventPayment.TokenID,
		eventPayment.Amount,
		eventPayment.FromID,
		eventPayment.MerchantID,
		eventPayment.RoyaltyAmount,
		eventPayment.TxHash,
		eventPayment.BlockHeight,
		eventPayment.BlockTimestamp,
	)
	if err != nil {
		c.mu.Lock()
		c.stats.Errors++
		c.mu.Unlock()
		return fmt.Errorf("failed to insert event_payment: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		c.mu.Lock()
		c.stats.Errors++
		c.mu.Unlock()
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	c.mu.Lock()
	c.stats.PaymentsSaved++
	c.mu.Unlock()

	log.Printf("SaveEventPaymentToPostgreSQL: Saved payment id=%s, payment_id=%s, block=%d",
		eventPayment.ID, eventPayment.PaymentID, eventPayment.BlockHeight)

	return nil
}

// Subscribe is not implemented for consumers (they are end of pipeline)
func (c *SaveEventPaymentToPostgreSQL) Subscribe(processor processor.Processor) {
	log.Printf("SaveEventPaymentToPostgreSQL: Subscribe called but consumers don't support subscriptions")
}

// GetStats returns current processing statistics
func (c *SaveEventPaymentToPostgreSQL) GetStats() SaveEventPaymentStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats
}

// Close closes the database connection and prints statistics
func (c *SaveEventPaymentToPostgreSQL) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("SaveEventPaymentToPostgreSQL Stats: Processed %d events, Saved %d payments, Upserted %d accounts, Errors %d",
		c.stats.TotalProcessed, c.stats.PaymentsSaved, c.stats.AccountsUpserted, c.stats.Errors)

	if c.db != nil {
		return c.db.Close()
	}

	return nil
}

// Helper functions for config parsing
func getStringConfig(config map[string]interface{}, key, defaultValue string) string {
	if val, ok := config[key].(string); ok {
		return val
	}
	return defaultValue
}

func getIntConfig(config map[string]interface{}, key string, defaultValue int) int {
	if val, ok := config[key].(int); ok {
		return val
	}
	if val, ok := config[key].(float64); ok {
		return int(val)
	}
	return defaultValue
}

// Ensure SaveEventPaymentToPostgreSQL implements Processor interface
var _ processor.Processor = (*SaveEventPaymentToPostgreSQL)(nil)
