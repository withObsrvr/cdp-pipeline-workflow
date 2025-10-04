package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// BufferedPostgreSQLConfig extends PostgresConfig with buffer settings
type BufferedPostgreSQLConfig struct {
	PostgresConfig
	BufferSize       int           // Number of records to buffer before flushing
	FlushIntervalMs  int           // Time in milliseconds between flushes
	MaxRetries       int           // Maximum number of retry attempts
	RetryDelayMs     int           // Delay between retries in milliseconds
}

// BufferedPostgreSQL implements a buffered database consumer with batch inserts
type BufferedPostgreSQL struct {
	db          *sql.DB
	processors  []processor.Processor
	config      BufferedPostgreSQLConfig
	
	// Buffer management
	buffer      []interface{}
	bufferMu    sync.Mutex
	flushTicker *time.Ticker
	done        chan bool
	wg          sync.WaitGroup
	
	// Metrics
	totalProcessed uint64
	totalFlushed   uint64
	failedFlushes  uint64
}

// ConsumerBufferedPostgreSQL creates a new buffered PostgreSQL consumer
func ConsumerBufferedPostgreSQL(config map[string]interface{}) processor.Processor {
	// Parse configuration
	bufConfig := BufferedPostgreSQLConfig{
		PostgresConfig: PostgresConfig{
			Host:           config["host"].(string),
			Port:           int(config["port"].(float64)),
			Database:       config["database"].(string),
			Username:       config["username"].(string),
			Password:       config["password"].(string),
			SSLMode:        "require",
			MaxOpenConns:   10,
			MaxIdleConns:   5,
			ConnectTimeout: 10,
		},
		BufferSize:      1000,
		FlushIntervalMs: 5000,
		MaxRetries:      3,
		RetryDelayMs:    1000,
	}
	
	// Override defaults with config values
	if sslMode, ok := config["sslmode"].(string); ok {
		bufConfig.SSLMode = sslMode
	}
	if bufferSize, ok := config["buffer_size"].(float64); ok {
		bufConfig.BufferSize = int(bufferSize)
	}
	if flushInterval, ok := config["flush_interval_ms"].(float64); ok {
		bufConfig.FlushIntervalMs = int(flushInterval)
	}
	if maxRetries, ok := config["max_retries"].(float64); ok {
		bufConfig.MaxRetries = int(maxRetries)
	}
	if retryDelay, ok := config["retry_delay_ms"].(float64); ok {
		bufConfig.RetryDelayMs = int(retryDelay)
	}
	if maxOpenConns, ok := config["max_open_conns"].(float64); ok {
		bufConfig.MaxOpenConns = int(maxOpenConns)
	}
	if maxIdleConns, ok := config["max_idle_conns"].(float64); ok {
		bufConfig.MaxIdleConns = int(maxIdleConns)
	}
	
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s connect_timeout=%d",
		bufConfig.Host, bufConfig.Port, bufConfig.Username, bufConfig.Password, 
		bufConfig.Database, bufConfig.SSLMode, bufConfig.ConnectTimeout)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(bufConfig.MaxOpenConns)
	db.SetMaxIdleConns(bufConfig.MaxIdleConns)
	db.SetConnMaxLifetime(time.Hour)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("Failed to ping PostgreSQL: %v", err)
	}

	// Initialize schema
	if err := initializeBufferedSchema(db); err != nil {
		log.Fatalf("Failed to initialize schema: %v", err)
	}

	consumer := &BufferedPostgreSQL{
		db:         db,
		config:     bufConfig,
		buffer:     make([]interface{}, 0, bufConfig.BufferSize),
		done:       make(chan bool),
		processors: []processor.Processor{},
	}

	// Start background flush routine
	consumer.startFlushRoutine()

	return consumer
}

// Subscribe sets the processors for this consumer
func (c *BufferedPostgreSQL) Subscribe(p processor.Processor) {
	c.processors = append(c.processors, p)
}

// Process adds a message to the buffer for batch processing
func (c *BufferedPostgreSQL) Process(ctx context.Context, msg processor.Message) error {
	c.bufferMu.Lock()
	defer c.bufferMu.Unlock()
	
	c.buffer = append(c.buffer, msg.Payload)
	c.totalProcessed++
	
	// Check if buffer is full
	if len(c.buffer) >= c.config.BufferSize {
		// Clone buffer for flushing
		toFlush := make([]interface{}, len(c.buffer))
		copy(toFlush, c.buffer)
		c.buffer = c.buffer[:0] // Clear buffer
		
		// Flush asynchronously
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			if err := c.flushBatch(ctx, toFlush); err != nil {
				log.Printf("Failed to flush batch: %v", err)
				c.failedFlushes++
			}
		}()
	}
	
	return nil
}

// startFlushRoutine starts the background flush routine
func (c *BufferedPostgreSQL) startFlushRoutine() {
	c.flushTicker = time.NewTicker(time.Duration(c.config.FlushIntervalMs) * time.Millisecond)
	
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-c.flushTicker.C:
				c.flushPending()
			case <-c.done:
				// Final flush before shutdown
				c.flushPending()
				return
			}
		}
	}()
}

// flushPending flushes any pending records in the buffer
func (c *BufferedPostgreSQL) flushPending() {
	c.bufferMu.Lock()
	if len(c.buffer) == 0 {
		c.bufferMu.Unlock()
		return
	}
	
	// Clone buffer for flushing
	toFlush := make([]interface{}, len(c.buffer))
	copy(toFlush, c.buffer)
	c.buffer = c.buffer[:0] // Clear buffer
	c.bufferMu.Unlock()
	
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	if err := c.flushBatch(ctx, toFlush); err != nil {
		log.Printf("Failed to flush pending batch: %v", err)
		c.failedFlushes++
	}
}

// flushBatch performs a batch insert with retry logic
func (c *BufferedPostgreSQL) flushBatch(ctx context.Context, batch []interface{}) error {
	if len(batch) == 0 {
		return nil
	}
	
	var err error
	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(c.config.RetryDelayMs) * time.Millisecond)
		}
		
		err = c.executeBatchInsert(ctx, batch)
		if err == nil {
			c.totalFlushed += uint64(len(batch))
			return nil
		}
		
		log.Printf("Batch insert attempt %d failed: %v", attempt+1, err)
	}
	
	return fmt.Errorf("batch insert failed after %d attempts: %w", c.config.MaxRetries+1, err)
}

// executeBatchInsert performs the actual batch database insert
func (c *BufferedPostgreSQL) executeBatchInsert(ctx context.Context, batch []interface{}) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	
	// Group records by type for efficient batch inserts
	stateChanges := make([]interface{}, 0)
	participants := make([]interface{}, 0)
	genericRecords := make([]interface{}, 0)
	
	for _, record := range batch {
		switch v := record.(type) {
		case *processor.WalletBackendOutput:
			stateChanges = append(stateChanges, v)
		case *processor.ParticipantOutput:
			participants = append(participants, v)
		default:
			genericRecords = append(genericRecords, v)
		}
	}
	
	// Insert state changes
	if len(stateChanges) > 0 {
		if err := c.batchInsertStateChanges(ctx, tx, stateChanges); err != nil {
			return err
		}
	}
	
	// Insert participants
	if len(participants) > 0 {
		if err := c.batchInsertParticipants(ctx, tx, participants); err != nil {
			return err
		}
	}
	
	// Insert generic records
	if len(genericRecords) > 0 {
		if err := c.batchInsertGeneric(ctx, tx, genericRecords); err != nil {
			return err
		}
	}
	
	return tx.Commit()
}

// batchInsertStateChanges inserts wallet backend state changes
func (c *BufferedPostgreSQL) batchInsertStateChanges(ctx context.Context, tx *sql.Tx, records []interface{}) error {
	// Prepare statements
	stmtTransaction, err := tx.PrepareContext(ctx, `
		INSERT INTO transactions (ledger_sequence, transaction_hash, transaction_index, timestamp)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (transaction_hash) DO NOTHING
		RETURNING id
	`)
	if err != nil {
		return err
	}
	defer stmtTransaction.Close()
	
	stmtStateChange, err := tx.PrepareContext(ctx, `
		INSERT INTO state_changes (
			transaction_id, operation_index, application_order, change_type,
			account, timestamp, asset_code, asset_issuer, asset_type,
			amount, balance_before, balance_after, metadata
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
	`)
	if err != nil {
		return err
	}
	defer stmtStateChange.Close()
	
	// Process each record
	for _, record := range records {
		output := record.(*processor.WalletBackendOutput)
		
		// Insert transaction
		var txID int64
		err := stmtTransaction.QueryRowContext(ctx,
			output.LedgerSequence,
			output.Changes[0].TransactionHash,
			output.Changes[0].TransactionIndex,
			time.Unix(output.Changes[0].Timestamp, 0),
		).Scan(&txID)
		
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		
		// Insert state changes
		for _, change := range output.Changes {
			metadata, _ := json.Marshal(change.Metadata)
			
			var assetCode, assetIssuer, assetType sql.NullString
			if change.Asset != nil {
				assetCode = sql.NullString{String: change.Asset.Code, Valid: true}
				assetIssuer = sql.NullString{String: change.Asset.Issuer, Valid: change.Asset.Issuer != ""}
				assetType = sql.NullString{String: change.Asset.Type, Valid: true}
			}
			
			_, err = stmtStateChange.ExecContext(ctx,
				txID,
				change.OperationIndex,
				change.ApplicationOrder,
				string(change.Type),
				change.Account,
				time.Unix(change.Timestamp, 0),
				assetCode,
				assetIssuer,
				assetType,
				change.Amount,
				change.BalanceBefore,
				change.BalanceAfter,
				metadata,
			)
			if err != nil {
				return err
			}
		}
	}
	
	return nil
}

// batchInsertParticipants inserts participant records
func (c *BufferedPostgreSQL) batchInsertParticipants(ctx context.Context, tx *sql.Tx, records []interface{}) error {
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO transaction_participants (
			transaction_hash, ledger_sequence, timestamp,
			participant, role
		) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (transaction_hash, participant, role) DO NOTHING
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	
	for _, record := range records {
		output := record.(*processor.ParticipantOutput)
		
		// Insert source accounts
		for _, account := range output.SourceAccounts {
			_, err = stmt.ExecContext(ctx,
				output.TransactionHash,
				output.LedgerSequence,
				time.Unix(output.Timestamp, 0),
				account,
				"source",
			)
			if err != nil {
				return err
			}
		}
		
		// Insert destination accounts
		for _, account := range output.DestinationAccounts {
			_, err = stmt.ExecContext(ctx,
				output.TransactionHash,
				output.LedgerSequence,
				time.Unix(output.Timestamp, 0),
				account,
				"destination",
			)
			if err != nil {
				return err
			}
		}
	}
	
	return nil
}

// batchInsertGeneric handles generic record types
func (c *BufferedPostgreSQL) batchInsertGeneric(ctx context.Context, tx *sql.Tx, records []interface{}) error {
	// For generic records, store as JSON
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO generic_events (timestamp, data)
		VALUES ($1, $2)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	
	for _, record := range records {
		data, err := json.Marshal(record)
		if err != nil {
			return err
		}
		
		_, err = stmt.ExecContext(ctx, time.Now(), data)
		if err != nil {
			return err
		}
	}
	
	return nil
}

// Close gracefully shuts down the consumer
func (c *BufferedPostgreSQL) Close() error {
	// Signal shutdown
	close(c.done)
	
	// Stop ticker
	if c.flushTicker != nil {
		c.flushTicker.Stop()
	}
	
	// Wait for all goroutines to finish
	c.wg.Wait()
	
	// Final metrics
	log.Printf("BufferedPostgreSQL consumer shutting down. Total processed: %d, Total flushed: %d, Failed flushes: %d",
		c.totalProcessed, c.totalFlushed, c.failedFlushes)
	
	// Close database connection
	return c.db.Close()
}

// initializeBufferedSchema creates the necessary database schema
func initializeBufferedSchema(db *sql.DB) error {
	schema := `
	-- Transactions table
	CREATE TABLE IF NOT EXISTS transactions (
		id BIGSERIAL PRIMARY KEY,
		ledger_sequence BIGINT NOT NULL,
		transaction_hash VARCHAR(64) UNIQUE NOT NULL,
		transaction_index INT NOT NULL,
		timestamp TIMESTAMPTZ NOT NULL,
		created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
	);
	
	-- State changes table (wallet-backend compatible)
	CREATE TABLE IF NOT EXISTS state_changes (
		id BIGSERIAL PRIMARY KEY,
		transaction_id BIGINT REFERENCES transactions(id),
		operation_index INT NOT NULL,
		application_order INT NOT NULL,
		change_type VARCHAR(50) NOT NULL,
		account VARCHAR(56) NOT NULL,
		timestamp TIMESTAMPTZ NOT NULL,
		asset_code VARCHAR(12),
		asset_issuer VARCHAR(56),
		asset_type VARCHAR(20),
		amount VARCHAR(40),
		balance_before VARCHAR(40),
		balance_after VARCHAR(40),
		metadata JSONB,
		created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
	);
	
	-- Transaction participants table
	CREATE TABLE IF NOT EXISTS transaction_participants (
		id BIGSERIAL PRIMARY KEY,
		transaction_hash VARCHAR(64) NOT NULL,
		ledger_sequence BIGINT NOT NULL,
		timestamp TIMESTAMPTZ NOT NULL,
		participant VARCHAR(56) NOT NULL,
		role VARCHAR(20) NOT NULL,
		created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
		UNIQUE(transaction_hash, participant, role)
	);
	
	-- Generic events table (fallback for unknown types)
	CREATE TABLE IF NOT EXISTS generic_events (
		id BIGSERIAL PRIMARY KEY,
		timestamp TIMESTAMPTZ NOT NULL,
		data JSONB NOT NULL,
		created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
	);
	
	-- Create indexes
	CREATE INDEX IF NOT EXISTS idx_transactions_ledger ON transactions(ledger_sequence);
	CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp);
	CREATE INDEX IF NOT EXISTS idx_state_changes_account ON state_changes(account);
	CREATE INDEX IF NOT EXISTS idx_state_changes_type ON state_changes(change_type);
	CREATE INDEX IF NOT EXISTS idx_state_changes_timestamp ON state_changes(timestamp);
	CREATE INDEX IF NOT EXISTS idx_participants_account ON transaction_participants(participant);
	CREATE INDEX IF NOT EXISTS idx_participants_tx ON transaction_participants(transaction_hash);
	`
	
	_, err := db.Exec(schema)
	return err
}