package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// SaveSoroswapToPostgreSQL is a consumer that saves Soroswap events to PostgreSQL
type SaveSoroswapToPostgreSQL struct {
	db         *sql.DB
	processors []processor.Processor
}

// NewSaveSoroswapToPostgreSQL creates a new PostgreSQL consumer for Soroswap events
func NewSaveSoroswapToPostgreSQL(config map[string]interface{}) (*SaveSoroswapToPostgreSQL, error) {
	// Parse PostgreSQL configuration
	pgConfig, err := parsePostgresConfig(config)
	if err != nil {
		return nil, fmt.Errorf("invalid PostgreSQL configuration: %w", err)
	}

	// Build connection string
	connStr := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		pgConfig.Host, pgConfig.Port, pgConfig.Database, pgConfig.Username, pgConfig.Password, pgConfig.SSLMode,
	)

	// Connect to PostgreSQL
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(pgConfig.MaxOpenConns)
	db.SetMaxIdleConns(pgConfig.MaxIdleConns)

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}

	// Initialize schema
	if err := initializeSoroswapSchema(db); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	// Clean up any existing entries with unknown tokens
	if err := cleanupUnknownTokens(db); err != nil {
		log.Printf("Warning: Failed to clean up unknown tokens: %v", err)
		// Continue anyway, this is not a fatal error
	}

	return &SaveSoroswapToPostgreSQL{
		db:         db,
		processors: make([]processor.Processor, 0),
	}, nil
}

// initializeSoroswapSchema creates the necessary table for Soroswap pairs
func initializeSoroswapSchema(db *sql.DB) error {
	// Create soroswap_pairs table with both pair info and reserves
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS soroswap_pairs (
			id SERIAL,
			pair_address TEXT NOT NULL,
			token0 TEXT NOT NULL,
			token1 TEXT NOT NULL,
			reserve0 TEXT NOT NULL DEFAULT '0',
			reserve1 TEXT NOT NULL DEFAULT '0',
			factory_contract_id TEXT,
			ledger_sequence INTEGER NOT NULL,
			timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			last_sync_at TIMESTAMP WITH TIME ZONE,
			last_sync_ledger INTEGER,
			
			-- Primary key constraint
			PRIMARY KEY (pair_address),
			
			-- Add constraints to prevent empty strings
			CONSTRAINT check_pair_address CHECK (length(pair_address) > 0),
			CONSTRAINT check_token0 CHECK (length(token0) > 0 AND token0 != 'unknown'),
			CONSTRAINT check_token1 CHECK (length(token1) > 0 AND token1 != 'unknown')
		);
		
		-- Create indexes for efficient querying
		CREATE INDEX IF NOT EXISTS idx_soroswap_pairs_tokens ON soroswap_pairs(token0, token1);
		CREATE INDEX IF NOT EXISTS idx_soroswap_pairs_timestamp ON soroswap_pairs(timestamp);
		CREATE INDEX IF NOT EXISTS idx_soroswap_pairs_last_sync ON soroswap_pairs(last_sync_at);
	`)

	if err != nil {
		return fmt.Errorf("failed to create soroswap_pairs table: %w", err)
	}

	return nil
}

// cleanupUnknownTokens removes any existing entries with unknown tokens
func cleanupUnknownTokens(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Delete entries with unknown tokens
	result, err := db.ExecContext(ctx,
		`DELETE FROM soroswap_pairs WHERE token0 = 'unknown' OR token1 = 'unknown'`)
	if err != nil {
		return fmt.Errorf("failed to delete entries with unknown tokens: %w", err)
	}

	// Log how many entries were deleted
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected > 0 {
		log.Printf("Cleaned up %d entries with unknown tokens", rowsAffected)
	}

	return nil
}

// Subscribe adds a processor to the chain
func (p *SaveSoroswapToPostgreSQL) Subscribe(processor processor.Processor) {
	p.processors = append(p.processors, processor)
}

// Process handles incoming Soroswap event messages
func (p *SaveSoroswapToPostgreSQL) Process(ctx context.Context, msg processor.Message) error {
	// Create a timeout context
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Check if the message is JSON
	jsonBytes, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected JSON payload, got %T", msg.Payload)
	}

	// First unmarshal into a temporary struct to check the type
	var temp struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(jsonBytes, &temp); err != nil {
		return fmt.Errorf("error decoding event type: %w", err)
	}

	log.Printf("Processing Soroswap event type: %s", temp.Type)

	switch temp.Type {
	case "new_pair":
		var newPairEvent processor.NewPairEvent
		if err := json.Unmarshal(jsonBytes, &newPairEvent); err != nil {
			return fmt.Errorf("error decoding new pair event: %w", err)
		}
		return p.handleNewPair(ctx, newPairEvent)

	case "sync":
		var syncEvent processor.SyncEvent
		if err := json.Unmarshal(jsonBytes, &syncEvent); err != nil {
			return fmt.Errorf("error decoding sync event: %w", err)
		}
		return p.handleSync(ctx, syncEvent)

	default:
		return fmt.Errorf("unknown event type: %s", temp.Type)
	}
}

// handleNewPair processes a new pair event
func (p *SaveSoroswapToPostgreSQL) handleNewPair(ctx context.Context, event processor.NewPairEvent) error {
	// Begin transaction
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Check if pair already exists
	var exists bool
	err = tx.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM soroswap_pairs WHERE pair_address = $1)",
		event.PairAddress,
	).Scan(&exists)

	if err != nil {
		return fmt.Errorf("failed to check if pair exists: %w", err)
	}

	if exists {
		log.Printf("Pair already exists: %s", event.PairAddress)
		return tx.Commit()
	}

	// Insert new pair with default reserves of 0
	_, err = tx.ExecContext(ctx,
		`INSERT INTO soroswap_pairs (
			pair_address, token0, token1, factory_contract_id, 
			reserve0, reserve1, ledger_sequence, timestamp
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		event.PairAddress,
		event.Token0,
		event.Token1,
		event.ContractID,
		"0", // Default reserve0
		"0", // Default reserve1
		event.LedgerSequence,
		event.Timestamp,
	)

	if err != nil {
		return fmt.Errorf("failed to insert new pair: %w", err)
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("Inserted new Soroswap pair: %s (token0: %s, token1: %s)",
		event.PairAddress, event.Token0, event.Token1)

	// Forward to next processor if any
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("error marshaling new pair event: %w", err)
	}

	for _, proc := range p.processors {
		if err := proc.Process(ctx, processor.Message{Payload: eventBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

// handleSync processes a sync event
func (p *SaveSoroswapToPostgreSQL) handleSync(ctx context.Context, event processor.SyncEvent) error {
	// Begin transaction
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Check if pair exists
	var exists bool
	var token0, token1 string
	err = tx.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM soroswap_pairs WHERE pair_address = $1), token0, token1 FROM soroswap_pairs WHERE pair_address = $1",
		event.ContractID,
	).Scan(&exists, &token0, &token1)

	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to check if pair exists: %w", err)
	}

	if !exists {
		log.Printf("Warning: Sync event for unknown pair: %s - skipping insertion", event.ContractID)
		// Instead of inserting with unknown tokens, we'll skip this entry
		// We'll wait until we receive a proper new_pair event with token information

		// Log this event for debugging purposes
		log.Printf("Skipped sync event for pair %s (reserve0: %s, reserve1: %s, ledger: %d)",
			event.ContractID, event.NewReserve0, event.NewReserve1, event.LedgerSequence)

		// Still commit the transaction (even though we didn't make changes)
		return tx.Commit()
	} else {
		// Only update if tokens are not "unknown"
		if token0 == "unknown" || token1 == "unknown" {
			log.Printf("Warning: Pair %s has unknown tokens - skipping update", event.ContractID)
			return tx.Commit()
		}

		// Update existing pair with new reserves
		_, err = tx.ExecContext(ctx,
			`UPDATE soroswap_pairs SET 
				reserve0 = $1, 
				reserve1 = $2, 
				ledger_sequence = $3, 
				timestamp = $4,
				last_sync_at = $5,
				last_sync_ledger = $6
			WHERE pair_address = $7`,
			event.NewReserve0,
			event.NewReserve1,
			event.LedgerSequence,
			event.Timestamp,
			event.Timestamp,      // Set last_sync_at to current timestamp
			event.LedgerSequence, // Set last_sync_ledger
			event.ContractID,
		)
	}

	if err != nil {
		return fmt.Errorf("failed to update reserves: %w", err)
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("Updated Soroswap pair reserves: %s (reserve0: %s, reserve1: %s)",
		event.ContractID, event.NewReserve0, event.NewReserve1)

	// Forward to next processor if any
	jsonBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("error marshaling sync event: %w", err)
	}

	for _, proc := range p.processors {
		if err := proc.Process(ctx, processor.Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

// Close closes the database connection
func (p *SaveSoroswapToPostgreSQL) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}
