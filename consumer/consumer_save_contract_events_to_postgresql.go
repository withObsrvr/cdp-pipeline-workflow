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

// SaveContractEventsToPostgreSQL is a consumer that saves contract events to PostgreSQL
type SaveContractEventsToPostgreSQL struct {
	db         *sql.DB
	processors []processor.Processor
}

// NewSaveContractEventsToPostgreSQL creates a new PostgreSQL consumer for contract events
func NewSaveContractEventsToPostgreSQL(config map[string]interface{}) (*SaveContractEventsToPostgreSQL, error) {
	// Parse PostgreSQL configuration
	pgConfig, err := parsePostgresConfig(config)
	if err != nil {
		return nil, fmt.Errorf("invalid PostgreSQL configuration: %w", err)
	}

	// Build connection string with timeout
	connStr := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s connect_timeout=%d",
		pgConfig.Host, pgConfig.Port, pgConfig.Database, pgConfig.Username, pgConfig.Password, pgConfig.SSLMode, pgConfig.ConnectTimeout,
	)

	log.Printf("Connecting to PostgreSQL at %s:%d...", pgConfig.Host, pgConfig.Port)

	// Connect to PostgreSQL
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(pgConfig.MaxOpenConns)
	db.SetMaxIdleConns(pgConfig.MaxIdleConns)

	// Test connection with context timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(pgConfig.ConnectTimeout)*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}

	log.Printf("Successfully connected to PostgreSQL at %s:%d", pgConfig.Host, pgConfig.Port)

	// Initialize schema
	if err := initializeContractEventsSchema(db); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return &SaveContractEventsToPostgreSQL{
		db: db,
	}, nil
}

// initializeContractEventsSchema creates the necessary tables for contract events
func initializeContractEventsSchema(db *sql.DB) error {
	// Create contract_events table
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS contract_events (
			id SERIAL PRIMARY KEY,
			timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
			ledger_sequence INTEGER NOT NULL,
			transaction_hash TEXT NOT NULL,
			contract_id TEXT NOT NULL,
			type TEXT NOT NULL,
			topic JSONB NOT NULL,
			data JSONB,
			in_successful_tx BOOLEAN NOT NULL,
			event_index INTEGER NOT NULL,
			operation_index INTEGER NOT NULL,
			network_passphrase TEXT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		);
		
		-- Create indexes for efficient querying
		CREATE INDEX IF NOT EXISTS idx_contract_events_contract_id ON contract_events(contract_id);
		CREATE INDEX IF NOT EXISTS idx_contract_events_timestamp ON contract_events(timestamp);
		CREATE INDEX IF NOT EXISTS idx_contract_events_ledger_sequence ON contract_events(ledger_sequence);
		CREATE INDEX IF NOT EXISTS idx_contract_events_transaction_hash ON contract_events(transaction_hash);
		CREATE INDEX IF NOT EXISTS idx_contract_events_type ON contract_events(type);
	`)

	if err != nil {
		return fmt.Errorf("failed to create contract_events table: %w", err)
	}

	// Create diagnostic_events table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS diagnostic_events (
			id SERIAL PRIMARY KEY,
			contract_event_id INTEGER NOT NULL REFERENCES contract_events(id) ON DELETE CASCADE,
			event JSONB NOT NULL,
			in_successful_contract_call BOOLEAN NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		);
		
		CREATE INDEX IF NOT EXISTS idx_diagnostic_events_contract_event_id ON diagnostic_events(contract_event_id);
	`)

	if err != nil {
		return fmt.Errorf("failed to create diagnostic_events table: %w", err)
	}

	return nil
}

// Subscribe adds a processor to the chain
func (p *SaveContractEventsToPostgreSQL) Subscribe(processor processor.Processor) {
	p.processors = append(p.processors, processor)
}

// Process handles incoming contract event messages
func (p *SaveContractEventsToPostgreSQL) Process(ctx context.Context, msg processor.Message) error {
	// Check if the message is JSON
	jsonBytes, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected JSON payload, got %T", msg.Payload)
	}

	// Parse the contract event
	var contractEvent processor.ContractEvent
	if err := json.Unmarshal(jsonBytes, &contractEvent); err != nil {
		return fmt.Errorf("failed to unmarshal contract event: %w", err)
	}

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

	// Insert contract event
	var contractEventID int64
	err = tx.QueryRowContext(
		ctx,
		`INSERT INTO contract_events (
			timestamp, ledger_sequence, transaction_hash, contract_id, 
			type, topic, data, in_successful_tx, event_index, 
			operation_index, network_passphrase
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING id`,
		contractEvent.Timestamp,
		contractEvent.LedgerSequence,
		contractEvent.TransactionHash,
		contractEvent.ContractID,
		contractEvent.Type,
		json.RawMessage(fmt.Sprintf("%v", contractEvent.Topic)), // Convert topic to JSON
		contractEvent.Data,
		contractEvent.InSuccessfulTx,
		contractEvent.EventIndex,
		contractEvent.OperationIndex,
		contractEvent.NetworkPassphrase,
	).Scan(&contractEventID)

	if err != nil {
		return fmt.Errorf("failed to insert contract event: %w", err)
	}

	// Insert diagnostic events if any
	if len(contractEvent.DiagnosticEvents) > 0 {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO diagnostic_events (
				contract_event_id, event, in_successful_contract_call
			) VALUES ($1, $2, $3)
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare diagnostic events statement: %w", err)
		}
		defer stmt.Close()

		for _, diagnosticEvent := range contractEvent.DiagnosticEvents {
			_, err = stmt.ExecContext(
				ctx,
				contractEventID,
				diagnosticEvent.Event,
				diagnosticEvent.InSuccessfulContractCall,
			)
			if err != nil {
				return fmt.Errorf("failed to insert diagnostic event: %w", err)
			}
		}
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("Saved contract event: %s (ledger: %d, contract: %s)",
		contractEvent.TransactionHash, contractEvent.LedgerSequence, contractEvent.ContractID)

	// Forward to next processor if any
	for _, processor := range p.processors {
		if err := processor.Process(ctx, msg); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

// Close closes the database connection
func (p *SaveContractEventsToPostgreSQL) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}
