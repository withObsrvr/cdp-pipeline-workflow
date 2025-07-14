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

// SaveExtractedContractInvocationsToPostgreSQL is a consumer that saves extracted contract invocations to PostgreSQL
type SaveExtractedContractInvocationsToPostgreSQL struct {
	db         *sql.DB
	processors []processor.Processor
}

// NewSaveExtractedContractInvocationsToPostgreSQL creates a new PostgreSQL consumer for extracted contract invocations
func NewSaveExtractedContractInvocationsToPostgreSQL(config map[string]interface{}) (*SaveExtractedContractInvocationsToPostgreSQL, error) {
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

	log.Printf("Connecting to PostgreSQL for extracted contract invocations at %s:%d, database: %s", pgConfig.Host, pgConfig.Port, pgConfig.Database)

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

	log.Printf("Successfully connected to PostgreSQL for extracted contract invocations at %s:%d, database: %s", pgConfig.Host, pgConfig.Port, pgConfig.Database)

	// Initialize schema
	if err := initializeExtractedContractInvocationsSchema(db); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return &SaveExtractedContractInvocationsToPostgreSQL{
		db:         db,
		processors: make([]processor.Processor, 0),
	}, nil
}

// initializeExtractedContractInvocationsSchema creates the necessary tables for extracted contract invocations
func initializeExtractedContractInvocationsSchema(db *sql.DB) error {
	// Create extracted_contract_invocations table
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS extracted_contract_invocations (
			id SERIAL PRIMARY KEY,
			
			-- Core identifiers
			toid BIGINT UNIQUE NOT NULL,
			ledger INTEGER NOT NULL,
			timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
			contract_id TEXT NOT NULL,
			function_name TEXT NOT NULL,
			invoking_account TEXT NOT NULL,
			tx_hash TEXT NOT NULL,
			
			-- Extracted business fields
			funder TEXT,
			recipient TEXT,
			amount BIGINT,
			project_id TEXT,
			memo_text TEXT,
			email TEXT,
			
			-- Metadata
			processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			schema_name TEXT NOT NULL,
			successful BOOLEAN NOT NULL DEFAULT true,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			
			-- Constraints
			CONSTRAINT check_toid_positive CHECK (toid > 0),
			CONSTRAINT check_ledger_positive CHECK (ledger > 0),
			CONSTRAINT check_amount_non_negative CHECK (amount IS NULL OR amount >= 0),
			CONSTRAINT check_contract_id_not_empty CHECK (length(contract_id) > 0),
			CONSTRAINT check_tx_hash_not_empty CHECK (length(tx_hash) > 0),
			CONSTRAINT check_schema_name_not_empty CHECK (length(schema_name) > 0)
		);
	`)

	if err != nil {
		return fmt.Errorf("failed to create extracted_contract_invocations table: %w", err)
	}

	// Create indexes for efficient querying
	_, err = db.Exec(`
		-- Core indexes
		CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_toid 
			ON extracted_contract_invocations(toid);
		CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_ledger 
			ON extracted_contract_invocations(ledger);
		CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_timestamp 
			ON extracted_contract_invocations(timestamp);
		CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_contract_id 
			ON extracted_contract_invocations(contract_id);
		CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_function_name 
			ON extracted_contract_invocations(function_name);
		CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_tx_hash 
			ON extracted_contract_invocations(tx_hash);
		CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_invoking_account 
			ON extracted_contract_invocations(invoking_account);
		
		-- Business field indexes
		CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_funder 
			ON extracted_contract_invocations(funder);
		CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_recipient 
			ON extracted_contract_invocations(recipient);
		CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_project_id 
			ON extracted_contract_invocations(project_id);
		CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_schema_name 
			ON extracted_contract_invocations(schema_name);
		CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_successful 
			ON extracted_contract_invocations(successful);
		
		-- Composite indexes for common queries
		CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_contract_function 
			ON extracted_contract_invocations(contract_id, function_name);
		CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_ledger_timestamp 
			ON extracted_contract_invocations(ledger, timestamp);
		CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_funder_recipient 
			ON extracted_contract_invocations(funder, recipient);
		CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_funder_project 
			ON extracted_contract_invocations(funder, project_id);
		CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_recipient_project 
			ON extracted_contract_invocations(recipient, project_id);
	`)

	if err != nil {
		return fmt.Errorf("failed to create indexes: %w", err)
	}

	// Add migration for existing tables if they don't have all columns
	_, err = db.Exec(`
		DO $$ 
		BEGIN 
			-- Add amount column if it doesn't exist
			IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
						   WHERE table_name = 'extracted_contract_invocations' 
						   AND column_name = 'amount') THEN
				ALTER TABLE extracted_contract_invocations ADD COLUMN amount BIGINT;
			END IF;
			
			-- Add project_id column if it doesn't exist
			IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
						   WHERE table_name = 'extracted_contract_invocations' 
						   AND column_name = 'project_id') THEN
				ALTER TABLE extracted_contract_invocations ADD COLUMN project_id TEXT;
			END IF;
			
			-- Add memo_text column if it doesn't exist
			IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
						   WHERE table_name = 'extracted_contract_invocations' 
						   AND column_name = 'memo_text') THEN
				ALTER TABLE extracted_contract_invocations ADD COLUMN memo_text TEXT;
			END IF;
			
			-- Add email column if it doesn't exist
			IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
						   WHERE table_name = 'extracted_contract_invocations' 
						   AND column_name = 'email') THEN
				ALTER TABLE extracted_contract_invocations ADD COLUMN email TEXT;
			END IF;
		END $$;
	`)

	if err != nil {
		return fmt.Errorf("failed to migrate extracted_contract_invocations table: %w", err)
	}

	log.Printf("Successfully initialized extracted_contract_invocations schema")
	return nil
}

// Subscribe adds a processor to the chain
func (p *SaveExtractedContractInvocationsToPostgreSQL) Subscribe(processor processor.Processor) {
	p.processors = append(p.processors, processor)
}

// Process handles incoming extracted contract invocation messages
func (p *SaveExtractedContractInvocationsToPostgreSQL) Process(ctx context.Context, msg processor.Message) error {
	// Create a timeout context
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Check if the message is JSON
	jsonBytes, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected JSON payload, got %T", msg.Payload)
	}

	// Parse the extracted contract invocation
	var extracted processor.ExtractedContractInvocation
	if err := json.Unmarshal(jsonBytes, &extracted); err != nil {
		return fmt.Errorf("failed to unmarshal extracted contract invocation: %w", err)
	}

	// Insert into database with conflict resolution
	_, err := p.db.ExecContext(ctx, `
		INSERT INTO extracted_contract_invocations (
			toid, ledger, timestamp, contract_id, function_name, invoking_account, tx_hash,
			funder, recipient, amount, project_id, memo_text, email, schema_name, successful
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
		ON CONFLICT (toid) DO UPDATE SET
			processed_at = NOW(),
			funder = EXCLUDED.funder,
			recipient = EXCLUDED.recipient,
			amount = EXCLUDED.amount,
			project_id = EXCLUDED.project_id,
			memo_text = EXCLUDED.memo_text,
			email = EXCLUDED.email,
			schema_name = EXCLUDED.schema_name,
			successful = EXCLUDED.successful
	`,
		extracted.Toid,
		extracted.Ledger,
		extracted.Timestamp,
		extracted.ContractID,
		extracted.FunctionName,
		extracted.InvokingAccount,
		extracted.TxHash,
		nullString(extracted.Funder),
		nullString(extracted.Recipient),
		nullUint64(extracted.Amount),
		nullString(extracted.ProjectID),
		nullString(extracted.MemoText),
		nullString(extracted.Email),
		extracted.SchemaName,
		extracted.Successful,
	)

	if err != nil {
		return fmt.Errorf("failed to insert extracted contract invocation: %w", err)
	}

	// Enhanced logging with extracted business data
	log.Printf("Saved extracted contract invocation: %s (schema: %s, toid: %d, funder: %s, recipient: %s, amount: %d, project: %s)",
		extracted.TxHash, extracted.SchemaName, extracted.Toid,
		extracted.Funder, extracted.Recipient, extracted.Amount, extracted.ProjectID)

	// Forward to next processor if any
	for _, proc := range p.processors {
		if err := proc.Process(ctx, processor.Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

// Close closes the database connection
func (p *SaveExtractedContractInvocationsToPostgreSQL) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

// Helper functions for handling null values
func nullString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func nullUint64(u uint64) interface{} {
	if u == 0 {
		return nil
	}
	return u
}