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

// SaveContractInvocationsToPostgreSQL is a consumer that saves contract invocations to PostgreSQL
type SaveContractInvocationsToPostgreSQL struct {
	db         *sql.DB
	processors []processor.Processor
}

// NewSaveContractInvocationsToPostgreSQL creates a new PostgreSQL consumer for contract invocations
func NewSaveContractInvocationsToPostgreSQL(config map[string]interface{}) (*SaveContractInvocationsToPostgreSQL, error) {
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
	if err := initializeContractInvocationsSchema(db); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return &SaveContractInvocationsToPostgreSQL{
		db:         db,
		processors: make([]processor.Processor, 0),
	}, nil
}

// initializeContractInvocationsSchema creates the necessary tables for contract invocations
func initializeContractInvocationsSchema(db *sql.DB) error {
	// Create contract_invocations table with dual representation support
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS contract_invocations (
			id SERIAL PRIMARY KEY,
			timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
			ledger_sequence INTEGER NOT NULL,
			transaction_hash TEXT NOT NULL,
			contract_id TEXT NOT NULL,
			invoking_account TEXT NOT NULL,
			function_name TEXT,
			arguments_raw JSONB,
			arguments JSONB,
			arguments_decoded JSONB,
			successful BOOLEAN NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			
			-- Add constraints
			CONSTRAINT check_contract_id CHECK (length(contract_id) > 0),
			CONSTRAINT check_invoking_account CHECK (length(invoking_account) > 0),
			CONSTRAINT check_transaction_hash CHECK (length(transaction_hash) > 0)
		);
		
		-- Create indexes for efficient querying
		CREATE INDEX IF NOT EXISTS idx_contract_invocations_contract_id ON contract_invocations(contract_id);
		CREATE INDEX IF NOT EXISTS idx_contract_invocations_timestamp ON contract_invocations(timestamp);
		CREATE INDEX IF NOT EXISTS idx_contract_invocations_ledger_sequence ON contract_invocations(ledger_sequence);
		CREATE INDEX IF NOT EXISTS idx_contract_invocations_transaction_hash ON contract_invocations(transaction_hash);
		CREATE INDEX IF NOT EXISTS idx_contract_invocations_function_name ON contract_invocations(function_name);
		CREATE INDEX IF NOT EXISTS idx_contract_invocations_arguments_raw_gin ON contract_invocations USING gin(arguments_raw);
		CREATE INDEX IF NOT EXISTS idx_contract_invocations_arguments_gin ON contract_invocations USING gin(arguments);
		CREATE INDEX IF NOT EXISTS idx_contract_invocations_arguments_decoded_gin ON contract_invocations USING gin(arguments_decoded);
	`)

	if err != nil {
		return fmt.Errorf("failed to create contract_invocations table: %w", err)
	}

	// Add migration for existing columns if they don't exist
	_, err = db.Exec(`
		DO $$ 
		BEGIN 
			-- Add arguments_raw column if it doesn't exist
			IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
						   WHERE table_name = 'contract_invocations' AND column_name = 'arguments_raw') THEN
				ALTER TABLE contract_invocations ADD COLUMN arguments_raw JSONB;
			END IF;
			
			-- Add arguments column if it doesn't exist
			IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
						   WHERE table_name = 'contract_invocations' AND column_name = 'arguments') THEN
				ALTER TABLE contract_invocations ADD COLUMN arguments JSONB;
			END IF;
			
			-- Add arguments_decoded column if it doesn't exist
			IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
						   WHERE table_name = 'contract_invocations' AND column_name = 'arguments_decoded') THEN
				ALTER TABLE contract_invocations ADD COLUMN arguments_decoded JSONB;
			END IF;
		END $$;
		
		-- Create GIN indexes for JSONB columns if they don't exist
		CREATE INDEX IF NOT EXISTS idx_contract_invocations_arguments_raw_gin ON contract_invocations USING gin(arguments_raw);
		CREATE INDEX IF NOT EXISTS idx_contract_invocations_arguments_gin ON contract_invocations USING gin(arguments);
		CREATE INDEX IF NOT EXISTS idx_contract_invocations_arguments_decoded_gin ON contract_invocations USING gin(arguments_decoded);
	`)

	if err != nil {
		return fmt.Errorf("failed to create contract_invocations table: %w", err)
	}

	// Create diagnostic_events table with dual representation support
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS contract_diagnostic_events (
			id SERIAL PRIMARY KEY,
			invocation_id INTEGER NOT NULL REFERENCES contract_invocations(id) ON DELETE CASCADE,
			contract_id TEXT NOT NULL,
			topics JSONB NOT NULL,
			topics_decoded JSONB,
			data JSONB,
			data_decoded JSONB,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			
			CONSTRAINT check_diagnostic_contract_id CHECK (length(contract_id) > 0)
		);
		
		CREATE INDEX IF NOT EXISTS idx_diagnostic_events_invocation_id ON contract_diagnostic_events(invocation_id);
		CREATE INDEX IF NOT EXISTS idx_diagnostic_events_contract_id ON contract_diagnostic_events(contract_id);
		CREATE INDEX IF NOT EXISTS idx_diagnostic_events_topics_decoded_gin ON contract_diagnostic_events USING gin(topics_decoded);
		CREATE INDEX IF NOT EXISTS idx_diagnostic_events_data_decoded_gin ON contract_diagnostic_events USING gin(data_decoded);
	`)

	if err != nil {
		return fmt.Errorf("failed to create contract_diagnostic_events table: %w", err)
	}

	// Add migration for diagnostic events dual representation
	_, err = db.Exec(`
		DO $$ 
		BEGIN 
			-- Add topics_decoded column if it doesn't exist
			IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
						   WHERE table_name = 'contract_diagnostic_events' AND column_name = 'topics_decoded') THEN
				ALTER TABLE contract_diagnostic_events ADD COLUMN topics_decoded JSONB;
			END IF;
			
			-- Add data_decoded column if it doesn't exist
			IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
						   WHERE table_name = 'contract_diagnostic_events' AND column_name = 'data_decoded') THEN
				ALTER TABLE contract_diagnostic_events ADD COLUMN data_decoded JSONB;
			END IF;
		END $$;
		
		-- Create GIN indexes for JSONB columns if they don't exist
		CREATE INDEX IF NOT EXISTS idx_diagnostic_events_topics_decoded_gin ON contract_diagnostic_events USING gin(topics_decoded);
		CREATE INDEX IF NOT EXISTS idx_diagnostic_events_data_decoded_gin ON contract_diagnostic_events USING gin(data_decoded);
	`)

	if err != nil {
		return fmt.Errorf("failed to migrate diagnostic events table: %w", err)
	}

	// Create contract_calls table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS contract_calls (
			id SERIAL PRIMARY KEY,
			invocation_id INTEGER NOT NULL REFERENCES contract_invocations(id) ON DELETE CASCADE,
			from_contract TEXT NOT NULL,
			to_contract TEXT NOT NULL,
			function TEXT,
			successful BOOLEAN NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			
			CONSTRAINT check_from_contract CHECK (length(from_contract) > 0),
			CONSTRAINT check_to_contract CHECK (length(to_contract) > 0)
		);
		
		CREATE INDEX IF NOT EXISTS idx_contract_calls_invocation_id ON contract_calls(invocation_id);
		CREATE INDEX IF NOT EXISTS idx_contract_calls_from_contract ON contract_calls(from_contract);
		CREATE INDEX IF NOT EXISTS idx_contract_calls_to_contract ON contract_calls(to_contract);
	`)

	if err != nil {
		return fmt.Errorf("failed to create contract_calls table: %w", err)
	}

	// Create state_changes table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS contract_state_changes (
			id SERIAL PRIMARY KEY,
			invocation_id INTEGER NOT NULL REFERENCES contract_invocations(id) ON DELETE CASCADE,
			contract_id TEXT NOT NULL,
			key TEXT NOT NULL,
			old_value JSONB,
			new_value JSONB,
			operation TEXT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			
			CONSTRAINT check_state_change_contract_id CHECK (length(contract_id) > 0),
			CONSTRAINT check_key CHECK (length(key) > 0),
			CONSTRAINT check_operation CHECK (operation IN ('create', 'update', 'delete'))
		);
		
		CREATE INDEX IF NOT EXISTS idx_state_changes_invocation_id ON contract_state_changes(invocation_id);
		CREATE INDEX IF NOT EXISTS idx_state_changes_contract_id ON contract_state_changes(contract_id);
		CREATE INDEX IF NOT EXISTS idx_state_changes_operation ON contract_state_changes(operation);
	`)

	if err != nil {
		return fmt.Errorf("failed to create contract_state_changes table: %w", err)
	}

	// Create ttl_extensions table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS contract_ttl_extensions (
			id SERIAL PRIMARY KEY,
			invocation_id INTEGER NOT NULL REFERENCES contract_invocations(id) ON DELETE CASCADE,
			contract_id TEXT NOT NULL,
			old_ttl INTEGER NOT NULL,
			new_ttl INTEGER NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			
			CONSTRAINT check_ttl_extension_contract_id CHECK (length(contract_id) > 0)
		);
		
		CREATE INDEX IF NOT EXISTS idx_ttl_extensions_invocation_id ON contract_ttl_extensions(invocation_id);
		CREATE INDEX IF NOT EXISTS idx_ttl_extensions_contract_id ON contract_ttl_extensions(contract_id);
	`)

	if err != nil {
		return fmt.Errorf("failed to create contract_ttl_extensions table: %w", err)
	}

	return nil
}

// Subscribe adds a processor to the chain
func (p *SaveContractInvocationsToPostgreSQL) Subscribe(processor processor.Processor) {
	p.processors = append(p.processors, processor)
}

// Process handles incoming contract invocation messages
func (p *SaveContractInvocationsToPostgreSQL) Process(ctx context.Context, msg processor.Message) error {
	// Create a timeout context
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Check if the message is JSON
	jsonBytes, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected JSON payload, got %T", msg.Payload)
	}

	// Parse the contract invocation
	var invocation processor.ContractInvocation
	if err := json.Unmarshal(jsonBytes, &invocation); err != nil {
		return fmt.Errorf("failed to unmarshal contract invocation: %w", err)
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

	// Prepare arguments for database insertion with dual representation
	var argumentsRawJSON, argumentsJSON, argumentsDecodedJSON interface{}
	
	// Marshal raw arguments if present
	if len(invocation.ArgumentsRaw) > 0 {
		argsRawBytes, err := json.Marshal(invocation.ArgumentsRaw)
		if err != nil {
			log.Printf("Warning: failed to marshal raw arguments for invocation %s: %v", invocation.TransactionHash, err)
		} else {
			argumentsRawJSON = argsRawBytes
		}
	}
	
	// Marshal arguments if present
	if len(invocation.Arguments) > 0 {
		argsBytes, err := json.Marshal(invocation.Arguments)
		if err != nil {
			log.Printf("Warning: failed to marshal arguments for invocation %s: %v", invocation.TransactionHash, err)
		} else {
			argumentsJSON = argsBytes
		}
	}
	
	// Marshal decoded arguments if present
	if len(invocation.ArgumentsDecoded) > 0 {
		argsDecodedBytes, err := json.Marshal(invocation.ArgumentsDecoded)
		if err != nil {
			log.Printf("Warning: failed to marshal decoded arguments for invocation %s: %v", invocation.TransactionHash, err)
		} else {
			argumentsDecodedJSON = argsDecodedBytes
		}
	}

	// Insert contract invocation
	var invocationID int64
	err = tx.QueryRowContext(
		ctx,
		`INSERT INTO contract_invocations (
			timestamp, ledger_sequence, transaction_hash, contract_id, 
			invoking_account, function_name, arguments_raw, arguments, arguments_decoded, successful
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING id`,
		invocation.Timestamp,
		invocation.LedgerSequence,
		invocation.TransactionHash,
		invocation.ContractID,
		invocation.InvokingAccount,
		invocation.FunctionName,
		argumentsRawJSON,
		argumentsJSON,
		argumentsDecodedJSON,
		invocation.Successful,
	).Scan(&invocationID)

	if err != nil {
		return fmt.Errorf("failed to insert contract invocation: %w", err)
	}

	// Insert diagnostic events if any
	if len(invocation.DiagnosticEvents) > 0 {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO contract_diagnostic_events (
				invocation_id, contract_id, topics, topics_decoded, data, data_decoded
			) VALUES ($1, $2, $3, $4, $5, $6)
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare diagnostic events statement: %w", err)
		}
		defer stmt.Close()

		for _, event := range invocation.DiagnosticEvents {
			// Serialize raw topics as JSON (XDR structures)
			topicsRawJSON, err := json.Marshal(event.Topics)
			if err != nil {
				return fmt.Errorf("failed to marshal raw topics: %w", err)
			}

			// Use decoded topics for human-readable format
			topicsDecodedJSON, err := json.Marshal(event.TopicsDecoded)
			if err != nil {
				return fmt.Errorf("failed to marshal decoded topics: %w", err)
			}

			// Serialize raw data as JSON (XDR structure)
			dataRawJSON, err := json.Marshal(event.Data)
			if err != nil {
				return fmt.Errorf("failed to marshal raw data: %w", err)
			}

			// Use decoded data for human-readable format
			dataDecodedJSON, err := json.Marshal(event.DataDecoded)
			if err != nil {
				return fmt.Errorf("failed to marshal decoded data: %w", err)
			}

			_, err = stmt.ExecContext(
				ctx,
				invocationID,
				event.ContractID,
				topicsRawJSON,
				topicsDecodedJSON,
				dataRawJSON,
				dataDecodedJSON,
			)
			if err != nil {
				return fmt.Errorf("failed to insert diagnostic event: %w", err)
			}
		}
	}

	// Insert contract calls if any
	if len(invocation.ContractCalls) > 0 {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO contract_calls (
				invocation_id, from_contract, to_contract, function, successful
			) VALUES ($1, $2, $3, $4, $5)
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare contract calls statement: %w", err)
		}
		defer stmt.Close()

		for _, call := range invocation.ContractCalls {
			_, err = stmt.ExecContext(
				ctx,
				invocationID,
				call.FromContract,
				call.ToContract,
				call.Function,
				call.Successful,
			)
			if err != nil {
				return fmt.Errorf("failed to insert contract call: %w", err)
			}
		}
	}

	// Insert state changes if any
	if len(invocation.StateChanges) > 0 {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO contract_state_changes (
				invocation_id, contract_id, key, old_value, new_value, operation
			) VALUES ($1, $2, $3, $4, $5, $6)
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare state changes statement: %w", err)
		}
		defer stmt.Close()

		for _, change := range invocation.StateChanges {
			_, err = stmt.ExecContext(
				ctx,
				invocationID,
				change.ContractID,
				change.Key,
				change.OldValue,
				change.NewValue,
				change.Operation,
			)
			if err != nil {
				return fmt.Errorf("failed to insert state change: %w", err)
			}
		}
	}

	// Insert TTL extensions if any
	if len(invocation.TtlExtensions) > 0 {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO contract_ttl_extensions (
				invocation_id, contract_id, old_ttl, new_ttl
			) VALUES ($1, $2, $3, $4)
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare TTL extensions statement: %w", err)
		}
		defer stmt.Close()

		for _, extension := range invocation.TtlExtensions {
			_, err = stmt.ExecContext(
				ctx,
				invocationID,
				extension.ContractID,
				extension.OldTtl,
				extension.NewTtl,
			)
			if err != nil {
				return fmt.Errorf("failed to insert TTL extension: %w", err)
			}
		}
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Enhanced logging with argument information
	rawArgCount := len(invocation.ArgumentsRaw)
	argCount := len(invocation.Arguments)
	decodedArgCount := len(invocation.ArgumentsDecoded)
	
	if rawArgCount > 0 || argCount > 0 || decodedArgCount > 0 {
		log.Printf("Saved contract invocation: %s (contract: %s, function: %s, raw_args: %d, args: %d, decoded: %d)",
			invocation.TransactionHash, invocation.ContractID, invocation.FunctionName, rawArgCount, argCount, decodedArgCount)
	} else {
		log.Printf("Saved contract invocation: %s (contract: %s, function: %s)",
			invocation.TransactionHash, invocation.ContractID, invocation.FunctionName)
	}

	// Forward to next processor if any
	for _, proc := range p.processors {
		if err := proc.Process(ctx, processor.Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

// Close closes the database connection
func (p *SaveContractInvocationsToPostgreSQL) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}
