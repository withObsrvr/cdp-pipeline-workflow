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

// SaveAccountDataToPostgreSQL is a consumer that saves account data to PostgreSQL
type SaveAccountDataToPostgreSQL struct {
	db         *sql.DB
	processors []processor.Processor
}

// NewSaveAccountDataToPostgreSQL creates a new PostgreSQL consumer for account data
func NewSaveAccountDataToPostgreSQL(config map[string]interface{}) (*SaveAccountDataToPostgreSQL, error) {
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
	if err := initializeAccountDataSchema(db); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return &SaveAccountDataToPostgreSQL{
		db:         db,
		processors: make([]processor.Processor, 0),
	}, nil
}

// initializeAccountDataSchema creates the necessary tables for account data
func initializeAccountDataSchema(db *sql.DB) error {
	// Create schema if it doesn't exist
	_, err := db.Exec("CREATE SCHEMA IF NOT EXISTS stellar")
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	// Create accounts table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS stellar.accounts (
			id SERIAL,
			account_id TEXT NOT NULL,
			balance TEXT,
			buying_liabilities TEXT,
			selling_liabilities TEXT,
			sequence_number BIGINT,
			sequence_ledger INTEGER,
			sequence_time TIMESTAMP WITH TIME ZONE,
			num_subentries INTEGER,
			inflation_dest TEXT,
			flags INTEGER,
			home_domain TEXT,
			master_weight INTEGER,
			threshold_low INTEGER,
			threshold_medium INTEGER,
			threshold_high INTEGER,
			sponsor TEXT,
			num_sponsored INTEGER,
			num_sponsoring INTEGER,
			last_modified_ledger INTEGER,
			ledger_entry_change INTEGER,
			deleted BOOLEAN,
			closed_at TIMESTAMP WITH TIME ZONE,
			ledger_sequence INTEGER,
			timestamp TIMESTAMP WITH TIME ZONE,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			
			PRIMARY KEY (id),
			UNIQUE (account_id, ledger_sequence)
		);
		
		-- Create indexes for efficient querying
		CREATE INDEX IF NOT EXISTS idx_accounts_account_id ON stellar.accounts(account_id);
		CREATE INDEX IF NOT EXISTS idx_accounts_ledger_sequence ON stellar.accounts(ledger_sequence);
		CREATE INDEX IF NOT EXISTS idx_accounts_timestamp ON stellar.accounts(timestamp);
		CREATE INDEX IF NOT EXISTS idx_accounts_deleted ON stellar.accounts(deleted);
	`)

	if err != nil {
		return fmt.Errorf("failed to create accounts table: %w", err)
	}

	return nil
}

// Subscribe adds a processor to the chain
func (p *SaveAccountDataToPostgreSQL) Subscribe(processor processor.Processor) {
	p.processors = append(p.processors, processor)
}

// Process handles incoming account data messages
func (p *SaveAccountDataToPostgreSQL) Process(ctx context.Context, msg processor.Message) error {
	// Create a new context with a longer timeout
	dbCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	jsonBytes, ok := msg.Payload.([]byte)
	if !ok {
		log.Printf("Error: expected []byte, got %T", msg.Payload)
		return fmt.Errorf("expected []byte, got %T", msg.Payload)
	}

	var accountData processor.AccountRecord
	if err := json.Unmarshal(jsonBytes, &accountData); err != nil {
		log.Printf("Error unmarshaling JSON: %v", err)
		return fmt.Errorf("failed to unmarshal JSON: %v", err)
	}

	log.Printf("Processing account data for account %s at ledger %d (deleted: %v)",
		accountData.AccountID, accountData.LedgerSequence, accountData.Deleted)

	// Begin transaction
	tx, err := p.db.BeginTx(dbCtx, nil)
	if err != nil {
		log.Printf("Error starting transaction: %v", err)
		return fmt.Errorf("error starting transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// For deleted accounts, use a simpler query
	if accountData.Deleted {
		_, err = tx.ExecContext(dbCtx, `
			INSERT INTO stellar.accounts (
				account_id, deleted, closed_at, ledger_sequence, timestamp
			) VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (account_id, ledger_sequence) 
			DO UPDATE SET 
				deleted = EXCLUDED.deleted,
				closed_at = EXCLUDED.closed_at,
				timestamp = EXCLUDED.timestamp
		`, accountData.AccountID, true, accountData.ClosedAt, accountData.LedgerSequence, accountData.Timestamp)

		if err != nil {
			log.Printf("Error inserting deleted account: %v", err)
			return fmt.Errorf("failed to insert deleted account: %w", err)
		}
	} else {
		// For regular accounts, use the full query
		_, err = tx.ExecContext(dbCtx, `
			INSERT INTO stellar.accounts (
				account_id, balance, buying_liabilities,
				selling_liabilities, sequence_number, sequence_ledger,
				sequence_time, num_subentries,
				inflation_dest, flags, home_domain, master_weight,
				threshold_low, threshold_medium, threshold_high,
				sponsor, num_sponsored, num_sponsoring,
				last_modified_ledger, ledger_entry_change, deleted,
				closed_at, ledger_sequence, timestamp
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 
				$13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24
			)
			ON CONFLICT (account_id, ledger_sequence) 
			DO UPDATE SET 
				balance = EXCLUDED.balance,
				buying_liabilities = EXCLUDED.buying_liabilities,
				selling_liabilities = EXCLUDED.selling_liabilities,
				sequence_number = EXCLUDED.sequence_number,
				sequence_ledger = EXCLUDED.sequence_ledger,
				sequence_time = EXCLUDED.sequence_time,
				num_subentries = EXCLUDED.num_subentries,
				inflation_dest = EXCLUDED.inflation_dest,
				flags = EXCLUDED.flags,
				home_domain = EXCLUDED.home_domain,
				master_weight = EXCLUDED.master_weight,
				threshold_low = EXCLUDED.threshold_low,
				threshold_medium = EXCLUDED.threshold_medium,
				threshold_high = EXCLUDED.threshold_high,
				sponsor = EXCLUDED.sponsor,
				num_sponsored = EXCLUDED.num_sponsored,
				num_sponsoring = EXCLUDED.num_sponsoring,
				last_modified_ledger = EXCLUDED.last_modified_ledger,
				ledger_entry_change = EXCLUDED.ledger_entry_change,
				deleted = EXCLUDED.deleted,
				closed_at = EXCLUDED.closed_at,
				timestamp = EXCLUDED.timestamp
		`,
			accountData.AccountID,
			accountData.Balance,
			accountData.BuyingLiabilities,
			accountData.SellingLiabilities,
			accountData.Sequence,
			accountData.SequenceLedger,
			accountData.SequenceTime,
			accountData.NumSubentries,
			accountData.InflationDest,
			accountData.Flags,
			accountData.HomeDomain,
			accountData.MasterWeight,
			accountData.LowThreshold,
			accountData.MediumThreshold,
			accountData.HighThreshold,
			accountData.Sponsor,
			accountData.NumSponsored,
			accountData.NumSponsoring,
			accountData.LastModifiedLedger,
			accountData.LedgerEntryChange,
			accountData.Deleted,
			accountData.ClosedAt,
			accountData.LedgerSequence,
			accountData.Timestamp,
		)

		if err != nil {
			log.Printf("Error inserting account data: %v", err)
			return fmt.Errorf("failed to insert account data: %w", err)
		}
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		log.Printf("Error committing transaction: %v", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("Successfully saved account data for %s at ledger %d to PostgreSQL",
		accountData.AccountID, accountData.LedgerSequence)

	// Forward to next processor if any
	for _, processor := range p.processors {
		// Create a new context for each processor
		procCtx, procCancel := context.WithTimeout(ctx, 30*time.Second)
		err := processor.Process(procCtx, msg)
		procCancel()

		if err != nil {
			log.Printf("Error in processor chain: %v", err)
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

// Close closes the database connection
func (p *SaveAccountDataToPostgreSQL) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}
