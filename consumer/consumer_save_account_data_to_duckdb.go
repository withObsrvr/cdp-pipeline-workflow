package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// SaveAccountDataToDuckDB is a consumer that saves account data to DuckDB
type SaveAccountDataToDuckDB struct {
	db         *sql.DB
	processors []processor.Processor
}

// NewSaveAccountDataToDuckDB creates a new DuckDB consumer for account data
func NewSaveAccountDataToDuckDB(config map[string]interface{}) (*SaveAccountDataToDuckDB, error) {
	dbPath, ok := config["db_path"].(string)
	if !ok {
		return nil, fmt.Errorf("db_path is required")
	}

	// Connect to DuckDB
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to DuckDB: %v", err)
	}

	// Create schema if it doesn't exist
	_, err = db.Exec("CREATE SCHEMA IF NOT EXISTS stellar")
	if err != nil {
		return nil, fmt.Errorf("failed to create schema: %v", err)
	}

	// Check if the table exists and has the correct schema
	schemaValid, err := checkTableSchema(db)
	if err != nil {
		log.Printf("Error checking table schema: %v", err)
		// Continue anyway, we'll recreate the table
	}

	// If the schema is invalid or there was an error checking it, recreate the table
	if !schemaValid {
		log.Printf("Table schema is invalid or doesn't exist, recreating table...")

		// Drop the existing table if it exists
		_, err = db.Exec("DROP TABLE IF EXISTS stellar.accounts")
		if err != nil {
			return nil, fmt.Errorf("failed to drop existing accounts table: %v", err)
		}

		// Create accounts table with the correct column names
		_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS stellar.accounts (
				account_id VARCHAR NOT NULL,
				balance VARCHAR,
				buying_liabilities VARCHAR,
				selling_liabilities VARCHAR,
				sequence_number BIGINT,
				sequence_ledger INTEGER,
				sequence_time TIMESTAMP,
				num_subentries INTEGER,
				inflation_dest VARCHAR,
				flags INTEGER,
				home_domain VARCHAR,
				master_weight INTEGER,
				threshold_low INTEGER,
				threshold_medium INTEGER,
				threshold_high INTEGER,
				sponsor VARCHAR,
				num_sponsored INTEGER,
				num_sponsoring INTEGER,
				last_modified_ledger INTEGER,
				ledger_entry_change INTEGER,
				deleted BOOLEAN,
				closed_at TIMESTAMP,
				ledger_sequence INTEGER,
				timestamp TIMESTAMP,
				PRIMARY KEY (account_id, ledger_sequence)
			)
		`)

		if err != nil {
			return nil, fmt.Errorf("failed to create accounts table: %v", err)
		}

		log.Printf("Successfully recreated accounts table with correct schema")
	} else {
		log.Printf("Table schema is valid, using existing table")
	}

	return &SaveAccountDataToDuckDB{
		db:         db,
		processors: make([]processor.Processor, 0),
	}, nil
}

func (d *SaveAccountDataToDuckDB) Process(ctx context.Context, msg processor.Message) error {
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

	// For deleted accounts, use a simpler query
	if accountData.Deleted {
		_, err := d.db.ExecContext(dbCtx, `
			INSERT INTO stellar.accounts (
				account_id, deleted, closed_at, ledger_sequence, timestamp
			) VALUES (?, ?, ?, ?, ?)
		`, accountData.AccountID, true, accountData.ClosedAt, accountData.LedgerSequence, accountData.Timestamp)

		if err != nil {
			log.Printf("Error inserting deleted account: %v", err)
			return fmt.Errorf("failed to insert deleted account: %v", err)
		}

		log.Printf("Successfully saved deleted account %s at ledger %d",
			accountData.AccountID, accountData.LedgerSequence)
	} else {
		// For regular accounts, use the full query
		stmt, err := d.db.PrepareContext(dbCtx, `
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
				?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
			)
		`)
		if err != nil {
			log.Printf("Error preparing statement: %v", err)
			return fmt.Errorf("failed to prepare statement: %v", err)
		}
		defer stmt.Close()

		_, err = stmt.ExecContext(dbCtx,
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
			log.Printf("Error executing statement: %v", err)
			return fmt.Errorf("failed to insert account data: %v", err)
		}

		log.Printf("Successfully saved account data for %s at ledger %d",
			accountData.AccountID, accountData.LedgerSequence)
	}

	// Forward to next processor if any
	for _, processor := range d.processors {
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

func (d *SaveAccountDataToDuckDB) Subscribe(processor processor.Processor) {
	d.processors = append(d.processors, processor)
}

func (d *SaveAccountDataToDuckDB) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

// checkTableSchema checks if the accounts table exists and has the correct schema
func checkTableSchema(db *sql.DB) (bool, error) {
	// Check if the table exists
	var tableExists bool
	err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 
			FROM information_schema.tables 
			WHERE table_schema = 'stellar' 
			AND table_name = 'accounts'
		)
	`).Scan(&tableExists)

	if err != nil {
		return false, fmt.Errorf("failed to check if table exists: %v", err)
	}

	if !tableExists {
		return false, nil
	}

	// Check if the table has the required columns
	rows, err := db.Query(`
		SELECT column_name 
		FROM information_schema.columns 
		WHERE table_schema = 'stellar' 
		AND table_name = 'accounts'
	`)
	if err != nil {
		return false, fmt.Errorf("failed to query table columns: %v", err)
	}
	defer rows.Close()

	// Create a map of required columns
	requiredColumns := map[string]bool{
		"account_id":           true,
		"balance":              true,
		"buying_liabilities":   true,
		"selling_liabilities":  true,
		"sequence_number":      true,
		"sequence_ledger":      true,
		"sequence_time":        true,
		"num_subentries":       true,
		"inflation_dest":       true,
		"flags":                true,
		"home_domain":          true,
		"master_weight":        true,
		"threshold_low":        true,
		"threshold_medium":     true,
		"threshold_high":       true,
		"sponsor":              true,
		"num_sponsored":        true,
		"num_sponsoring":       true,
		"last_modified_ledger": true,
		"ledger_entry_change":  true,
		"deleted":              true,
		"closed_at":            true,
		"ledger_sequence":      true,
		"timestamp":            true,
	}

	// Check each column
	var columnName string
	foundColumns := make(map[string]bool)
	for rows.Next() {
		if err := rows.Scan(&columnName); err != nil {
			return false, fmt.Errorf("failed to scan column name: %v", err)
		}
		foundColumns[columnName] = true
	}

	// Check if all required columns exist
	for col := range requiredColumns {
		if !foundColumns[col] {
			log.Printf("Missing column in accounts table: %s", col)
			return false, nil
		}
	}

	return true, nil
}
