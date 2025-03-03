package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

type SaveAccountDataToDuckDB struct {
	db         *sql.DB
	processors []processor.Processor
}

func NewSaveAccountDataToDuckDB(config map[string]interface{}) (*SaveAccountDataToDuckDB, error) {
	dbPath, ok := config["db_path"].(string)
	if !ok {
		dbPath = "stellar.duckdb"
	}

	// Open DuckDB connection with optimized settings
	db, err := sql.Open("duckdb", fmt.Sprintf("%s?access_mode=READ_WRITE&memory_limit=4GB&threads=4", dbPath))
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %v", err)
	}

	// Verify connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping DuckDB: %v", err)
	}

	// Create accounts table
	_, err = db.Exec(`
        CREATE SCHEMA IF NOT EXISTS stellar;

        CREATE TABLE IF NOT EXISTS stellar.accounts (
            closed_at_date DATE,
            account_id VARCHAR,
            balance DOUBLE,
            buying_liabilities DOUBLE,
            selling_liabilities DOUBLE,
            sequence_number BIGINT,
            num_subentries BIGINT,
            inflation_destination VARCHAR,
            flags BIGINT,
            home_domain VARCHAR,
            master_weight BIGINT,
            threshold_low BIGINT,
            threshold_medium BIGINT,
            threshold_high BIGINT,
            last_modified_ledger BIGINT,
            ledger_entry_change BIGINT,
            deleted BOOLEAN,
            sponsor VARCHAR,
            num_sponsored BIGINT,
            num_sponsoring BIGINT,
            sequence_time TIMESTAMP,
            closed_at TIMESTAMP,
            ledger_sequence BIGINT,
            account_sequence_last_modified_ledger BIGINT,
            updated_at TIMESTAMP,
            ingested_at TIMESTAMP
        );

        -- Create indexes separately
        CREATE INDEX IF NOT EXISTS idx_accounts_account_id ON stellar.accounts(account_id);
        CREATE INDEX IF NOT EXISTS idx_accounts_closed_at_date ON stellar.accounts(closed_at_date);
        CREATE INDEX IF NOT EXISTS idx_accounts_ledger_sequence ON stellar.accounts(ledger_sequence);
    `)
	if err != nil {
		return nil, fmt.Errorf("failed to create accounts table: %v", err)
	}

	return &SaveAccountDataToDuckDB{
		db:         db,
		processors: make([]processor.Processor, 0),
	}, nil
}

func (d *SaveAccountDataToDuckDB) Process(ctx context.Context, msg processor.Message) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	jsonBytes, ok := msg.Payload.([]byte)
	if !ok {
		log.Printf("Error: expected []byte, got %T", msg.Payload)
		return fmt.Errorf("expected []byte, got %T", msg.Payload)
	}

	var accountData processor.AccountDataFull
	if err := json.Unmarshal(jsonBytes, &accountData); err != nil {
		log.Printf("Error unmarshaling JSON: %v", err)
		return fmt.Errorf("failed to unmarshal JSON: %v", err)
	}

	log.Printf("Processing account data for account %s at ledger %d (deleted: %v)",
		accountData.AccountID, accountData.LedgerSequence, accountData.Deleted)

	stmt, err := d.db.PrepareContext(ctx, `
        INSERT INTO stellar.accounts (
            closed_at_date, account_id, balance, buying_liabilities,
            selling_liabilities, sequence_number, num_subentries,
            inflation_destination, flags, home_domain, master_weight,
            threshold_low, threshold_medium, threshold_high,
            last_modified_ledger, ledger_entry_change, deleted,
            sponsor, num_sponsored, num_sponsoring, sequence_time,
            closed_at, ledger_sequence,
            account_sequence_last_modified_ledger,
            updated_at, ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `)
	if err != nil {
		log.Printf("Error preparing statement: %v", err)
		return fmt.Errorf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	if _, err := stmt.ExecContext(ctx,
		accountData.ClosedAtDate,
		accountData.AccountID,
		accountData.Balance,
		accountData.BuyingLiabilities,
		accountData.SellingLiabilities,
		accountData.SequenceNumber,
		accountData.NumSubentries,
		accountData.InflationDestination,
		accountData.Flags,
		accountData.HomeDomain,
		accountData.MasterWeight,
		accountData.ThresholdLow,
		accountData.ThresholdMedium,
		accountData.ThresholdHigh,
		accountData.LastModifiedLedger,
		accountData.LedgerEntryChange,
		accountData.Deleted,
		accountData.Sponsor,
		accountData.NumSponsored,
		accountData.NumSponsoring,
		accountData.SequenceTime,
		accountData.ClosedAt,
		accountData.LedgerSequence,
		accountData.AccountSequenceLastModifiedLedger,
		accountData.UpdatedAt,
		accountData.IngestedAt,
	); err != nil {
		return fmt.Errorf("failed to execute statement: %v", err)
	}

	return nil
}

func (d *SaveAccountDataToDuckDB) Subscribe(processor processor.Processor) {
	d.processors = append(d.processors, processor)
}
