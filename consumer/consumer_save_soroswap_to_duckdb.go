package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

type SaveSoroswapToDuckDB struct {
	db         *sql.DB
	processors []processor.Processor
}

func NewSaveSoroswapToDuckDB(config map[string]interface{}) (*SaveSoroswapToDuckDB, error) {
	dbPath, ok := config["db_path"].(string)
	if !ok {
		dbPath = "soroswap.duckdb"
	}

	// Open DuckDB connection
	db, err := sql.Open("duckdb", fmt.Sprintf("%s?access_mode=READ_WRITE", dbPath))
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %v", err)
	}

	// Verify connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping DuckDB: %v", err)
	}

	// Create tables
	_, err = db.Exec(`
		-- Create sequence for event IDs
        CREATE SEQUENCE IF NOT EXISTS soroswap_events_id_seq;

        -- Create pairs table
        CREATE TABLE IF NOT EXISTS soroswap_pairs (
            pair_address STRING NOT NULL PRIMARY KEY,
            token_0 STRING NOT NULL,
            token_1 STRING NOT NULL,
            reserve_0 STRING NOT NULL DEFAULT '0',
            reserve_1 STRING NOT NULL DEFAULT '0',
            created_at TIMESTAMP NOT NULL,
            last_sync_at TIMESTAMP,
            last_sync_ledger INTEGER,
            

        );

        -- Create events table
        CREATE TABLE IF NOT EXISTS soroswap_events (
            event_id BIGINT PRIMARY KEY DEFAULT nextval('soroswap_events_id_seq'),
            event_type STRING NOT NULL,
            account STRING NOT NULL,
            token_a STRING NOT NULL,
            token_b STRING NOT NULL,
            amount_a STRING NOT NULL,
            amount_b STRING NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            ledger_sequence INTEGER NOT NULL,
            tx_hash STRING NOT NULL,
            contract_id STRING NOT NULL,
            pair_address STRING,
            reserve_0 STRING,
            reserve_1 STRING,
            
            CONSTRAINT valid_event_type CHECK (event_type IN ('swap', 'add', 'remove', 'new_pair', 'sync')),

        );

        

        -- Create indexes
        CREATE INDEX IF NOT EXISTS idx_pairs_tokens ON soroswap_pairs(token_0, token_1);
        CREATE INDEX IF NOT EXISTS idx_events_type ON soroswap_events(event_type);
        CREATE INDEX IF NOT EXISTS idx_events_account ON soroswap_events(account);
        CREATE INDEX IF NOT EXISTS idx_events_tokens ON soroswap_events(token_a, token_b);
        CREATE INDEX IF NOT EXISTS idx_events_timestamp ON soroswap_events(timestamp DESC);
    `)
	if err != nil {
		return nil, fmt.Errorf("failed to create tables: %v", err)
	}

	return &SaveSoroswapToDuckDB{
		db:         db,
		processors: make([]processor.Processor, 0),
	}, nil
}

func (d *SaveSoroswapToDuckDB) Subscribe(processor processor.Processor) {
	d.processors = append(d.processors, processor)
}

func (d *SaveSoroswapToDuckDB) Process(ctx context.Context, msg processor.Message) error {
	payload, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte payload, got %T", msg.Payload)
	}

	var event processor.SoroswapEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return fmt.Errorf("error decoding event: %w", err)
	}

	log.Printf("Processing %s event: %+v", event.Type, event)

	// Begin transaction
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Handle event based on type
	switch event.Type {
	case "new_pair":
		log.Printf("Inserting new pair: %s (tokens: %s/%s)",
			event.PairAddress, event.TokenA, event.TokenB)

		// Insert into pairs table
		if _, err := tx.ExecContext(ctx, `
            INSERT INTO soroswap_pairs (
                pair_address, token_0, token_1, created_at, 
                reserve_0, reserve_1
            ) VALUES (?, ?, ?, ?, '0', '0')
            ON CONFLICT (pair_address) DO NOTHING
        `, event.PairAddress, event.TokenA, event.TokenB, event.Timestamp); err != nil {
			return fmt.Errorf("failed to insert pair: %v", err)
		}

	case "sync":
		// Update pair reserves
		if _, err := tx.ExecContext(ctx, `
            UPDATE soroswap_pairs 
            SET reserve_0 = ?,
                reserve_1 = ?,
                last_sync_at = ?,
                last_sync_ledger = ?
            WHERE pair_address = ?
        `, event.Reserve0, event.Reserve1, event.Timestamp, event.LedgerSequence, event.ContractID); err != nil {
			return fmt.Errorf("failed to update pair reserves: %v", err)
		}
	}

	// Insert into events table with proper handling of optional fields
	if _, err := tx.ExecContext(ctx, `
        INSERT INTO soroswap_events (
            event_type, timestamp, ledger_sequence, contract_id,
            account, token_a, token_b, amount_a, amount_b,
            tx_hash, pair_address, reserve_0, reserve_1
        ) VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE(?, '0'), COALESCE(?, '0'), ?, ?, ?, ?)
    `, event.Type, event.Timestamp, event.LedgerSequence, event.ContractID,
		event.Account, event.TokenA, event.TokenB, event.AmountA, event.AmountB,
		event.TxHash, event.PairAddress, event.Reserve0, event.Reserve1); err != nil {
		return fmt.Errorf("failed to insert event: %v", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	log.Printf("Successfully processed %s event", event.Type)
	return nil
}
