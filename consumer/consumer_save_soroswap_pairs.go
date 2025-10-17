package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

type SaveSoroswapPairsToDuckDB struct {
	db         *sql.DB
	processors []processor.Processor
}

func NewSaveSoroswapPairsToDuckDB(config map[string]interface{}) (*SaveSoroswapPairsToDuckDB, error) {
	dbPath, ok := config["db_path"].(string)
	if !ok {
		dbPath = "soroswap_pairs.duckdb"
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

	// Drop existing table and recreate with proper constraints
	_, err = db.Exec(`
        
        
        CREATE TABLE IF NOT EXISTS soroswap_pairs (
            pair_address STRING NOT NULL PRIMARY KEY,
            token_0 STRING NOT NULL,
            token_1 STRING NOT NULL,
            reserve_0 STRING NOT NULL DEFAULT '0',
            reserve_1 STRING NOT NULL DEFAULT '0',
            created_at TIMESTAMP NOT NULL,
            last_sync_at TIMESTAMP,
            last_sync_ledger INTEGER,
            
            -- Add constraints to prevent empty strings
            CONSTRAINT valid_pair_address CHECK (length(pair_address) > 0),
            CONSTRAINT valid_token_0 CHECK (length(token_0) > 0),
            CONSTRAINT valid_token_1 CHECK (length(token_1) > 0)
        );

        -- Add an index for faster token lookups
        CREATE INDEX IF NOT EXISTS idx_tokens ON soroswap_pairs(token_0, token_1);
    `)
	if err != nil {
		return nil, fmt.Errorf("failed to create soroswap_pairs table: %v", err)
	}

	return &SaveSoroswapPairsToDuckDB{
		db:         db,
		processors: make([]processor.Processor, 0),
	}, nil
}

func (d *SaveSoroswapPairsToDuckDB) Subscribe(processor processor.Processor) {
	d.processors = append(d.processors, processor)
}

func (d *SaveSoroswapPairsToDuckDB) Process(ctx context.Context, msg processor.Message) error {
	switch payload := msg.Payload.(type) {
	case []byte:
		// First unmarshal into a temporary struct to check the type
		var temp struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(payload, &temp); err != nil {
			return fmt.Errorf("error decoding event type: %w", err)
		}

		log.Printf("Processing event type: %s", temp.Type)

		switch temp.Type {
		case "new_pair":
			var newPairEvent processor.NewPairEvent
			if err := json.Unmarshal(payload, &newPairEvent); err != nil {
				return fmt.Errorf("error decoding new pair event: %w", err)
			}
			return d.handleNewPair(ctx, newPairEvent)

		case "sync":
			var syncEvent processor.SyncEvent
			if err := json.Unmarshal(payload, &syncEvent); err != nil {
				return fmt.Errorf("error decoding sync event: %w", err)
			}
			return d.handleSync(ctx, syncEvent)

		default:
			return fmt.Errorf("unknown event type: %s", temp.Type)
		}

	default:
		return fmt.Errorf("unexpected payload type: %T", msg.Payload)
	}
}

func (d *SaveSoroswapPairsToDuckDB) handleNewPair(ctx context.Context, event processor.NewPairEvent) error {
	// Validate input data
	if event.PairAddress == "" || event.Token0 == "" || event.Token1 == "" {
		return fmt.Errorf("invalid new pair event data: missing required fields")
	}

	log.Printf("Attempting to insert new Soroswap pair: %s (tokens: %s/%s)",
		event.PairAddress, event.Token0, event.Token1)

	stmt, err := d.db.PrepareContext(ctx, `
        INSERT INTO soroswap_pairs (
            pair_address, token_0, token_1, created_at,
            reserve_0, reserve_1
        ) VALUES (?, ?, ?, ?, '0', '0')
        ON CONFLICT (pair_address) DO NOTHING
    `)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	result, err := stmt.ExecContext(ctx,
		event.PairAddress,
		event.Token0,
		event.Token1,
		event.Timestamp,
	)
	if err != nil {
		return fmt.Errorf("failed to insert pair: %v", err)
	}

	affectedRows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %v", err)
	}

	log.Printf("Inserted new Soroswap pair: %s (rows affected: %d)", event.PairAddress, affectedRows)
	return nil
}

func (d *SaveSoroswapPairsToDuckDB) handleSync(ctx context.Context, event processor.SyncEvent) error {
	log.Printf("Checking existence of pair: %s", event.ContractID)

	// First check if the pair exists with more detailed logging
	var exists bool
	query := `SELECT EXISTS (
		SELECT 1 FROM soroswap_pairs WHERE pair_address = ?
	)`

	// Log the actual pair addresses in the database
	rows, err := d.db.QueryContext(ctx, "SELECT pair_address FROM soroswap_pairs")
	if err != nil {
		log.Printf("Error querying pairs: %v", err)
	} else {
		defer rows.Close()
		log.Printf("Existing pairs in database:")
		for rows.Next() {
			var addr string
			if err := rows.Scan(&addr); err == nil {
				log.Printf("- %s", addr)
			}
		}
	}

	err = d.db.QueryRowContext(ctx, query, event.ContractID).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check pair existence: %v", err)
	}

	if !exists {
		log.Printf("Warning: Received sync event for unknown pair: %s", event.ContractID)
		return nil
	}

	stmt, err := d.db.PrepareContext(ctx, `
        UPDATE soroswap_pairs 
        SET reserve_0 = ?,
            reserve_1 = ?,
            last_sync_at = ?,
            last_sync_ledger = ?
        WHERE pair_address = ?
    `)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	result, err := stmt.ExecContext(ctx,
		event.NewReserve0,
		event.NewReserve1,
		event.Timestamp,
		event.LedgerSequence,
		event.ContractID,
	)
	if err != nil {
		return fmt.Errorf("failed to update pair reserves: %v", err)
	}

	affectedRows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %v", err)
	}

	log.Printf("Updated Soroswap pair reserves: %s (rows affected: %d)", event.ContractID, affectedRows)
	return nil
}
