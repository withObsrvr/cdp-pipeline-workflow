package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

type SaveAssetsToDuckDB struct {
	db         *sql.DB
	processors []processor.Processor
}

func NewSaveAssetsToDuckDB(config map[string]interface{}) (*SaveAssetsToDuckDB, error) {
	dbPath, ok := config["db_path"].(string)
	if !ok {
		dbPath = "assets.duckdb"
	}

	db, err := sql.Open("duckdb", fmt.Sprintf("%s?access_mode=READ_WRITE", dbPath))
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %v", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping DuckDB: %v", err)
	}

	// Create assets table
	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS assets (
            asset_id BIGINT PRIMARY KEY,
            asset_code STRING NOT NULL,
            asset_issuer STRING NOT NULL,
            asset_type STRING NOT NULL,
            first_seen_at TIMESTAMP NOT NULL,
            last_seen_at TIMESTAMP NOT NULL,
            operation_count INTEGER NOT NULL DEFAULT 0,
            
            CONSTRAINT valid_asset_type CHECK (asset_type IN ('native', 'credit_alphanum4', 'credit_alphanum12'))
        );

        CREATE INDEX IF NOT EXISTS idx_assets_code ON assets(asset_code);
        CREATE INDEX IF NOT EXISTS idx_assets_issuer ON assets(asset_issuer);
    `)
	if err != nil {
		return nil, fmt.Errorf("failed to create tables: %v", err)
	}

	return &SaveAssetsToDuckDB{
		db:         db,
		processors: make([]processor.Processor, 0),
	}, nil
}

func (d *SaveAssetsToDuckDB) Process(ctx context.Context, msg processor.Message) error {
	payload, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte payload, got %T", msg.Payload)
	}

	var event processor.AssetEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return fmt.Errorf("error decoding event: %w", err)
	}

	log.Printf("Processing asset event: %+v", event)

	// Begin transaction
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Upsert asset record
	_, err = tx.ExecContext(ctx, `
        INSERT INTO assets (
            asset_id, asset_code, asset_issuer, asset_type,
            first_seen_at, last_seen_at, operation_count
        ) VALUES (?, ?, ?, ?, ?, ?, 1)
        ON CONFLICT (asset_id) DO UPDATE SET
            last_seen_at = ?,
            operation_count = operation_count + 1
    `, event.AssetID, event.AssetCode, event.AssetIssuer, event.AssetType,
		event.Timestamp, event.Timestamp, // for insert
		event.Timestamp) // for update

	if err != nil {
		return fmt.Errorf("failed to upsert asset: %v", err)
	}

	return tx.Commit()
}
