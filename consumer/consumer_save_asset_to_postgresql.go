package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

type SaveAssetToPostgreSQL struct {
	db         *sql.DB
	processors []processor.Processor
	batchSize  int
	assetBatch []TickerAsset
	stats      struct {
		messagesReceived int64
		batchesProcessed int64
		lastProcessedAt  time.Time
	}
}

type PostgreSQLConfig struct {
	ConnectionString string
	BatchSize        int
	ConnectTimeout   int
}

type TickerAsset struct {
	Code                        string    `json:"code"`
	Issuer                      string    `json:"issuer"`
	AssetType                   string    `json:"asset_type"`
	Amount                      string    `json:"amount"`
	AuthRequired                bool      `json:"auth_required"`
	AuthRevocable               bool      `json:"auth_revocable"`
	IsValid                     bool      `json:"is_valid"`
	ValidationError             string    `json:"validation_error"`
	LastValid                   time.Time `json:"last_valid"`
	LastChecked                 time.Time `json:"last_checked"`
	FirstSeenLedger             uint32    `json:"first_seen_ledger"`
	DisplayDecimals             int       `json:"display_decimals"`
	HomeDomain                  string    `json:"home_domain"`
	Name                        string    `json:"name"`
	Desc                        string    `json:"description"`
	Conditions                  string    `json:"conditions"`
	IsAssetAnchored             bool      `json:"is_asset_anchored"`
	FixedNumber                 int       `json:"fixed_number"`
	MaxNumber                   int       `json:"max_number"`
	IsUnlimited                 bool      `json:"is_unlimited"`
	RedemptionInstructions      string    `json:"redemption_instructions"`
	CollateralAddresses         string    `json:"collateral_addresses"`
	CollateralAddressSignatures string    `json:"collateral_address_signatures"`
	Countries                   string    `json:"countries"`
	Status                      string    `json:"status"`
	Type                        string    `json:"type"`
	OperationType               string    `json:"operation_type"`
}

func NewSaveAssetToPostgreSQL(config map[string]interface{}) (*SaveAssetToPostgreSQL, error) {
	// Parse configuration
	pgConfig, err := parsePostgreSQLConfig(config)
	if err != nil {
		return nil, err
	}

	log.Printf("Connecting to PostgreSQL with connection string: %s", pgConfig.ConnectionString)

	// Connect to PostgreSQL
	db, err := sql.Open("postgres", pgConfig.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %v", err)
	}

	// Test connection with context timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(pgConfig.ConnectTimeout)*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping PostgreSQL: %v", err)
	}

	log.Printf("Successfully connected to PostgreSQL")

	consumer := &SaveAssetToPostgreSQL{
		db:         db,
		batchSize:  pgConfig.BatchSize,
		assetBatch: make([]TickerAsset, 0, pgConfig.BatchSize),
	}

	// Initialize database schema
	if err := consumer.initializeDatabase(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %v", err)
	}

	return consumer, nil
}

func parsePostgreSQLConfig(config map[string]interface{}) (PostgreSQLConfig, error) {
	var pgConfig PostgreSQLConfig

	// Set defaults
	pgConfig.BatchSize = 100
	pgConfig.ConnectTimeout = 30 // Default to 30 seconds

	// Override with config if provided
	if batchSize, ok := config["batch_size"].(float64); ok {
		pgConfig.BatchSize = int(batchSize)
	}

	if connectTimeout, ok := config["connect_timeout"].(float64); ok {
		pgConfig.ConnectTimeout = int(connectTimeout)
	}

	// Check if connection string is provided directly
	connStr, ok := config["connection_string"].(string)
	if ok && connStr != "" {
		pgConfig.ConnectionString = connStr
		return pgConfig, nil
	}

	// Otherwise, build connection string from individual parameters
	host, ok := config["host"].(string)
	if !ok {
		return pgConfig, fmt.Errorf("missing host in config")
	}

	port := 5432 // Default PostgreSQL port
	if portVal, ok := config["port"].(float64); ok {
		port = int(portVal)
	}

	database, ok := config["database"].(string)
	if !ok {
		return pgConfig, fmt.Errorf("missing database in config")
	}

	username, ok := config["username"].(string)
	if !ok {
		return pgConfig, fmt.Errorf("missing username in config")
	}

	password, ok := config["password"].(string)
	if !ok {
		return pgConfig, fmt.Errorf("missing password in config")
	}

	sslMode := "disable" // Default to disable
	if sslModeVal, ok := config["sslmode"].(string); ok {
		sslMode = sslModeVal
	}

	// Build connection string
	pgConfig.ConnectionString = fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s connect_timeout=%d",
		host, port, database, username, password, sslMode, pgConfig.ConnectTimeout,
	)

	return pgConfig, nil
}

func (p *SaveAssetToPostgreSQL) Subscribe(processor processor.Processor) {
	p.processors = append(p.processors, processor)
}

func (p *SaveAssetToPostgreSQL) Process(ctx context.Context, msg processor.Message) error {
	log.Printf("[SaveAssetToPostgreSQL] Received message. Current batch size: %d/%d",
		len(p.assetBatch), p.batchSize)

	// Parse message payload
	var asset TickerAsset
	if err := json.Unmarshal(msg.Payload.([]byte), &asset); err != nil {
		log.Printf("[SaveAssetToPostgreSQL] Error unmarshaling payload: %v", err)
		return fmt.Errorf("error unmarshaling payload: %w", err)
	}

	log.Printf("[SaveAssetToPostgreSQL] Processing asset: %s (Issuer: %s)",
		asset.Code, asset.Issuer)

	// Add asset to batch
	p.assetBatch = append(p.assetBatch, asset)
	p.stats.messagesReceived++

	// If batch size reached, flush batch
	if len(p.assetBatch) >= p.batchSize {
		log.Printf("[SaveAssetToPostgreSQL] Batch size reached (%d). Flushing...", p.batchSize)
		if err := p.flushBatch(ctx); err != nil {
			log.Printf("[SaveAssetToPostgreSQL] Error flushing batch: %v", err)
			return err
		}
	}

	return nil
}

func (p *SaveAssetToPostgreSQL) flushBatch(ctx context.Context) error {
	if len(p.assetBatch) == 0 {
		return nil
	}

	startTime := time.Now()
	log.Printf("[SaveAssetToPostgreSQL] Starting batch flush of %d assets", len(p.assetBatch))

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("[SaveAssetToPostgreSQL] Failed to begin transaction: %v", err)
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	stmt, err := tx.PrepareContext(ctx, `
        INSERT INTO assets (
            code, issuer, asset_type, auth_required, auth_revocable, is_valid,
            validation_error, last_valid, last_checked, first_seen_ledger, display_decimals, home_domain,
            name, description, conditions, is_asset_anchored, fixed_number, max_number,
            is_unlimited, redemption_instructions, collateral_addresses,
            collateral_address_signatures, countries, status, type, operation_type
        ) VALUES (
            $1, $2, $3, $4, $5, $6,
            $7, $8, $9, $10, $11, $12,
            $13, $14, $15, $16, $17, $18,
            $19, $20, $21,
            $22, $23, $24, $25, $26
        )
        ON CONFLICT (code, issuer) DO UPDATE SET
            auth_required = EXCLUDED.auth_required,
            auth_revocable = EXCLUDED.auth_revocable,
            is_valid = EXCLUDED.is_valid,
            validation_error = EXCLUDED.validation_error,
            last_valid = EXCLUDED.last_valid,
            last_checked = EXCLUDED.last_checked,
            display_decimals = EXCLUDED.display_decimals,
            home_domain = EXCLUDED.home_domain,
            name = EXCLUDED.name,
            description = EXCLUDED.description,
            conditions = EXCLUDED.conditions,
            is_asset_anchored = EXCLUDED.is_asset_anchored,
            fixed_number = EXCLUDED.fixed_number,
            max_number = EXCLUDED.max_number,
            is_unlimited = EXCLUDED.is_unlimited,
            redemption_instructions = EXCLUDED.redemption_instructions,
            collateral_addresses = EXCLUDED.collateral_addresses,
            collateral_address_signatures = EXCLUDED.collateral_address_signatures,
            countries = EXCLUDED.countries,
            status = EXCLUDED.status,
            type = EXCLUDED.type,
            operation_type = EXCLUDED.operation_type
    `)
	if err != nil {
		tx.Rollback()
		log.Printf("[SaveAssetToPostgreSQL] Failed to prepare statement: %v", err)
		return fmt.Errorf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Process each asset in batch
	for i, asset := range p.assetBatch {
		_, err := stmt.ExecContext(ctx,
			asset.Code, asset.Issuer, asset.AssetType, asset.AuthRequired, asset.AuthRevocable, asset.IsValid,
			asset.ValidationError, asset.LastValid, asset.LastChecked, asset.FirstSeenLedger, asset.DisplayDecimals, asset.HomeDomain,
			asset.Name, asset.Desc, asset.Conditions, asset.IsAssetAnchored, asset.FixedNumber, asset.MaxNumber,
			asset.IsUnlimited, asset.RedemptionInstructions, asset.CollateralAddresses,
			asset.CollateralAddressSignatures, asset.Countries, asset.Status, asset.Type, asset.OperationType,
		)
		if err != nil {
			tx.Rollback()
			log.Printf("[SaveAssetToPostgreSQL] Failed to insert asset %s (Issuer: %s): %v",
				asset.Code, asset.Issuer, err)
			return fmt.Errorf("failed to insert asset: %v", err)
		}

		if (i+1)%100 == 0 {
			log.Printf("[SaveAssetToPostgreSQL] Processed %d/%d assets in current batch",
				i+1, len(p.assetBatch))
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("[SaveAssetToPostgreSQL] Failed to commit transaction: %v", err)
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	duration := time.Since(startTime)
	p.stats.batchesProcessed++
	p.stats.lastProcessedAt = time.Now()

	log.Printf("[SaveAssetToPostgreSQL] Successfully flushed batch of %d assets in %v. "+
		"Total processed: %d messages in %d batches",
		len(p.assetBatch), duration, p.stats.messagesReceived, p.stats.batchesProcessed)

	// Clear the batch
	p.assetBatch = p.assetBatch[:0]

	return nil
}

func (p *SaveAssetToPostgreSQL) Close() error {
	// Flush any remaining assets
	if err := p.flushBatch(context.Background()); err != nil {
		return err
	}

	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

func (p *SaveAssetToPostgreSQL) initializeDatabase(ctx context.Context) error {
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS public.assets (
		code text COLLATE pg_catalog."default" NOT NULL,
		issuer text COLLATE pg_catalog."default" NOT NULL,
		asset_type text COLLATE pg_catalog."default" NOT NULL,
		auth_required boolean,
		auth_revocable boolean,
		auth_immutable boolean,
		auth_clawback boolean,
		is_valid boolean,
		validation_error text COLLATE pg_catalog."default",
		last_valid timestamp without time zone,
		last_checked timestamp without time zone,
		display_decimals integer,
		home_domain text COLLATE pg_catalog."default",
		toml_url text COLLATE pg_catalog."default",
		name text COLLATE pg_catalog."default",
		description text COLLATE pg_catalog."default",
		conditions text COLLATE pg_catalog."default",
		is_asset_anchored boolean,
		fixed_number integer,
		max_number integer,
		is_unlimited boolean,
		redemption_instructions text COLLATE pg_catalog."default",
		collateral_addresses text COLLATE pg_catalog."default",
		collateral_address_signatures text COLLATE pg_catalog."default",
		countries text COLLATE pg_catalog."default",
		status text COLLATE pg_catalog."default",
		type text COLLATE pg_catalog."default",
		operation_type text COLLATE pg_catalog."default",
		first_seen_ledger integer,
		last_updated timestamp with time zone,
		CONSTRAINT assets_pkey PRIMARY KEY (code, issuer)
	);

	CREATE INDEX IF NOT EXISTS idx_assets_first_seen_ledger
		ON public.assets USING btree
		(first_seen_ledger ASC NULLS LAST);
	`

	_, err := p.db.ExecContext(ctx, createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create assets table: %w", err)
	}

	return nil
}
