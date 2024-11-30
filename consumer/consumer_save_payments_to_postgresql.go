package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

type SavePaymentToPostgreSQL struct {
	db           *sql.DB
	processors   []processor.Processor
	batchSize    int
	paymentBatch []Payment
	stats        struct {
		messagesReceived int64
		batchesProcessed int64
		lastProcessedAt  time.Time
	}
}

type Payment struct {
	Timestamp       time.Time
	LedgerSequence  int64
	BuyerAccountID  string
	SellerAccountID string
	AssetCode       string
	AssetIssuer     string
	Amount          string
	Memo            string
	TxHash          string
}

type PaymentsPostgreSQLConfig struct {
	ConnectionString string
	BatchSize        int
}

func parsePaymentsPostgreSQLConfig(config map[string]interface{}) (PaymentsPostgreSQLConfig, error) {
	var pgConfig PaymentsPostgreSQLConfig

	// Set default batch size
	pgConfig.BatchSize = 1000

	// Override with config if provided
	if batchSize, ok := config["batch_size"].(int); ok {
		pgConfig.BatchSize = batchSize
	}

	// Get connection string
	connStr, ok := config["connection_string"].(string)
	if !ok || connStr == "" {
		return pgConfig, fmt.Errorf("missing or empty connection_string in config")
	}
	pgConfig.ConnectionString = connStr

	return pgConfig, nil
}

func NewSavePaymentToPostgreSQL(config map[string]interface{}) (*SavePaymentToPostgreSQL, error) {
	pgConfig, err := parsePaymentsPostgreSQLConfig(config)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("postgres", pgConfig.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %v", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping PostgreSQL: %v", err)
	}

	if err := initializePaymentDatabase(db); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %v", err)
	}

	return &SavePaymentToPostgreSQL{
		db:        db,
		batchSize: pgConfig.BatchSize,
	}, nil
}

func initializePaymentDatabase(db *sql.DB) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS payments (
			id                BIGSERIAL PRIMARY KEY,
			timestamp        TIMESTAMPTZ NOT NULL,
			ledger_sequence  BIGINT NOT NULL,
			buyer_account_id VARCHAR(128) NOT NULL,
			seller_account_id VARCHAR(128) NOT NULL,
			asset_code       VARCHAR(32) NOT NULL,
			asset_issuer     VARCHAR(128),
			amount          NUMERIC(20,7) NOT NULL,
			memo            TEXT,
			tx_hash         VARCHAR(64),
			created_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
		)`,

		`DO $$ 
		BEGIN
			ALTER TABLE payments
			ADD CONSTRAINT check_timestamp_range 
			CHECK (timestamp >= '2015-01-01' AND timestamp <= '2100-01-01');
		EXCEPTION
			WHEN duplicate_object THEN
				NULL;
		END $$;`,

		`CREATE INDEX IF NOT EXISTS idx_payments_timestamp ON payments(timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_payments_accounts ON payments(buyer_account_id, seller_account_id)`,
		`CREATE INDEX IF NOT EXISTS idx_payments_asset ON payments(asset_code, asset_issuer)`,
	}

	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			return fmt.Errorf("error executing query: %w", err)
		}
	}

	return nil
}

func (p *SavePaymentToPostgreSQL) Subscribe(processor processor.Processor) {
	p.processors = append(p.processors, processor)
}

func (p *SavePaymentToPostgreSQL) Process(ctx context.Context, msg processor.Message) error {
	payload, ok := msg.Payload.([]byte)
	if !ok {
		log.Printf("[SavePaymentToPostgreSQL] Invalid payload type: expected []byte, got %T", msg.Payload)
		return fmt.Errorf("expected []byte payload, got %T", msg.Payload)
	}

	var data map[string]interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		log.Printf("[SavePaymentToPostgreSQL] Failed to unmarshal payload: %v", err)
		return fmt.Errorf("error unmarshaling payload: %w", err)
	}

	log.Printf("[SavePaymentToPostgreSQL] Processing payment data: %+v", data)

	payment, err := p.parsePayment(data)
	if err != nil {
		log.Printf("[SavePaymentToPostgreSQL] Failed to parse payment: %v", err)
		return fmt.Errorf("error parsing payment: %w", err)
	}

	log.Printf("[SavePaymentToPostgreSQL] Successfully parsed payment: %+v", payment)

	p.paymentBatch = append(p.paymentBatch, payment)
	p.stats.messagesReceived++

	log.Printf("[SavePaymentToPostgreSQL] Current batch size: %d/%d", len(p.paymentBatch), p.batchSize)

	if len(p.paymentBatch) >= p.batchSize {
		if err := p.processBatch(ctx); err != nil {
			log.Printf("[SavePaymentToPostgreSQL] Failed to process batch: %v", err)
			return fmt.Errorf("error processing batch: %w", err)
		}
		log.Printf("[SavePaymentToPostgreSQL] Successfully processed batch of %d payments", p.batchSize)
	}

	return nil
}

func (p *SavePaymentToPostgreSQL) parsePayment(data map[string]interface{}) (Payment, error) {
	log.Printf("[SavePaymentToPostgreSQL] Starting to parse payment data: %+v", data)

	timestamp, err := parsePostgresTimestamp(data)
	if err != nil {
		log.Printf("[SavePaymentToPostgreSQL] Failed to parse timestamp: %v", err)
		return Payment{}, fmt.Errorf("error parsing timestamp: %w", err)
	}
	log.Printf("[SavePaymentToPostgreSQL] Parsed timestamp: %v", timestamp)

	ledgerSeq, err := extractLedgerSequence(data)
	if err != nil {
		log.Printf("[SavePaymentToPostgreSQL] Failed to extract ledger sequence: %v", err)
		return Payment{}, fmt.Errorf("error extracting ledger sequence: %w", err)
	}
	log.Printf("[SavePaymentToPostgreSQL] Extracted ledger sequence: %d", ledgerSeq)

	// Safely get string values with nil checks
	buyerAccountID, ok := data["buyer_account_id"]
	if !ok || buyerAccountID == nil {
		log.Printf("[SavePaymentToPostgreSQL] Missing or nil buyer_account_id")
		return Payment{}, fmt.Errorf("missing or nil buyer_account_id")
	}
	buyerAccountIDStr, ok := buyerAccountID.(string)
	if !ok {
		log.Printf("[SavePaymentToPostgreSQL] Invalid buyer_account_id type: %T", buyerAccountID)
		return Payment{}, fmt.Errorf("buyer_account_id is not a string")
	}

	sellerAccountID, ok := data["seller_account_id"]
	if !ok || sellerAccountID == nil {
		log.Printf("[SavePaymentToPostgreSQL] Missing or nil seller_account_id")
		return Payment{}, fmt.Errorf("missing or nil seller_account_id")
	}
	sellerAccountIDStr, ok := sellerAccountID.(string)
	if !ok {
		log.Printf("[SavePaymentToPostgreSQL] Invalid seller_account_id type: %T", sellerAccountID)
		return Payment{}, fmt.Errorf("seller_account_id is not a string")
	}

	assetCode, ok := data["asset_code"]
	if !ok || assetCode == nil {
		log.Printf("[SavePaymentToPostgreSQL] Missing or nil asset_code")
		return Payment{}, fmt.Errorf("missing or nil asset_code")
	}
	assetCodeStr, ok := assetCode.(string)
	if !ok {
		log.Printf("[SavePaymentToPostgreSQL] Invalid asset_code type: %T", assetCode)
		return Payment{}, fmt.Errorf("asset_code is not a string")
	}

	// Split asset code and issuer
	var assetCodeOnly, assetIssuer string
	parts := strings.Split(assetCodeStr, ":")
	if len(parts) > 1 {
		assetCodeOnly = parts[0]
		assetIssuer = parts[1]
		log.Printf("[SavePaymentToPostgreSQL] Split asset code: %s, issuer: %s", assetCodeOnly, assetIssuer)
	} else {
		assetCodeOnly = assetCodeStr
		log.Printf("[SavePaymentToPostgreSQL] Using simple asset code: %s", assetCodeOnly)
	}

	amount, ok := data["amount"]
	if !ok || amount == nil {
		log.Printf("[SavePaymentToPostgreSQL] Missing or nil amount")
		return Payment{}, fmt.Errorf("missing or nil amount")
	}
	var amountStr string
	if amountStr, ok = amount.(string); !ok {
		if amountNum, ok := amount.(float64); ok {
			amountStr = fmt.Sprintf("%f", amountNum)
			log.Printf("[SavePaymentToPostgreSQL] Converted numeric amount to string: %v -> %s", amountNum, amountStr)
		} else {
			log.Printf("[SavePaymentToPostgreSQL] Invalid amount type: %T", amount)
			return Payment{}, fmt.Errorf("amount is not a string or number")
		}
	}

	var memoStr, txHashStr string
	if memo, ok := data["memo"]; ok && memo != nil {
		if memoStr, ok = memo.(string); !ok {
			log.Printf("[SavePaymentToPostgreSQL] Invalid memo type: %T", memo)
			return Payment{}, fmt.Errorf("memo is not a string")
		}
		log.Printf("[SavePaymentToPostgreSQL] Found memo: %s", memoStr)
	}

	if txHash, ok := data["tx_hash"]; ok && txHash != nil {
		if txHashStr, ok = txHash.(string); !ok {
			log.Printf("[SavePaymentToPostgreSQL] Invalid tx_hash type: %T", txHash)
			return Payment{}, fmt.Errorf("tx_hash is not a string")
		}
		log.Printf("[SavePaymentToPostgreSQL] Found tx_hash: %s", txHashStr)
	}

	payment := Payment{
		Timestamp:       timestamp,
		LedgerSequence:  ledgerSeq,
		BuyerAccountID:  buyerAccountIDStr,
		SellerAccountID: sellerAccountIDStr,
		AssetCode:       assetCodeOnly,
		AssetIssuer:     assetIssuer,
		Amount:          amountStr,
		Memo:            memoStr,
		TxHash:          txHashStr,
	}
	log.Printf("[SavePaymentToPostgreSQL] Successfully created payment object: %+v", payment)
	return payment, nil
}

func (p *SavePaymentToPostgreSQL) processBatch(ctx context.Context) error {
	if len(p.paymentBatch) == 0 {
		log.Printf("[SavePaymentToPostgreSQL] No payments to process in batch")
		return nil
	}

	log.Printf("[SavePaymentToPostgreSQL] Starting to process batch of %d payments", len(p.paymentBatch))

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("[SavePaymentToPostgreSQL] Failed to begin transaction: %v", err)
		return fmt.Errorf("error starting transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO payments (
			timestamp, ledger_sequence, buyer_account_id, seller_account_id,
			asset_code, asset_issuer, amount, memo, tx_hash
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`)
	if err != nil {
		log.Printf("[SavePaymentToPostgreSQL] Failed to prepare statement: %v", err)
		return fmt.Errorf("error preparing statement: %w", err)
	}
	defer stmt.Close()

	for i, payment := range p.paymentBatch {
		_, err := stmt.ExecContext(ctx,
			payment.Timestamp,
			payment.LedgerSequence,
			payment.BuyerAccountID,
			payment.SellerAccountID,
			payment.AssetCode,
			payment.AssetIssuer,
			payment.Amount,
			payment.Memo,
			payment.TxHash,
		)
		if err != nil {
			log.Printf("[SavePaymentToPostgreSQL] Failed to insert payment %d: %v", i, err)
			return fmt.Errorf("error inserting payment %d: %w", i, err)
		}
		log.Printf("[SavePaymentToPostgreSQL] Successfully inserted payment %d: %+v", i, payment)
	}

	if err := tx.Commit(); err != nil {
		log.Printf("[SavePaymentToPostgreSQL] Failed to commit transaction: %v", err)
		return fmt.Errorf("error committing transaction: %w", err)
	}

	p.stats.batchesProcessed++
	p.stats.lastProcessedAt = time.Now()
	log.Printf("[SavePaymentToPostgreSQL] Successfully processed batch. Total batches: %d, Total messages: %d, Last processed: %v",
		p.stats.batchesProcessed, p.stats.messagesReceived, p.stats.lastProcessedAt)

	p.paymentBatch = p.paymentBatch[:0]
	return nil
}

func (p *SavePaymentToPostgreSQL) Close() error {
	log.Printf("[SavePaymentToPostgreSQL] Closing consumer. Processing final batch...")
	ctx := context.Background()
	if err := p.processBatch(ctx); err != nil {
		log.Printf("[SavePaymentToPostgreSQL] Error processing final batch: %v", err)
	}

	if p.db != nil {
		log.Printf("[SavePaymentToPostgreSQL] Closing database connection")
		return p.db.Close()
	}
	return nil
}
