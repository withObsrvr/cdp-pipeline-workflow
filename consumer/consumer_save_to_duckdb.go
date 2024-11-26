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

type SaveToDuckDB struct {
	db         *sql.DB
	processors []processor.Processor
}

func NewSaveToDuckDB(config map[string]interface{}) (*SaveToDuckDB, error) {
	dbPath, ok := config["db_path"].(string)
	if !ok {
		dbPath = "stellar_operations.duckdb"
	}

	// Open DuckDB connection
	db, err := sql.Open("duckdb", dbPath+"?access_mode=READ_WRITE")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %v", err)
	}

	// Verify connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping DuckDB: %v", err)
	}

	// Initialize tables
	if err := initializeTables(db); err != nil {
		return nil, err
	}

	return &SaveToDuckDB{
		db: db,
	}, nil
}

func initializeTables(db *sql.DB) error {
	ctx := context.Background()

	// Create payments table
	_, err := db.ExecContext(ctx, `
        CREATE TABLE IF NOT EXISTS payments (
            timestamp VARCHAR,
            buyer_account_id VARCHAR,
            seller_account_id VARCHAR,
            asset_code VARCHAR,
            amount VARCHAR,
            type VARCHAR,
            created_at TIMESTAMP
        )
    `)
	if err != nil {
		return fmt.Errorf("failed to create payments table: %v", err)
	}

	// Create create_accounts table
	_, err = db.ExecContext(ctx, `
        CREATE TABLE IF NOT EXISTS create_accounts (
            timestamp VARCHAR,
            funder VARCHAR,
            account VARCHAR,
            starting_balance VARCHAR,
            type VARCHAR,
            created_at TIMESTAMP
        )
    `)
	if err != nil {
		return fmt.Errorf("failed to create create_accounts table: %v", err)
	}

	log.Println("DuckDB tables initialized successfully")
	return nil
}

func (d *SaveToDuckDB) Subscribe(processor processor.Processor) {
	d.processors = append(d.processors, processor)
}

func (d *SaveToDuckDB) Process(ctx context.Context, msg processor.Message) error {
	payloadBytes, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte type for message.Payload, got %T", msg.Payload)
	}
	// Determine operation type
	var rawDoc map[string]interface{}
	if err := json.Unmarshal(payloadBytes, &rawDoc); err != nil {
		return fmt.Errorf("failed to unmarshal payload: %v", err)
	}

	operationType, ok := rawDoc["type"].(string)
	if !ok {
		return fmt.Errorf("missing operation type in payload")
	}

	switch operationType {
	case "payment":
		var payment AppPayment
		if err := json.Unmarshal(payloadBytes, &payment); err != nil {
			return fmt.Errorf("failed to unmarshal payment: %v", err)
		}
		return d.insertPayment(ctx, &payment)

	case "create_account":
		var createAccount CreateAccountOp
		if err := json.Unmarshal(payloadBytes, &createAccount); err != nil {
			return fmt.Errorf("failed to unmarshal create account: %v", err)
		}
		return d.insertCreateAccount(ctx, &createAccount)

	default:
		return fmt.Errorf("unknown operation type: %s", operationType)
	}
}

func (d *SaveToDuckDB) insertPayment(ctx context.Context, payment *AppPayment) error {
	stmt, err := d.db.PrepareContext(ctx, `
        INSERT INTO payments (
            timestamp, buyer_account_id, seller_account_id,
            asset_code, amount, type, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    `)
	if err != nil {
		return fmt.Errorf("failed to prepare payment statement: %v", err)
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx,
		payment.Timestamp,
		payment.BuyerAccountId,
		payment.SellerAccountId,
		payment.AssetCode,
		payment.Amount,
		"payment",
		time.Now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("failed to insert payment: %v", err)
	}

	log.Printf("Payment record inserted successfully")
	return nil
}

func (d *SaveToDuckDB) insertCreateAccount(ctx context.Context, createAccount *CreateAccountOp) error {
	stmt, err := d.db.PrepareContext(ctx, `
        INSERT INTO create_accounts (
            timestamp, funder, account,
            starting_balance, type, created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
    `)
	if err != nil {
		return fmt.Errorf("failed to prepare create account statement: %v", err)
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx,
		createAccount.Timestamp,
		createAccount.Funder,
		createAccount.Account,
		createAccount.StartingBalance,
		"create_account",
		time.Now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("failed to insert create account: %v", err)
	}

	log.Printf("Create account record inserted successfully")
	return nil
}

func (d *SaveToDuckDB) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

type CreateAccountOp struct {
	Timestamp       string `json:"timestamp"`
	Funder          string `json:"funder"`
	Account         string `json:"account"`
	StartingBalance string `json:"starting_balance"`
	Type            string `json:"type"`
}
