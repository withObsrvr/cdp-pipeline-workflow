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

type SaveSoroswapRouterToDuckDB struct {
	db         *sql.DB
	processors []processor.Processor
}

func NewSaveSoroswapRouterToDuckDB(config map[string]interface{}) (*SaveSoroswapRouterToDuckDB, error) {
	dbPath, ok := config["db_path"].(string)
	if !ok {
		dbPath = "soroswap_router.duckdb"
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

	// Update the SQL statements around line 37:
	_, err = db.Exec(`
    -- Create sequence first
    CREATE SEQUENCE IF NOT EXISTS soroswap_router_events_id_seq;

    -- Then create table using the sequence
    CREATE TABLE IF NOT EXISTS soroswap_router_events (
        event_id BIGINT PRIMARY KEY DEFAULT nextval('soroswap_router_events_id_seq'),
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
        
        -- Add constraints
        CONSTRAINT valid_event_type CHECK (event_type IN ('swap', 'add', 'remove')),
        CONSTRAINT valid_account CHECK (length(account) > 0),
        CONSTRAINT valid_token_a CHECK (length(token_a) > 0),
        CONSTRAINT valid_token_b CHECK (length(token_b) > 0),
        CONSTRAINT valid_contract_id CHECK (length(contract_id) > 0)
    );

    -- Add indexes for common queries
    CREATE INDEX IF NOT EXISTS idx_router_event_type ON soroswap_router_events(event_type);
    CREATE INDEX IF NOT EXISTS idx_router_account ON soroswap_router_events(account);
    CREATE INDEX IF NOT EXISTS idx_router_tokens ON soroswap_router_events(token_a, token_b);
    CREATE INDEX IF NOT EXISTS idx_router_timestamp ON soroswap_router_events(timestamp DESC);
`)
	if err != nil {
		return nil, fmt.Errorf("failed to create soroswap_router_events table: %v", err)
	}

	return &SaveSoroswapRouterToDuckDB{
		db:         db,
		processors: make([]processor.Processor, 0),
	}, nil
}

func (d *SaveSoroswapRouterToDuckDB) Subscribe(processor processor.Processor) {
	d.processors = append(d.processors, processor)
}

func (d *SaveSoroswapRouterToDuckDB) Process(ctx context.Context, msg processor.Message) error {
	payload, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte payload, got %T", msg.Payload)
	}

	var routerEvent processor.RouterEvent
	if err := json.Unmarshal(payload, &routerEvent); err != nil {
		return fmt.Errorf("error decoding router event: %w", err)
	}

	log.Printf("Processing %s event from account %s (tokens: %s/%s, amounts: %s/%s)",
		routerEvent.Type, routerEvent.Account, routerEvent.TokenA, routerEvent.TokenB,
		routerEvent.AmountA, routerEvent.AmountB)

	log.Printf("DuckDB Consumer - Attempting to insert event:")
	log.Printf("  Type: %s", routerEvent.Type)
	log.Printf("  Account: %s", routerEvent.Account)
	log.Printf("  TokenA: %s", routerEvent.TokenA)
	log.Printf("  TokenB: %s", routerEvent.TokenB)
	log.Printf("  AmountA: %s", routerEvent.AmountA)
	log.Printf("  AmountB: %s", routerEvent.AmountB)
	log.Printf("  TxHash: %s", routerEvent.TxHash)
	log.Printf("  ContractID: %s", routerEvent.ContractID)

	// Insert the event
	stmt, err := d.db.PrepareContext(ctx, `
        INSERT INTO soroswap_router_events (
            event_type, account, token_a, token_b,
            amount_a, amount_b, timestamp, ledger_sequence,
            tx_hash, contract_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	result, err := stmt.ExecContext(ctx,
		routerEvent.Type,
		routerEvent.Account,
		routerEvent.TokenA,
		routerEvent.TokenB,
		routerEvent.AmountA,
		routerEvent.AmountB,
		routerEvent.Timestamp,
		routerEvent.LedgerSequence,
		routerEvent.TxHash,
		routerEvent.ContractID,
	)
	if err != nil {
		log.Printf("Error inserting into DuckDB: %v", err)
		log.Printf("SQL Statement: INSERT INTO soroswap_router_events (...) VALUES (%s, %s, %s, %s, %s, %s, %s, %d, %s, %s)",
			routerEvent.Type, routerEvent.Account, routerEvent.TokenA, routerEvent.TokenB,
			routerEvent.AmountA, routerEvent.AmountB, routerEvent.Timestamp,
			routerEvent.LedgerSequence, routerEvent.TxHash, routerEvent.ContractID)
		return fmt.Errorf("failed to insert router event: %v", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %v", err)
	}

	log.Printf("Inserted Soroswap router event: %s (rows affected: %d)", routerEvent.Type, rows)

	// Forward to downstream processors
	for _, processor := range d.processors {
		if err := processor.Process(ctx, msg); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

// Helper methods for analytics

func (d *SaveSoroswapRouterToDuckDB) GetEventsByAccount(ctx context.Context, account string) ([]processor.RouterEvent, error) {
	rows, err := d.db.QueryContext(ctx, `
        SELECT event_type, account, token_a, token_b,
               amount_a, amount_b, timestamp, ledger_sequence,
               tx_hash, contract_id
        FROM soroswap_router_events
        WHERE account = ?
        ORDER BY timestamp DESC
    `, account)
	if err != nil {
		return nil, fmt.Errorf("failed to query events: %v", err)
	}
	defer rows.Close()

	var events []processor.RouterEvent
	for rows.Next() {
		var event processor.RouterEvent
		if err := rows.Scan(
			&event.Type,
			&event.Account,
			&event.TokenA,
			&event.TokenB,
			&event.AmountA,
			&event.AmountB,
			&event.Timestamp,
			&event.LedgerSequence,
			&event.TxHash,
			&event.ContractID,
		); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}
		events = append(events, event)
	}

	return events, nil
}

func (d *SaveSoroswapRouterToDuckDB) GetEventsByTokenPair(ctx context.Context, tokenA, tokenB string) ([]processor.RouterEvent, error) {
	rows, err := d.db.QueryContext(ctx, `
        SELECT event_type, account, token_a, token_b,
               amount_a, amount_b, timestamp, ledger_sequence,
               tx_hash, contract_id
        FROM soroswap_router_events
        WHERE (token_a = ? AND token_b = ?) OR (token_a = ? AND token_b = ?)
        ORDER BY timestamp DESC
    `, tokenA, tokenB, tokenB, tokenA)
	if err != nil {
		return nil, fmt.Errorf("failed to query events: %v", err)
	}
	defer rows.Close()

	var events []processor.RouterEvent
	for rows.Next() {
		var event processor.RouterEvent
		if err := rows.Scan(
			&event.Type,
			&event.Account,
			&event.TokenA,
			&event.TokenB,
			&event.AmountA,
			&event.AmountB,
			&event.Timestamp,
			&event.LedgerSequence,
			&event.TxHash,
			&event.ContractID,
		); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}
		events = append(events, event)
	}

	return events, nil
}
