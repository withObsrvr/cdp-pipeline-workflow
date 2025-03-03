package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

type SaveSoroswapRouterToSQLite struct {
	db         *sql.DB
	processors []processor.Processor
}

func NewSaveSoroswapRouterToSQLite(config map[string]interface{}) (*SaveSoroswapRouterToSQLite, error) {
	dbPath, ok := config["db_path"].(string)
	if !ok {
		dbPath = "soroswap_router.sqlite"
	}

	// Open SQLite connection
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite: %v", err)
	}

	// Verify connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping SQLite: %v", err)
	}

	// Set pragmas for better performance
	if _, err := db.Exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;"); err != nil {
		return nil, fmt.Errorf("failed to set SQLite pragmas: %v", err)
	}

	// Create router events table
	_, err = db.Exec(`
    CREATE TABLE IF NOT EXISTS soroswap_router_events (
        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_type TEXT NOT NULL,
        account TEXT NOT NULL,
        token_a TEXT NOT NULL,
        token_b TEXT NOT NULL,
        amount_a TEXT NOT NULL,
        amount_b TEXT NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        ledger_sequence INTEGER NOT NULL,
        tx_hash TEXT NOT NULL,
        contract_id TEXT NOT NULL,
        
        -- Add constraints
        CHECK (event_type IN ('swap', 'add', 'remove')),
        CHECK (length(account) > 0),
        CHECK (length(token_a) > 0),
        CHECK (length(token_b) > 0),
        CHECK (length(contract_id) > 0)
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

	return &SaveSoroswapRouterToSQLite{
		db:         db,
		processors: make([]processor.Processor, 0),
	}, nil
}

func (d *SaveSoroswapRouterToSQLite) Subscribe(processor processor.Processor) {
	d.processors = append(d.processors, processor)
}

func (d *SaveSoroswapRouterToSQLite) Process(ctx context.Context, msg processor.Message) error {
	// Add timeout to context
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

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

	// Begin transaction for better error handling
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback() // Will be ignored if transaction is committed

	// Insert the event
	stmt, err := tx.PrepareContext(ctx, `
        INSERT INTO soroswap_router_events (
            event_type, account, token_a, token_b,
            amount_a, amount_b, timestamp, ledger_sequence,
            tx_hash, contract_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `)
	if err != nil {
		log.Printf("Error preparing statement: %v", err)
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
		log.Printf("Error inserting into SQLite: %v", err)
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

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	// Forward to downstream processors
	for _, processor := range d.processors {
		if err := processor.Process(ctx, msg); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

// Helper methods for analytics

func (d *SaveSoroswapRouterToSQLite) GetEventsByAccount(ctx context.Context, account string) ([]processor.RouterEvent, error) {
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

func (d *SaveSoroswapRouterToSQLite) GetEventsByTokenPair(ctx context.Context, tokenA, tokenB string) ([]processor.RouterEvent, error) {
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

func (d *SaveSoroswapRouterToSQLite) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}
