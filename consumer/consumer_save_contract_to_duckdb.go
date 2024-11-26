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

// FormattedEvent represents a processed Soroban event
type FormattedEvent struct {
	Type                     string      `json:"type"`
	Ledger                   uint64      `json:"ledger"`
	LedgerClosedAt           string      `json:"ledgerClosedAt"`
	ContractID               string      `json:"contractId"`
	ID                       string      `json:"id"`
	PagingToken              string      `json:"pagingToken"`
	Topic                    interface{} `json:"topic"`
	Value                    interface{} `json:"value"`
	InSuccessfulContractCall bool        `json:"inSuccessfulContractCall"`
	TxHash                   string      `json:"txHash"`
}

type SaveContractToDuckDB struct {
	db         *sql.DB
	processors []processor.Processor
}

func NewSaveContractToDuckDB(config map[string]interface{}) (*SaveContractToDuckDB, error) {
	dbPath, ok := config["db_path"].(string)
	if !ok {
		dbPath = "soroban_events.duckdb"
	}

	// Open DuckDB connection with memory_limit and threads parameters
	db, err := sql.Open("duckdb", fmt.Sprintf("%s?access_mode=READ_WRITE&memory_limit=4GB&threads=4", dbPath))
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %v", err)
	}

	// Verify connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping DuckDB: %v", err)
	}

	// Initialize tables and views
	if err := initializeDuckDBTables(db); err != nil {
		return nil, err
	}

	return &SaveContractToDuckDB{
		db: db,
	}, nil
}

func initializeDuckDBTables(db *sql.DB) error {
	queries := []string{
		// Main events table
		`CREATE TABLE IF NOT EXISTS soroban_events (
			timestamp TIMESTAMP,
			ledger BIGINT,
			contract_id VARCHAR,
			event_id VARCHAR,
			paging_token VARCHAR,
			tx_hash VARCHAR,
			event_type VARCHAR,
			topic JSON,
			value JSON,
			in_successful_contract_call BOOLEAN,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,

		// Create view for contract activity analysis
		`CREATE VIEW IF NOT EXISTS contract_activity AS 
		SELECT 
			contract_id,
			DATE_TRUNC('hour', timestamp) as hour,
			COUNT(*) as event_count,
			COUNT(DISTINCT tx_hash) as unique_txs,
			SUM(CASE WHEN in_successful_contract_call THEN 1 ELSE 0 END) as successful_calls
		FROM soroban_events
		GROUP BY contract_id, DATE_TRUNC('hour', timestamp)`,

		// Create view for hourly event statistics
		`CREATE VIEW IF NOT EXISTS hourly_event_stats AS
		SELECT 
			DATE_TRUNC('hour', timestamp) as hour,
			event_type,
			COUNT(*) as event_count,
			COUNT(DISTINCT contract_id) as unique_contracts,
			COUNT(DISTINCT tx_hash) as unique_transactions
		FROM soroban_events
		GROUP BY DATE_TRUNC('hour', timestamp), event_type`,

		// Create indexes for better query performance
		`CREATE INDEX IF NOT EXISTS idx_soroban_events_contract_id ON soroban_events(contract_id)`,
		`CREATE INDEX IF NOT EXISTS idx_soroban_events_timestamp ON soroban_events(timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_soroban_events_tx_hash ON soroban_events(tx_hash)`,
	}

	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			return fmt.Errorf("failed to execute query [%s]: %v", query, err)
		}
	}

	log.Println("Successfully initialized DuckDB tables and views")
	return nil
}

func (d *SaveContractToDuckDB) Subscribe(processor processor.Processor) {
	d.processors = append(d.processors, processor)
}

func (d *SaveContractToDuckDB) Process(ctx context.Context, msg processor.Message) error {
	event, ok := msg.Payload.(FormattedEvent)
	if !ok {
		return fmt.Errorf("expected FormattedEvent, got %T", msg.Payload)
	}

	// Convert topic and value to JSON strings
	topicJSON, err := json.Marshal(event.Topic)
	if err != nil {
		return fmt.Errorf("error marshaling topic: %w", err)
	}

	valueJSON, err := json.Marshal(event.Value)
	if err != nil {
		return fmt.Errorf("error marshaling value: %w", err)
	}

	// Parse timestamp
	timestamp, err := time.Parse(time.RFC3339, event.LedgerClosedAt)
	if err != nil {
		return fmt.Errorf("error parsing timestamp: %w", err)
	}

	// Insert event
	log.Printf("Inserting event %s for contract %s", event.ID, event.ContractID)
	stmt, err := d.db.PrepareContext(ctx, `
		INSERT INTO soroban_events (
			timestamp, ledger, contract_id, event_id,
			paging_token, tx_hash, event_type, topic,
			value, in_successful_contract_call
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx,
		timestamp,
		event.Ledger,
		event.ContractID,
		event.ID,
		event.PagingToken,
		event.TxHash,
		event.Type,
		string(topicJSON),
		string(valueJSON),
		event.InSuccessfulContractCall,
	)

	if err != nil {
		return fmt.Errorf("failed to insert event: %v", err)
	}

	log.Printf("Successfully stored Soroban event ID %s for contract %s",
		event.ID, event.ContractID)
	return nil
}

// Analytics helper methods

// GetContractActivity retrieves activity metrics for a specific contract
func (d *SaveContractToDuckDB) GetContractActivity(ctx context.Context, contractID string, startTime, endTime time.Time) ([]ContractActivityMetric, error) {
	query := `
		SELECT 
			hour,
			event_count,
			unique_txs,
			successful_calls
		FROM contract_activity
		WHERE contract_id = ?
		AND hour BETWEEN ? AND ?
		ORDER BY hour
	`

	rows, err := d.db.QueryContext(ctx, query, contractID, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract activity: %v", err)
	}
	defer rows.Close()

	var metrics []ContractActivityMetric
	for rows.Next() {
		var m ContractActivityMetric
		if err := rows.Scan(&m.Hour, &m.EventCount, &m.UniqueTxs, &m.SuccessfulCalls); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}
		metrics = append(metrics, m)
	}

	return metrics, nil
}

// GetEventTypeDistribution retrieves the distribution of event types
func (d *SaveContractToDuckDB) GetEventTypeDistribution(ctx context.Context, startTime, endTime time.Time) ([]EventTypeMetric, error) {
	query := `
		SELECT 
			event_type,
			COUNT(*) as event_count,
			COUNT(DISTINCT contract_id) as contract_count
		FROM soroban_events
		WHERE timestamp BETWEEN ? AND ?
		GROUP BY event_type
		ORDER BY event_count DESC
	`

	rows, err := d.db.QueryContext(ctx, query, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to query event distribution: %v", err)
	}
	defer rows.Close()

	var metrics []EventTypeMetric
	for rows.Next() {
		var m EventTypeMetric
		if err := rows.Scan(&m.EventType, &m.EventCount, &m.ContractCount); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}
		metrics = append(metrics, m)
	}

	return metrics, nil
}

// SearchEvents performs a full-text search across event topics and values
func (d *SaveContractToDuckDB) SearchEvents(ctx context.Context, searchTerm string, limit int) ([]FormattedEvent, error) {
	query := `
		SELECT 
			timestamp,
			ledger,
			contract_id,
			event_id,
			paging_token,
			tx_hash,
			event_type,
			topic,
			value,
			in_successful_contract_call
		FROM soroban_events
		WHERE 
			topic::TEXT LIKE ? OR 
			value::TEXT LIKE ?
		ORDER BY timestamp DESC
		LIMIT ?
	`

	searchPattern := "%" + searchTerm + "%"
	rows, err := d.db.QueryContext(ctx, query, searchPattern, searchPattern, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to search events: %v", err)
	}
	defer rows.Close()

	var events []FormattedEvent
	for rows.Next() {
		var e FormattedEvent
		var topicJSON, valueJSON string
		var timestamp time.Time

		if err := rows.Scan(
			&timestamp,
			&e.Ledger,
			&e.ContractID,
			&e.ID,
			&e.PagingToken,
			&e.TxHash,
			&e.Type,
			&topicJSON,
			&valueJSON,
			&e.InSuccessfulContractCall,
		); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		e.LedgerClosedAt = timestamp.Format(time.RFC3339)

		// Parse JSON fields
		if err := json.Unmarshal([]byte(topicJSON), &e.Topic); err != nil {
			return nil, fmt.Errorf("failed to parse topic JSON: %v", err)
		}
		if err := json.Unmarshal([]byte(valueJSON), &e.Value); err != nil {
			return nil, fmt.Errorf("failed to parse value JSON: %v", err)
		}

		events = append(events, e)
	}

	return events, nil
}

func (d *SaveContractToDuckDB) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

// Metric types for analytics

type ContractActivityMetric struct {
	Hour            time.Time
	EventCount      int
	UniqueTxs       int
	SuccessfulCalls int
}

type EventTypeMetric struct {
	EventType     string
	EventCount    int
	ContractCount int
}
