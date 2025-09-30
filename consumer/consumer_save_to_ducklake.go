package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/stellar/go/support/log"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// SaveToDuckLake is a consumer that saves data directly to DuckLake tables
type SaveToDuckLake struct {
	// DuckLake configuration
	CatalogPath      string // Path to DuckLake catalog (e.g., "ducklake:stellar.ducklake")
	DataPath         string // Storage path (e.g., "s3://bucket/ducklake" or "./data")
	TableName        string // Target table name
	SchemaName       string // Schema name (default: main)
	
	// Connection settings
	MaxConnections   int
	BatchSize        int
	CommitInterval   time.Duration
	
	// Internal state
	db               *sql.DB
	insertStmt       *sql.Stmt
	buffer           []map[string]interface{}
	lastCommit       time.Time
	mutex            sync.Mutex
	logger           *log.Entry
}

// NewSaveToDuckLake creates a new DuckLake consumer
func NewSaveToDuckLake(config map[string]interface{}) (processor.Processor, error) {
	consumer := &SaveToDuckLake{
		CatalogPath:    "ducklake:stellar.ducklake",
		DataPath:       "./ducklake_data",
		TableName:      "ledgers",
		SchemaName:     "main",
		MaxConnections: 5,
		BatchSize:      100,
		CommitInterval: 10 * time.Second,
		lastCommit:     time.Now(),
		buffer:         make([]map[string]interface{}, 0, 100),
		logger:         log.DefaultLogger.WithField("consumer", "SaveToDuckLake"),
	}

	// Parse configuration
	if catalogPath, ok := config["catalog_path"].(string); ok {
		consumer.CatalogPath = catalogPath
	}
	if dataPath, ok := config["data_path"].(string); ok {
		consumer.DataPath = dataPath
	}
	if tableName, ok := config["table_name"].(string); ok {
		consumer.TableName = tableName
	}
	if schemaName, ok := config["schema_name"].(string); ok {
		consumer.SchemaName = schemaName
	}
	if batchSize, ok := config["batch_size"].(float64); ok {
		consumer.BatchSize = int(batchSize)
	}
	if commitSeconds, ok := config["commit_interval_seconds"].(float64); ok {
		consumer.CommitInterval = time.Duration(commitSeconds) * time.Second
	}

	// Initialize DuckDB connection
	if err := consumer.initializeConnection(); err != nil {
		return nil, fmt.Errorf("failed to initialize DuckLake connection: %w", err)
	}

	consumer.logger.Info("SaveToDuckLake consumer initialized",
		"catalog_path", consumer.CatalogPath,
		"data_path", consumer.DataPath,
		"table", fmt.Sprintf("%s.%s", consumer.SchemaName, consumer.TableName),
		"batch_size", consumer.BatchSize,
	)

	return consumer, nil
}

// initializeConnection sets up the DuckDB connection with DuckLake
func (c *SaveToDuckLake) initializeConnection() error {
	// Open DuckDB connection
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	c.db = db

	// Install and load required extensions
	extensions := []string{
		"INSTALL ducklake",
		"INSTALL httpfs",
		"INSTALL json",
		"LOAD ducklake",
		"LOAD httpfs",
		"LOAD json",
	}

	for _, ext := range extensions {
		if _, err := c.db.Exec(ext); err != nil {
			// Extension might already be installed, log but continue
			c.logger.Debug("Extension setup", "command", ext, "error", err)
		}
	}

	// Attach DuckLake catalog
	attachSQL := fmt.Sprintf(
		"ATTACH '%s' AS stellar_lake (DATA_PATH '%s')",
		c.CatalogPath,
		c.DataPath,
	)
	if _, err := c.db.Exec(attachSQL); err != nil {
		return fmt.Errorf("failed to attach DuckLake catalog: %w", err)
	}

	// Use the schema
	if _, err := c.db.Exec(fmt.Sprintf("USE stellar_lake.%s", c.SchemaName)); err != nil {
		// Schema might not exist, try to create it
		if _, err := c.db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS stellar_lake.%s", c.SchemaName)); err != nil {
			return fmt.Errorf("failed to create schema: %w", err)
		}
		if _, err := c.db.Exec(fmt.Sprintf("USE stellar_lake.%s", c.SchemaName)); err != nil {
			return fmt.Errorf("failed to use schema: %w", err)
		}
	}

	// Create table if needed (based on the data structure)
	if err := c.ensureTable(); err != nil {
		return fmt.Errorf("failed to ensure table exists: %w", err)
	}

	return nil
}

// ensureTable creates the table if it doesn't exist
func (c *SaveToDuckLake) ensureTable() error {
	// Check if table exists
	var exists bool
	checkSQL := fmt.Sprintf(
		"SELECT EXISTS (SELECT 1 FROM duckdb_tables() WHERE database_name = 'stellar_lake' AND schema_name = '%s' AND table_name = '%s')",
		c.SchemaName,
		c.TableName,
	)
	
	if err := c.db.QueryRow(checkSQL).Scan(&exists); err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	if !exists {
		// Create table based on the expected data structure
		createSQL := c.getCreateTableSQL()
		if _, err := c.db.Exec(createSQL); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		c.logger.Info("Created DuckLake table", "table", c.TableName)
	}

	return nil
}

// getCreateTableSQL returns the CREATE TABLE statement based on table type
func (c *SaveToDuckLake) getCreateTableSQL() string {
	switch c.TableName {
	case "ledgers", "ledgers_nested":
		// AWS-compatible nested ledger structure
		return fmt.Sprintf(`
		CREATE TABLE stellar_lake.%s.%s (
			sequence BIGINT NOT NULL,
			ledger_hash VARCHAR NOT NULL,
			previous_ledger_hash VARCHAR NOT NULL,
			closed_at TIMESTAMP NOT NULL,
			protocol_version INT NOT NULL,
			total_coins BIGINT NOT NULL,
			fee_pool BIGINT NOT NULL,
			base_fee INT NOT NULL,
			base_reserve INT NOT NULL,
			max_tx_set_size INT NOT NULL,
			successful_transaction_count INT NOT NULL,
			failed_transaction_count INT NOT NULL,
			soroban_fee_write_1kb BIGINT,
			node_id VARCHAR,
			signature VARCHAR,
			transactions JSON,  -- Store as JSON for flexibility
			ingestion_timestamp TIMESTAMP DEFAULT current_timestamp
		)`, c.SchemaName, c.TableName)
		
	case "transactions", "transaction_analytics":
		// Flattened transaction table
		return fmt.Sprintf(`
		CREATE TABLE stellar_lake.%s.%s (
			ledger_sequence BIGINT NOT NULL,
			transaction_hash VARCHAR NOT NULL,
			account VARCHAR NOT NULL,
			account_muxed VARCHAR,
			account_sequence BIGINT,
			max_fee BIGINT,
			fee_charged BIGINT,
			successful BOOLEAN,
			operation_count INT,
			resource_fee BIGINT,
			soroban_resources_instructions BIGINT,
			memo_type VARCHAR,
			memo VARCHAR,
			closed_at TIMESTAMP,
			ingestion_timestamp TIMESTAMP DEFAULT current_timestamp
		)`, c.SchemaName, c.TableName)
		
	case "contract_events":
		// Contract events table
		return fmt.Sprintf(`
		CREATE TABLE stellar_lake.%s.%s (
			ledger_sequence BIGINT NOT NULL,
			transaction_hash VARCHAR NOT NULL,
			event_id VARCHAR NOT NULL,
			event_type VARCHAR,
			contract_id VARCHAR,
			topics JSON,
			data JSON,
			in_successful_contract_call BOOLEAN,
			closed_at TIMESTAMP,
			ingestion_timestamp TIMESTAMP DEFAULT current_timestamp
		)`, c.SchemaName, c.TableName)
		
	default:
		// Generic JSON table
		return fmt.Sprintf(`
		CREATE TABLE stellar_lake.%s.%s (
			id BIGINT,
			data JSON,
			ingestion_timestamp TIMESTAMP DEFAULT current_timestamp
		)`, c.SchemaName, c.TableName)
	}
}

// Subscribe is not used for consumers
func (c *SaveToDuckLake) Subscribe(processor.Processor) {}

// Process handles incoming data
func (c *SaveToDuckLake) Process(ctx context.Context, msg processor.Message) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Parse JSON payload
	var data map[string]interface{}
	if err := json.Unmarshal(msg.Payload.([]byte), &data); err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	// Add to buffer
	c.buffer = append(c.buffer, data)

	// Check if we should flush
	if len(c.buffer) >= c.BatchSize || time.Since(c.lastCommit) > c.CommitInterval {
		if err := c.flush(ctx); err != nil {
			return fmt.Errorf("failed to flush: %w", err)
		}
	}

	return nil
}

// flush writes buffered data to DuckLake
func (c *SaveToDuckLake) flush(ctx context.Context) error {
	if len(c.buffer) == 0 {
		return nil
	}

	// Begin transaction
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Prepare insert statement based on table type
	insertSQL := c.getInsertSQL()
	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Insert all buffered records
	for _, record := range c.buffer {
		if err := c.insertRecord(stmt, record); err != nil {
			c.logger.Error("Failed to insert record", "error", err)
			// Continue with other records
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	c.logger.Info("Flushed data to DuckLake",
		"records", len(c.buffer),
		"table", c.TableName,
	)

	// Clear buffer and update timestamp
	c.buffer = c.buffer[:0]
	c.lastCommit = time.Now()

	return nil
}

// getInsertSQL returns the INSERT statement based on table type
func (c *SaveToDuckLake) getInsertSQL() string {
	switch c.TableName {
	case "ledgers", "ledgers_nested":
		return fmt.Sprintf(`
		INSERT INTO stellar_lake.%s.%s 
		(sequence, ledger_hash, previous_ledger_hash, closed_at, protocol_version,
		 total_coins, fee_pool, base_fee, base_reserve, max_tx_set_size,
		 successful_transaction_count, failed_transaction_count, soroban_fee_write_1kb,
		 transactions)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, c.SchemaName, c.TableName)
		
	case "transactions", "transaction_analytics":
		return fmt.Sprintf(`
		INSERT INTO stellar_lake.%s.%s
		(ledger_sequence, transaction_hash, account, account_sequence, max_fee,
		 fee_charged, successful, operation_count, resource_fee, closed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, c.SchemaName, c.TableName)
		
	default:
		return fmt.Sprintf(`
		INSERT INTO stellar_lake.%s.%s (data)
		VALUES (?)`, c.SchemaName, c.TableName)
	}
}

// insertRecord inserts a single record based on table type
func (c *SaveToDuckLake) insertRecord(stmt *sql.Stmt, record map[string]interface{}) error {
	switch c.TableName {
	case "ledgers", "ledgers_nested":
		// Convert transactions to JSON
		txJSON, err := json.Marshal(record["transactions"])
		if err != nil {
			return fmt.Errorf("failed to marshal transactions: %w", err)
		}
		
		// Convert closed_at to timestamp
		closedAt := time.Unix(getInt64ValueOrDefault(record, "closed_at", 0), 0)
		
		_, err = stmt.Exec(
			getInt64ValueOrDefault(record, "sequence", 0),
			getStringValueOrDefault(record, "ledger_hash", ""),
			getStringValueOrDefault(record, "previous_ledger_hash", ""),
			closedAt,
			getInt32ValueOrDefault(record, "protocol_version", 0),
			getInt64ValueOrDefault(record, "total_coins", 0),
			getInt64ValueOrDefault(record, "fee_pool", 0),
			getInt32ValueOrDefault(record, "base_fee", 0),
			getInt32ValueOrDefault(record, "base_reserve", 0),
			getInt32ValueOrDefault(record, "max_tx_set_size", 0),
			getInt32ValueOrDefault(record, "successful_transaction_count", 0),
			getInt32ValueOrDefault(record, "failed_transaction_count", 0),
			getInt64ValueOrNull(record, "soroban_fee_write_1kb"),
			string(txJSON),
		)
		return err
		
	case "transactions", "transaction_analytics":
		closedAt := time.Unix(getInt64ValueOrDefault(record, "closed_at", 0), 0)
		
		_, err := stmt.Exec(
			getInt64ValueOrDefault(record, "ledger_sequence", 0),
			getStringValueOrDefault(record, "transaction_hash", ""),
			getStringValueOrDefault(record, "account", ""),
			getInt64ValueOrDefault(record, "account_sequence", 0),
			getInt64ValueOrDefault(record, "max_fee", 0),
			getInt64ValueOrDefault(record, "fee_charged", 0),
			getBoolValueOrDefault(record, "successful", false),
			getInt32ValueOrDefault(record, "operation_count", 0),
			getInt64ValueOrDefault(record, "resource_fee", 0),
			closedAt,
		)
		return err
		
	default:
		// Generic JSON insert
		jsonData, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("failed to marshal record: %w", err)
		}
		_, err = stmt.Exec(string(jsonData))
		return err
	}
}

// Helper functions for value extraction
func getStringValueOrDefault(data map[string]interface{}, key, defaultVal string) string {
	if val, ok := data[key].(string); ok {
		return val
	}
	return defaultVal
}

func getInt64ValueOrDefault(data map[string]interface{}, key string, defaultVal int64) int64 {
	if val, ok := data[key]; ok {
		switch v := val.(type) {
		case int64:
			return v
		case float64:
			return int64(v)
		case int:
			return int64(v)
		}
	}
	return defaultVal
}

func getInt32ValueOrDefault(data map[string]interface{}, key string, defaultVal int32) int32 {
	if val, ok := data[key]; ok {
		switch v := val.(type) {
		case int32:
			return v
		case float64:
			return int32(v)
		case int:
			return int32(v)
		case int64:
			return int32(v)
		}
	}
	return defaultVal
}

func getBoolValueOrDefault(data map[string]interface{}, key string, defaultVal bool) bool {
	if val, ok := data[key].(bool); ok {
		return val
	}
	return defaultVal
}

func getInt64ValueOrNull(data map[string]interface{}, key string) interface{} {
	if val, ok := data[key]; ok && val != nil {
		switch v := val.(type) {
		case int64:
			return v
		case float64:
			return int64(v)
		case int:
			return int64(v)
		}
	}
	return nil
}

// Close closes the database connection
func (c *SaveToDuckLake) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Flush any remaining data
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	if err := c.flush(ctx); err != nil {
		c.logger.Error("Failed to flush on close", "error", err)
	}

	// Close database connection
	if c.db != nil {
		if err := c.db.Close(); err != nil {
			return fmt.Errorf("failed to close database: %w", err)
		}
	}

	c.logger.Info("SaveToDuckLake consumer closed")
	return nil
}