package consumer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	duckdb "github.com/marcboeker/go-duckdb/v2"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// Note: getStringConfig is defined in consumer_save_event_payment_to_postgresql.go
// Note: getIntConfig and getBoolConfig are defined at the bottom of this file

const (
	// Default configuration values
	// BatchSize controls how many ledgers to accumulate before flushing
	// (like ducklake-ingestion-obsrvr-v2, NOT total records)
	DefaultBatchSize             = 100
	DefaultCommitIntervalSeconds = 30
	DefaultSilverVersion         = "v1"

	// Processor version for lineage tracking
	SilverIngesterVersion = "1.0.0"
)

// SilverIngesterConsumer writes Silver layer data to DuckLake using the high-performance Appender API
// This consumer implements the processor.Processor interface and supports:
// - Version-isolated catalogs (obsrvr_{network}_bronze_{version})
// - Per-table buffering with time-based flush
// - DuckDB Appender API (534x faster than SQL INSERT)
// - 19 Hubble-compatible Silver tables + 4 metadata tables
type SilverIngesterConsumer struct {
	// Bronze version for isolation
	SilverVersion string

	// DuckLake connection settings
	CatalogPath string
	DataPath    string
	CatalogName string // auto-generated: obsrvr_{schema}_{silver_version}
	SchemaName  string // "mainnet" or "testnet"

	// AWS credentials for S3 data path
	AWSAccessKeyID     string
	AWSSecretAccessKey string
	AWSRegion          string

	// Batching configuration
	BatchSize      int
	CommitInterval time.Duration

	// Database connection (shared connector pattern for Appender API)
	connector *duckdb.Connector
	db        *sql.DB
	duckConn  *duckdb.Conn // Native DuckDB connection for appenders

	// Per-table Appenders (534x faster than SQL INSERT)
	appenders map[string]*duckdb.Appender

	// Per-table buffers
	buffers    map[string][]interface{}
	buffersMu  sync.Mutex
	lastCommit time.Time

	// Statistics (atomic for thread safety)
	stats SilverIngesterStats

	// Metadata settings
	EnableMetadata bool
}

// SilverIngesterStats tracks processing statistics using atomic operations
type SilverIngesterStats struct {
	TotalProcessed atomic.Uint64
	TotalFlushed   atomic.Uint64
	FlushCount     atomic.Uint64
	FailedFlushes  atomic.Uint64
}

// NewSilverIngesterConsumer creates a new Bronze Copier consumer
func NewSilverIngesterConsumer(config map[string]interface{}) (*SilverIngesterConsumer, error) {
	consumer := &SilverIngesterConsumer{
		SilverVersion:      getStringConfig(config, "silver_version", DefaultSilverVersion),
		CatalogPath:        getStringConfig(config, "catalog_path", ""),
		DataPath:           getStringConfig(config, "data_path", ""),
		SchemaName:         getStringConfig(config, "schema_name", "mainnet"),
		AWSAccessKeyID:     getStringConfig(config, "aws_access_key_id", ""),
		AWSSecretAccessKey: getStringConfig(config, "aws_secret_access_key", ""),
		AWSRegion:          getStringConfig(config, "aws_region", "us-east-1"),
		BatchSize:          getIntConfig(config, "batch_size", DefaultBatchSize),
		CommitInterval:     time.Duration(getIntConfig(config, "commit_interval_seconds", DefaultCommitIntervalSeconds)) * time.Second,
		EnableMetadata:     getBoolConfigSilver(config, "enable_metadata", true),
		buffers:            make(map[string][]interface{}),
		appenders:          make(map[string]*duckdb.Appender),
		lastCommit:         time.Now(),
	}

	// Support environment variable overrides
	if envAWSKeyID := os.Getenv("AWS_ACCESS_KEY_ID"); envAWSKeyID != "" {
		consumer.AWSAccessKeyID = envAWSKeyID
	}
	if envAWSSecret := os.Getenv("AWS_SECRET_ACCESS_KEY"); envAWSSecret != "" {
		consumer.AWSSecretAccessKey = envAWSSecret
	}
	if envAWSRegion := os.Getenv("AWS_REGION"); envAWSRegion != "" {
		consumer.AWSRegion = envAWSRegion
	}

	// Validate required config
	if consumer.CatalogPath == "" {
		return nil, fmt.Errorf("catalog_path is required")
	}
	if consumer.DataPath == "" {
		return nil, fmt.Errorf("data_path is required")
	}

	// Generate catalog name: obsrvr_{schema}_{silver_version}
	consumer.CatalogName = fmt.Sprintf("obsrvr_%s_silver_%s", consumer.SchemaName, consumer.SilverVersion)

	log.Printf("SilverIngester: Initializing consumer")
	log.Printf("SilverIngester: Catalog=%s, Schema=%s, Version=%s",
		consumer.CatalogName, consumer.SchemaName, consumer.SilverVersion)
	log.Printf("SilverIngester: BatchSize=%d, CommitInterval=%s",
		consumer.BatchSize, consumer.CommitInterval)

	// Initialize DuckDB connection
	if err := consumer.initializeDatabase(); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	// Create tables
	if err := consumer.createTables(); err != nil {
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	// Initialize appenders
	if err := consumer.initializeAppenders(); err != nil {
		return nil, fmt.Errorf("failed to initialize appenders: %w", err)
	}

	log.Printf("SilverIngester: Initialized successfully")
	return consumer, nil
}

// initializeDatabase establishes the DuckDB connection using shared connector pattern
// This pattern is required for the Appender API to work with DuckLake catalogs
func (c *SilverIngesterConsumer) initializeDatabase() error {
	// Step 1: Create shared connector (required for Appender API)
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return fmt.Errorf("failed to create DuckDB connector: %w", err)
	}
	c.connector = connector
	log.Printf("SilverIngester: DuckDB connector created")

	// Step 2: Open sql.DB with shared connector
	c.db = sql.OpenDB(connector)
	log.Printf("SilverIngester: sql.DB opened with shared connector")

	// Install and load DuckLake extension
	if _, err := c.db.Exec("INSTALL ducklake"); err != nil {
		log.Printf("SilverIngester: DuckLake extension may already be installed: %v", err)
	}
	if _, err := c.db.Exec("LOAD ducklake"); err != nil {
		return fmt.Errorf("failed to load DuckLake extension: %w", err)
	}
	log.Printf("SilverIngester: DuckLake extension loaded")

	// Configure AWS credentials for S3 access if provided
	if c.AWSAccessKeyID != "" && c.AWSSecretAccessKey != "" {
		credSQL := fmt.Sprintf(`
			CREATE SECRET IF NOT EXISTS silver_s3_secret (
				TYPE S3,
				KEY_ID '%s',
				SECRET '%s',
				REGION '%s'
			)`,
			c.AWSAccessKeyID, c.AWSSecretAccessKey, c.AWSRegion)
		if _, err := c.db.Exec(credSQL); err != nil {
			return fmt.Errorf("failed to create S3 secret: %w", err)
		}
		log.Printf("SilverIngester: S3 credentials configured for region %s", c.AWSRegion)
	}

	// Attach DuckLake catalog
	// Format: ducklake:postgres:connection_string or ducklake:local:path
	attachSQL := fmt.Sprintf(`
		ATTACH '%s' AS %s (
			DATA_PATH '%s'
		)`,
		c.CatalogPath, c.CatalogName, c.DataPath)
	if _, err := c.db.Exec(attachSQL); err != nil {
		return fmt.Errorf("failed to attach DuckLake catalog: %w", err)
	}
	log.Printf("SilverIngester: Attached catalog %s with data path %s", c.CatalogName, c.DataPath)

	// Create schema if not exists
	schemaSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", c.CatalogName, c.SchemaName)
	if _, err := c.db.Exec(schemaSQL); err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}
	log.Printf("SilverIngester: Schema %s.%s ready", c.CatalogName, c.SchemaName)

	return nil
}

// createTables creates all Silver and metadata tables
func (c *SilverIngesterConsumer) createTables() error {
	// Create all 19 Silver tables
	if err := CreateSilverTables(c.db, c.CatalogName, c.SchemaName); err != nil {
		return err
	}

	// Create metadata tables if enabled
	if c.EnableMetadata {
		if err := CreateMetadataTables(c.db, c.CatalogName, c.SchemaName); err != nil {
			return err
		}
	}

	return nil
}

// initializeAppenders creates Appenders for all Silver tables using native DuckDB connection
func (c *SilverIngesterConsumer) initializeAppenders() error {
	// Get native DuckDB connection from shared connector
	// This is required for Appender API - sql.Conn doesn't work
	conn, err := c.connector.Connect(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get native DuckDB connection: %w", err)
	}
	c.duckConn = conn.(*duckdb.Conn)
	log.Printf("SilverIngester: Native DuckDB connection created")

	// Set schema context before creating appenders
	useSQL := fmt.Sprintf("USE %s.%s", c.CatalogName, c.SchemaName)
	if _, err := c.duckConn.ExecContext(context.Background(), useSQL, nil); err != nil {
		return fmt.Errorf("failed to set schema context: %w", err)
	}
	log.Printf("SilverIngester: Using %s.%s", c.CatalogName, c.SchemaName)

	// Create appenders for all Silver tables
	for _, tableName := range SilverTableNames {
		appender, err := duckdb.NewAppenderFromConn(c.duckConn, "", tableName)
		if err != nil {
			return fmt.Errorf("failed to create appender for %s: %w", tableName, err)
		}
		c.appenders[tableName] = appender
		log.Printf("SilverIngester: Appender ready for %s", tableName)
	}

	log.Printf("SilverIngester: All %d appenders initialized", len(c.appenders))
	return nil
}

// Process handles incoming messages and routes them to the appropriate table buffer
func (c *SilverIngesterConsumer) Process(ctx context.Context, msg processor.Message) error {
	c.stats.TotalProcessed.Add(1)

	// Get table type from message metadata
	tableType, ok := msg.Metadata["table_type"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid table_type in message metadata")
	}

	// Verify table type is valid
	if _, exists := c.appenders[tableType]; !exists {
		return fmt.Errorf("unknown table_type: %s", tableType)
	}

	// Add to buffer
	c.buffersMu.Lock()
	c.buffers[tableType] = append(c.buffers[tableType], msg.Payload)
	// Check ledger count for flush decision (like ducklake-ingestion-obsrvr-v2)
	ledgerCount := len(c.buffers["ledgers_row_v2"])
	c.buffersMu.Unlock()

	// Check if we should flush (based on ledger count or time interval)
	// This matches ducklake-ingestion-obsrvr-v2 behavior where batch_size controls ledgers processed
	if ledgerCount >= c.BatchSize || time.Since(c.lastCommit) >= c.CommitInterval {
		return c.flush(ctx)
	}

	return nil
}

// getBufferSize returns the total number of records across all buffers (must be called with lock held)
func (c *SilverIngesterConsumer) getBufferSize() int {
	total := 0
	for _, records := range c.buffers {
		total += len(records)
	}
	return total
}

// flush writes all buffered records to DuckLake using the Appender API
func (c *SilverIngesterConsumer) flush(ctx context.Context) error {
	c.buffersMu.Lock()
	defer c.buffersMu.Unlock()

	totalRecords := c.getBufferSize()
	if totalRecords == 0 {
		return nil
	}

	flushStart := time.Now()

	// Log summary of what we're flushing (like ducklake-ingestion-obsrvr-v2)
	log.Printf("[FLUSH] Starting multi-table flush: %d ledgers, %d transactions, %d operations, %d balances, %d effects, %d trades, %d accounts, %d trustlines, %d offers, %d claimable balances, %d liquidity pools, %d contract events, %d contract data, %d contract code, %d config settings, %d ttl, %d evicted keys, %d restored keys, %d account signers",
		len(c.buffers["ledgers_row_v2"]),
		len(c.buffers["transactions_row_v2"]),
		len(c.buffers["operations_row_v2"]),
		len(c.buffers["native_balances_snapshot_v1"]),
		len(c.buffers["effects_row_v1"]),
		len(c.buffers["trades_row_v1"]),
		len(c.buffers["accounts_snapshot_v1"]),
		len(c.buffers["trustlines_snapshot_v1"]),
		len(c.buffers["offers_snapshot_v1"]),
		len(c.buffers["claimable_balances_snapshot_v1"]),
		len(c.buffers["liquidity_pools_snapshot_v1"]),
		len(c.buffers["contract_events_stream_v1"]),
		len(c.buffers["contract_data_snapshot_v1"]),
		len(c.buffers["contract_code_snapshot_v1"]),
		len(c.buffers["config_settings_snapshot_v1"]),
		len(c.buffers["ttl_snapshot_v1"]),
		len(c.buffers["evicted_keys_state_v1"]),
		len(c.buffers["restored_keys_state_v1"]),
		len(c.buffers["account_signers_snapshot_v1"]),
	)

	totalFlushed := 0

	// Flush each table's buffer
	for tableType, records := range c.buffers {
		if len(records) == 0 {
			continue
		}

		appender := c.appenders[tableType]
		if appender == nil {
			c.stats.FailedFlushes.Add(1)
			return fmt.Errorf("no appender for table %s", tableType)
		}

		appendStart := time.Now()

		// Use Appender to write records
		for _, record := range records {
			if err := c.appendRecord(appender, tableType, record); err != nil {
				c.stats.FailedFlushes.Add(1)
				return fmt.Errorf("failed to append record to %s: %w", tableType, err)
			}
		}

		appendDuration := time.Since(appendStart)

		// Flush the appender
		flushTableStart := time.Now()
		if err := appender.Flush(); err != nil {
			c.stats.FailedFlushes.Add(1)
			return fmt.Errorf("failed to flush appender for %s: %w", tableType, err)
		}
		flushDuration := time.Since(flushTableStart)

		totalFlushed += len(records)
		log.Printf("[FLUSH] ✓ Appended and flushed %d %s in %v (append: %v, flush: %v)",
			len(records), tableType, time.Since(appendStart), appendDuration, flushDuration)
	}

	// Clear buffers
	c.buffers = make(map[string][]interface{})
	c.lastCommit = time.Now()

	c.stats.TotalFlushed.Add(uint64(totalFlushed))
	c.stats.FlushCount.Add(1)

	log.Printf("[FLUSH] ✅ COMPLETE: Flushed %d total records in %v", totalFlushed, time.Since(flushStart))
	return nil
}

// appendRecord appends a single record to the appender based on table type
// This method handles the mapping from Go structs to Appender.AppendRow() calls
func (c *SilverIngesterConsumer) appendRecord(appender *duckdb.Appender, tableType string, record interface{}) error {
	// Records must implement SilverRowGetter to provide their row values
	// The BronzeLedgerReaderProcessor (Cycle 2) will emit records that implement this interface
	if rowGetter, ok := record.(SilverRowGetter); ok {
		values := rowGetter.GetSilverRow()
		// Convert []driver.Value to variadic args
		return appender.AppendRow(values...)
	}
	return fmt.Errorf("record type %T does not implement SilverRowGetter for table %s", record, tableType)
}

// SilverRowGetter interface for records that can provide their row values
// Records emitted by BronzeLedgerReaderProcessor must implement this interface
// Returns []driver.Value for compatibility with DuckDB Appender API
type SilverRowGetter interface {
	GetSilverRow() []driver.Value
}

// Subscribe is not implemented for consumers (they are end of pipeline)
func (c *SilverIngesterConsumer) Subscribe(p processor.Processor) {
	log.Printf("SilverIngester: Subscribe called but consumers don't support subscriptions")
}

// GetStats returns current processing statistics
func (c *SilverIngesterConsumer) GetStats() SilverIngesterStats {
	return c.stats
}

// Close flushes remaining data and closes the connection
func (c *SilverIngesterConsumer) Close() error {
	log.Printf("SilverIngester: Closing consumer...")

	// Final flush
	if err := c.flush(context.Background()); err != nil {
		log.Printf("SilverIngester: Warning - final flush failed: %v", err)
	}

	// Log statistics
	log.Printf("SilverIngester Stats: Processed=%d, Flushed=%d, FlushCount=%d, Failed=%d",
		c.stats.TotalProcessed.Load(),
		c.stats.TotalFlushed.Load(),
		c.stats.FlushCount.Load(),
		c.stats.FailedFlushes.Load())

	// Close appenders
	for tableName, appender := range c.appenders {
		if err := appender.Close(); err != nil {
			log.Printf("SilverIngester: Warning - failed to close appender for %s: %v", tableName, err)
		}
	}

	// Close native DuckDB connection
	if c.duckConn != nil {
		if err := c.duckConn.Close(); err != nil {
			log.Printf("SilverIngester: Warning - failed to close native connection: %v", err)
		}
	}

	// Close database
	if c.db != nil {
		if err := c.db.Close(); err != nil {
			return fmt.Errorf("failed to close database: %w", err)
		}
	}

	// Close connector
	if c.connector != nil {
		if err := c.connector.Close(); err != nil {
			log.Printf("SilverIngester: Warning - failed to close connector: %v", err)
		}
	}

	log.Printf("SilverIngester: Closed successfully")
	return nil
}

// Note: getStringConfig, getIntConfig are defined in consumer_save_event_payment_to_postgresql.go

// getBoolConfigSilver retrieves a bool value from config with a default fallback
// Using a different name to avoid redeclaration since getBoolConfig may be defined elsewhere
func getBoolConfigSilver(config map[string]interface{}, key string, defaultValue bool) bool {
	if val, ok := config[key].(bool); ok {
		return val
	}
	return defaultValue
}

// Ensure SilverIngesterConsumer implements Processor interface
var _ processor.Processor = (*SilverIngesterConsumer)(nil)
