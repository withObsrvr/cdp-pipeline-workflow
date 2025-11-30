package consumer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/marcboeker/go-duckdb/v2"
	"github.com/sirupsen/logrus"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// Default configuration constants for Bronze consumer
const (
	DefaultBronzeBatchSize       = 100
	DefaultBronzeFlushInterval   = 10 * time.Second
	BronzeCloseFlushTimeout      = 30 * time.Second
	MaxTableNameLength           = 64
)

// BronzeToDuckDB is a simple consumer that writes Bronze records to DuckDB tables
type BronzeToDuckDB struct {
	// Configuration
	DBPath      string
	BatchSize   int
	FlushInterval time.Duration

	// Internal state
	db          *sql.DB
	connector   *duckdb.Connector
	nativeConn  *duckdb.Conn
	appenders   map[string]*duckdb.Appender
	appendersMu sync.Mutex
	buffer      []map[string]interface{}
	lastFlush   time.Time
	mutex       sync.Mutex
	logger      *logrus.Entry
	registry    *DuckLakeSchemaRegistry
}

// NewBronzeToDuckDB creates a new Bronze to DuckDB consumer
func NewBronzeToDuckDB(config map[string]interface{}) (processor.Processor, error) {
	c := &BronzeToDuckDB{
		DBPath:        "bronze.duckdb",
		BatchSize:     DefaultBronzeBatchSize,
		FlushInterval: DefaultBronzeFlushInterval,
		appenders:     make(map[string]*duckdb.Appender),
		logger:        logrus.WithField("consumer", "BronzeToDuckDB"),
	}

	// Parse config
	if dbPath, ok := config["db_path"].(string); ok {
		c.DBPath = dbPath
	}
	if batchSize, ok := config["batch_size"].(float64); ok {
		c.BatchSize = int(batchSize)
	}
	if flushInterval, ok := config["flush_interval_seconds"].(float64); ok {
		c.FlushInterval = time.Duration(flushInterval) * time.Second
	}

	// Initialize schema registry for Bronze tables
	c.registry = NewDuckLakeSchemaRegistry()

	// Initialize database connection
	if err := c.initDB(); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	c.logger.Info("Initialized BronzeToDuckDB consumer",
		"db_path", c.DBPath,
		"batch_size", c.BatchSize,
		"flush_interval", c.FlushInterval)

	return c, nil
}

// initDB initializes the DuckDB connection and native connection for appenders
func (c *BronzeToDuckDB) initDB() error {
	// Create connector
	connector, err := duckdb.NewConnector(c.DBPath, nil)
	if err != nil {
		return fmt.Errorf("failed to create connector: %w", err)
	}
	c.connector = connector

	// Create sql.DB from connector
	c.db = sql.OpenDB(connector)

	// Get native connection for appenders
	conn, err := c.connector.Connect(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get native connection: %w", err)
	}
	duckConn, ok := conn.(*duckdb.Conn)
	if !ok {
		return fmt.Errorf("failed to cast to *duckdb.Conn")
	}
	c.nativeConn = duckConn

	c.logger.Info("Database connection initialized")
	return nil
}

// getOrCreateAppender gets an existing appender or creates a new one for the given table
func (c *BronzeToDuckDB) getOrCreateAppender(tableName string) (*duckdb.Appender, error) {
	c.appendersMu.Lock()
	defer c.appendersMu.Unlock()

	// Check if appender already exists
	if appender, exists := c.appenders[tableName]; exists {
		return appender, nil
	}

	// Ensure table exists
	if err := c.ensureTable(tableName); err != nil {
		return nil, fmt.Errorf("failed to ensure table %s exists: %w", tableName, err)
	}

	// Create new appender
	appender, err := duckdb.NewAppenderFromConn(c.nativeConn, "", tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to create appender for table %s: %w", tableName, err)
	}

	c.appenders[tableName] = appender
	c.logger.Info("Created appender for table", "table", tableName)

	return appender, nil
}

// validTableName checks if a table name is safe for use in SQL statements
// Returns an error if the name contains invalid characters or exceeds length limits
var validTableNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

func validTableName(tableName string) error {
	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}
	if len(tableName) > MaxTableNameLength {
		return fmt.Errorf("table name exceeds maximum length of %d characters: %s", MaxTableNameLength, tableName)
	}
	if !validTableNameRegex.MatchString(tableName) {
		return fmt.Errorf("table name contains invalid characters (only alphanumeric and underscores allowed): %s", tableName)
	}
	return nil
}

// ensureTable creates the table if it doesn't exist
func (c *BronzeToDuckDB) ensureTable(tableName string) error {
	// Validate table name to prevent SQL injection
	if err := validTableName(tableName); err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}

	// Check if table exists
	var exists bool
	checkSQL := "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = ?)"
	if err := c.db.QueryRow(checkSQL, tableName).Scan(&exists); err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}

	if exists {
		return nil
	}

	// Get schema from registry
	processorType := processor.ProcessorType(tableName) // For Bronze, table name = processor type
	schema, ok := c.registry.GetSchema(processorType)
	if !ok {
		return fmt.Errorf("schema not found for table %s", tableName)
	}

	// Create table - for local DuckDB, just use table name directly
	// Replace the catalog.schema.table pattern with just table name
	createSQL := fmt.Sprintf(schema.CreateSQL, "", "", tableName)
	// Fix the ".." pattern that results from empty catalog/schema
	createSQL = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s %s",
		tableName,
		createSQL[strings.Index(createSQL, "("):])

	c.logger.Info("Creating table", "table", tableName, "version", schema.Version)
	if _, err := c.db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	c.logger.Info("Successfully created table", "table", tableName)
	return nil
}

// Process processes a message and adds it to the buffer
func (c *BronzeToDuckDB) Process(ctx context.Context, msg processor.Message) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Extract table name from metadata
	var tableName string
	if msg.Metadata != nil {
		if pType, ok := msg.Metadata["processor_type"].(string); ok {
			tableName = pType
		} else if tableType, ok := msg.Metadata["table_type"].(string); ok {
			tableName = tableType
		}
	}

	if tableName == "" {
		c.logger.Warn("No table name in metadata, skipping record")
		return nil
	}

	// Extract Bronze row data
	var bronzeRow []driver.Value

	// Check if this implements GetBronzeRow interface
	type BronzeRowGetter interface {
		GetBronzeRow() []driver.Value
	}

	if bronzeGetter, ok := msg.Payload.(BronzeRowGetter); ok {
		bronzeRow = bronzeGetter.GetBronzeRow()
	} else {
		c.logger.Warn("Payload doesn't implement GetBronzeRow", "table", tableName)
		return nil
	}

	// Add to buffer
	record := map[string]interface{}{
		"__table_name__": tableName,
		"__bronze_row__": bronzeRow,
	}
	c.buffer = append(c.buffer, record)

	// Flush if batch size reached or flush interval exceeded
	if len(c.buffer) >= c.BatchSize || time.Since(c.lastFlush) > c.FlushInterval {
		// Propagate caller context for proper cancellation
		if err := c.flush(ctx); err != nil {
			return fmt.Errorf("failed to flush: %w", err)
		}
	}

	return nil
}

// flush writes buffered data to DuckDB
func (c *BronzeToDuckDB) flush(ctx context.Context) error {
	if len(c.buffer) == 0 {
		return nil
	}

	// Group records by table name
	recordsByTable := make(map[string][]map[string]interface{})
	for _, record := range c.buffer {
		tableName, ok := record["__table_name__"].(string)
		if !ok {
			c.logger.Error("Invalid table name type in buffer record", "record", record)
			continue
		}
		recordsByTable[tableName] = append(recordsByTable[tableName], record)
	}

	// Track successfully flushed tables to know which records to remove from buffer
	successfullyFlushedTables := make(map[string]bool)
	totalRecords := 0
	var flushErrors []error

	// Process each table group
	for tableName, records := range recordsByTable {
		if len(records) == 0 {
			continue
		}

		// Get or create appender for this table
		appender, err := c.getOrCreateAppender(tableName)
		if err != nil {
			c.logger.Error("Failed to get appender", "table", tableName, "error", err)
			flushErrors = append(flushErrors, fmt.Errorf("table %s: get appender: %w", tableName, err))
			continue
		}

		// Append each record
		appendFailed := false
		for _, record := range records {
			bronzeRow, ok := record["__bronze_row__"].([]driver.Value)
			if !ok {
				c.logger.Error("Invalid bronze row type in buffer record", "table", tableName)
				flushErrors = append(flushErrors, fmt.Errorf("table %s: invalid bronze row type", tableName))
				appendFailed = true
				break
			}
			if err := appender.AppendRow(bronzeRow...); err != nil {
				c.logger.Error("Failed to append row", "table", tableName, "error", err)
				flushErrors = append(flushErrors, fmt.Errorf("table %s: append row: %w", tableName, err))
				appendFailed = true
				break // Stop processing this table if any row fails
			}
		}

		if appendFailed {
			continue
		}

		// Flush appender
		if err := appender.Flush(); err != nil {
			c.logger.Error("Failed to flush appender", "table", tableName, "error", err)
			flushErrors = append(flushErrors, fmt.Errorf("table %s: flush: %w", tableName, err))
			continue
		}

		// Mark this table as successfully flushed
		successfullyFlushedTables[tableName] = true
		totalRecords += len(records)
		c.logger.Info("Flushed data to DuckDB", "table", tableName, "records", len(records))
	}

	// Only clear successfully flushed records from buffer
	if len(successfullyFlushedTables) > 0 {
		newBuffer := make([]map[string]interface{}, 0)
		for _, record := range c.buffer {
			tableName, ok := record["__table_name__"].(string)
			if !ok {
				// Keep records with invalid table names for manual inspection
				c.logger.Warn("Record with invalid table name type retained in buffer")
				newBuffer = append(newBuffer, record)
				continue
			}
			if !successfullyFlushedTables[tableName] {
				// Keep failed records in buffer for retry
				newBuffer = append(newBuffer, record)
			}
		}
		c.buffer = newBuffer
		c.logger.Info("Retained failed records in buffer", "failed_records", len(newBuffer))
	}

	c.lastFlush = time.Now()

	c.logger.Info("Completed flush cycle", "total_records", totalRecords, "tables", len(recordsByTable), "errors", len(flushErrors))

	// Return error if any flush operations failed
	if len(flushErrors) > 0 {
		return fmt.Errorf("flush encountered %d errors (first: %v)", len(flushErrors), flushErrors[0])
	}

	return nil
}

// Subscribe is a no-op for consumers (they don't have subscribers)
func (c *BronzeToDuckDB) Subscribe(processor processor.Processor) {
	// No-op for consumers
}

// Close closes the consumer and flushes remaining data
func (c *BronzeToDuckDB) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Flush remaining data
	ctx, cancel := context.WithTimeout(context.Background(), BronzeCloseFlushTimeout)
	defer cancel()

	if err := c.flush(ctx); err != nil {
		c.logger.Error("Failed to flush on close", "error", err)
	}

	// Close all appenders
	c.appendersMu.Lock()
	for tableName, appender := range c.appenders {
		if err := appender.Close(); err != nil {
			c.logger.Error("Failed to close appender", "table", tableName, "error", err)
		}
	}
	c.appenders = make(map[string]*duckdb.Appender)
	c.appendersMu.Unlock()

	// Close native connection
	if c.nativeConn != nil {
		if err := c.nativeConn.Close(); err != nil {
			c.logger.Error("Failed to close native connection", "error", err)
		}
	}

	// Close database
	if c.db != nil {
		if err := c.db.Close(); err != nil {
			return fmt.Errorf("failed to close database: %w", err)
		}
	}

	c.logger.Info("BronzeToDuckDB consumer closed")
	return nil
}
