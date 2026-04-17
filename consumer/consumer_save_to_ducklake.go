package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/stellar/go/support/log"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// SaveToDuckLake is a consumer that saves data directly to DuckLake tables
type SaveToDuckLake struct {
	// DuckLake configuration
	CatalogPath      string // Path to DuckLake catalog (e.g., "ducklake:stellar.ducklake", "ducklake:sqlite:metadata.sqlite", "ducklake:postgres:dbname=ducklake_catalog")
	DataPath         string // Storage path (e.g., "s3://bucket/ducklake" or "./data")
	CatalogName      string // Catalog alias name (default: stellar_lake)
	TableName        string // Target table name
	SchemaName       string // Schema name (default: main)
	SchemaType       string // Processor type for schema registry (e.g., "contract_invocation")

	// AWS/S3 configuration
	AWSAccessKeyID     string // AWS access key for S3 authentication
	AWSSecretAccessKey string // AWS secret key for S3 authentication
	AWSRegion          string // AWS region (e.g., "us-east-1")
	AWSEndpoint        string // Optional: Custom S3 endpoint (for R2, MinIO, etc.)

	// Public archive features (for transparency customers)
	EnablePublicArchive bool   // Enable public archive features (hashing, manifests)
	ComputeHashes       bool   // Compute SHA256 hashes for each parquet file
	PublishManifest     bool   // Publish manifest.json for discovery

	// Connection settings
	MaxConnections   int
	BatchSize        int
	CommitInterval   time.Duration

	// Schema registry settings
	UseUpsert        bool   // Use MERGE/UPSERT instead of INSERT (default: true)
	CreateIndexes    bool   // Create indexes from schema definition (default: true)

	// Internal state
	db               *sql.DB
	insertStmt       *sql.Stmt
	buffer           []map[string]interface{}
	lastCommit       time.Time
	mutex            sync.Mutex
	logger           *log.Entry
	registry         *DuckLakeSchemaRegistry
	schemaDefinition *SchemaDefinition
}

// contains checks if a string contains a substring (helper for error checking)
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

// findSubstring checks if substr exists in s
func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		match := true
		for j := 0; j < len(substr); j++ {
			if s[i+j] != substr[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// validateBucketNaming validates bucket naming conventions for hybrid model
func validateBucketNaming(dataPath string, schemaName string, enablePublicArchive bool) error {
	// Check if using S3 path
	if len(dataPath) < 5 || dataPath[:5] != "s3://" {
		return nil // Local path, no validation needed
	}

	// Extract bucket name from s3://bucket-name/path
	bucketName := ""
	if len(dataPath) > 5 {
		pathParts := dataPath[5:] // Remove "s3://"

		// Find first slash (if any)
		slashIdx := -1
		for i, ch := range pathParts {
			if ch == '/' {
				slashIdx = i
				break
			}
		}

		// Extract bucket name
		if slashIdx > 0 {
			bucketName = pathParts[:slashIdx]
		} else if slashIdx == 0 {
			// Path starts with slash (e.g., "s3:///data")
			bucketName = ""
		} else {
			// No slash found (e.g., "s3://bucket-name")
			bucketName = pathParts
		}
	}

	if bucketName == "" {
		return fmt.Errorf("invalid S3 path format: %s (expected s3://bucket-name/path)", dataPath)
	}

	// Validate naming conventions for hybrid model
	if enablePublicArchive {
		// Public customers should use shared bucket (allow -test variants for testing)
		validPublicNames := []string{
			"obsrvr-public-archives",
			"obsrvr-public-archives-test",
			"obsrvr-public-archives-test-aws",
		}
		isValid := false
		for _, validName := range validPublicNames {
			if bucketName == validName {
				isValid = true
				break
			}
		}

		if !isValid {
			return fmt.Errorf(
				"public archive must use 'obsrvr-public-archives' bucket (got: %s). "+
					"Multi-tenant isolation is provided via schema_name (%s)",
				bucketName, schemaName,
			)
		}
	} else {
		// Private customers should use dedicated bucket with "obsrvr-private-" prefix
		prefix := "obsrvr-private-"
		if len(bucketName) < len(prefix) {
			return fmt.Errorf(
				"private customer bucket must follow 'obsrvr-private-{customer_name}' convention (got: %s)",
				bucketName,
			)
		}

		// Check if bucket starts with the prefix
		hasPrefix := true
		for i := 0; i < len(prefix); i++ {
			if bucketName[i] != prefix[i] {
				hasPrefix = false
				break
			}
		}

		if !hasPrefix {
			return fmt.Errorf(
				"private customer bucket must follow 'obsrvr-private-{customer_name}' convention (got: %s)",
				bucketName,
			)
		}

		// Ensure there's a customer name after the prefix
		if len(bucketName) == len(prefix) {
			return fmt.Errorf(
				"private customer bucket must include customer name after 'obsrvr-private-' (got: %s)",
				bucketName,
			)
		}
	}

	return nil
}

// NewSaveToDuckLake creates a new DuckLake consumer
func NewSaveToDuckLake(config map[string]interface{}) (processor.Processor, error) {
	consumer := &SaveToDuckLake{
		CatalogPath:    "ducklake:stellar.ducklake",
		DataPath:       "./ducklake_data",
		CatalogName:    "stellar_lake",
		TableName:      "ledgers",
		SchemaName:     "main",
		SchemaType:     "",
		MaxConnections: 5,
		BatchSize:      100,
		CommitInterval: 10 * time.Second,
		UseUpsert:      true,  // Default to idempotent writes
		CreateIndexes:  true,  // Default to creating indexes
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
	if catalogName, ok := config["catalog_name"].(string); ok {
		consumer.CatalogName = catalogName
	}
	if tableName, ok := config["table_name"].(string); ok {
		consumer.TableName = tableName
	}
	if schemaName, ok := config["schema_name"].(string); ok {
		consumer.SchemaName = schemaName
	}
	if schemaType, ok := config["schema_type"].(string); ok {
		consumer.SchemaType = schemaType
	}
	if batchSize, ok := config["batch_size"].(float64); ok {
		consumer.BatchSize = int(batchSize)
	}
	if commitSeconds, ok := config["commit_interval_seconds"].(float64); ok {
		consumer.CommitInterval = time.Duration(commitSeconds) * time.Second
	}
	if useUpsert, ok := config["use_upsert"].(bool); ok {
		consumer.UseUpsert = useUpsert
	}
	if createIndexes, ok := config["create_indexes"].(bool); ok {
		consumer.CreateIndexes = createIndexes
	}

	// Parse AWS/S3 configuration
	if accessKey, ok := config["aws_access_key_id"].(string); ok {
		consumer.AWSAccessKeyID = accessKey
	}
	if secretKey, ok := config["aws_secret_access_key"].(string); ok {
		consumer.AWSSecretAccessKey = secretKey
	}
	if region, ok := config["aws_region"].(string); ok {
		consumer.AWSRegion = region
	}
	if endpoint, ok := config["aws_endpoint"].(string); ok {
		consumer.AWSEndpoint = endpoint
	}

	// Parse public archive features
	if enablePublic, ok := config["enable_public_archive"].(bool); ok {
		consumer.EnablePublicArchive = enablePublic
	}
	if computeHashes, ok := config["compute_hashes"].(bool); ok {
		consumer.ComputeHashes = computeHashes
	}
	if publishManifest, ok := config["publish_manifest"].(bool); ok {
		consumer.PublishManifest = publishManifest
	}

	// Validate bucket naming conventions for S3 paths
	if err := validateBucketNaming(consumer.DataPath, consumer.SchemaName, consumer.EnablePublicArchive); err != nil {
		return nil, fmt.Errorf("bucket naming validation failed: %w", err)
	}

	// Initialize schema registry
	consumer.registry = NewDuckLakeSchemaRegistry()

	// If schema_type is specified, load the schema definition
	if consumer.SchemaType != "" {
		processorType := processor.ProcessorType(consumer.SchemaType)
		if schema, ok := consumer.registry.GetSchema(processorType); ok {
			consumer.schemaDefinition = &schema
			consumer.logger.Info("Loaded schema definition",
				"schema_type", consumer.SchemaType,
				"version", schema.Version,
			)
		} else {
			return nil, fmt.Errorf("unknown schema type: %s", consumer.SchemaType)
		}
	} else {
		// Try to map table name to processor type for backward compatibility
		processorType := consumer.mapTableNameToProcessorType(consumer.TableName)
		if processorType != "" {
			if schema, ok := consumer.registry.GetSchema(processorType); ok {
				consumer.schemaDefinition = &schema
				consumer.logger.Info("Auto-detected schema from table name",
					"table_name", consumer.TableName,
					"schema_type", processorType,
					"version", schema.Version,
				)
			}
		}
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
		"use_upsert", consumer.UseUpsert,
		"create_indexes", consumer.CreateIndexes,
	)

	return consumer, nil
}

// configureS3 applies S3 configuration settings to DuckDB using CREATE SECRET
// This ensures DuckLake extension also uses the same S3 configuration
func (c *SaveToDuckLake) configureS3() {
	// Build CREATE SECRET statement
	urlStyle := "path" // Use path-style URLs for S3-compatible services
	endpoint := c.AWSEndpoint
	region := c.AWSRegion

	if endpoint == "" {
		// AWS S3 defaults
		endpoint = "s3.amazonaws.com"
		urlStyle = "vhost" // AWS S3 uses virtual-hosted-style by default
	} else {
		// For S3-compatible services (B2, R2, MinIO), use path-style
		urlStyle = "path"
	}

	if region == "" {
		region = "us-east-1"
	}

	// Remove https:// prefix from endpoint if present (DuckDB adds it automatically)
	if len(endpoint) > 8 && endpoint[:8] == "https://" {
		endpoint = endpoint[8:]
	} else if len(endpoint) > 7 && endpoint[:7] == "http://" {
		endpoint = endpoint[7:]
	}

	// Use unnamed secret (like MinIO example) for better compatibility
	// Drop any existing unnamed secret first
	c.db.Exec("DROP SECRET IF EXISTS __default_s3")

	createSecretSQL := fmt.Sprintf(`
		CREATE SECRET (
			TYPE S3,
			KEY_ID '%s',
			SECRET '%s',
			REGION '%s',
			ENDPOINT '%s',
			URL_STYLE '%s'
		)
	`, c.AWSAccessKeyID, c.AWSSecretAccessKey, region, endpoint, urlStyle)

	c.logger.Info("Creating S3 secret (unnamed, following MinIO pattern)",
		"endpoint", endpoint,
		"region", region,
		"url_style", urlStyle,
		"has_custom_endpoint", c.AWSEndpoint != "",
	)

	if _, err := c.db.Exec(createSecretSQL); err != nil {
		c.logger.Error("Failed to create S3 secret", "error", err, "sql", createSecretSQL)
		c.logger.Warn("Falling back to SET-based S3 configuration")

		// Fallback to SET-based configuration
		c.db.Exec(fmt.Sprintf("SET s3_access_key_id='%s'", c.AWSAccessKeyID))
		c.db.Exec(fmt.Sprintf("SET s3_secret_access_key='%s'", c.AWSSecretAccessKey))
		c.db.Exec(fmt.Sprintf("SET s3_region='%s'", region))
		if c.AWSEndpoint != "" {
			c.db.Exec("SET s3_url_style='path'")
			c.db.Exec(fmt.Sprintf("SET s3_endpoint='%s'", endpoint))
		}
		c.logger.Info("SET-based configuration applied")
	} else {
		c.logger.Info("Created unnamed S3 secret successfully",
			"endpoint", endpoint,
			"region", region,
			"url_style", urlStyle,
		)
	}

	// Verify credentials are set by querying system tables
	var count int
	if err := c.db.QueryRow("SELECT COUNT(*) FROM duckdb_secrets()").Scan(&count); err == nil {
		c.logger.Info("S3 secrets registered", "count", count)
	}

	c.logger.Info("S3 credentials configured successfully")
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
		"INSTALL azure",
		"LOAD ducklake",
		"LOAD httpfs",
		"LOAD json",
		"LOAD azure",
	}

	for _, ext := range extensions {
		if _, err := c.db.Exec(ext); err != nil {
			// Extension might already be installed or not available, log but continue
			c.logger.Debug("Extension setup", "command", ext, "error", err)
		}
	}

	// Configure S3 credentials if provided
	if c.AWSAccessKeyID != "" {
		c.logger.Info("Configuring S3 credentials for DuckDB")
		c.configureS3()
	}

	// Attach DuckLake catalog (or create if it doesn't exist)
	attachSQL := fmt.Sprintf(
		"ATTACH '%s' AS %s (DATA_PATH '%s')",
		c.CatalogPath,
		c.CatalogName,
		c.DataPath,
	)
	if _, err := c.db.Exec(attachSQL); err != nil {
		// Check if error is due to database not existing (first-time initialization)
		errStr := err.Error()
		if contains(errStr, "database does not exist") || contains(errStr, "Cannot open database") {
			c.logger.Info("DuckLake catalog does not exist, creating new catalog",
				"catalog_path", c.CatalogPath,
				"data_path", c.DataPath,
			)

			// Try to create the catalog by using CREATE SCHEMA (which will initialize the catalog)
			// First attach with TYPE ducklake (allows creation)
			createAttachSQL := fmt.Sprintf(
				"ATTACH '%s' AS %s (TYPE ducklake, DATA_PATH '%s')",
				c.CatalogPath,
				c.CatalogName,
				c.DataPath,
			)
			if _, err := c.db.Exec(createAttachSQL); err != nil {
				return fmt.Errorf("failed to create and attach DuckLake catalog: %w", err)
			}
			c.logger.Info("Created and attached new DuckLake catalog")
		} else {
			return fmt.Errorf("failed to attach DuckLake catalog: %w", err)
		}
	} else {
		c.logger.Info("Attached existing DuckLake catalog",
			"catalog_path", c.CatalogPath,
			"catalog_name", c.CatalogName,
			"data_path", c.DataPath,
		)
	}

	// Use the catalog.schema
	useSQL := fmt.Sprintf("USE %s.%s", c.CatalogName, c.SchemaName)
	if _, err := c.db.Exec(useSQL); err != nil {
		// Schema might not exist, try to create it
		createSchemaSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", c.CatalogName, c.SchemaName)
		if _, err := c.db.Exec(createSchemaSQL); err != nil {
			return fmt.Errorf("failed to create schema: %w", err)
		}
		c.logger.Info("Created schema", "schema", fmt.Sprintf("%s.%s", c.CatalogName, c.SchemaName))

		// Try to use it again
		if _, err := c.db.Exec(useSQL); err != nil {
			return fmt.Errorf("failed to use schema: %w", err)
		}
	}

	c.logger.Info("Using DuckLake catalog",
		"catalog", c.CatalogName,
		"schema", c.SchemaName,
	)

	// Re-apply S3 configuration after catalog attachment
	// DuckLake may reset some S3 settings during ATTACH
	if c.AWSAccessKeyID != "" {
		c.logger.Info("Re-applying S3 configuration after catalog attachment")
		c.configureS3()
	}

	// Create table if schema is already known (from config)
	// Otherwise, defer table creation until first message (for auto-detection)
	if c.schemaDefinition != nil {
		if err := c.ensureTable(); err != nil {
			return fmt.Errorf("failed to ensure table exists: %w", err)
		}
	} else {
		c.logger.Info("Deferring table creation until first message (for schema auto-detection)")
	}

	return nil
}

// ensureTable creates the table if it doesn't exist
func (c *SaveToDuckLake) ensureTable() error {
	// Check if table exists
	var exists bool
	checkSQL := fmt.Sprintf(
		"SELECT EXISTS (SELECT 1 FROM duckdb_tables() WHERE database_name = '%s' AND schema_name = '%s' AND table_name = '%s')",
		c.CatalogName,
		c.SchemaName,
		c.TableName,
	)

	if err := c.db.QueryRow(checkSQL).Scan(&exists); err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	if !exists {
		// Create table based on schema registry or fallback to legacy method
		var createSQL string
		if c.schemaDefinition != nil {
			// Use schema registry to create table
			createSQL = fmt.Sprintf(c.schemaDefinition.CreateSQL, c.CatalogName, c.SchemaName, c.TableName)
			c.logger.Info("Creating table from schema registry",
				"schema_type", c.schemaDefinition.ProcessorType,
				"version", c.schemaDefinition.Version,
			)
		} else {
			// Fallback to legacy table creation
			createSQL = c.getCreateTableSQL()
			c.logger.Warn("Creating table with legacy method (no schema definition)",
				"table", c.TableName,
			)
		}

		if _, err := c.db.Exec(createSQL); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		c.logger.Info("Created DuckLake table", "table", fmt.Sprintf("%s.%s.%s", c.CatalogName, c.SchemaName, c.TableName))

		// Create indexes if specified in schema
		if c.CreateIndexes && c.schemaDefinition != nil {
			for _, indexDef := range c.schemaDefinition.Indexes {
				indexSQL := c.buildIndexSQL(indexDef)
				if _, err := c.db.Exec(indexSQL); err != nil {
					c.logger.Warn("Failed to create index (non-fatal)",
						"index", indexDef.Name,
						"error", err,
					)
					// Continue with other indexes
				} else {
					c.logger.Info("Created index", "index", indexDef.Name)
				}
			}
		}
	}

	return nil
}

// getCreateTableSQL returns the CREATE TABLE statement based on table type
func (c *SaveToDuckLake) getCreateTableSQL() string {
	switch c.TableName {
	case "ledgers", "ledgers_nested":
		// AWS-compatible nested ledger structure
		return fmt.Sprintf(`
		CREATE TABLE %s.%s.%s (
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
			ingestion_timestamp TIMESTAMP
		)`, c.CatalogName, c.SchemaName, c.TableName)
		
	case "transactions", "transaction_analytics":
		// Flattened transaction table
		return fmt.Sprintf(`
		CREATE TABLE %s.%s.%s (
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
			ingestion_timestamp TIMESTAMP
		)`, c.CatalogName, c.SchemaName, c.TableName)
		
	case "contract_events":
		// Contract events table
		return fmt.Sprintf(`
		CREATE TABLE %s.%s.%s (
			ledger_sequence BIGINT NOT NULL,
			transaction_hash VARCHAR NOT NULL,
			event_id VARCHAR NOT NULL,
			event_type VARCHAR,
			contract_id VARCHAR,
			topics JSON,
			data JSON,
			in_successful_contract_call BOOLEAN,
			closed_at TIMESTAMP,
			ingestion_timestamp TIMESTAMP
		)`, c.CatalogName, c.SchemaName, c.TableName)
		
	default:
		// Generic JSON table
		return fmt.Sprintf(`
		CREATE TABLE %s.%s.%s (
			id BIGINT,
			data JSON,
			ingestion_timestamp TIMESTAMP
		)`, c.CatalogName, c.SchemaName, c.TableName)
	}
}

// Subscribe is not used for consumers
func (c *SaveToDuckLake) Subscribe(processor.Processor) {}

// Process handles incoming data
func (c *SaveToDuckLake) Process(ctx context.Context, msg processor.Message) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Detect schema from first message if not already set
	if c.schemaDefinition == nil && msg.Metadata != nil {
		if pType, ok := msg.Metadata["processor_type"].(string); ok {
			processorType := processor.ProcessorType(pType)
			if schema, ok := c.registry.GetSchema(processorType); ok {
				c.schemaDefinition = &schema
				c.TableName = c.mapProcessorTypeToTableName(processorType)
				c.logger.Info("Auto-detected schema from message metadata",
					"processor_type", pType,
					"table_name", c.TableName,
					"version", schema.Version,
				)

				// Re-initialize to create the correct table
				if err := c.ensureTable(); err != nil {
					return fmt.Errorf("failed to ensure table with detected schema: %w", err)
				}
			}
		}
	}

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

	// Determine SQL statement based on configuration
	var sqlStatement string
	var useSchemaRegistry bool

	if c.schemaDefinition != nil && c.UseUpsert {
		// Use MERGE/UPSERT from schema registry
		sqlStatement = fmt.Sprintf(c.schemaDefinition.UpsertSQL, c.CatalogName, c.SchemaName, c.TableName)
		useSchemaRegistry = true
	} else if c.schemaDefinition != nil {
		// Use INSERT from schema registry
		sqlStatement = fmt.Sprintf(c.schemaDefinition.InsertSQL, c.CatalogName, c.SchemaName, c.TableName)
		useSchemaRegistry = true
	} else {
		// Fallback to legacy insert
		sqlStatement = c.getInsertSQL()
		useSchemaRegistry = false
	}

	stmt, err := tx.Prepare(sqlStatement)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Insert/upsert all buffered records
	for _, record := range c.buffer {
		var execErr error
		if useSchemaRegistry {
			execErr = c.insertRecordWithSchema(stmt, record)
		} else {
			execErr = c.insertRecord(stmt, record)
		}

		if execErr != nil {
			c.logger.Error("Failed to insert/upsert record", "error", execErr)
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
		"mode", map[bool]string{true: "UPSERT", false: "INSERT"}[c.UseUpsert],
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
		INSERT INTO %s.%s.%s 
		(sequence, ledger_hash, previous_ledger_hash, closed_at, protocol_version,
		 total_coins, fee_pool, base_fee, base_reserve, max_tx_set_size,
		 successful_transaction_count, failed_transaction_count, soroban_fee_write_1kb,
		 transactions)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, c.CatalogName, c.SchemaName, c.TableName)
		
	case "transactions", "transaction_analytics":
		return fmt.Sprintf(`
		INSERT INTO %s.%s.%s
		(ledger_sequence, transaction_hash, account, account_sequence, max_fee,
		 fee_charged, successful, operation_count, resource_fee, closed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, c.CatalogName, c.SchemaName, c.TableName)
		
	default:
		return fmt.Sprintf(`
		INSERT INTO %s.%s.%s (data)
		VALUES (?)`, c.CatalogName, c.SchemaName, c.TableName)
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

// insertRecordWithSchema inserts a record using the schema registry column mapping
func (c *SaveToDuckLake) insertRecordWithSchema(stmt *sql.Stmt, record map[string]interface{}) error {
	values, err := c.extractValues(record, c.schemaDefinition.ColumnMapping)
	if err != nil {
		return fmt.Errorf("failed to extract values: %w", err)
	}

	_, err = stmt.Exec(values...)
	return err
}

// extractValues extracts values from a JSON record based on ColumnMapping
// Returns values in the order matching the SQL INSERT statement
func (c *SaveToDuckLake) extractValues(record map[string]interface{}, columnMapping map[string]ColumnInfo) ([]interface{}, error) {
	// Define column order matching the INSERT SQL statement
	// This MUST match the order in InsertSQL exactly!
	columnOrder := []string{
		"ledger_sequence", "transaction_hash", "operation_index",
		"ledger_close_time", "contract_id", "invoking_account", "function_name",
		"function_args", "auth_entries", "soroban_resources", "soroban_data",
		"transaction_successful", "operation_successful", "result_value",
		"resource_fee", "instructions_used", "read_bytes", "write_bytes",
		"contract_calls", "state_changes", "archive_source", "metadata",
	}

	values := make([]interface{}, 0, len(columnOrder))
	missingRequired := []string{}

	// Extract values in the exact order matching SQL
	for i, sqlColumn := range columnOrder {
		colInfo, exists := columnMapping[sqlColumn]
		if !exists {
			// Column not in mapping, use nil
			values = append(values, nil)
			continue
		}

		value := c.extractValue(record, colInfo.JSONPath)

		// Check if required field is missing
		if colInfo.Required && value == nil {
			missingRequired = append(missingRequired, colInfo.SQLColumn)
			// Use default value if available
			if colInfo.DefaultValue != "" {
				value = colInfo.DefaultValue
			}
		}

		// Apply default value if value is nil
		if value == nil && colInfo.DefaultValue != "" {
			value = colInfo.DefaultValue
		}

		// Convert value to appropriate SQL type
		convertedValue, err := c.convertToSQLType(value, colInfo.DataType)
		if err != nil {
			c.logger.Error("Failed to convert value",
				"column", colInfo.SQLColumn,
				"index", i,
				"json_path", colInfo.JSONPath,
				"value", value,
				"value_type", fmt.Sprintf("%T", value),
				"data_type", colInfo.DataType,
				"error", err,
			)
			values = append(values, nil)
		} else {
			values = append(values, convertedValue)
		}
	}

	// Log warning for missing required fields
	if len(missingRequired) > 0 {
		c.logger.Warn("Missing required fields in record",
			"fields", missingRequired,
		)
	}

	return values, nil
}

// extractValue extracts a value from nested JSON using a JSON path
// Supports dot notation (e.g., "metadata.ledger_sequence")
func (c *SaveToDuckLake) extractValue(data map[string]interface{}, jsonPath string) interface{} {
	if jsonPath == "" {
		return nil
	}

	// Split path by dots to handle nested fields
	parts := splitJSONPath(jsonPath)

	current := interface{}(data)
	for _, part := range parts {
		switch v := current.(type) {
		case map[string]interface{}:
			var ok bool
			current, ok = v[part]
			if !ok {
				return nil
			}
		default:
			return nil
		}
	}

	return current
}

// splitJSONPath splits a JSON path by dots, handling escaped dots
func splitJSONPath(path string) []string {
	// Simple implementation - just split by dots
	// Could be enhanced to handle escaped dots or array indices
	parts := []string{}
	current := ""

	for _, char := range path {
		if char == '.' {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else {
			current += string(char)
		}
	}

	if current != "" {
		parts = append(parts, current)
	}

	return parts
}

// convertToSQLType converts a value to the appropriate SQL type
func (c *SaveToDuckLake) convertToSQLType(value interface{}, dataType string) (interface{}, error) {
	// Special handling for JSON columns:
	// DuckDB's Go driver requires JSON values to be strings, NOT Go nil
	// For NULL JSON values, we must pass "null" as a string
	if dataType == "JSON" {
		if value == nil {
			return "null", nil  // Return JSON null as a string, not Go nil
		}

		// Check if value is already a string
		if strVal, ok := value.(string); ok {
			return strVal, nil
		}

		// Convert to JSON string
		jsonBytes, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal JSON (type=%T): %w", value, err)
		}
		return string(jsonBytes), nil
	}

	// For all other data types, nil is acceptable
	if value == nil {
		return nil, nil
	}

	switch dataType {
	case "BIGINT":
		return convertInt64Value(value), nil
	case "INT", "INTEGER":
		return convertInt32Value(value), nil
	case "VARCHAR", "TEXT":
		return convertStringValue(value), nil
	case "BOOLEAN":
		return convertBoolValue(value), nil
	case "TIMESTAMP":
		return convertTimestampValue(value), nil
	default:
		// Default: return as-is
		return value, nil
	}
}

// Type conversion helpers for schema registry
func convertInt64Value(value interface{}) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case float64:
		return int64(v)
	case int:
		return int64(v)
	case int32:
		return int64(v)
	default:
		return 0
	}
}

func convertInt32Value(value interface{}) int32 {
	switch v := value.(type) {
	case int32:
		return v
	case float64:
		return int32(v)
	case int:
		return int32(v)
	case int64:
		return int32(v)
	case string:
		// Parse string default values
		var i int32
		fmt.Sscanf(v, "%d", &i)
		return i
	default:
		return 0
	}
}

func convertStringValue(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}

func convertBoolValue(value interface{}) bool {
	switch v := value.(type) {
	case bool:
		return v
	default:
		return false
	}
}

func convertTimestampValue(value interface{}) time.Time {
	switch v := value.(type) {
	case time.Time:
		return v
	case string:
		// Try to parse ISO 8601 timestamp
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return time.Time{}
		}
		return t
	case int64:
		// Unix timestamp
		return time.Unix(v, 0)
	case float64:
		// Unix timestamp
		return time.Unix(int64(v), 0)
	default:
		return time.Time{}
	}
}

// buildIndexSQL generates CREATE INDEX statement from IndexDefinition
func (c *SaveToDuckLake) buildIndexSQL(indexDef IndexDefinition) string {
	var sql string

	if indexDef.Expression != "" {
		// Computed/expression index
		sql = fmt.Sprintf(
			"CREATE INDEX IF NOT EXISTS %s ON %s.%s.%s (%s)",
			indexDef.Name,
			c.CatalogName,
			c.SchemaName,
			c.TableName,
			indexDef.Expression,
		)
	} else if len(indexDef.Columns) > 0 {
		// Regular column index
		unique := ""
		if indexDef.Unique {
			unique = "UNIQUE "
		}

		columns := ""
		for i, col := range indexDef.Columns {
			if i > 0 {
				columns += ", "
			}
			columns += col
		}

		sql = fmt.Sprintf(
			"CREATE %sINDEX IF NOT EXISTS %s ON %s.%s.%s (%s)",
			unique,
			indexDef.Name,
			c.CatalogName,
			c.SchemaName,
			c.TableName,
			columns,
		)
	}

	return sql
}

// mapTableNameToProcessorType maps legacy table names to processor types
// This provides backward compatibility for existing configurations
func (c *SaveToDuckLake) mapTableNameToProcessorType(tableName string) processor.ProcessorType {
	switch tableName {
	case "contract_invocations":
		return processor.ProcessorTypeContractInvocation
	case "contract_events":
		return processor.ProcessorTypeContractEvent
	case "contract_data":
		return processor.ProcessorTypeContractData
	case "payments":
		return processor.ProcessorTypePayment
	case "trades":
		return processor.ProcessorTypeTrade
	case "account_data":
		return processor.ProcessorTypeAccountData
	case "transactions":
		return processor.ProcessorTypeTransaction
	case "ledgers":
		return processor.ProcessorTypeLedger
	default:
		return "" // Unknown mapping
	}
}

// mapProcessorTypeToTableName maps processor types to table names
func (c *SaveToDuckLake) mapProcessorTypeToTableName(processorType processor.ProcessorType) string {
	switch processorType {
	case processor.ProcessorTypeContractInvocation:
		return "contract_invocations"
	case processor.ProcessorTypeContractEvent:
		return "contract_events"
	case processor.ProcessorTypeContractData:
		return "contract_data"
	case processor.ProcessorTypePayment:
		return "payments"
	case processor.ProcessorTypeTrade:
		return "trades"
	case processor.ProcessorTypeAccountData:
		return "account_data"
	case processor.ProcessorTypeTransaction:
		return "transactions"
	case processor.ProcessorTypeLedger:
		return "ledgers"
	default:
		return "data" // Default fallback
	}
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