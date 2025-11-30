package consumer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/marcboeker/go-duckdb/v2"
	"github.com/stellar/go/support/log"
	"github.com/withObsrvr/cdp-pipeline-workflow/internal/checkpoint"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// SaveToDuckLakeEnhanced is a consumer that saves data directly to DuckLake tables
type SaveToDuckLakeEnhanced struct {
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

	// Checkpoint features (for crash recovery)
	EnableCheckpoint bool   // Enable checkpoint for resume capability
	CheckpointDir    string // Directory for checkpoint files

	// Connection settings
	MaxConnections   int
	BatchSize        int
	CommitInterval   time.Duration

	// Schema registry settings
	UseUpsert        bool   // Use MERGE/UPSERT instead of INSERT (default: true)
	CreateIndexes    bool   // Create indexes from schema definition (default: true)

	// Internal state
	db               *sql.DB
	connector        *duckdb.Connector  // Shared connector for both sql.DB and native connection
	nativeConn       *duckdb.Conn       // Native connection for appenders
	appenders        map[string]*duckdb.Appender // key = table_name
	appendersMu      sync.Mutex         // protect appenders map
	buffer           []map[string]interface{}
	lastCommit       time.Time
	mutex            sync.Mutex
	logger           *log.Entry
	registry         *DuckLakeSchemaRegistry
	schemaDefinition *SchemaDefinition

	// Checkpoint state
	lastLedger   uint32
	checkpointer *checkpoint.Checkpointer
}

// contains checks if a string contains a substring (helper for error checking)

// NewSaveToDuckLakeEnhanced creates a new DuckLake consumer
func NewSaveToDuckLakeEnhanced(config map[string]interface{}) (processor.Processor, error) {
	consumer := &SaveToDuckLakeEnhanced{
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
		appenders:      make(map[string]*duckdb.Appender),
		logger:         log.DefaultLogger.WithField("consumer", "SaveToDuckLakeEnhanced"),
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

	// Parse checkpoint configuration
	if enableCheckpoint, ok := config["enable_checkpoint"].(bool); ok {
		consumer.EnableCheckpoint = enableCheckpoint
	}
	if checkpointDir, ok := config["checkpoint_dir"].(string); ok {
		consumer.CheckpointDir = checkpointDir
	} else {
		consumer.CheckpointDir = "./state/ducklake"
	}

	// Validate bucket naming conventions for S3 paths (only when public archive is enabled)
	if consumer.EnablePublicArchive {
		if err := validateBucketNaming(consumer.DataPath, consumer.SchemaName, consumer.EnablePublicArchive); err != nil {
			return nil, fmt.Errorf("bucket naming validation failed: %w", err)
		}
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

	// Initialize checkpoint if enabled
	if consumer.EnableCheckpoint {
		cpConfig := checkpoint.Config{
			Enabled:  true,
			Dir:      consumer.CheckpointDir,
			Filename: consumer.TableName + "_checkpoint.json",
		}
		var err error
		consumer.checkpointer, err = checkpoint.NewCheckpointer(cpConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create checkpointer: %w", err)
		}

		cp, err := consumer.checkpointer.Load()
		if err != nil {
			consumer.logger.Warn("Failed to load checkpoint, starting fresh", "error", err)
		} else if cp != nil {
			consumer.lastLedger = cp.LastCommittedLedger
			consumer.logger.Info("Loaded checkpoint", "last_ledger", cp.LastCommittedLedger)
		}
	}

	consumer.logger.Info("SaveToDuckLakeEnhanced consumer initialized",
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
func (c *SaveToDuckLakeEnhanced) configureS3() {
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
func (c *SaveToDuckLakeEnhanced) initializeConnection() error {
	// Create shared connector for both sql.DB and native connection
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return fmt.Errorf("failed to create DuckDB connector: %w", err)
	}
	c.connector = connector

	// Create sql.DB from shared connector
	c.db = sql.OpenDB(connector)

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
	// Include METADATA_SCHEMA if SchemaName is set (for DuckLake catalog with custom schema)
	var attachSQL string
	if c.SchemaName != "" && c.SchemaName != "main" {
		attachSQL = fmt.Sprintf(
			"ATTACH '%s' AS %s (DATA_PATH '%s', METADATA_SCHEMA '%s')",
			c.CatalogPath,
			c.CatalogName,
			c.DataPath,
			c.SchemaName,
		)
	} else {
		attachSQL = fmt.Sprintf(
			"ATTACH '%s' AS %s (DATA_PATH '%s')",
			c.CatalogPath,
			c.CatalogName,
			c.DataPath,
		)
	}
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
			var createAttachSQL string
			if c.SchemaName != "" && c.SchemaName != "main" {
				createAttachSQL = fmt.Sprintf(
					"ATTACH '%s' AS %s (TYPE ducklake, DATA_PATH '%s', METADATA_SCHEMA '%s')",
					c.CatalogPath,
					c.CatalogName,
					c.DataPath,
					c.SchemaName,
				)
			} else {
				createAttachSQL = fmt.Sprintf(
					"ATTACH '%s' AS %s (TYPE ducklake, DATA_PATH '%s')",
					c.CatalogPath,
					c.CatalogName,
					c.DataPath,
				)
			}
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

	// Get native connection from shared connector for Appender API
	conn, err := c.connector.Connect(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get native connection: %w", err)
	}
	duckConn, ok := conn.(*duckdb.Conn)
	if !ok {
		return fmt.Errorf("failed to cast to *duckdb.Conn")
	}
	c.nativeConn = duckConn

	// Execute USE statement on native connection for appenders
	// Appender requires current catalog/schema to be set via USE statement
	// Reuse useSQL from earlier (already declared at line 415)
	_, err2 := c.nativeConn.ExecContext(context.Background(), useSQL, nil)
	if err2 != nil {
		return fmt.Errorf("failed to set schema on native connection: %w", err2)
	}

	c.logger.Info("Native connection initialized for appenders",
		"catalog", c.CatalogName,
		"schema", c.SchemaName,
	)

	return nil
}

// getOrCreateAppender gets an existing appender or creates a new one for the given table
func (c *SaveToDuckLakeEnhanced) getOrCreateAppender(tableName string) (*duckdb.Appender, error) {
	c.appendersMu.Lock()
	defer c.appendersMu.Unlock()

	// Check if appender already exists
	if appender, exists := c.appenders[tableName]; exists {
		return appender, nil
	}

	// Ensure table exists before creating appender
	if err := c.ensureTableExists(tableName); err != nil {
		return nil, fmt.Errorf("failed to ensure table %s exists: %w", tableName, err)
	}

	// Create new appender (use empty string for schema since we set it with USE statement)
	appender, err := duckdb.NewAppenderFromConn(c.nativeConn, "", tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to create appender for table %s: %w", tableName, err)
	}

	c.appenders[tableName] = appender
	c.logger.Info("Created appender for table", "table", tableName)

	return appender, nil
}

// ensureTableExists checks if a table exists and creates it if not
func (c *SaveToDuckLakeEnhanced) ensureTableExists(tableName string) error {
	// Check if table exists
	var exists bool
	checkSQL := fmt.Sprintf(
		"SELECT EXISTS (SELECT 1 FROM duckdb_tables() WHERE database_name = '%s' AND schema_name = '%s' AND table_name = '%s')",
		c.CatalogName,
		c.SchemaName,
		tableName,
	)

	if err := c.db.QueryRow(checkSQL).Scan(&exists); err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}

	if exists {
		c.logger.Debug("Table already exists", "table", tableName)
		return nil
	}

	// Get schema definition from registry
	if c.registry == nil {
		return fmt.Errorf("schema registry not initialized")
	}

	// Map table name to processor type to look up schema
	processorType := c.mapTableNameToProcessorType(tableName)
	if processorType == "" {
		return fmt.Errorf("unknown table name: %s", tableName)
	}

	schema, ok := c.registry.GetSchema(processorType)
	if !ok {
		return fmt.Errorf("schema not found for processor type %s (table: %s)", processorType, tableName)
	}

	// Build CREATE TABLE statement using schema's CreateSQL template
	createSQL := fmt.Sprintf(schema.CreateSQL, c.CatalogName, c.SchemaName, tableName)

	c.logger.Info("Creating table",
		"table", tableName,
		"processor_type", processorType,
		"version", schema.Version)

	if _, err := c.db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	c.logger.Info("Successfully created table", "table", tableName)
	return nil
}

// ensureTable creates the table if it doesn't exist
func (c *SaveToDuckLakeEnhanced) ensureTable() error {
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
func (c *SaveToDuckLakeEnhanced) getCreateTableSQL() string {
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
func (c *SaveToDuckLakeEnhanced) Subscribe(processor.Processor) {}

// Process handles incoming data
func (c *SaveToDuckLakeEnhanced) Process(ctx context.Context, msg processor.Message) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Detect schema from first message if not already set
	if c.schemaDefinition == nil && msg.Metadata != nil {
		var processorType processor.ProcessorType
		var metadataKey string

		// Check for processor_type first (standard metadata)
		if pType, ok := msg.Metadata["processor_type"].(string); ok {
			processorType = processor.ProcessorType(pType)
			metadataKey = "processor_type"
		} else if tableType, ok := msg.Metadata["table_type"].(string); ok {
			// Check for table_type (used by BronzeExtractorsProcessor)
			processorType = processor.ProcessorType(tableType)
			metadataKey = "table_type"
		}

		if processorType != "" {
			if schema, ok := c.registry.GetSchema(processorType); ok {
				c.schemaDefinition = &schema
				c.TableName = c.mapProcessorTypeToTableName(processorType)
				c.logger.Info("Auto-detected schema from message metadata",
					metadataKey, string(processorType),
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

	// Determine table name from metadata for multi-table processing
	var targetTableName string
	if msg.Metadata != nil {
		// Try processor_type or table_type to determine table name
		if pType, ok := msg.Metadata["processor_type"].(string); ok {
			targetTableName = c.mapProcessorTypeToTableName(processor.ProcessorType(pType))
		} else if tableType, ok := msg.Metadata["table_type"].(string); ok {
			targetTableName = c.mapProcessorTypeToTableName(processor.ProcessorType(tableType))
		}
	}
	// Fallback to configured table name if not in metadata
	if targetTableName == "" {
		targetTableName = c.TableName
	}

	// Handle both JSON payloads and Bronze record structs
	var data map[string]interface{}

	// BronzeRowGetter interface for all Bronze types
	type BronzeRowGetter interface {
		GetBronzeRow() []driver.Value
	}

	switch payload := msg.Payload.(type) {
	case []byte:
		// Legacy JSON payload
		if err := json.Unmarshal(payload, &data); err != nil {
			return fmt.Errorf("failed to unmarshal JSON: %w", err)
		}
	case *processor.BronzeLedgerRecord:
		// Bronze ledger record - use GetBronzeRow() directly
		data = c.bronzeLedgerRecordToMap(payload) // Still need map for ledger tracking
		// Store Bronze row values for efficient INSERT (avoids column mapping)
		bronzeRow := payload.GetBronzeRow()
		data["__bronze_row__"] = bronzeRow
		data["__table_name__"] = targetTableName
	case *processor.BronzeTransactionRecord:
		// Bronze transaction record - use GetBronzeRow() directly
		data = c.bronzeTransactionRecordToMap(payload)
		data["__bronze_row__"] = payload.GetBronzeRow()
		data["__table_name__"] = targetTableName
	case *processor.BronzeOperationRecord:
		// Bronze operation record - use GetBronzeRow() directly
		data = c.bronzeOperationRecordToMap(payload)
		data["__bronze_row__"] = payload.GetBronzeRow()
		data["__table_name__"] = targetTableName
	default:
		c.logger.Info("Processing payload in default case",
			"type", fmt.Sprintf("%T", msg.Payload))

		// Check if payload implements BronzeRowGetter (all other Bronze types)
		if bronzeGetter, ok := msg.Payload.(BronzeRowGetter); ok {
			bronzeRow := bronzeGetter.GetBronzeRow()
			c.logger.Info("Payload implements BronzeRowGetter",
				"type", fmt.Sprintf("%T", msg.Payload),
				"row_length", len(bronzeRow))

			// This is a Bronze record - use GetBronzeRow()
			// Convert to map via JSON for ledger tracking
			jsonBytes, err := json.Marshal(payload)
			if err != nil {
				return fmt.Errorf("failed to marshal Bronze record of type %T to JSON: %w", payload, err)
			}
			if err := json.Unmarshal(jsonBytes, &data); err != nil {
				return fmt.Errorf("failed to unmarshal JSON: %w", err)
			}
			// Store Bronze row values for efficient INSERT
			data["__bronze_row__"] = bronzeRow
			data["__table_name__"] = targetTableName
		} else {
			c.logger.Info("Payload does NOT implement BronzeRowGetter",
				"type", fmt.Sprintf("%T", msg.Payload))
			// For non-Bronze record types, use JSON as fallback
			jsonBytes, err := json.Marshal(payload)
			if err != nil {
				return fmt.Errorf("failed to marshal payload of type %T to JSON: %w", payload, err)
			}
			if err := json.Unmarshal(jsonBytes, &data); err != nil {
				return fmt.Errorf("failed to unmarshal JSON: %w", err)
			}
		}
	}

	// Track ledger sequence for checkpoint
	// Handle both float64 (from JSON) and uint32 (from Bronze structs)
	if ledgerSeq, ok := data["ledger_sequence"].(float64); ok {
		if uint32(ledgerSeq) > c.lastLedger {
			c.lastLedger = uint32(ledgerSeq)
		}
	} else if ledgerSeq, ok := data["ledger_sequence"].(uint32); ok {
		if ledgerSeq > c.lastLedger {
			c.lastLedger = ledgerSeq
		}
	} else if sequence, ok := data["sequence"].(float64); ok {
		if uint32(sequence) > c.lastLedger {
			c.lastLedger = uint32(sequence)
		}
	} else if sequence, ok := data["sequence"].(uint32); ok {
		if sequence > c.lastLedger {
			c.lastLedger = sequence
		}
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
func (c *SaveToDuckLakeEnhanced) flush(ctx context.Context) error {
	if len(c.buffer) == 0 {
		return nil
	}

	// Group records by table name
	recordsByTable := make(map[string][]map[string]interface{})
	for _, record := range c.buffer {
		// Determine table name from record metadata or use configured table
		tableName := c.TableName
		if tableNameFromRecord, ok := record["__table_name__"].(string); ok {
			tableName = tableNameFromRecord
		}

		recordsByTable[tableName] = append(recordsByTable[tableName], record)
	}

	// Process each table group
	totalRecords := 0
	for tableName, records := range recordsByTable {
		if len(records) == 0 {
			continue
		}

		// Get or create appender for this table
		appender, err := c.getOrCreateAppender(tableName)
		if err != nil {
			c.logger.Error("Failed to get appender", "table", tableName, "error", err)
			continue
		}

		// Append each record using the appender
		for _, record := range records {
			// Check if this is a Bronze record with pre-extracted values
			if bronzeRow, ok := record["__bronze_row__"].([]driver.Value); ok {
				// Use Bronze row values directly (already in correct column order)
				err := appender.AppendRow(bronzeRow...)
				if err != nil {
					c.logger.Error("Failed to append Bronze row",
						"table", tableName,
						"error", err,
						"value_count", len(bronzeRow))
					continue
				}
			} else {
				// Legacy path - this shouldn't happen with Bronze records but keeping for safety
				c.logger.Warn("Record without __bronze_row__, skipping", "table", tableName)
				continue
			}
		}

		// Flush appender to commit rows to DuckLake
		if err := appender.Flush(); err != nil {
			c.logger.Error("Failed to flush appender", "table", tableName, "error", err)
			continue
		}

		totalRecords += len(records)
		c.logger.Info("Flushed data to DuckLake via Appender",
			"table", tableName,
			"records", len(records),
		)
	}

	// Update checkpoint after successful flush
	if c.EnableCheckpoint && c.checkpointer != nil && c.lastLedger > 0 {
		if err := c.checkpointer.Update(c.lastLedger, "consumer", "unknown", "v1.0", 0, 0); err != nil {
			c.logger.Warn("Failed to update checkpoint", "error", err, "last_ledger", c.lastLedger)
		} else {
			c.logger.Debug("Updated checkpoint", "last_ledger", c.lastLedger)
		}
	}

	// Clear buffer and update timestamp
	c.buffer = c.buffer[:0]
	c.lastCommit = time.Now()

	c.logger.Info("Completed flush cycle",
		"total_records", totalRecords,
		"tables", len(recordsByTable),
	)

	return nil
}

// getInsertSQL returns the INSERT statement based on table type
func (c *SaveToDuckLakeEnhanced) getInsertSQL() string {
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
func (c *SaveToDuckLakeEnhanced) insertRecord(stmt *sql.Stmt, record map[string]interface{}) error {
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

// Bronze record conversion helpers
// These functions convert Bronze record structs to map[string]interface{} for DuckDB insertion

// bronzeLedgerRecordToMap converts BronzeLedgerRecord to map
func (c *SaveToDuckLakeEnhanced) bronzeLedgerRecordToMap(record *processor.BronzeLedgerRecord) map[string]interface{} {
	data := map[string]interface{}{
		"sequence":              record.Sequence,
		"ledger_hash":           record.LedgerHash,
		"previous_ledger_hash":  record.PreviousLedgerHash,
		"closed_at":             record.ClosedAt.Unix(),
		"protocol_version":      record.ProtocolVersion,
		"total_coins":           record.TotalCoins,
		"fee_pool":              record.FeePool,
		"base_fee":              record.BaseFee,
		"base_reserve":          record.BaseReserve,
		"max_tx_set_size":       record.MaxTxSetSize,
		"successful_tx_count":   record.SuccessfulTxCount,
		"failed_tx_count":       record.FailedTxCount,
		"ingestion_timestamp":   record.IngestionTimestamp,
	}

	// Add optional fields
	if record.LedgerRange != nil {
		data["ledger_range"] = *record.LedgerRange
	}
	if record.TransactionCount != nil {
		data["transaction_count"] = *record.TransactionCount
	}
	if record.OperationCount != nil {
		data["operation_count"] = *record.OperationCount
	}
	if record.TxSetOperationCount != nil {
		data["tx_set_operation_count"] = *record.TxSetOperationCount
	}
	if record.SorobanFeeWrite1KB != nil {
		data["soroban_fee_write_1kb"] = *record.SorobanFeeWrite1KB
	}
	if record.NodeID != nil {
		data["node_id"] = *record.NodeID
	}
	if record.Signature != nil {
		data["signature"] = *record.Signature
	}
	if record.LedgerHeader != nil {
		data["ledger_header"] = *record.LedgerHeader
	}
	if record.BucketListSize != nil {
		data["bucket_list_size"] = *record.BucketListSize
	}
	if record.LiveSorobanStateSize != nil {
		data["live_soroban_state_size"] = *record.LiveSorobanStateSize
	}
	if record.EvictedKeysCount != nil {
		data["evicted_keys_count"] = *record.EvictedKeysCount
	}

	return data
}

// bronzeTransactionRecordToMap converts BronzeTransactionRecord to map
func (c *SaveToDuckLakeEnhanced) bronzeTransactionRecordToMap(record *processor.BronzeTransactionRecord) map[string]interface{} {
	data := map[string]interface{}{
		"ledger_sequence":         record.LedgerSequence,
		"transaction_hash":        record.TransactionHash,
		"source_account":          record.SourceAccount,
		"fee_charged":             record.FeeCharged,
		"max_fee":                 record.MaxFee,
		"successful":              record.Successful,
		"transaction_result_code": record.TransactionResultCode,
		"operation_count":         record.OperationCount,
		"created_at":              record.CreatedAt.Unix(),
		"signatures_count":        record.SignaturesCount,
		"new_account":             record.NewAccount,
	}

	// Add optional fields
	if record.AccountSequence != nil {
		data["account_sequence"] = *record.AccountSequence
	}
	if record.LedgerRange != nil {
		data["ledger_range"] = *record.LedgerRange
	}
	if record.MemoType != nil {
		data["memo_type"] = *record.MemoType
	}
	if record.Memo != nil {
		data["memo"] = *record.Memo
	}
	if record.TimeboundsMinTime != nil {
		data["timebounds_min_time"] = *record.TimeboundsMinTime
	}
	if record.TimeboundsMaxTime != nil {
		data["timebounds_max_time"] = *record.TimeboundsMaxTime
	}
	if record.SorobanResourcesInstructions != nil {
		data["soroban_resources_instructions"] = *record.SorobanResourcesInstructions
	}
	if record.SorobanContractEventsCount != nil {
		data["soroban_contract_events_count"] = *record.SorobanContractEventsCount
	}
	if record.TxEnvelope != nil {
		data["tx_envelope"] = *record.TxEnvelope
	}
	if record.TxResult != nil {
		data["tx_result"] = *record.TxResult
	}

	return data
}

// bronzeOperationRecordToMap converts BronzeOperationRecord to map
func (c *SaveToDuckLakeEnhanced) bronzeOperationRecordToMap(record *processor.BronzeOperationRecord) map[string]interface{} {
	data := map[string]interface{}{
		"transaction_hash":       record.TransactionHash,
		"operation_index":        record.OperationIndex,
		"ledger_sequence":        record.LedgerSequence,
		"type":                   record.Type,
		"type_string":            record.TypeString,
		"source_account":         record.SourceAccount,
		"created_at":             record.CreatedAt.Unix(),
		"transaction_successful": record.TransactionSuccessful,
	}

	// Add optional fields (there are many, so we only add non-nil ones)
	if record.LedgerRange != nil {
		data["ledger_range"] = *record.LedgerRange
	}
	if record.Destination != nil {
		data["destination"] = *record.Destination
	}
	if record.Amount != nil {
		data["amount"] = *record.Amount
	}
	if record.Asset != nil {
		data["asset"] = *record.Asset
	}
	if record.AssetType != nil {
		data["asset_type"] = *record.AssetType
	}
	if record.AssetCode != nil {
		data["asset_code"] = *record.AssetCode
	}
	if record.AssetIssuer != nil {
		data["asset_issuer"] = *record.AssetIssuer
	}

	return data
}

// Helper functions for value extraction

// buildIndexSQL generates CREATE INDEX statement from IndexDefinition
func (c *SaveToDuckLakeEnhanced) buildIndexSQL(indexDef IndexDefinition) string {
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

// mapTableNameToProcessorType maps table names to processor types (reverse of mapProcessorTypeToTableName)
func (c *SaveToDuckLakeEnhanced) mapTableNameToProcessorType(tableName string) processor.ProcessorType {
	switch tableName {
	// Silver layer tables
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

	// Bronze layer tables - table name IS the processor type
	default:
		// For Bronze tables, the table name is the processor type
		// (e.g., "ledgers_row_v2" -> ProcessorTypeBronzeLedger)
		return processor.ProcessorType(tableName)
	}
}

// mapProcessorTypeToTableName maps processor types to table names
func (c *SaveToDuckLakeEnhanced) mapProcessorTypeToTableName(processorType processor.ProcessorType) string {
	switch processorType {
	// Silver layer tables (custom naming)
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

	// Bronze layer tables (ProcessorType IS the table name)
	case processor.ProcessorTypeBronzeLedger,
		processor.ProcessorTypeBronzeTransaction,
		processor.ProcessorTypeBronzeOperation,
		processor.ProcessorTypeBronzeEffect,
		processor.ProcessorTypeBronzeTrade,
		processor.ProcessorTypeBronzeNativeBalance,
		processor.ProcessorTypeBronzeAccount,
		processor.ProcessorTypeBronzeTrustline,
		processor.ProcessorTypeBronzeOffer,
		processor.ProcessorTypeBronzeClaimableBalance,
		processor.ProcessorTypeBronzeLiquidityPool,
		processor.ProcessorTypeBronzeContractEvent,
		processor.ProcessorTypeBronzeContractData,
		processor.ProcessorTypeBronzeContractCode,
		processor.ProcessorTypeBronzeConfigSetting,
		processor.ProcessorTypeBronzeTTL,
		processor.ProcessorTypeBronzeEvictedKey,
		processor.ProcessorTypeBronzeRestoredKey,
		processor.ProcessorTypeBronzeAccountSigner:
		return string(processorType) // Return the processor type as-is (e.g., "ledgers_row_v2")

	default:
		return "data" // Default fallback
	}
}

// Close closes the database connection
func (c *SaveToDuckLakeEnhanced) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Flush any remaining data
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	// Close database connection
	if c.db != nil {
		if err := c.db.Close(); err != nil {
			return fmt.Errorf("failed to close database: %w", err)
		}
	}

	c.logger.Info("SaveToDuckLakeEnhanced consumer closed")
	return nil
}

func (c *SaveToDuckLakeEnhanced) insertRecordWithSchema(stmt *sql.Stmt, record map[string]interface{}) error {
	// Check if this is a Bronze record with pre-extracted values
	if bronzeRow, ok := record["__bronze_row__"].([]driver.Value); ok {
		// Use Bronze row values directly (already in correct column order)
		// Convert []driver.Value to []interface{} for Exec
		values := make([]interface{}, len(bronzeRow))
		for i, v := range bronzeRow {
			values[i] = v
		}

		// For UPSERT (MERGE), we need to prepend the key column value
		// MERGE INTO ... USING (SELECT ? as key) ... INSERT VALUES (?, ?, ?)
		// The first ? is for the USING clause (key for matching)
		// Then all values including the key for INSERT
		if c.UseUpsert {
			// Prepend the first value (key column) to the values array
			upsertValues := make([]interface{}, len(values)+1)
			upsertValues[0] = values[0] // Key column for USING clause
			copy(upsertValues[1:], values) // All columns for INSERT
			_, err := stmt.Exec(upsertValues...)
			if err != nil {
				c.logger.Error("UPSERT Bronze record failed",
					"processor_type", c.schemaDefinition.ProcessorType,
					"table_name", c.TableName,
					"error", err)
			}
			return err
		}

		_, err := stmt.Exec(values...)
		if err != nil {
			c.logger.Error("INSERT Bronze record failed",
				"processor_type", c.schemaDefinition.ProcessorType,
				"table_name", c.TableName,
				"value_count", len(values),
				"error", err)
		}
		return err
	}

	// Otherwise use column mapping (for Silver layer or legacy records)
	values, err := c.extractValues(record, c.schemaDefinition.ColumnMapping)
	if err != nil {
		return fmt.Errorf("failed to extract values: %w", err)
	}

	_, err = stmt.Exec(values...)
	return err
}

// extractValues extracts values from a JSON record based on ColumnMapping
// Returns values in the order matching the SQL INSERT statement
func (c *SaveToDuckLakeEnhanced) extractValues(record map[string]interface{}, columnMapping map[string]ColumnInfo) ([]interface{}, error) {
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
func (c *SaveToDuckLakeEnhanced) extractValue(data map[string]interface{}, jsonPath string) interface{} {
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

func (c *SaveToDuckLakeEnhanced) convertToSQLType(value interface{}, dataType string) (interface{}, error) {
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
