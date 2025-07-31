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
	"github.com/tidwall/gjson"
	processor "github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// FieldConfig defines configuration for a single field to extract and store
type FieldConfig struct {
	Name       string `json:"name" yaml:"name"`
	SourcePath string `json:"source_path,omitempty" yaml:"source_path,omitempty"`
	Type       string `json:"type" yaml:"type"`
	Required   bool   `json:"required,omitempty" yaml:"required,omitempty"`
	Index      string `json:"index,omitempty" yaml:"index,omitempty"`
	Generated  bool   `json:"generated,omitempty" yaml:"generated,omitempty"`
	Default    string `json:"default,omitempty" yaml:"default,omitempty"`
}

// IndexConfig defines configuration for table indexes
type IndexConfig struct {
	Name      string   `json:"name" yaml:"name"`
	Columns   []string `json:"columns" yaml:"columns"`
	Type      string   `json:"type,omitempty" yaml:"type,omitempty"`     // btree, gin, gist, etc.
	Condition string   `json:"condition,omitempty" yaml:"condition,omitempty"` // WHERE clause
}

// SaveContractDataToPostgreSQLConfig holds configuration for the consumer
type SaveContractDataToPostgreSQLConfig struct {
	// Database connection parameters
	DatabaseURL string `json:"database_url,omitempty" yaml:"database_url,omitempty"`
	Host        string `json:"host,omitempty" yaml:"host,omitempty"`
	Port        int    `json:"port,omitempty" yaml:"port,omitempty"`
	Database    string `json:"database,omitempty" yaml:"database,omitempty"`
	Username    string `json:"username,omitempty" yaml:"username,omitempty"`
	Password    string `json:"password,omitempty" yaml:"password,omitempty"`
	SSLMode     string `json:"sslmode,omitempty" yaml:"sslmode,omitempty"`
	
	// Connection pool settings
	MaxOpenConns    int `json:"max_open_conns,omitempty" yaml:"max_open_conns,omitempty"`
	MaxIdleConns    int `json:"max_idle_conns,omitempty" yaml:"max_idle_conns,omitempty"`
	ConnectTimeout  int `json:"connect_timeout,omitempty" yaml:"connect_timeout,omitempty"`
	
	// Table configuration
	TableName  string   `json:"table_name" yaml:"table_name"`
	DataTypes  []string `json:"data_types,omitempty" yaml:"data_types,omitempty"` // Filter by contract data type
	
	// Field selection schema
	Fields  []FieldConfig  `json:"fields" yaml:"fields"`
	Indexes []IndexConfig  `json:"indexes,omitempty" yaml:"indexes,omitempty"`
	
	// Processing options
	BatchSize       int  `json:"batch_size,omitempty" yaml:"batch_size,omitempty"`
	CreateTableIfNotExists bool `json:"create_table_if_not_exists,omitempty" yaml:"create_table_if_not_exists,omitempty"`
}

// SaveContractDataToPostgreSQL is a configurable consumer for storing contract data
type SaveContractDataToPostgreSQL struct {
	config          SaveContractDataToPostgreSQLConfig
	db              *sql.DB
	insertStmt      *sql.Stmt
	schemaCreated   bool
	processors      []processor.Processor
	fieldExtractor  *FieldExtractor
	schemaManager   *SchemaManager
}

// getKeys returns the keys of a map for debugging
func getKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// convertMapInterface converts map[interface{}]interface{} to map[string]interface{} recursively
func convertMapInterface(input interface{}) interface{} {
	switch v := input.(type) {
	case map[interface{}]interface{}:
		result := make(map[string]interface{})
		for key, value := range v {
			if strKey, ok := key.(string); ok {
				result[strKey] = convertMapInterface(value)
			}
		}
		return result
	case map[string]interface{}:
		// Also process map[string]interface{} in case it contains nested map[interface{}]interface{}
		result := make(map[string]interface{})
		for key, value := range v {
			result[key] = convertMapInterface(value)
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = convertMapInterface(item)
		}
		return result
	default:
		return v
	}
}

// NewSaveContractDataToPostgreSQL creates a new configurable PostgreSQL consumer
func NewSaveContractDataToPostgreSQL(config map[string]interface{}) (*SaveContractDataToPostgreSQL, error) {
	var cfg SaveContractDataToPostgreSQLConfig
	
	// Convert any nested map[interface{}]interface{} to map[string]interface{}
	cleanConfig := convertMapInterface(config).(map[string]interface{})
	
	// Convert config map to struct
	jsonBytes, err := json.Marshal(cleanConfig)
	if err != nil {
		return nil, fmt.Errorf("error marshaling config: %w", err)
	}
	
	if err := json.Unmarshal(jsonBytes, &cfg); err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %w", err)
	}
	
	// Validate configuration
	if err := validateConfig(&cfg); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	
	// Set defaults
	setConfigDefaults(&cfg)
	
	// Create consumer instance
	consumer := &SaveContractDataToPostgreSQL{
		config:         cfg,
		processors:     make([]processor.Processor, 0),
		fieldExtractor: NewFieldExtractor(),
		schemaManager:  NewSchemaManager(),
	}
	
	// Initialize database connection
	if err := consumer.initDatabase(); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}
	
	return consumer, nil
}

// Subscribe adds a processor to receive data from this consumer
func (c *SaveContractDataToPostgreSQL) Subscribe(processor processor.Processor) {
	c.processors = append(c.processors, processor)
}

// Process handles incoming contract data messages
func (c *SaveContractDataToPostgreSQL) Process(ctx context.Context, msg processor.Message) error {
	// Ensure schema is created
	if !c.schemaCreated && c.config.CreateTableIfNotExists {
		if err := c.ensureSchema(ctx); err != nil {
			return fmt.Errorf("failed to ensure schema: %w", err)
		}
	}
	
	// Extract message type
	msgBytes, ok := msg.Payload.([]byte)
	if !ok {
		// Try to marshal if it's not already bytes
		var err error
		msgBytes, err = json.Marshal(msg.Payload)
		if err != nil {
			return fmt.Errorf("failed to marshal message payload: %w", err)
		}
	}
	
	// Extract message type for logging and filtering
	messageType := gjson.GetBytes(msgBytes, "message_type").String()
	ledgerSeq := gjson.GetBytes(msgBytes, "ledger_sequence").Int()
	contractID := gjson.GetBytes(msgBytes, "contract_id").String()
	
	log.Printf("SaveContractDataPostgreSQL: Processing message type='%s', ledger=%d, contract_id='%s'", messageType, ledgerSeq, contractID)
	
	// Check if this is a supported contract data type
	if !c.isDataTypeSupported(messageType) {
		log.Printf("SaveContractDataPostgreSQL: Skipping unsupported message type: %s", messageType)
		return nil
	}
	
	// Extract fields according to configuration
	fieldValues, err := c.fieldExtractor.ExtractFields(msgBytes, c.config.Fields)
	if err != nil {
		return fmt.Errorf("failed to extract fields: %w", err)
	}
	
	log.Printf("SaveContractDataPostgreSQL: Extracted %d fields for ledger %d", len(fieldValues), ledgerSeq)
	
	// Store in database
	if err := c.storeData(ctx, fieldValues); err != nil {
		return fmt.Errorf("failed to store data: %w", err)
	}
	
	log.Printf("SaveContractDataPostgreSQL: Successfully stored contract data for ledger %d, contract %s", ledgerSeq, contractID)
	
	// Forward to downstream processors
	for _, proc := range c.processors {
		if err := proc.Process(ctx, msg); err != nil {
			log.Printf("Error in downstream processor: %v", err)
		}
	}
	
	return nil
}

// initDatabase initializes the database connection
func (c *SaveContractDataToPostgreSQL) initDatabase() error {
	var connStr string
	
	if c.config.DatabaseURL != "" {
		connStr = c.config.DatabaseURL
	} else {
		connStr = fmt.Sprintf(
			"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s connect_timeout=%d",
			c.config.Host, c.config.Port, c.config.Username, 
			c.config.Password, c.config.Database, c.config.SSLMode,
			c.config.ConnectTimeout,
		)
	}
	
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	
	// Configure connection pool
	db.SetMaxOpenConns(c.config.MaxOpenConns)
	db.SetMaxIdleConns(c.config.MaxIdleConns)
	
	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}
	
	c.db = db
	return nil
}

// ensureSchema creates the table and indexes if they don't exist
func (c *SaveContractDataToPostgreSQL) ensureSchema(ctx context.Context) error {
	// Generate CREATE TABLE statement
	createTableSQL := c.schemaManager.GenerateCreateTableSQL(c.config.TableName, c.config.Fields)
	
	// Execute CREATE TABLE
	if _, err := c.db.ExecContext(ctx, createTableSQL); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	
	// Create indexes
	for _, indexCfg := range c.config.Indexes {
		indexSQL := c.schemaManager.GenerateCreateIndexSQL(c.config.TableName, indexCfg)
		if _, err := c.db.ExecContext(ctx, indexSQL); err != nil {
			log.Printf("Warning: failed to create index %s: %v", indexCfg.Name, err)
			// Continue even if index creation fails (it might already exist)
		}
	}
	
	// Prepare insert statement
	if err := c.prepareInsertStatement(); err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	
	c.schemaCreated = true
	log.Printf("Schema created/verified for table: %s", c.config.TableName)
	return nil
}

// prepareInsertStatement creates the prepared statement for inserts
func (c *SaveContractDataToPostgreSQL) prepareInsertStatement() error {
	// Get non-generated fields
	var columns []string
	var placeholders []string
	
	idx := 1
	for _, field := range c.config.Fields {
		if !field.Generated {
			columns = append(columns, field.Name)
			placeholders = append(placeholders, fmt.Sprintf("$%d", idx))
			idx++
		}
	}
	
	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		c.config.TableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)
	
	stmt, err := c.db.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	
	c.insertStmt = stmt
	return nil
}

// storeData stores the extracted field values in the database
func (c *SaveContractDataToPostgreSQL) storeData(ctx context.Context, fieldValues map[string]interface{}) error {
	// Build values array in the same order as the prepared statement
	var values []interface{}
	
	for _, field := range c.config.Fields {
		if !field.Generated {
			value, exists := fieldValues[field.Name]
			if !exists {
				if field.Required {
					return fmt.Errorf("required field %s not found", field.Name)
				}
				value = nil
			}
			values = append(values, value)
		}
	}
	
	// Execute insert
	if _, err := c.insertStmt.ExecContext(ctx, values...); err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}
	
	return nil
}

// isDataTypeSupported checks if the message type is in the configured data types
func (c *SaveContractDataToPostgreSQL) isDataTypeSupported(dataType string) bool {
	if len(c.config.DataTypes) == 0 {
		// No filter configured, accept all types
		return true
	}
	
	for _, supportedType := range c.config.DataTypes {
		if supportedType == dataType {
			return true
		}
	}
	
	return false
}

// Close cleans up resources
func (c *SaveContractDataToPostgreSQL) Close() error {
	if c.insertStmt != nil {
		c.insertStmt.Close()
	}
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// validateConfig validates the consumer configuration
func validateConfig(cfg *SaveContractDataToPostgreSQLConfig) error {
	// Validate database connection
	if cfg.DatabaseURL == "" && cfg.Host == "" {
		return fmt.Errorf("either database_url or host must be specified")
	}
	
	// Validate table name
	if cfg.TableName == "" {
		return fmt.Errorf("table_name must be specified")
	}
	
	// Validate fields
	if len(cfg.Fields) == 0 {
		return fmt.Errorf("at least one field must be specified")
	}
	
	// Check for duplicate field names
	fieldNames := make(map[string]bool)
	for _, field := range cfg.Fields {
		if fieldNames[field.Name] {
			return fmt.Errorf("duplicate field name: %s", field.Name)
		}
		fieldNames[field.Name] = true
		
		// Validate field configuration
		if field.Name == "" {
			return fmt.Errorf("field name cannot be empty")
		}
		if field.Type == "" {
			return fmt.Errorf("field type cannot be empty for field %s", field.Name)
		}
		if !field.Generated && field.Required && field.SourcePath == "" {
			return fmt.Errorf("required field %s must have source_path", field.Name)
		}
	}
	
	return nil
}

// setConfigDefaults sets default values for optional configuration
func setConfigDefaults(cfg *SaveContractDataToPostgreSQLConfig) {
	if cfg.Port == 0 {
		cfg.Port = 5432
	}
	if cfg.SSLMode == "" {
		cfg.SSLMode = "disable"
	}
	if cfg.MaxOpenConns == 0 {
		cfg.MaxOpenConns = 25
	}
	if cfg.MaxIdleConns == 0 {
		cfg.MaxIdleConns = 5
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 30
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}
	// Default to creating table if not exists
	if !cfg.CreateTableIfNotExists {
		cfg.CreateTableIfNotExists = true
	}
}