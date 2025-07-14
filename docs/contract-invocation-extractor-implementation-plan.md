# Contract Invocation Extractor Implementation Plan

## Overview

This document outlines the implementation plan for the **ContractInvocationExtractor** processor, which extracts structured business data from contract invocations and transforms them into a canonical format suitable for analytics, reporting, and downstream processing.

## Executive Summary

The ContractInvocationExtractor processor will:
- Process filtered contract invocations from the existing pipeline
- Extract structured data fields from contract arguments using configurable schemas
- Generate unique Transaction Operation IDs (TOID) for each operation
- Store canonical data in a new PostgreSQL table optimized for analytics
- Support multiple contract types and argument patterns through configuration

## Technical Architecture

### Pipeline Integration
```
BufferedStorageSourceAdapter 
  ↓
ContractInvocation (existing)
  ↓
ContractFilter (existing)
  ↓
ContractInvocationExtractor (NEW)
  ↓
SaveContractInvocationsToPostgreSQL (existing)
SaveExtractedContractInvocationsToPostgreSQL (NEW)
```

### Data Flow
1. **Input**: JSON contract invocation data from ContractFilter
2. **Processing**: Schema-based field extraction and validation
3. **Output**: Structured canonical data with TOID generation
4. **Storage**: Parallel storage in existing and new database tables

## Implementation Phases

### Phase 1: Core Processor Implementation (Week 1)

#### 1.1 File Structure
```
processor/
├── processor_contract_invocation_extractor.go     # Main processor
├── processor_contract_invocation_extractor_test.go # Unit tests
└── types_contract_invocation_extractor.go         # Data structures

consumer/
├── consumer_save_extracted_contract_invocations_to_postgresql.go
├── consumer_save_extracted_contract_invocations_to_postgresql_test.go
└── schema_extracted_contract_invocations.go       # Database schema

config/
├── base/
│   └── extracted_contract_invocations_pipeline.yaml
└── schemas/
    └── contract_extraction_schemas.yaml           # Schema definitions
```

#### 1.2 Core Data Structures
**File**: `processor/types_contract_invocation_extractor.go`

```go
package processor

import (
    "time"
)

// ExtractedContractInvocation represents a contract invocation with extracted business data
type ExtractedContractInvocation struct {
    // Core identifiers
    Toid            uint64 `json:"toid"`
    Ledger          uint32 `json:"ledger"`
    Timestamp       string `json:"timestamp"`
    ContractID      string `json:"contract_id"`
    FunctionName    string `json:"function_name"`
    InvokingAccount string `json:"invoking_account"`
    TxHash          string `json:"transaction_hash"`
    
    // Extracted business fields
    Funder      string `json:"funder,omitempty"`
    Recipient   string `json:"recipient,omitempty"`
    Amount      uint64 `json:"amount,omitempty"`
    ProjectID   string `json:"project_id,omitempty"`
    MemoText    string `json:"memo_text,omitempty"`
    Email       string `json:"email,omitempty"`
    
    // Metadata
    ProcessedAt time.Time `json:"processed_at"`
    SchemaName  string    `json:"schema_name"`
    Successful  bool      `json:"successful"`
}

// ExtractionSchema defines how to extract fields from contract arguments
type ExtractionSchema struct {
    SchemaName   string                        `json:"schema_name"`
    FunctionName string                        `json:"function_name"`
    ContractIDs  []string                      `json:"contract_ids"`
    Extractors   map[string]FieldExtractor     `json:"extractors"`
    Validation   map[string]ValidationRule     `json:"validation"`
}

// FieldExtractor defines how to extract a specific field from arguments
type FieldExtractor struct {
    ArgumentIndex int    `json:"argument_index"`
    FieldPath     string `json:"field_path"`
    FieldType     string `json:"field_type"`
    Required      bool   `json:"required"`
    DefaultValue  string `json:"default_value,omitempty"`
}

// ValidationRule defines validation constraints for extracted fields
type ValidationRule struct {
    MinLength int      `json:"min_length,omitempty"`
    MaxLength int      `json:"max_length,omitempty"`
    Pattern   string   `json:"pattern,omitempty"`
    MinValue  *float64 `json:"min_value,omitempty"`
    MaxValue  *float64 `json:"max_value,omitempty"`
}
```

#### 1.3 Main Processor Implementation
**File**: `processor/processor_contract_invocation_extractor.go`

```go
package processor

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "regex"
    "sync"
    "time"
)

type ContractInvocationExtractor struct {
    processors       []Processor
    extractionSchemas map[string]ExtractionSchema
    mu               sync.RWMutex
    stats            struct {
        ProcessedInvocations uint64
        SuccessfulExtractions uint64
        FailedExtractions    uint64
        SchemaNotFound       uint64
        ValidationErrors     uint64
        LastProcessedTime    time.Time
    }
}

func NewContractInvocationExtractor(config map[string]interface{}) (*ContractInvocationExtractor, error) {
    processor := &ContractInvocationExtractor{
        extractionSchemas: make(map[string]ExtractionSchema),
    }
    
    if err := processor.loadExtractionSchemas(config); err != nil {
        return nil, fmt.Errorf("failed to load extraction schemas: %w", err)
    }
    
    log.Printf("ContractInvocationExtractor initialized with %d schemas", len(processor.extractionSchemas))
    return processor, nil
}

func (p *ContractInvocationExtractor) Subscribe(processor Processor) {
    p.processors = append(p.processors, processor)
}

func (p *ContractInvocationExtractor) Process(ctx context.Context, msg Message) error {
    p.mu.Lock()
    p.stats.ProcessedInvocations++
    p.stats.LastProcessedTime = time.Now()
    p.mu.Unlock()
    
    // Parse contract invocation
    jsonBytes, ok := msg.Payload.([]byte)
    if !ok {
        return fmt.Errorf("expected JSON payload, got %T", msg.Payload)
    }
    
    var invocation ContractInvocation
    if err := json.Unmarshal(jsonBytes, &invocation); err != nil {
        return fmt.Errorf("failed to unmarshal contract invocation: %w", err)
    }
    
    // Extract structured data
    extracted, err := p.extractContractData(&invocation)
    if err != nil {
        p.mu.Lock()
        p.stats.FailedExtractions++
        p.mu.Unlock()
        log.Printf("Failed to extract contract data: %v", err)
        return nil // Don't fail the pipeline, just log the error
    }
    
    p.mu.Lock()
    p.stats.SuccessfulExtractions++
    p.mu.Unlock()
    
    // Forward to processors
    extractedBytes, err := json.Marshal(extracted)
    if err != nil {
        return fmt.Errorf("failed to marshal extracted data: %w", err)
    }
    
    for _, processor := range p.processors {
        if err := processor.Process(ctx, Message{Payload: extractedBytes}); err != nil {
            return fmt.Errorf("error in processor chain: %w", err)
        }
    }
    
    return nil
}

// extractContractData extracts structured data from contract invocation
func (p *ContractInvocationExtractor) extractContractData(invocation *ContractInvocation) (*ExtractedContractInvocation, error) {
    // Find matching schema
    schema, exists := p.findMatchingSchema(invocation.ContractID, invocation.FunctionName)
    if !exists {
        p.mu.Lock()
        p.stats.SchemaNotFound++
        p.mu.Unlock()
        return nil, fmt.Errorf("no extraction schema found for contract %s function %s", 
            invocation.ContractID, invocation.FunctionName)
    }
    
    // Generate TOID (Transaction Operation ID)
    toid := p.generateTOID(invocation.LedgerSequence, 0, 0) // TODO: Add tx/op indices
    
    // Create base extracted structure
    extracted := &ExtractedContractInvocation{
        Toid:            toid,
        Ledger:          invocation.LedgerSequence,
        Timestamp:       mustToUTC(invocation.Timestamp),
        ContractID:      invocation.ContractID,
        FunctionName:    invocation.FunctionName,
        InvokingAccount: invocation.InvokingAccount,
        TxHash:          invocation.TransactionHash,
        ProcessedAt:     time.Now(),
        SchemaName:      schema.SchemaName,
        Successful:      invocation.Successful,
    }
    
    // Extract fields according to schema
    if err := p.extractFieldsFromArguments(extracted, invocation.ArgumentsDecoded, schema); err != nil {
        return nil, fmt.Errorf("failed to extract fields: %w", err)
    }
    
    // Validate extracted data
    if err := p.validateExtractedData(extracted, schema); err != nil {
        p.mu.Lock()
        p.stats.ValidationErrors++
        p.mu.Unlock()
        return nil, fmt.Errorf("validation failed: %w", err)
    }
    
    return extracted, nil
}

// generateTOID creates a unique Transaction Operation ID
func (p *ContractInvocationExtractor) generateTOID(ledger uint32, txIdx, opIdx uint32) uint64 {
    return (uint64(ledger) << 32) | (uint64(txIdx) << 20) | uint64(opIdx)
}

// findMatchingSchema finds the extraction schema for a contract and function
func (p *ContractInvocationExtractor) findMatchingSchema(contractID, functionName string) (ExtractionSchema, bool) {
    p.mu.RLock()
    defer p.mu.RUnlock()
    
    key := fmt.Sprintf("%s:%s", contractID, functionName)
    schema, exists := p.extractionSchemas[key]
    return schema, exists
}

// extractFieldsFromArguments extracts fields from arguments according to schema
func (p *ContractInvocationExtractor) extractFieldsFromArguments(
    extracted *ExtractedContractInvocation,
    args map[string]interface{},
    schema ExtractionSchema,
) error {
    for fieldName, extractor := range schema.Extractors {
        value, err := p.extractFieldValue(args, extractor)
        if err != nil {
            if extractor.Required {
                return fmt.Errorf("required field %s: %w", fieldName, err)
            }
            continue
        }
        
        // Set field value based on field name
        if err := p.setFieldValue(extracted, fieldName, value); err != nil {
            return fmt.Errorf("failed to set field %s: %w", fieldName, err)
        }
    }
    
    return nil
}

// extractFieldValue extracts a single field value from arguments
func (p *ContractInvocationExtractor) extractFieldValue(args map[string]interface{}, extractor FieldExtractor) (interface{}, error) {
    argKey := fmt.Sprintf("arg_%d", extractor.ArgumentIndex)
    arg, exists := args[argKey]
    if !exists {
        if extractor.DefaultValue != "" {
            return extractor.DefaultValue, nil
        }
        return nil, fmt.Errorf("argument %s not found", argKey)
    }
    
    // Navigate nested field path if specified
    if extractor.FieldPath != "" {
        if argMap, ok := arg.(map[string]interface{}); ok {
            if value, exists := argMap[extractor.FieldPath]; exists {
                return p.convertFieldType(value, extractor.FieldType)
            }
        }
        return nil, fmt.Errorf("field %s not found in argument %s", extractor.FieldPath, argKey)
    }
    
    return p.convertFieldType(arg, extractor.FieldType)
}

// convertFieldType converts a value to the specified type
func (p *ContractInvocationExtractor) convertFieldType(value interface{}, fieldType string) (interface{}, error) {
    switch fieldType {
    case "string":
        if str, ok := value.(string); ok {
            return str, nil
        }
        return fmt.Sprintf("%v", value), nil
        
    case "uint64":
        if num, ok := value.(float64); ok {
            return uint64(num), nil
        }
        return nil, fmt.Errorf("cannot convert %v to uint64", value)
        
    case "address":
        if str, ok := value.(string); ok {
            return str, nil
        }
        return nil, fmt.Errorf("cannot convert %v to address", value)
        
    default:
        return value, nil
    }
}

// setFieldValue sets a field value on the extracted structure
func (p *ContractInvocationExtractor) setFieldValue(extracted *ExtractedContractInvocation, fieldName string, value interface{}) error {
    switch fieldName {
    case "funder":
        if str, ok := value.(string); ok {
            extracted.Funder = str
        }
    case "recipient":
        if str, ok := value.(string); ok {
            extracted.Recipient = str
        }
    case "amount":
        if num, ok := value.(uint64); ok {
            extracted.Amount = num
        }
    case "project_id":
        if str, ok := value.(string); ok {
            extracted.ProjectID = str
        }
    case "memo_text":
        if str, ok := value.(string); ok {
            extracted.MemoText = str
        }
    case "email":
        if str, ok := value.(string); ok {
            extracted.Email = str
        }
    default:
        return fmt.Errorf("unknown field name: %s", fieldName)
    }
    
    return nil
}

// validateExtractedData validates the extracted data against schema rules
func (p *ContractInvocationExtractor) validateExtractedData(extracted *ExtractedContractInvocation, schema ExtractionSchema) error {
    for fieldName, rule := range schema.Validation {
        if err := p.validateField(extracted, fieldName, rule); err != nil {
            return fmt.Errorf("validation failed for field %s: %w", fieldName, err)
        }
    }
    
    return nil
}

// validateField validates a single field against its validation rule
func (p *ContractInvocationExtractor) validateField(extracted *ExtractedContractInvocation, fieldName string, rule ValidationRule) error {
    var value interface{}
    
    switch fieldName {
    case "funder":
        value = extracted.Funder
    case "recipient":
        value = extracted.Recipient
    case "amount":
        value = extracted.Amount
    case "project_id":
        value = extracted.ProjectID
    case "memo_text":
        value = extracted.MemoText
    case "email":
        value = extracted.Email
    default:
        return fmt.Errorf("unknown field for validation: %s", fieldName)
    }
    
    // Validate string fields
    if str, ok := value.(string); ok {
        if rule.MinLength > 0 && len(str) < rule.MinLength {
            return fmt.Errorf("string too short: %d < %d", len(str), rule.MinLength)
        }
        if rule.MaxLength > 0 && len(str) > rule.MaxLength {
            return fmt.Errorf("string too long: %d > %d", len(str), rule.MaxLength)
        }
        if rule.Pattern != "" {
            if matched, err := regexp.MatchString(rule.Pattern, str); err != nil {
                return fmt.Errorf("pattern validation error: %w", err)
            } else if !matched {
                return fmt.Errorf("string does not match pattern: %s", rule.Pattern)
            }
        }
    }
    
    // Validate numeric fields
    if num, ok := value.(uint64); ok {
        if rule.MinValue != nil && float64(num) < *rule.MinValue {
            return fmt.Errorf("value too small: %d < %f", num, *rule.MinValue)
        }
        if rule.MaxValue != nil && float64(num) > *rule.MaxValue {
            return fmt.Errorf("value too large: %d > %f", num, *rule.MaxValue)
        }
    }
    
    return nil
}

// loadExtractionSchemas loads extraction schemas from configuration
func (p *ContractInvocationExtractor) loadExtractionSchemas(config map[string]interface{}) error {
    schemas, ok := config["extraction_schemas"].(map[string]interface{})
    if !ok {
        return fmt.Errorf("missing extraction_schemas in configuration")
    }
    
    for schemaName, schemaConfig := range schemas {
        schema, err := p.parseExtractionSchema(schemaName, schemaConfig)
        if err != nil {
            return fmt.Errorf("failed to parse schema %s: %w", schemaName, err)
        }
        
        // Register schema for each contract ID
        for _, contractID := range schema.ContractIDs {
            key := fmt.Sprintf("%s:%s", contractID, schema.FunctionName)
            p.extractionSchemas[key] = schema
        }
    }
    
    return nil
}

// parseExtractionSchema parses a schema configuration
func (p *ContractInvocationExtractor) parseExtractionSchema(name string, config interface{}) (ExtractionSchema, error) {
    // Implementation depends on config structure
    // This is a placeholder for the actual parsing logic
    return ExtractionSchema{}, nil
}

// mustToUTC converts timestamp to UTC string format
func mustToUTC(timestamp interface{}) string {
    switch t := timestamp.(type) {
    case time.Time:
        return t.UTC().Format(time.RFC3339)
    case string:
        parsed, err := time.Parse(time.RFC3339, t)
        if err != nil {
            log.Printf("Error parsing timestamp %s: %v", t, err)
            return t
        }
        return parsed.UTC().Format(time.RFC3339)
    default:
        log.Printf("Unknown timestamp type: %T", t)
        return time.Now().UTC().Format(time.RFC3339)
    }
}

// GetStats returns processor statistics
func (p *ContractInvocationExtractor) GetStats() struct {
    ProcessedInvocations  uint64
    SuccessfulExtractions uint64
    FailedExtractions     uint64
    SchemaNotFound        uint64
    ValidationErrors      uint64
    LastProcessedTime     time.Time
} {
    p.mu.RLock()
    defer p.mu.RUnlock()
    return p.stats
}
```

### Phase 2: Database Schema and Consumer (Week 2)

#### 2.1 Database Schema
**File**: `consumer/schema_extracted_contract_invocations.go`

```go
package consumer

const extractedContractInvocationsSchema = `
-- Create extracted_contract_invocations table
CREATE TABLE IF NOT EXISTS extracted_contract_invocations (
    id SERIAL PRIMARY KEY,
    
    -- Core identifiers
    toid BIGINT UNIQUE NOT NULL,
    ledger INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    contract_id TEXT NOT NULL,
    function_name TEXT NOT NULL,
    invoking_account TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    
    -- Extracted business fields
    funder TEXT,
    recipient TEXT,
    amount BIGINT,
    project_id TEXT,
    memo_text TEXT,
    email TEXT,
    
    -- Metadata
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    schema_name TEXT NOT NULL,
    successful BOOLEAN NOT NULL DEFAULT true,
    
    -- Constraints
    CONSTRAINT check_toid_positive CHECK (toid > 0),
    CONSTRAINT check_ledger_positive CHECK (ledger > 0),
    CONSTRAINT check_amount_non_negative CHECK (amount IS NULL OR amount >= 0),
    CONSTRAINT check_contract_id_not_empty CHECK (length(contract_id) > 0),
    CONSTRAINT check_tx_hash_not_empty CHECK (length(tx_hash) > 0),
    CONSTRAINT check_schema_name_not_empty CHECK (length(schema_name) > 0)
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_toid 
    ON extracted_contract_invocations(toid);
CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_ledger 
    ON extracted_contract_invocations(ledger);
CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_timestamp 
    ON extracted_contract_invocations(timestamp);
CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_contract_id 
    ON extracted_contract_invocations(contract_id);
CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_function_name 
    ON extracted_contract_invocations(function_name);
CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_funder 
    ON extracted_contract_invocations(funder);
CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_recipient 
    ON extracted_contract_invocations(recipient);
CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_project_id 
    ON extracted_contract_invocations(project_id);
CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_schema_name 
    ON extracted_contract_invocations(schema_name);
CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_successful 
    ON extracted_contract_invocations(successful);

-- Create composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_contract_function 
    ON extracted_contract_invocations(contract_id, function_name);
CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_ledger_timestamp 
    ON extracted_contract_invocations(ledger, timestamp);
CREATE INDEX IF NOT EXISTS idx_extracted_contract_invocations_funder_recipient 
    ON extracted_contract_invocations(funder, recipient);
`;
```

#### 2.2 PostgreSQL Consumer
**File**: `consumer/consumer_save_extracted_contract_invocations_to_postgresql.go`

```go
package consumer

import (
    "context"
    "database/sql"
    "encoding/json"
    "fmt"
    "log"
    "time"
    
    _ "github.com/lib/pq"
    "github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

type SaveExtractedContractInvocationsToPostgreSQL struct {
    db         *sql.DB
    processors []processor.Processor
}

func NewSaveExtractedContractInvocationsToPostgreSQL(config map[string]interface{}) (*SaveExtractedContractInvocationsToPostgreSQL, error) {
    // Parse PostgreSQL configuration (reuse existing parsePostgresConfig)
    pgConfig, err := parsePostgresConfig(config)
    if err != nil {
        return nil, fmt.Errorf("invalid PostgreSQL configuration: %w", err)
    }
    
    // Build connection string
    connStr := fmt.Sprintf(
        "host=%s port=%d dbname=%s user=%s password=%s sslmode=%s connect_timeout=%d",
        pgConfig.Host, pgConfig.Port, pgConfig.Database, pgConfig.Username, 
        pgConfig.Password, pgConfig.SSLMode, pgConfig.ConnectTimeout,
    )
    
    log.Printf("Connecting to PostgreSQL for extracted contract invocations at %s:%d...", 
        pgConfig.Host, pgConfig.Port)
    
    // Connect to PostgreSQL
    db, err := sql.Open("postgres", connStr)
    if err != nil {
        return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
    }
    
    // Set connection pool settings
    db.SetMaxOpenConns(pgConfig.MaxOpenConns)
    db.SetMaxIdleConns(pgConfig.MaxIdleConns)
    
    // Test connection
    ctx, cancel := context.WithTimeout(context.Background(), 
        time.Duration(pgConfig.ConnectTimeout)*time.Second)
    defer cancel()
    
    if err := db.PingContext(ctx); err != nil {
        return nil, fmt.Errorf("failed to ping PostgreSQL: %w", err)
    }
    
    log.Printf("Successfully connected to PostgreSQL for extracted contract invocations")
    
    // Initialize schema
    if err := initializeExtractedContractInvocationsSchema(db); err != nil {
        return nil, fmt.Errorf("failed to initialize schema: %w", err)
    }
    
    return &SaveExtractedContractInvocationsToPostgreSQL{
        db:         db,
        processors: make([]processor.Processor, 0),
    }, nil
}

func initializeExtractedContractInvocationsSchema(db *sql.DB) error {
    _, err := db.Exec(extractedContractInvocationsSchema)
    if err != nil {
        return fmt.Errorf("failed to create extracted_contract_invocations table: %w", err)
    }
    
    log.Printf("Successfully initialized extracted_contract_invocations schema")
    return nil
}

func (p *SaveExtractedContractInvocationsToPostgreSQL) Subscribe(processor processor.Processor) {
    p.processors = append(p.processors, processor)
}

func (p *SaveExtractedContractInvocationsToPostgreSQL) Process(ctx context.Context, msg processor.Message) error {
    // Create timeout context
    ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
    defer cancel()
    
    // Parse extracted contract invocation
    jsonBytes, ok := msg.Payload.([]byte)
    if !ok {
        return fmt.Errorf("expected JSON payload, got %T", msg.Payload)
    }
    
    var extracted processor.ExtractedContractInvocation
    if err := json.Unmarshal(jsonBytes, &extracted); err != nil {
        return fmt.Errorf("failed to unmarshal extracted contract invocation: %w", err)
    }
    
    // Insert into database
    _, err := p.db.ExecContext(ctx, `
        INSERT INTO extracted_contract_invocations (
            toid, ledger, timestamp, contract_id, function_name, invoking_account, tx_hash,
            funder, recipient, amount, project_id, memo_text, email, schema_name, successful
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        ON CONFLICT (toid) DO UPDATE SET
            processed_at = NOW(),
            funder = EXCLUDED.funder,
            recipient = EXCLUDED.recipient,
            amount = EXCLUDED.amount,
            project_id = EXCLUDED.project_id,
            memo_text = EXCLUDED.memo_text,
            email = EXCLUDED.email,
            schema_name = EXCLUDED.schema_name,
            successful = EXCLUDED.successful
    `,
        extracted.Toid,
        extracted.Ledger,
        extracted.Timestamp,
        extracted.ContractID,
        extracted.FunctionName,
        extracted.InvokingAccount,
        extracted.TxHash,
        nullString(extracted.Funder),
        nullString(extracted.Recipient),
        nullUint64(extracted.Amount),
        nullString(extracted.ProjectID),
        nullString(extracted.MemoText),
        nullString(extracted.Email),
        extracted.SchemaName,
        extracted.Successful,
    )
    
    if err != nil {
        return fmt.Errorf("failed to insert extracted contract invocation: %w", err)
    }
    
    log.Printf("Saved extracted contract invocation: %s (schema: %s, toid: %d, funder: %s, recipient: %s, amount: %d)", 
        extracted.TxHash, extracted.SchemaName, extracted.Toid, 
        extracted.Funder, extracted.Recipient, extracted.Amount)
    
    // Forward to next processor
    for _, proc := range p.processors {
        if err := proc.Process(ctx, processor.Message{Payload: jsonBytes}); err != nil {
            return fmt.Errorf("error in processor chain: %w", err)
        }
    }
    
    return nil
}

func (p *SaveExtractedContractInvocationsToPostgreSQL) Close() error {
    if p.db != nil {
        return p.db.Close()
    }
    return nil
}

// Helper functions for handling null values
func nullString(s string) interface{} {
    if s == "" {
        return nil
    }
    return s
}

func nullUint64(u uint64) interface{} {
    if u == 0 {
        return nil
    }
    return u
}
```

### Phase 3: Configuration Management (Week 2)

#### 3.1 Schema Configuration
**File**: `config/schemas/contract_extraction_schemas.yaml`

```yaml
# Contract Extraction Schemas Configuration
extraction_schemas:
  carbon_sink_v1:
    schema_name: "carbon_sink_v1"
    function_name: "sink_carbon"
    contract_ids:
      - "CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O"
    extractors:
      funder:
        argument_index: 0
        field_path: "address"
        field_type: "string"
        required: true
      recipient:
        argument_index: 1
        field_path: "address"
        field_type: "string"
        required: true
      amount:
        argument_index: 2
        field_path: ""
        field_type: "uint64"
        required: true
      project_id:
        argument_index: 3
        field_path: ""
        field_type: "string"
        required: true
      memo_text:
        argument_index: 4
        field_path: ""
        field_type: "string"
        required: false
        default_value: ""
      email:
        argument_index: 5
        field_path: ""
        field_type: "string"
        required: false
        default_value: ""
    validation:
      funder:
        min_length: 56
        max_length: 56
        pattern: "^G[A-Z2-7]{55}$"
      recipient:
        min_length: 56
        max_length: 56
        pattern: "^G[A-Z2-7]{55}$"
      amount:
        min_value: 1
        max_value: 9223372036854775807
      project_id:
        min_length: 1
        max_length: 100
      email:
        pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
        
  # Template for additional schema types
  funding_v1:
    schema_name: "funding_v1"
    function_name: "fund_project"
    contract_ids:
      - "CXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    extractors:
      funder:
        argument_index: 0
        field_path: "address"
        field_type: "string"
        required: true
      recipient:
        argument_index: 1
        field_path: "address"
        field_type: "string"
        required: true
      amount:
        argument_index: 2
        field_path: ""
        field_type: "uint64"
        required: true
      project_id:
        argument_index: 3
        field_path: ""
        field_type: "string"
        required: true
      memo_text:
        argument_index: 4
        field_path: ""
        field_type: "string"
        required: false
      email:
        argument_index: 5
        field_path: ""
        field_type: "string"
        required: false
    validation:
      funder:
        min_length: 56
        max_length: 56
        pattern: "^G[A-Z2-7]{55}$"
      recipient:
        min_length: 56
        max_length: 56
        pattern: "^G[A-Z2-7]{55}$"
      amount:
        min_value: 1
      project_id:
        min_length: 1
        max_length: 100
      email:
        pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
```

#### 3.2 Pipeline Configuration
**File**: `config/base/extracted_contract_invocations_pipeline.yaml`

```yaml
pipelines:
  ExtractedContractInvocationsPipeline:
    source:
      type: BufferedStorageSourceAdapter
      config:
        bucket_name: "obsrvr-stellar-ledger-data-testnet-data/landing/ledgers/testnet"
        network: "testnet"
        num_workers: 10
        retry_limit: 3
        retry_wait: 5
        start_ledger: 84100
        end_ledger: 84300
        ledgers_per_file: 1
        files_per_partition: 64000
        
    processors:
      - type: ContractInvocation
        config:
          network_passphrase: "Test SDF Network ; September 2015"
          
      - type: ContractFilter
        config:
          contract_id: "CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O"
          
      - type: ContractInvocationExtractor
        config:
          extraction_schemas:
            carbon_sink_v1:
              schema_name: "carbon_sink_v1"
              function_name: "sink_carbon"
              contract_ids:
                - "CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O"
              extractors:
                funder:
                  argument_index: 0
                  field_path: "address"
                  field_type: "string"
                  required: true
                recipient:
                  argument_index: 1
                  field_path: "address"
                  field_type: "string"
                  required: true
                amount:
                  argument_index: 2
                  field_path: ""
                  field_type: "uint64"
                  required: true
                project_id:
                  argument_index: 3
                  field_path: ""
                  field_type: "string"
                  required: true
                memo_text:
                  argument_index: 4
                  field_path: ""
                  field_type: "string"
                  required: false
                email:
                  argument_index: 5
                  field_path: ""
                  field_type: "string"
                  required: false
              validation:
                funder:
                  min_length: 56
                  max_length: 56
                  pattern: "^G[A-Z2-7]{55}$"
                recipient:
                  min_length: 56
                  max_length: 56
                  pattern: "^G[A-Z2-7]{55}$"
                amount:
                  min_value: 1
                project_id:
                  min_length: 1
                  max_length: 100
                email:
                  pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
                  
    consumers:
      - type: SaveContractInvocationsToPostgreSQL
        config:
          host: "localhost"
          port: 5432
          database: "obsrvr_contract_invocations"
          username: "postgres"
          password: "SecureTh1sSh1t"
          sslmode: "disable"
          max_open_conns: 10
          max_idle_conns: 5
          
      - type: SaveExtractedContractInvocationsToPostgreSQL
        config:
          host: "localhost"
          port: 5432
          database: "obsrvr_contract_invocations"
          username: "postgres"
          password: "SecureTh1sSh1t"
          sslmode: "disable"
          max_open_conns: 10
          max_idle_conns: 5
```

### Phase 4: Factory Integration (Week 3)

#### 4.1 Factory Registration
**File**: `main.go` (updates needed)

```go
// Add to processor factory
func createProcessor(processorType string, config map[string]interface{}) (processor.Processor, error) {
    switch processorType {
    // ... existing cases ...
    case "ContractInvocationExtractor":
        return processor.NewContractInvocationExtractor(config)
    // ... other cases ...
    }
}

// Add to consumer factory
func createConsumer(consumerType string, config map[string]interface{}) (processor.Processor, error) {
    switch consumerType {
    // ... existing cases ...
    case "SaveExtractedContractInvocationsToPostgreSQL":
        return consumer.NewSaveExtractedContractInvocationsToPostgreSQL(config)
    // ... other cases ...
    }
}
```

### Phase 5: Testing Strategy (Week 3)

#### 5.1 Unit Tests
**File**: `processor/processor_contract_invocation_extractor_test.go`

```go
package processor

import (
    "context"
    "encoding/json"
    "testing"
    "time"
)

func TestContractInvocationExtractor_ExtractCarbonSink(t *testing.T) {
    // Test data based on actual contract invocation
    invocation := &ContractInvocation{
        Timestamp:       time.Date(2025, 6, 23, 14, 24, 23, 0, time.UTC),
        LedgerSequence:  84191,
        TransactionHash: "edd9e4226df7c41432c186a8b7231ead112f6ca6c744e46598453fc5b9d7d1f4",
        ContractID:      "CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O",
        FunctionName:    "sink_carbon",
        InvokingAccount: "GAN4SL6DHOQO4POKWOUL4PPCIVJBSDX7SVOLL4GVM4CC27S6WCV7FQZL",
        ArgumentsDecoded: map[string]interface{}{
            "arg_0": map[string]interface{}{
                "type":    "account",
                "address": "GAN4SL6DHOQO4POKWOUL4PPCIVJBSDX7SVOLL4GVM4CC27S6WCV7FQZL",
            },
            "arg_1": map[string]interface{}{
                "type":    "account",
                "address": "GAN4SL6DHOQO4POKWOUL4PPCIVJBSDX7SVOLL4GVM4CC27S6WCV7FQZL",
            },
            "arg_2": float64(1000000),
            "arg_3": "VCS1360",
            "arg_4": "first",
            "arg_5": "account@domain.xyz",
        },
        Successful: true,
    }
    
    // Create extractor with schema
    extractor := &ContractInvocationExtractor{
        extractionSchemas: map[string]ExtractionSchema{
            "CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O:sink_carbon": {
                SchemaName:   "carbon_sink_v1",
                FunctionName: "sink_carbon",
                ContractIDs:  []string{"CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O"},
                Extractors: map[string]FieldExtractor{
                    "funder": {
                        ArgumentIndex: 0,
                        FieldPath:     "address",
                        FieldType:     "string",
                        Required:      true,
                    },
                    "recipient": {
                        ArgumentIndex: 1,
                        FieldPath:     "address",
                        FieldType:     "string",
                        Required:      true,
                    },
                    "amount": {
                        ArgumentIndex: 2,
                        FieldPath:     "",
                        FieldType:     "uint64",
                        Required:      true,
                    },
                    "project_id": {
                        ArgumentIndex: 3,
                        FieldPath:     "",
                        FieldType:     "string",
                        Required:      true,
                    },
                    "memo_text": {
                        ArgumentIndex: 4,
                        FieldPath:     "",
                        FieldType:     "string",
                        Required:      false,
                    },
                    "email": {
                        ArgumentIndex: 5,
                        FieldPath:     "",
                        FieldType:     "string",
                        Required:      false,
                    },
                },
            },
        },
    }
    
    // Extract data
    extracted, err := extractor.extractContractData(invocation)
    if err != nil {
        t.Fatalf("Failed to extract contract data: %v", err)
    }
    
    // Validate results
    if extracted.Toid != 361850667008 {
        t.Errorf("Expected TOID 361850667008, got %d", extracted.Toid)
    }
    
    if extracted.Ledger != 84191 {
        t.Errorf("Expected ledger 84191, got %d", extracted.Ledger)
    }
    
    if extracted.Funder != "GAN4SL6DHOQO4POKWOUL4PPCIVJBSDX7SVOLL4GVM4CC27S6WCV7FQZL" {
        t.Errorf("Expected funder GAN4SL6DHOQO4POKWOUL4PPCIVJBSDX7SVOLL4GVM4CC27S6WCV7FQZL, got %s", extracted.Funder)
    }
    
    if extracted.Recipient != "GAN4SL6DHOQO4POKWOUL4PPCIVJBSDX7SVOLL4GVM4CC27S6WCV7FQZL" {
        t.Errorf("Expected recipient GAN4SL6DHOQO4POKWOUL4PPCIVJBSDX7SVOLL4GVM4CC27S6WCV7FQZL, got %s", extracted.Recipient)
    }
    
    if extracted.Amount != 1000000 {
        t.Errorf("Expected amount 1000000, got %d", extracted.Amount)
    }
    
    if extracted.ProjectID != "VCS1360" {
        t.Errorf("Expected project_id VCS1360, got %s", extracted.ProjectID)
    }
    
    if extracted.MemoText != "first" {
        t.Errorf("Expected memo_text 'first', got %s", extracted.MemoText)
    }
    
    if extracted.Email != "account@domain.xyz" {
        t.Errorf("Expected email 'account@domain.xyz', got %s", extracted.Email)
    }
    
    if extracted.SchemaName != "carbon_sink_v1" {
        t.Errorf("Expected schema_name 'carbon_sink_v1', got %s", extracted.SchemaName)
    }
    
    if !extracted.Successful {
        t.Errorf("Expected successful true, got %v", extracted.Successful)
    }
}

func TestContractInvocationExtractor_ValidationErrors(t *testing.T) {
    // Test validation failures
    tests := []struct {
        name    string
        field   string
        value   interface{}
        rule    ValidationRule
        wantErr bool
    }{
        {
            name:    "email_invalid_format",
            field:   "email",
            value:   "invalid-email",
            rule:    ValidationRule{Pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"},
            wantErr: true,
        },
        {
            name:    "amount_too_small",
            field:   "amount",
            value:   uint64(0),
            rule:    ValidationRule{MinValue: func() *float64 { v := float64(1); return &v }()},
            wantErr: true,
        },
        {
            name:    "project_id_empty",
            field:   "project_id",
            value:   "",
            rule:    ValidationRule{MinLength: 1},
            wantErr: true,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            extractor := &ContractInvocationExtractor{}
            extracted := &ExtractedContractInvocation{}
            
            err := extractor.validateField(extracted, tt.field, tt.rule)
            if (err != nil) != tt.wantErr {
                t.Errorf("validateField() error = %v, wantErr %v", err, tt.wantErr)
            }
        })
    }
}

func TestContractInvocationExtractor_Process(t *testing.T) {
    // Test full processing pipeline
    invocation := &ContractInvocation{
        Timestamp:       time.Now(),
        LedgerSequence:  84191,
        TransactionHash: "test_hash",
        ContractID:      "CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O",
        FunctionName:    "sink_carbon",
        InvokingAccount: "GAN4SL6DHOQO4POKWOUL4PPCIVJBSDX7SVOLL4GVM4CC27S6WCV7FQZL",
        ArgumentsDecoded: map[string]interface{}{
            "arg_0": map[string]interface{}{"address": "GFUNDER"},
            "arg_1": map[string]interface{}{"address": "GRECIPIENT"},
            "arg_2": float64(1000000),
            "arg_3": "VCS1360",
            "arg_4": "test memo",
            "arg_5": "test@example.com",
        },
        Successful: true,
    }
    
    jsonBytes, err := json.Marshal(invocation)
    if err != nil {
        t.Fatalf("Failed to marshal invocation: %v", err)
    }
    
    // Create processor with mock consumer
    mockConsumer := &MockConsumer{}
    extractor := &ContractInvocationExtractor{
        processors: []Processor{mockConsumer},
        extractionSchemas: map[string]ExtractionSchema{
            "CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O:sink_carbon": {
                SchemaName:   "carbon_sink_v1",
                FunctionName: "sink_carbon",
                ContractIDs:  []string{"CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O"},
                Extractors: map[string]FieldExtractor{
                    "funder":     {ArgumentIndex: 0, FieldPath: "address", FieldType: "string", Required: true},
                    "recipient":  {ArgumentIndex: 1, FieldPath: "address", FieldType: "string", Required: true},
                    "amount":     {ArgumentIndex: 2, FieldPath: "", FieldType: "uint64", Required: true},
                    "project_id": {ArgumentIndex: 3, FieldPath: "", FieldType: "string", Required: true},
                    "memo_text":  {ArgumentIndex: 4, FieldPath: "", FieldType: "string", Required: false},
                    "email":      {ArgumentIndex: 5, FieldPath: "", FieldType: "string", Required: false},
                },
            },
        },
    }
    
    // Process message
    err = extractor.Process(context.Background(), Message{Payload: jsonBytes})
    if err != nil {
        t.Fatalf("Process() error = %v", err)
    }
    
    // Verify mock consumer was called
    if !mockConsumer.Called {
        t.Error("Expected mock consumer to be called")
    }
}

// MockConsumer for testing
type MockConsumer struct {
    Called bool
}

func (m *MockConsumer) Process(ctx context.Context, msg Message) error {
    m.Called = true
    return nil
}

func (m *MockConsumer) Subscribe(processor Processor) {
    // No-op for mock
}
```

#### 5.2 Integration Tests
**File**: `processor/processor_contract_invocation_extractor_integration_test.go`

```go
package processor

import (
    "context"
    "testing"
    "time"
)

func TestContractInvocationExtractor_Integration(t *testing.T) {
    // Integration test with actual database
    if testing.Short() {
        t.Skip("skipping integration test")
    }
    
    // Set up test database
    db := setupTestDatabase(t)
    defer db.Close()
    
    // Create consumer
    consumer, err := consumer.NewSaveExtractedContractInvocationsToPostgreSQL(map[string]interface{}{
        "host":     "localhost",
        "port":     5432,
        "database": "test_db",
        "username": "test_user",
        "password": "test_pass",
        "sslmode":  "disable",
    })
    if err != nil {
        t.Fatalf("Failed to create consumer: %v", err)
    }
    defer consumer.Close()
    
    // Create extractor
    extractor, err := NewContractInvocationExtractor(map[string]interface{}{
        "extraction_schemas": map[string]interface{}{
            "carbon_sink_v1": map[string]interface{}{
                "schema_name":   "carbon_sink_v1",
                "function_name": "sink_carbon",
                "contract_ids":  []string{"CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O"},
                "extractors": map[string]interface{}{
                    "funder": map[string]interface{}{
                        "argument_index": 0,
                        "field_path":     "address",
                        "field_type":     "string",
                        "required":       true,
                    },
                    // ... other extractors
                },
            },
        },
    })
    if err != nil {
        t.Fatalf("Failed to create extractor: %v", err)
    }
    
    // Connect extractor to consumer
    extractor.Subscribe(consumer)
    
    // Process test data
    invocation := createTestInvocation()
    jsonBytes, _ := json.Marshal(invocation)
    
    err = extractor.Process(context.Background(), Message{Payload: jsonBytes})
    if err != nil {
        t.Fatalf("Process() error = %v", err)
    }
    
    // Verify data was stored correctly
    verifyDatabaseContent(t, db)
}

func createTestInvocation() *ContractInvocation {
    return &ContractInvocation{
        Timestamp:       time.Now(),
        LedgerSequence:  84191,
        TransactionHash: "test_hash",
        ContractID:      "CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O",
        FunctionName:    "sink_carbon",
        InvokingAccount: "GAN4SL6DHOQO4POKWOUL4PPCIVJBSDX7SVOLL4GVM4CC27S6WCV7FQZL",
        ArgumentsDecoded: map[string]interface{}{
            "arg_0": map[string]interface{}{"address": "GFUNDER"},
            "arg_1": map[string]interface{}{"address": "GRECIPIENT"},
            "arg_2": float64(1000000),
            "arg_3": "VCS1360",
            "arg_4": "test memo",
            "arg_5": "test@example.com",
        },
        Successful: true,
    }
}
```

## Implementation Timeline

### Week 1: Core Development
- [ ] Implement `ContractInvocationExtractor` processor
- [ ] Create data structures and types
- [ ] Implement TOID generation
- [ ] Create field extraction logic
- [ ] Add validation framework
- [ ] Write unit tests

### Week 2: Database and Consumer
- [ ] Design database schema
- [ ] Implement PostgreSQL consumer
- [ ] Create schema migration scripts
- [ ] Add database indexes
- [ ] Test database operations
- [ ] Write consumer tests

### Week 3: Integration and Testing
- [ ] Integrate with factory pattern
- [ ] Create pipeline configurations
- [ ] Write integration tests
- [ ] Performance testing
- [ ] Error handling validation
- [ ] Documentation updates

### Week 4: Deployment and Monitoring
- [ ] Deploy to development environment
- [ ] Test with sample data
- [ ] Monitor performance metrics
- [ ] Create operational runbooks
- [ ] Production deployment
- [ ] Post-deployment validation

## Success Metrics

### Functional Metrics
- [ ] Successfully extracts all configured fields
- [ ] Handles validation errors gracefully
- [ ] Supports multiple contract schemas
- [ ] Generates unique TOIDs correctly
- [ ] Stores data in database without errors

### Performance Metrics
- [ ] Processing latency < 100ms per invocation
- [ ] Database insertion rate > 1000 records/second
- [ ] Memory usage < 512MB under normal load
- [ ] Zero data loss during processing
- [ ] 99.9% uptime during processing

### Quality Metrics
- [ ] Code coverage > 90%
- [ ] All unit tests passing
- [ ] Integration tests passing
- [ ] Documentation complete
- [ ] Configuration validated

## Risk Mitigation

### Technical Risks
1. **Schema Evolution**: Contract arguments may change
   - **Mitigation**: Versioned schemas, backward compatibility
   
2. **Performance Impact**: Additional processing overhead
   - **Mitigation**: Efficient algorithms, database optimization
   
3. **Data Quality**: Invalid or missing arguments
   - **Mitigation**: Robust validation, error handling

### Operational Risks
1. **Database Failures**: PostgreSQL connection issues
   - **Mitigation**: Connection pooling, retry logic, monitoring
   
2. **Configuration Errors**: Invalid schema definitions
   - **Mitigation**: Configuration validation, testing
   
3. **Deployment Issues**: Pipeline disruption
   - **Mitigation**: Gradual rollout, rollback procedures

## Dependencies

### Internal Dependencies
- Existing ContractInvocation processor
- ContractFilter processor
- PostgreSQL consumer infrastructure
- Factory pattern implementation

### External Dependencies
- PostgreSQL database
- Go regex library
- JSON parsing libraries
- Configuration management

## Conclusion

The ContractInvocationExtractor processor will provide a robust, scalable solution for extracting structured business data from contract invocations. The implementation follows established patterns in the codebase while adding powerful new capabilities for analytics and reporting.

The phased approach ensures thorough testing and validation at each stage, while the modular design allows for easy extension to support additional contract types and extraction patterns.