# Canonical Contract Processor Implementation Plan

## Overview

This document outlines the implementation plan for a new **Canonical Contract Processor** that transforms contract invocations into a standardized, structured format. The processor will run after the existing contract invocations and contract filter processors, extracting specific argument patterns and converting them into a canonical representation suitable for analytics and reporting.

## Background

### Current Pipeline Flow
```
Source → Contract Invocations → Contract Filter → [NEW] Canonical Contract Processor → Consumer
```

### Purpose
The canonical processor will:
1. Process filtered contract invocations
2. Extract and validate specific argument patterns
3. Transform raw contract data into structured canonical format
4. Support multiple contract types with different argument schemas
5. Provide standardized data for analytics and reporting

## Requirements Analysis

### Input Data Structure
Based on existing contract invocations processor output:
```go
type ContractInvocation struct {
    Timestamp        time.Time              `json:"timestamp"`
    LedgerSequence   uint32                 `json:"ledger_sequence"`
    TransactionHash  string                 `json:"transaction_hash"`
    ContractID       string                 `json:"contract_id"`
    InvokingAccount  string                 `json:"invoking_account"`
    FunctionName     string                 `json:"function_name,omitempty"`
    ArgumentsRaw     []xdr.ScVal            `json:"arguments_raw,omitempty"`
    Arguments        []json.RawMessage      `json:"arguments,omitempty"`
    ArgumentsDecoded map[string]interface{} `json:"arguments_decoded,omitempty"`
    Successful       bool                   `json:"successful"`
    // ... other fields
}
```

### Target Output Structure
Based on provided code example:
```go
type Canonical struct {
    Toid      uint64 `json:"toid"`
    Ledger    uint32 `json:"ledger"`
    Timestamp string `json:"timestamp"`
    Funder    string `json:"funder"`
    Recipient string `json:"recipient"`
    Amount    uint64 `json:"amount"`
    ProjectID string `json:"project_id"`
    MemoText  string `json:"memo_text"`
    Email     string `json:"email"`
    TxHash    string `json:"transaction_hash"`
}
```

## Implementation Plan

### Phase 1: Core Processor Structure

#### 1.1 New Processor Definition
**File**: `processor/processor_canonical_contract.go`

```go
package processor

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "sync"
    "time"
)

// CanonicalContract represents a standardized contract invocation
type CanonicalContract struct {
    Toid            uint64 `json:"toid"`
    Ledger          uint32 `json:"ledger"`
    Timestamp       string `json:"timestamp"`
    ContractID      string `json:"contract_id"`
    FunctionName    string `json:"function_name"`
    InvokingAccount string `json:"invoking_account"`
    TxHash          string `json:"transaction_hash"`
    
    // Contract-specific fields (extendable)
    Funder      string `json:"funder,omitempty"`
    Recipient   string `json:"recipient,omitempty"`
    Amount      uint64 `json:"amount,omitempty"`
    ProjectID   string `json:"project_id,omitempty"`
    MemoText    string `json:"memo_text,omitempty"`
    Email       string `json:"email,omitempty"`
    
    // Additional metadata
    ProcessedAt time.Time `json:"processed_at"`
    Schema      string    `json:"schema"` // e.g., "funding_v1", "donation_v1"
}

// CanonicalContractProcessor processes contract invocations into canonical format
type CanonicalContractProcessor struct {
    processors       []Processor
    contractSchemas  map[string]ContractSchema
    mu               sync.RWMutex
    stats            struct {
        ProcessedInvocations uint64
        SuccessfulTransforms uint64
        FailedTransforms     uint64
        UnknownSchemas       uint64
        LastProcessedTime    time.Time
    }
}

// ContractSchema defines how to extract canonical fields from contract arguments
type ContractSchema struct {
    SchemaName   string                   `json:"schema_name"`
    FunctionName string                   `json:"function_name"`
    ContractIDs  []string                 `json:"contract_ids"`
    Extractors   map[string]FieldExtractor `json:"extractors"`
}

// FieldExtractor defines how to extract a specific field from arguments
type FieldExtractor struct {
    ArgumentIndex int    `json:"argument_index"`
    FieldPath     string `json:"field_path"`     // e.g., "address" for nested objects
    FieldType     string `json:"field_type"`     // "string", "uint64", "address"
    Required      bool   `json:"required"`
}
```

#### 1.2 TOID Generation
```go
// makeTOID generates a unique Transaction Operation ID
func makeTOID(ledger uint32, txIdx, opIdx uint32) uint64 {
    return (uint64(ledger) << 32) | (uint64(txIdx) << 20) | uint64(opIdx)
}

// extractTxAndOpIndex extracts transaction and operation indices from contract invocation
func extractTxAndOpIndex(invocation *ContractInvocation) (uint32, uint32) {
    // Implementation depends on how we can get tx/op indices from the invocation
    // This may require enhancing the contract invocation processor to include these
    return 0, 0 // Placeholder - needs implementation
}
```

#### 1.3 Transformation Logic
```go
// transformToCanonical converts a contract invocation to canonical format
func (p *CanonicalContractProcessor) transformToCanonical(invocation *ContractInvocation) (*CanonicalContract, error) {
    // Find matching schema
    schema, exists := p.findMatchingSchema(invocation.ContractID, invocation.FunctionName)
    if !exists {
        return nil, fmt.Errorf("no schema found for contract %s function %s", invocation.ContractID, invocation.FunctionName)
    }
    
    // Extract indices for TOID
    txIdx, opIdx := extractTxAndOpIndex(invocation)
    
    // Create base canonical structure
    canonical := &CanonicalContract{
        Toid:            makeTOID(invocation.LedgerSequence, txIdx, opIdx),
        Ledger:          invocation.LedgerSequence,
        Timestamp:       mustToUTC(invocation.Timestamp),
        ContractID:      invocation.ContractID,
        FunctionName:    invocation.FunctionName,
        InvokingAccount: invocation.InvokingAccount,
        TxHash:          invocation.TransactionHash,
        ProcessedAt:     time.Now(),
        Schema:          schema.SchemaName,
    }
    
    // Extract contract-specific fields
    if err := p.extractFields(canonical, invocation.ArgumentsDecoded, schema); err != nil {
        return nil, fmt.Errorf("failed to extract fields: %w", err)
    }
    
    return canonical, nil
}
```

### Phase 2: Database Schema Design

#### 2.1 New Table Structure
**File**: `consumer/consumer_save_canonical_contracts_to_postgresql.go`

```sql
CREATE TABLE IF NOT EXISTS canonical_contracts (
    id SERIAL PRIMARY KEY,
    
    -- Core identifiers
    toid BIGINT UNIQUE NOT NULL,
    ledger INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    contract_id TEXT NOT NULL,
    function_name TEXT NOT NULL,
    invoking_account TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    
    -- Contract-specific fields
    funder TEXT,
    recipient TEXT,
    amount BIGINT,
    project_id TEXT,
    memo_text TEXT,
    email TEXT,
    
    -- Metadata
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    schema_name TEXT NOT NULL,
    
    -- Indexes
    CONSTRAINT check_toid_positive CHECK (toid > 0),
    CONSTRAINT check_ledger_positive CHECK (ledger > 0),
    CONSTRAINT check_amount_positive CHECK (amount IS NULL OR amount >= 0)
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_canonical_contracts_toid ON canonical_contracts(toid);
CREATE INDEX IF NOT EXISTS idx_canonical_contracts_ledger ON canonical_contracts(ledger);
CREATE INDEX IF NOT EXISTS idx_canonical_contracts_timestamp ON canonical_contracts(timestamp);
CREATE INDEX IF NOT EXISTS idx_canonical_contracts_contract_id ON canonical_contracts(contract_id);
CREATE INDEX IF NOT EXISTS idx_canonical_contracts_function_name ON canonical_contracts(function_name);
CREATE INDEX IF NOT EXISTS idx_canonical_contracts_funder ON canonical_contracts(funder);
CREATE INDEX IF NOT EXISTS idx_canonical_contracts_recipient ON canonical_contracts(recipient);
CREATE INDEX IF NOT EXISTS idx_canonical_contracts_project_id ON canonical_contracts(project_id);
CREATE INDEX IF NOT EXISTS idx_canonical_contracts_schema ON canonical_contracts(schema_name);
```

#### 2.2 Schema Configuration Table
```sql
CREATE TABLE IF NOT EXISTS canonical_contract_schemas (
    id SERIAL PRIMARY KEY,
    schema_name TEXT UNIQUE NOT NULL,
    function_name TEXT NOT NULL,
    contract_ids TEXT[] NOT NULL,
    extractors JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

### Phase 3: Consumer Implementation

#### 3.1 PostgreSQL Consumer
**File**: `consumer/consumer_save_canonical_contracts_to_postgresql.go`

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

type SaveCanonicalContractsToPostgreSQL struct {
    db         *sql.DB
    processors []processor.Processor
}

func NewSaveCanonicalContractsToPostgreSQL(config map[string]interface{}) (*SaveCanonicalContractsToPostgreSQL, error) {
    // Similar to existing PostgreSQL consumer setup
    // ... connection logic ...
    
    if err := initializeCanonicalContractsSchema(db); err != nil {
        return nil, fmt.Errorf("failed to initialize schema: %w", err)
    }
    
    return &SaveCanonicalContractsToPostgreSQL{
        db:         db,
        processors: make([]processor.Processor, 0),
    }, nil
}

func (p *SaveCanonicalContractsToPostgreSQL) Process(ctx context.Context, msg processor.Message) error {
    // Parse canonical contract
    var canonical processor.CanonicalContract
    if err := json.Unmarshal(msg.Payload.([]byte), &canonical); err != nil {
        return fmt.Errorf("failed to unmarshal canonical contract: %w", err)
    }
    
    // Insert into database
    _, err := p.db.ExecContext(ctx, `
        INSERT INTO canonical_contracts (
            toid, ledger, timestamp, contract_id, function_name, invoking_account, tx_hash,
            funder, recipient, amount, project_id, memo_text, email, schema_name
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (toid) DO UPDATE SET
            processed_at = NOW(),
            funder = EXCLUDED.funder,
            recipient = EXCLUDED.recipient,
            amount = EXCLUDED.amount,
            project_id = EXCLUDED.project_id,
            memo_text = EXCLUDED.memo_text,
            email = EXCLUDED.email
    `,
        canonical.Toid,
        canonical.Ledger,
        canonical.Timestamp,
        canonical.ContractID,
        canonical.FunctionName,
        canonical.InvokingAccount,
        canonical.TxHash,
        canonical.Funder,
        canonical.Recipient,
        canonical.Amount,
        canonical.ProjectID,
        canonical.MemoText,
        canonical.Email,
        canonical.Schema,
    )
    
    if err != nil {
        return fmt.Errorf("failed to insert canonical contract: %w", err)
    }
    
    log.Printf("Saved canonical contract: %s (schema: %s, toid: %d)", 
        canonical.TxHash, canonical.Schema, canonical.Toid)
    
    // Forward to next processor
    return p.forwardToProcessors(ctx, msg)
}
```

### Phase 4: Configuration and Schema Management

#### 4.1 Configuration Structure
**File**: `config/canonical_schemas.yaml`

```yaml
canonical_schemas:
  funding_v1:
    schema_name: "funding_v1"
    function_name: "fund_project"
    contract_ids:
      - "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"
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
```

#### 4.2 Schema Loading
```go
func (p *CanonicalContractProcessor) loadSchemas(config map[string]interface{}) error {
    schemas, ok := config["canonical_schemas"].(map[string]interface{})
    if !ok {
        return fmt.Errorf("missing canonical_schemas in configuration")
    }
    
    for schemaName, schemaConfig := range schemas {
        schema, err := parseContractSchema(schemaName, schemaConfig)
        if err != nil {
            return fmt.Errorf("failed to parse schema %s: %w", schemaName, err)
        }
        
        // Register schema for each contract ID
        for _, contractID := range schema.ContractIDs {
            key := fmt.Sprintf("%s:%s", contractID, schema.FunctionName)
            p.contractSchemas[key] = schema
        }
    }
    
    return nil
}
```

### Phase 5: Testing Strategy

#### 5.1 Unit Tests
**File**: `processor/processor_canonical_contract_test.go`

```go
package processor

import (
    "testing"
    "time"
)

func TestCanonicalTransformation(t *testing.T) {
    // Test basic transformation
    invocation := &ContractInvocation{
        Timestamp:       time.Now(),
        LedgerSequence:  12345,
        TransactionHash: "abcd1234",
        ContractID:      "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
        FunctionName:    "fund_project",
        ArgumentsDecoded: map[string]interface{}{
            "arg_0": map[string]interface{}{"address": "GFUNDER1"},
            "arg_1": map[string]interface{}{"address": "GRECIPIENT1"},
            "arg_2": float64(1000),
            "arg_3": "PROJECT123",
            "arg_4": "Test memo",
            "arg_5": "test@example.com",
        },
    }
    
    processor := &CanonicalContractProcessor{
        contractSchemas: map[string]ContractSchema{
            "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC:fund_project": {
                SchemaName:   "funding_v1",
                FunctionName: "fund_project",
                ContractIDs:  []string{"CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"},
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
    
    canonical, err := processor.transformToCanonical(invocation)
    if err != nil {
        t.Fatalf("Transformation failed: %v", err)
    }
    
    if canonical.Funder != "GFUNDER1" {
        t.Errorf("Expected funder 'GFUNDER1', got '%s'", canonical.Funder)
    }
    
    if canonical.Amount != 1000 {
        t.Errorf("Expected amount 1000, got %d", canonical.Amount)
    }
    
    if canonical.Schema != "funding_v1" {
        t.Errorf("Expected schema 'funding_v1', got '%s'", canonical.Schema)
    }
}
```

#### 5.2 Integration Tests
```go
func TestCanonicalProcessorIntegration(t *testing.T) {
    // Test full processor pipeline
    // Mock contract invocation → canonical processor → consumer
}
```

### Phase 6: Pipeline Configuration

#### 6.1 Updated Pipeline Config
**File**: `config/base/canonical_pipeline_config.yaml`

```yaml
sources:
  - type: "captive_core_inbound"
    config:
      network_passphrase: "Test SDF Network ; September 2015"
      # ... other config

processors:
  - type: "contract_invocation"
    config:
      network_passphrase: "Test SDF Network ; September 2015"
      
  - type: "contract_filter"
    config:
      contract_ids:
        - "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"
      function_names:
        - "fund_project"
        
  - type: "canonical_contract"
    config:
      canonical_schemas:
        funding_v1:
          schema_name: "funding_v1"
          function_name: "fund_project"
          contract_ids:
            - "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"
          extractors:
            funder:
              argument_index: 0
              field_path: "address"
              field_type: "string"
              required: true
            # ... other extractors

consumers:
  - type: "save_canonical_contracts_to_postgresql"
    config:
      host: "localhost"
      port: 5432
      database: "stellar_contracts"
      username: "postgres"
      password: "password"
```

### Phase 7: Utility Functions

#### 7.1 Time and Type Conversion
```go
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

// extractFieldValue extracts a field value from arguments using the extractor config
func extractFieldValue(args map[string]interface{}, extractor FieldExtractor) (interface{}, error) {
    argKey := fmt.Sprintf("arg_%d", extractor.ArgumentIndex)
    arg, exists := args[argKey]
    if !exists {
        if extractor.Required {
            return nil, fmt.Errorf("required argument %s not found", argKey)
        }
        return nil, nil
    }
    
    // Navigate nested field path if specified
    if extractor.FieldPath != "" {
        if argMap, ok := arg.(map[string]interface{}); ok {
            if value, exists := argMap[extractor.FieldPath]; exists {
                return convertFieldType(value, extractor.FieldType)
            }
        }
        if extractor.Required {
            return nil, fmt.Errorf("required field %s not found in argument %s", extractor.FieldPath, argKey)
        }
        return nil, nil
    }
    
    return convertFieldType(arg, extractor.FieldType)
}
```

## Implementation Timeline

### Week 1: Core Implementation
- [ ] Implement CanonicalContractProcessor structure
- [ ] Create transformation logic
- [ ] Implement TOID generation
- [ ] Create database schema

### Week 2: Consumer and Configuration
- [ ] Implement PostgreSQL consumer
- [ ] Create configuration system
- [ ] Implement schema loading
- [ ] Add factory registration

### Week 3: Testing and Integration
- [ ] Write unit tests
- [ ] Create integration tests
- [ ] Test with sample data
- [ ] Performance testing

### Week 4: Documentation and Deployment
- [ ] Update documentation
- [ ] Create deployment configurations
- [ ] Staging deployment
- [ ] Production rollout

## Benefits

1. **Standardization**: Consistent data format across different contract types
2. **Analytics Ready**: Structured data optimized for querying and reporting
3. **Extensibility**: Easy to add new contract schemas
4. **Performance**: Optimized database schema with proper indexing
5. **Traceability**: TOID provides unique identification for each operation

## Risks and Mitigation

1. **Schema Changes**: Contract argument structures may change
   - **Mitigation**: Versioned schemas, backward compatibility
   
2. **Performance Impact**: Additional processing step
   - **Mitigation**: Efficient transformation logic, database optimization
   
3. **Data Quality**: Invalid or missing arguments
   - **Mitigation**: Robust validation, graceful error handling

This implementation plan provides a comprehensive approach to building a canonical contract processor that can transform raw contract invocations into structured, queryable data while maintaining flexibility for different contract types.