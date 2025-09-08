# Wallet-Backend Unified Pipeline Implementation Plan

## Overview

To create a single unified pipeline that fully replaces Stellar's wallet-backend, we need to implement two new components:

1. **TransactionXDRExtractor** - A processor that extracts raw XDR data
2. **WalletBackendPostgreSQL** - A consumer that implements wallet-backend's exact schema

## Component 1: TransactionXDRExtractor Processor

### Purpose
Extracts and preserves raw XDR data from transactions and operations, which wallet-backend stores for future reprocessing and detailed transaction analysis.

### Input
- `*ingest.LedgerTransaction` from the pipeline

### Output Structure
```go
type TransactionXDROutput struct {
    // Transaction identification
    LedgerSequence   uint32    `json:"ledger_sequence"`
    TransactionHash  string    `json:"transaction_hash"`
    TransactionIndex int32     `json:"transaction_index"`
    Timestamp        time.Time `json:"timestamp"`
    
    // Raw XDR data (base64 encoded)
    EnvelopeXDR      string    `json:"envelope_xdr"`       // Full transaction envelope
    ResultXDR        string    `json:"result_xdr"`         // Transaction result
    ResultMetaXDR    string    `json:"result_meta_xdr"`    // Transaction metadata
    
    // Parsed basic info for quick access
    SourceAccount    string    `json:"source_account"`
    Success          bool      `json:"success"`
    OperationCount   int       `json:"operation_count"`
    FeeCharged       int64     `json:"fee_charged"`
    
    // Operations with individual XDR
    Operations       []OperationXDR `json:"operations"`
}

type OperationXDR struct {
    Index            int       `json:"index"`
    Type             string    `json:"type"`
    SourceAccount    string    `json:"source_account"`
    OperationXDR     string    `json:"operation_xdr"`      // Raw operation XDR
}
```

### Key Implementation Details

1. **XDR Extraction**:
   - Use `MarshalBinary()` to get raw XDR bytes
   - Base64 encode for storage (matches wallet-backend format)
   - Handle all XDR types: envelope, result, meta

2. **Operation Processing**:
   - Extract each operation's XDR separately
   - Preserve operation order (critical for effects)
   - Include operation type for filtering

3. **Error Handling**:
   - Continue processing even if individual XDR marshaling fails
   - Log errors but don't stop the pipeline

## Component 2: WalletBackendPostgreSQL Consumer

### Purpose
Implements the exact database schema and write patterns of Stellar's wallet-backend, ensuring compatibility with existing wallet applications.

### Database Schema

```sql
-- 1. Transactions table (core transaction data)
CREATE TABLE transactions (
    id BIGSERIAL PRIMARY KEY,
    hash VARCHAR(64) UNIQUE NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    transaction_index INT NOT NULL,
    source_account VARCHAR(56) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    fee_charged BIGINT NOT NULL,
    successful BOOLEAN NOT NULL,
    operation_count INT NOT NULL,
    
    -- Raw XDR columns
    envelope_xdr TEXT NOT NULL,        -- Base64 encoded
    result_xdr TEXT NOT NULL,          -- Base64 encoded
    result_meta_xdr TEXT NOT NULL,     -- Base64 encoded
    
    -- Indexes
    INDEX idx_ledger_seq (ledger_sequence),
    INDEX idx_source_account (source_account),
    INDEX idx_created_at (created_at)
);

-- 2. Operations table (individual operations)
CREATE TABLE operations (
    id BIGSERIAL PRIMARY KEY,
    transaction_id BIGINT REFERENCES transactions(id),
    transaction_hash VARCHAR(64) NOT NULL,
    operation_index INT NOT NULL,
    type VARCHAR(50) NOT NULL,
    source_account VARCHAR(56) NOT NULL,
    operation_xdr TEXT NOT NULL,       -- Base64 encoded
    
    -- Metadata as JSONB for flexibility
    details JSONB,
    
    -- Composite index for lookups
    UNIQUE INDEX idx_tx_op (transaction_hash, operation_index),
    INDEX idx_type (type),
    INDEX idx_op_source (source_account)
);

-- 3. State changes table (from WalletBackend processor)
CREATE TABLE state_changes (
    id BIGSERIAL PRIMARY KEY,
    transaction_id BIGINT REFERENCES transactions(id),
    operation_index INT NOT NULL,
    application_order INT NOT NULL,
    change_type VARCHAR(50) NOT NULL,
    account VARCHAR(56) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    
    -- Optional fields based on change type
    asset_code VARCHAR(12),
    asset_issuer VARCHAR(56),
    asset_type VARCHAR(20),
    amount VARCHAR(40),
    balance_before VARCHAR(40),
    balance_after VARCHAR(40),
    
    -- Flexible metadata
    metadata JSONB,
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes
    INDEX idx_account (account),
    INDEX idx_change_type (change_type),
    INDEX idx_timestamp (timestamp)
);

-- 4. Transaction participants (from ParticipantExtractor)
CREATE TABLE transaction_participants (
    id BIGSERIAL PRIMARY KEY,
    transaction_id BIGINT REFERENCES transactions(id),
    transaction_hash VARCHAR(64) NOT NULL,
    account VARCHAR(56) NOT NULL,
    role VARCHAR(20) NOT NULL, -- source, destination, signer, etc.
    
    -- Prevent duplicates
    UNIQUE INDEX idx_tx_participant_role (transaction_hash, account, role),
    INDEX idx_participant (account)
);

-- 5. Effects table (from StellarEffects processor)
CREATE TABLE effects (
    id BIGSERIAL PRIMARY KEY,
    transaction_id BIGINT REFERENCES transactions(id),
    operation_index INT NOT NULL,
    effect_index INT NOT NULL,
    type VARCHAR(50) NOT NULL,
    type_i INT NOT NULL,
    account VARCHAR(56) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    
    -- Effect-specific data as JSONB
    details JSONB NOT NULL,
    
    -- Indexes
    INDEX idx_effect_account (account),
    INDEX idx_effect_type (type),
    INDEX idx_effect_created (created_at)
);

-- 6. Current account states (for quick balance lookups)
CREATE TABLE accounts (
    id VARCHAR(56) PRIMARY KEY,
    last_modified_ledger BIGINT NOT NULL,
    last_modified_time TIMESTAMPTZ NOT NULL,
    balance BIGINT NOT NULL,
    sequence_number BIGINT NOT NULL,
    num_subentries INT NOT NULL,
    flags INT NOT NULL,
    home_domain VARCHAR(255),
    master_weight INT NOT NULL,
    
    -- Thresholds
    threshold_low INT NOT NULL,
    threshold_medium INT NOT NULL,
    threshold_high INT NOT NULL,
    
    -- Additional data
    data JSONB,
    
    -- Indexes
    INDEX idx_last_modified (last_modified_ledger)
);
```

### Consumer Implementation Details

1. **Batch Processing**:
   ```go
   type WalletBackendBatch struct {
       Transactions []TransactionRecord
       Operations   []OperationRecord
       StateChanges []StateChangeRecord
       Participants []ParticipantRecord
       Effects      []EffectRecord
       Accounts     map[string]AccountState
   }
   ```

2. **Write Strategy**:
   - Accumulate data from all processors
   - Write in single database transaction
   - Use prepared statements for performance
   - Implement proper rollback on failure

3. **Data Coordination**:
   - Match transaction IDs across all tables
   - Maintain referential integrity
   - Handle updates to existing records

4. **Performance Optimizations**:
   - Bulk inserts using COPY or multi-value INSERT
   - Connection pooling
   - Prepared statement caching
   - Configurable batch sizes

### Message Processing Flow

```go
func (c *WalletBackendPostgreSQL) Process(ctx context.Context, msg Message) error {
    switch payload := msg.Payload.(type) {
    case *TransactionXDROutput:
        // Store in transactions and operations tables
        c.bufferTransaction(payload)
        
    case *WalletBackendOutput:
        // Store in state_changes table
        c.bufferStateChanges(payload)
        
    case *ParticipantOutput:
        // Store in transaction_participants table
        c.bufferParticipants(payload)
        
    case *StellarEffectsMessage:
        // Store in effects table
        c.bufferEffects(payload)
        
    case *AccountDataMessage:
        // Update accounts table
        c.bufferAccountState(payload)
    }
    
    // Flush when buffer is full or on interval
    if c.shouldFlush() {
        return c.flush(ctx)
    }
    
    return nil
}
```

## Unified Pipeline Configuration

```yaml
pipelines:
  WalletBackendUnified:
    source:
      type: BufferedStorageSourceAdapter
      config:
        bucket_name: "stellar-ledgers"
        network: "mainnet"
        buffer_size: 1000
        num_workers: 20
        start_ledger: 0
    
    processors:
      # 1. Extract raw XDR data first
      - type: TransactionXDRExtractor
        config: {}
      
      # 2. Extract state changes
      - type: WalletBackend
        config:
          extract_contract_events: false  # Wallets typically don't need
          track_participants: true
      
      # 3. Extract participants
      - type: ParticipantExtractor
        config: {}
      
      # 4. Generate effects
      - type: StellarEffects
        config:
          network_passphrase: "Public Global Stellar Network ; September 2015"
      
      # 5. Extract current account states
      - type: ProcessAccountDataFull
        config:
          network_passphrase: "Public Global Stellar Network ; September 2015"
    
    consumers:
      # Single consumer that handles all data types
      - type: WalletBackendPostgreSQL
        config:
          host: "localhost"
          port: 5432
          database: "wallet_backend"
          username: "postgres"
          password: "password"
          sslmode: "require"
          
          # Performance tuning
          buffer_size: 1000
          flush_interval_ms: 5000
          max_retries: 3
          retry_delay_ms: 1000
          
          # Connection pooling
          max_open_conns: 20
          max_idle_conns: 10
          
          # Schema options
          create_schema: true
          drop_existing: false
```

## Implementation Steps

1. **Phase 1: Core Components**
   - Implement TransactionXDRExtractor processor
   - Implement WalletBackendPostgreSQL consumer
   - Add both to main.go registry

2. **Phase 2: Testing**
   - Unit tests for XDR extraction
   - Integration tests with small ledger range
   - Schema validation tests

3. **Phase 3: Performance Tuning**
   - Benchmark different batch sizes
   - Optimize database indexes
   - Test with production-scale data

4. **Phase 4: Migration Tools**
   - Script to migrate from existing wallet-backend
   - Data validation tools
   - Performance comparison tools

## Benefits of This Approach

1. **Single Pipeline**: All data processed together, ensuring consistency
2. **Atomic Writes**: All related data written in one transaction
3. **Schema Compatibility**: Exact match with wallet-backend
4. **Performance**: Optimized batch processing
5. **Flexibility**: Easy to add/remove processors

## Considerations

1. **Storage Requirements**: XDR data increases storage by ~2-3x
2. **Processing Time**: Additional XDR extraction adds ~10% overhead
3. **Database Load**: Single consumer handles all writes
4. **Memory Usage**: Buffering all data types requires more RAM

This implementation provides a complete replacement for Stellar's wallet-backend with better performance and maintainability.