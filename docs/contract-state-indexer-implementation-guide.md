# Contract State Indexer Implementation Guide

## Overview

This guide details how to implement contract state indexers in the CDP Pipeline to track:
1. **Token Balances** - Real-time balance tracking for all token holders
2. **Asset Metadata** - Token information including name, symbol, decimals
3. **NFT Ownership** - NFT ownership and metadata tracking

These indexers process Stellar Asset Contract (SAC) events according to SEP-41 standards to maintain accurate state in databases, enabling fast queries for wallets and applications.

## Architecture

```
┌─────────────────┐     ┌───────────────────┐     ┌─────────────────┐
│  Stellar Core   │────▶│  CDP Processors   │────▶│  CDP Consumers  │
│  (SAC Events)   │     │  (State Tracking) │     │  (PostgreSQL)   │
└─────────────────┘     └───────────────────┘     └─────────────────┘
                               │
                               ├── ContractBalanceProcessor
                               ├── AssetMetadataProcessor
                               └── NFTOwnershipProcessor
```

## SAC Event Structure (SEP-41)

### Core Events
```go
// Transfer Event
Topics: ["transfer", from: Address, to: Address]
Data: amount: i128

// Mint Event
Topics: ["mint", to: Address]
Data: amount: i128

// Burn Event
Topics: ["burn", from: Address]
Data: amount: i128

// Clawback Event
Topics: ["clawback", from: Address]
Data: amount: i128
```

## Processor Implementations

### 1. ContractBalanceProcessor

Tracks token balances by processing transfer, mint, and burn events.

```go
// processor/processor_contract_balance.go
package processor

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "sync"
    "time"

    "github.com/stellar/go/xdr"
    "github.com/withObsrvr/cdp-pipeline-workflow/pkg/helpers/stellar"
)

type ContractBalanceProcessor struct {
    processors []Processor
    mu         sync.RWMutex
    // In-memory balance cache for current ledger
    balances   map[string]map[string]int64 // contract -> user -> balance
    stats      struct {
        ProcessedEvents uint64
        UpdatedBalances uint64
        LastLedger      uint32
        LastProcessed   time.Time
    }
}

type BalanceUpdate struct {
    LedgerSequence  uint32    `json:"ledger_sequence"`
    Timestamp       time.Time `json:"timestamp"`
    ContractAddress string    `json:"contract_address"`
    UserAddress     string    `json:"user_address"`
    AssetCode       string    `json:"asset_code,omitempty"`
    AssetIssuer     string    `json:"asset_issuer,omitempty"`
    PreviousBalance int64     `json:"previous_balance"`
    NewBalance      int64     `json:"new_balance"`
    EventType       string    `json:"event_type"` // transfer, mint, burn
    TransactionHash string    `json:"transaction_hash"`
}

func NewContractBalanceProcessor(config map[string]interface{}) (*ContractBalanceProcessor, error) {
    return &ContractBalanceProcessor{
        balances: make(map[string]map[string]int64),
    }, nil
}

func (p *ContractBalanceProcessor) Process(ctx context.Context, msg Message) error {
    // Accept ContractEvent messages from ContractEventProcessor
    var event ContractEvent
    if err := json.Unmarshal(msg.Payload.([]byte), &event); err != nil {
        return fmt.Errorf("failed to unmarshal contract event: %w", err)
    }

    // Only process token events
    if event.EventType != "transfer" && event.EventType != "mint" && 
       event.EventType != "burn" && event.EventType != "clawback" {
        return nil
    }

    update, err := p.processTokenEvent(&event)
    if err != nil {
        log.Printf("Error processing token event: %v", err)
        return nil // Don't fail the pipeline
    }

    if update != nil {
        p.updateStats()
        return p.forwardToConsumers(ctx, update)
    }

    return nil
}

func (p *ContractBalanceProcessor) processTokenEvent(event *ContractEvent) (*BalanceUpdate, error) {
    // Extract addresses and amount based on event type
    var fromAddr, toAddr string
    var amount int64

    switch event.EventType {
    case "transfer":
        if len(event.TopicDecoded) < 3 {
            return nil, fmt.Errorf("invalid transfer event topics")
        }
        fromAddr = p.extractAddress(event.TopicDecoded[1])
        toAddr = p.extractAddress(event.TopicDecoded[2])
        amount = p.extractAmount(event.DataDecoded)

    case "mint":
        if len(event.TopicDecoded) < 2 {
            return nil, fmt.Errorf("invalid mint event topics")
        }
        toAddr = p.extractAddress(event.TopicDecoded[1])
        amount = p.extractAmount(event.DataDecoded)

    case "burn":
        if len(event.TopicDecoded) < 2 {
            return nil, fmt.Errorf("invalid burn event topics")
        }
        fromAddr = p.extractAddress(event.TopicDecoded[1])
        amount = p.extractAmount(event.DataDecoded)

    case "clawback":
        if len(event.TopicDecoded) < 2 {
            return nil, fmt.Errorf("invalid clawback event topics")
        }
        fromAddr = p.extractAddress(event.TopicDecoded[1])
        amount = p.extractAmount(event.DataDecoded)
    }

    // Update balances
    p.mu.Lock()
    defer p.mu.Unlock()

    if p.balances[event.ContractID] == nil {
        p.balances[event.ContractID] = make(map[string]int64)
    }

    var updates []*BalanceUpdate

    // Handle from address
    if fromAddr != "" {
        prevBalance := p.balances[event.ContractID][fromAddr]
        newBalance := prevBalance - amount
        p.balances[event.ContractID][fromAddr] = newBalance

        updates = append(updates, &BalanceUpdate{
            LedgerSequence:  event.LedgerSequence,
            Timestamp:       event.Timestamp,
            ContractAddress: event.ContractID,
            UserAddress:     fromAddr,
            PreviousBalance: prevBalance,
            NewBalance:      newBalance,
            EventType:       event.EventType,
            TransactionHash: event.TransactionHash,
        })
    }

    // Handle to address
    if toAddr != "" {
        prevBalance := p.balances[event.ContractID][toAddr]
        newBalance := prevBalance + amount
        p.balances[event.ContractID][toAddr] = newBalance

        updates = append(updates, &BalanceUpdate{
            LedgerSequence:  event.LedgerSequence,
            Timestamp:       event.Timestamp,
            ContractAddress: event.ContractID,
            UserAddress:     toAddr,
            PreviousBalance: prevBalance,
            NewBalance:      newBalance,
            EventType:       event.EventType,
            TransactionHash: event.TransactionHash,
        })
    }

    p.stats.ProcessedEvents++
    p.stats.UpdatedBalances += uint64(len(updates))
    p.stats.LastLedger = event.LedgerSequence
    p.stats.LastProcessed = time.Now()

    // Return the last update for forwarding
    if len(updates) > 0 {
        return updates[len(updates)-1], nil
    }

    return nil, nil
}
```

### 2. AssetMetadataProcessor

Tracks token metadata by monitoring contract creation and initialization events.

```go
// processor/processor_asset_metadata.go
package processor

type AssetMetadataProcessor struct {
    processors []Processor
    mu         sync.RWMutex
    metadata   map[string]*AssetMetadata // contract -> metadata
    stats      ProcessorStats
}

type AssetMetadata struct {
    ContractAddress string    `json:"contract_address"`
    AssetCode       string    `json:"asset_code"`
    AssetIssuer     string    `json:"asset_issuer"`
    AssetType       string    `json:"asset_type"` // SAC, custom
    Name            string    `json:"name"`
    Symbol          string    `json:"symbol"`
    Decimals        int       `json:"decimals"`
    TotalSupply     int64     `json:"total_supply"`
    IconURL         string    `json:"icon_url,omitempty"`
    CreatedAt       time.Time `json:"created_at"`
    UpdatedAt       time.Time `json:"updated_at"`
    LedgerSequence  uint32    `json:"ledger_sequence"`
}

func (p *AssetMetadataProcessor) Process(ctx context.Context, msg Message) error {
    // Process contract creation events
    // Extract metadata from initialization parameters
    // Track supply changes from mint/burn events
    // Forward metadata updates to consumers
}
```

### 3. NFTOwnershipProcessor

Tracks NFT ownership and metadata.

```go
// processor/processor_nft_ownership.go
package processor

type NFTOwnershipProcessor struct {
    processors []Processor
    mu         sync.RWMutex
    ownership  map[string]map[string]string // contract -> tokenId -> owner
    metadata   map[string]map[string]*NFTMetadata // contract -> tokenId -> metadata
    stats      ProcessorStats
}

type NFTOwnership struct {
    ContractAddress string    `json:"contract_address"`
    TokenID         string    `json:"token_id"`
    OwnerAddress    string    `json:"owner_address"`
    PreviousOwner   string    `json:"previous_owner,omitempty"`
    Metadata        *NFTMetadata `json:"metadata,omitempty"`
    TransferredAt   time.Time `json:"transferred_at"`
    LedgerSequence  uint32    `json:"ledger_sequence"`
    TransactionHash string    `json:"transaction_hash"`
}

type NFTMetadata struct {
    Name        string                 `json:"name"`
    Description string                 `json:"description"`
    Image       string                 `json:"image"`
    Attributes  map[string]interface{} `json:"attributes,omitempty"`
}
```

## Consumer Implementations

### 1. SaveContractStateToPostgreSQL

Stores balance updates and maintains current state in PostgreSQL.

```go
// consumer/consumer_save_contract_state_to_postgresql.go
package consumer

const contractStateSchema = `
-- Current token balances
CREATE TABLE IF NOT EXISTS contract_balances (
    contract_address VARCHAR(56) NOT NULL,
    user_address VARCHAR(56) NOT NULL,
    balance NUMERIC(20,0) NOT NULL DEFAULT 0,
    last_updated_ledger BIGINT NOT NULL,
    last_updated_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (contract_address, user_address)
);

-- Balance history for audit trail
CREATE TABLE IF NOT EXISTS balance_history (
    id BIGSERIAL PRIMARY KEY,
    contract_address VARCHAR(56) NOT NULL,
    user_address VARCHAR(56) NOT NULL,
    previous_balance NUMERIC(20,0) NOT NULL,
    new_balance NUMERIC(20,0) NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    transaction_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_contract_user (contract_address, user_address),
    INDEX idx_ledger (ledger_sequence)
);

-- Asset metadata
CREATE TABLE IF NOT EXISTS asset_metadata (
    contract_address VARCHAR(56) PRIMARY KEY,
    asset_code VARCHAR(12),
    asset_issuer VARCHAR(56),
    asset_type VARCHAR(20) NOT NULL,
    name VARCHAR(255),
    symbol VARCHAR(20),
    decimals INTEGER DEFAULT 7,
    total_supply NUMERIC(20,0),
    icon_url VARCHAR(500),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- NFT ownership
CREATE TABLE IF NOT EXISTS nft_ownership (
    contract_address VARCHAR(56) NOT NULL,
    token_id VARCHAR(100) NOT NULL,
    owner_address VARCHAR(56) NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL,
    acquired_ledger BIGINT NOT NULL,
    PRIMARY KEY (contract_address, token_id)
);

-- NFT metadata
CREATE TABLE IF NOT EXISTS nft_metadata (
    contract_address VARCHAR(56) NOT NULL,
    token_id VARCHAR(100) NOT NULL,
    metadata JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (contract_address, token_id)
);

-- Indexes for performance
CREATE INDEX idx_balances_user ON contract_balances(user_address);
CREATE INDEX idx_balances_contract ON contract_balances(contract_address);
CREATE INDEX idx_nft_owner ON nft_ownership(owner_address);
CREATE INDEX idx_balance_history_time ON balance_history(created_at);
`

type SaveContractStateToPostgreSQL struct {
    db *sql.DB
    insertBalanceStmt *sql.Stmt
    updateBalanceStmt *sql.Stmt
}

func (c *SaveContractStateToPostgreSQL) Process(ctx context.Context, msg processor.Message) error {
    // Handle different message types
    switch payload := msg.Payload.(type) {
    case *BalanceUpdate:
        return c.processBalanceUpdate(ctx, payload)
    case *AssetMetadata:
        return c.processAssetMetadata(ctx, payload)
    case *NFTOwnership:
        return c.processNFTOwnership(ctx, payload)
    }
    return nil
}
```

### 2. Optimized Query Patterns

```sql
-- Get all balances for a contract address
SELECT user_address, balance 
FROM contract_balances 
WHERE contract_address = $1 AND balance > 0
ORDER BY balance DESC;

-- Get all tokens owned by a user
SELECT cb.contract_address, cb.balance, am.symbol, am.decimals, am.name
FROM contract_balances cb
JOIN asset_metadata am ON cb.contract_address = am.contract_address
WHERE cb.user_address = $1 AND cb.balance > 0;

-- Get NFTs owned by a user
SELECT no.contract_address, no.token_id, nm.metadata
FROM nft_ownership no
JOIN nft_metadata nm ON no.contract_address = nm.contract_address 
    AND no.token_id = nm.token_id
WHERE no.owner_address = $1;
```

## Pipeline Configuration

### Example: Token Balance Indexer Pipeline

```yaml
# config/base/contract_balance_indexer.yaml
pipelines:
  ContractBalanceIndexer:
    source:
      type: BufferedStorageSourceAdapter
      config:
        bucket_name: "stellar-ledger-data/mainnet"
        network: "mainnet"
        start_ledger: 55808000
        num_workers: 10
    processors:
      - type: ContractEvent
        config:
          network_passphrase: "Public Global Stellar Network ; September 2015"
      - type: ContractBalance
        config:
          track_zero_balances: false
          batch_size: 1000
      - type: AssetMetadata
        config:
          fetch_icons: true
          icon_service_url: "https://assets.stellar.org"
      - type: NFTOwnership
        config:
          metadata_extractor: "standard"
    consumers:
      - type: SaveContractStateToPostgreSQL
        config:
          host: "localhost"
          port: 5432
          database: "stellar_contract_state"
          username: "postgres"
          password: "${DB_PASSWORD}"
          max_batch_size: 1000
          flush_interval: 5
```

## Integration with Existing CDP Components

### 1. Chain with ContractEventProcessor

```yaml
processors:
  - type: ContractEvent
    config:
      network_passphrase: "Public Global Stellar Network ; September 2015"
  - type: ContractBalance
  - type: AssetMetadata
```

### 2. Parallel Processing

```yaml
processors:
  - type: ContractEvent
    config:
      network_passphrase: "Public Global Stellar Network ; September 2015"
      
consumers:
  # Multiple consumers can process same events
  - type: SaveContractEventsToPostgreSQL
  - type: SaveContractStateToPostgreSQL
  - type: SaveToParquet  # Archive raw events
```

## Performance Optimizations

### 1. Batch Processing
- Accumulate balance updates in memory
- Flush to database in batches
- Use PostgreSQL UPSERT for efficiency

### 2. Caching Strategy
- In-memory cache for hot contracts
- Redis for distributed caching
- Periodic snapshot persistence

### 3. Database Optimizations
- Partitioning by contract address
- Composite indexes for common queries
- Materialized views for aggregations

## Error Handling

### 1. State Recovery
- Track last processed ledger
- Resume from checkpoint on restart
- Verify state consistency

### 2. Event Validation
- Validate addresses
- Check amount overflows
- Handle malformed events

### 3. Database Failures
- Retry with exponential backoff
- Dead letter queue for failed updates
- Alert on persistent failures

## Monitoring and Metrics

### Key Metrics to Track
- Events processed per second
- Balance updates per second
- Database write latency
- Cache hit rate
- Last processed ledger lag

### Health Checks
```go
func (p *ContractBalanceProcessor) GetStats() ProcessorStats {
    return ProcessorStats{
        EventsProcessed: p.stats.ProcessedEvents,
        BalancesTracked: len(p.balances),
        LastLedger:      p.stats.LastLedger,
        LastProcessed:   p.stats.LastProcessed,
    }
}
```

## Testing Strategy

### 1. Unit Tests
- Event parsing
- Balance calculations
- Edge cases (overflows, invalid data)

### 2. Integration Tests
- Full pipeline with test events
- Database operations
- State consistency verification

### 3. Load Testing
- High volume event processing
- Database write performance
- Memory usage under load

## Future Enhancements

### 1. Advanced Features
- Historical balance queries
- Balance snapshots at specific ledgers
- Token holder analytics
- Cross-contract balance aggregation

### 2. Performance Improvements
- Parallel processing by contract
- Incremental state updates
- Bloom filters for existence checks

### 3. Additional Processors
- Liquidity pool share tracking
- Governance token voting power
- Staking and rewards tracking
- DEX trading volume per token

## Migration from Existing Systems

### 1. Initial State Load
- Query current state from Horizon/RPC
- Backfill historical events
- Verify balance consistency

### 2. Gradual Migration
- Run in parallel with existing system
- Compare results for validation
- Switch over when confident

## Conclusion

This implementation provides a robust foundation for tracking contract state in the CDP pipeline. The modular design allows for:
- Easy extension with new event types
- Flexible storage backends
- Horizontal scaling
- Integration with existing CDP infrastructure

The processors handle the complexities of SAC event processing while the consumers ensure reliable state persistence, enabling fast queries for wallets and applications through the separate Obsrvr Gateway API layer.