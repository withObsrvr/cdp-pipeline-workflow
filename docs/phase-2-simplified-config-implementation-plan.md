# Phase 2: Simplified Configuration Implementation Plan

## Executive Summary

This document outlines the implementation plan for Phase 2 of the CDP Pipeline Workflow evolution: **Simplified Configuration with Smart Defaults**. This phase aims to dramatically reduce configuration complexity while maintaining full backward compatibility, making the tool more accessible to new users while preserving the power needed by advanced users.

## Table of Contents

1. [Objectives](#objectives)
2. [Design Principles](#design-principles)
3. [Technical Architecture](#technical-architecture)
4. [Implementation Components](#implementation-components)
5. [Configuration Examples](#configuration-examples)
6. [Development Timeline](#development-timeline)
7. [Testing Strategy](#testing-strategy)
8. [Migration Plan](#migration-plan)
9. [Success Metrics](#success-metrics)

## Objectives

### Primary Goals
1. **Reduce Configuration Verbosity** by 60-80%
2. **Zero Breaking Changes** - All existing configurations continue to work
3. **Intuitive Syntax** - New users productive in <5 minutes
4. **Smart Defaults** - Sensible defaults for 90% of use cases
5. **Progressive Disclosure** - Advanced options available when needed

### Secondary Goals
1. Enable foundation for Phase 3 (Protocol Handlers)
2. Improve error messages and validation
3. Support environment variable expansion
4. Enable configuration composition and reuse

## Design Principles

### 1. Convention Over Configuration
- Infer types from context
- Apply smart defaults based on patterns
- Minimize required fields

### 2. Backward Compatibility First
- Detect legacy format automatically
- Support both formats simultaneously
- Provide migration tools, not mandates

### 3. User-Centric Design
- Use terminology users understand
- Hide implementation details
- Provide helpful error messages

### 4. Progressive Enhancement
- Simple things simple
- Complex things possible
- Don't show complexity until needed

## Technical Architecture

### Core Components

```
internal/config/
├── v2/
│   ├── loader.go          # Main configuration loader
│   ├── inference.go       # Type inference engine
│   ├── defaults.go        # Smart defaults engine
│   ├── aliases.go         # Name aliasing system
│   ├── validator.go       # Enhanced validation
│   ├── transformer.go     # Config transformation
│   └── compatibility.go   # Legacy format support
├── migrations/
│   ├── analyzer.go        # Config analysis tools
│   └── upgrader.go        # Automated upgrade tool
└── testing/
    └── equivalence.go     # Test config equivalence
```

### Data Flow

```
Raw YAML → Environment Expansion → Format Detection → 
  ↓
  ├─→ Legacy Path → Direct Use
  └─→ V2 Path → Alias Resolution → Type Inference → 
                 Defaults Application → Validation → 
                 Transform to Internal Format
```

## Implementation Components

### 1. Type Inference Engine

#### Design
```go
// internal/config/v2/inference.go
package v2

type TypeInferencer struct {
    rules []InferenceRule
}

type InferenceRule interface {
    Matches(config map[string]interface{}) bool
    InferType() string
    Priority() int
}

// Example implementation
type BucketInferenceRule struct{}

func (r *BucketInferenceRule) Matches(config map[string]interface{}) bool {
    _, hasBucket := config["bucket"]
    _, hasPath := config["path"]
    return hasBucket || hasPath
}

func (r *BucketInferenceRule) InferType() string {
    return "storage"
}
```

#### Inference Rules

**Source Type Inference:**
```yaml
# Input
source:
  bucket: "stellar-data"
  
# Inferred
source:
  type: "BufferedStorageSourceAdapter"
  config:
    bucket_name: "stellar-data"
```

**Processor Type Inference:**
```yaml
# Input
process:
  - payment_filter: { min: 100 }
  
# Inferred
processors:
  - type: "FilterPayments"
    config:
      min_amount: "100"
```

### 2. Configuration Aliases

#### Alias Definitions
```yaml
# internal/config/v2/aliases.yaml
version: 2
aliases:
  # Source aliases
  sources:
    # File/Storage sources
    file: FSBufferedStorageSourceAdapter
    fs: FSBufferedStorageSourceAdapter
    local: FSBufferedStorageSourceAdapter
    s3: S3BufferedStorageSourceAdapter
    aws: S3BufferedStorageSourceAdapter
    gcs: BufferedStorageSourceAdapter
    gcp: BufferedStorageSourceAdapter
    google: BufferedStorageSourceAdapter
    bucket: BufferedStorageSourceAdapter
    storage: BufferedStorageSourceAdapter
    
    # Stellar sources
    stellar: CaptiveCoreInboundAdapter
    stellar_core: CaptiveCoreInboundAdapter
    captive_core: CaptiveCoreInboundAdapter
    core: CaptiveCoreInboundAdapter
    
    # RPC/API sources
    rpc: RPCSourceAdapter
    stellar_rpc: RPCSourceAdapter
    soroban: SorobanSourceAdapter
    soroban_rpc: SorobanSourceAdapter
    
  # Processor aliases  
  processors:
    # Account processors
    account_create: CreateAccount
    create_account: CreateAccount
    account_data: AccountData
    account_info: ProcessAccountDataFull
    account_data_full: ProcessAccountDataFull
    account_tx: AccountTransaction
    account_transaction: AccountTransaction
    account_filter: AccountDataFilter
    account_data_filter: AccountDataFilter
    account_year: AccountYearAnalytics
    account_analytics: AccountYearAnalytics
    account_effect: AccountEffect
    account_effects: AccountEffect
    
    # Payment processors
    payments: FilterPayments
    payment_filter: FilterPayments
    filter_payments: FilterPayments
    payment_transform: TransformToAppPayment
    transform_payment: TransformToAppPayment
    app_payment: TransformToAppPayment
    
    # Trade/Market processors
    trades: TransformToAppTrade
    trade_transform: TransformToAppTrade
    app_trade: TransformToAppTrade
    market_metrics: MarketMetricsProcessor
    market_stats: MarketMetricsProcessor
    market_cap: TransformToMarketCapProcessor
    market_analytics: TransformToMarketAnalytics
    
    # Asset processors
    trustline: TransformToAppTrustline
    trustline_transform: TransformToAppTrustline
    app_trustline: TransformToAppTrustline
    asset_stats: TransformToAssetStats
    token_price: TransformToTokenPrice
    ticker_asset: TransformToTickerAsset
    ticker_orderbook: TransformToTickerOrderbook
    asset_enrichment: AssetEnrichment
    enrich_asset: AssetEnrichment
    
    # Contract processors
    contract_data: ContractData
    contracts: ContractData
    contract_ledger: ContractLedgerReader
    contract_invocation: ContractInvocation
    invocations: ContractInvocation
    contract_creation: ContractCreation
    contract_event: ContractEvent
    contract_events: ContractEvent
    contract_filter: ContractFilter
    filter_contracts: ContractFilter
    filtered_invocation: FilteredContractInvocation
    contract_extractor: ContractInvocationExtractor
    
    # Ledger processors
    ledger: LedgerReader
    ledger_reader: LedgerReader
    latest_ledger: LatestLedger
    ledger_changes: LedgerChanges
    ledger_rpc: LatestLedgerRPC
    latest_ledger_rpc: LatestLedgerRPC
    
    # Event processors
    events: FilterEventsProcessor
    event_filter: FilterEventsProcessor
    filter_events: FilterEventsProcessor
    events_rpc: GetEventsRPC
    get_events: GetEventsRPC
    
    # Metrics/Analytics processors
    metrics: TransformToAppMetrics
    app_metrics: TransformToAppMetrics
    
    # App account processor
    app_account: TransformToAppAccount
    transform_account: TransformToAppAccount
    
    # Utility processors
    blank: BlankProcessor
    passthrough: BlankProcessor
    stdout: StdoutSink
    debug: StdoutSink
    
    # DEX processors
    soroswap: Soroswap
    soroswap_router: SoroswapRouter
    kale: Kale
    
  # Consumer aliases
  consumers:
    # File/Archive consumers
    parquet: SaveToParquet
    parquet_archive: SaveToParquet
    excel: SaveToExcel
    xlsx: SaveToExcel
    
    # Database consumers
    postgres: SaveToPostgreSQL
    postgresql: SaveToPostgreSQL
    pg: SaveToPostgreSQL
    mongo: SaveToMongoDB
    mongodb: SaveToMongoDB
    duckdb: SaveToDuckDB
    duck: SaveToDuckDB
    clickhouse: SaveToClickHouse
    click: SaveToClickHouse
    timescale: SaveToTimescaleDB
    timescaledb: SaveToTimescaleDB
    
    # Cache/Memory consumers
    redis: SaveToRedis
    cache: SaveToRedis
    redis_orderbook: SaveToRedisOrderbook
    orderbook_cache: SaveToRedisOrderbook
    
    # Messaging consumers
    zeromq: SaveToZeroMQ
    zmq: SaveToZeroMQ
    websocket: SaveToWebSocket
    ws: SaveToWebSocket
    
    # Cloud storage consumers
    gcs: SaveToGCS
    google_storage: SaveToGCS
    
    # Specialized database consumers
    contract_duckdb: SaveContractToDuckDB
    asset_postgres: SaveAssetToPostgreSQL
    asset_enrichment_db: SaveAssetEnrichment
    payment_postgres: SavePaymentToPostgreSQL
    payment_redis: SavePaymentsToRedis
    latest_ledger_redis: SaveLatestLedgerToRedis
    latest_ledger_excel: SaveLatestLedgerToExcel
    account_postgres: SaveAccountDataToPostgreSQL
    account_duckdb: SaveAccountDataToDuckDB
    contract_events_postgres: SaveContractEventsToPostgreSQL
    contract_invocations_postgres: SaveContractInvocationsToPostgreSQL
    extracted_invocations_postgres: SaveExtractedContractInvocationsToPostgreSQL
    contract_data_postgres: SaveContractDataToPostgreSQL
    soroswap_postgres: SaveSoroswapToPostgreSQL
    soroswap_pairs_duckdb: SaveSoroswapPairsToDuckDB
    soroswap_router_duckdb: SaveSoroswapRouterToDuckDB
    market_analytics: SaveToMarketAnalytics
    
    # Notification/Alert consumers
    notification: NotificationDispatcher
    notify: NotificationDispatcher
    alerts: NotificationDispatcher
    
    # AI/ML consumers
    claude: AnthropicClaude
    anthropic: AnthropicClaude
    
    # Debug consumers
    stdout: StdoutConsumer
    console: StdoutConsumer
    debug: DebugLogger
    logger: DebugLogger
    
  # Field aliases (common across components)
  fields:
    # Storage fields
    bucket: bucket_name
    path: path_prefix
    prefix: path_prefix
    
    # Performance fields
    workers: num_workers
    threads: num_workers
    buffer: buffer_size
    batch: batch_size
    
    # Amount fields
    min: min_amount
    max: max_amount
    
    # Compression fields
    compress: compression
    compression_type: compression
    
    # Partitioning fields
    partition: partition_by
    partitioning: partition_by
    
    # Connection fields
    connection: connection_string
    conn: connection_string
    db: connection_string
    database: connection_string
    
    # Network fields
    net: network
    
    # Time fields
    ttl: time_to_live
    expire: time_to_live
    rotation: rotation_interval_minutes
    interval: rotation_interval_minutes
```

### 3. Smart Defaults Engine

#### Network-Aware Defaults
```go
// internal/config/v2/defaults.go
var NetworkDefaults = map[string]NetworkConfig{
    "mainnet": {
        Passphrase: "Public Global Stellar Network ; September 2015",
        HistoryURLs: []string{
            "https://history.stellar.org/prd/core-live-001",
            "https://history.stellar.org/prd/core-live-002",
        },
        DefaultStartLedger: 1,
    },
    "testnet": {
        Passphrase: "Test SDF Network ; September 2015",
        HistoryURLs: []string{
            "https://history.stellar.org/prd/core-testnet-001",
        },
        DefaultStartLedger: 1,
    },
}
```

#### Component-Specific Defaults
```go
var ComponentDefaults = map[string]map[string]interface{}{
    "BufferedStorageSourceAdapter": {
        "num_workers": 20,
        "retry_limit": 3,
        "retry_wait": 5,
        "ledgers_per_file": 1,
        "files_per_partition": 64000,
    },
    "SaveToParquet": {
        "compression": "snappy",
        "buffer_size": 10000,
        "max_file_size_mb": 128,
        "rotation_interval_minutes": 60,
        "schema_evolution": true,
        "include_metadata": true,
    },
    "SaveToPostgreSQL": {
        "batch_size": 1000,
        "connection_pool_size": 10,
        "retry_on_conflict": true,
    },
}
```

### 4. Configuration Loader

#### Main Loader Implementation
```go
// internal/config/v2/loader.go
type ConfigLoader struct {
    inferencer    *TypeInferencer
    defaultEngine *DefaultsEngine
    validator     *Validator
    transformer   *Transformer
}

func (l *ConfigLoader) Load(path string) (*Config, error) {
    // 1. Read raw YAML
    raw, err := l.readYAML(path)
    if err != nil {
        return nil, fmt.Errorf("reading config: %w", err)
    }
    
    // 2. Detect format version
    if isLegacyFormat(raw) {
        return l.loadLegacy(raw)
    }
    
    // 3. Expand environment variables
    expanded := l.expandEnvVars(raw)
    
    // 4. Apply aliases
    aliased := l.applyAliases(expanded)
    
    // 5. Infer types
    typed := l.inferTypes(aliased)
    
    // 6. Apply smart defaults
    defaulted := l.applyDefaults(typed)
    
    // 7. Validate configuration
    if err := l.validator.Validate(defaulted); err != nil {
        return nil, l.enhanceError(err, raw)
    }
    
    // 8. Transform to internal format
    return l.transformer.Transform(defaulted)
}
```

### 5. Enhanced Validation

#### Validation with Helpful Errors
```go
// internal/config/v2/validator.go
type ValidationError struct {
    Field      string
    Value      interface{}
    Problem    string
    Suggestion string
    ValidValues []string
}

func (e ValidationError) Error() string {
    var msg strings.Builder
    msg.WriteString(fmt.Sprintf("\nError in configuration:\n\n"))
    msg.WriteString(fmt.Sprintf("  %s: %v  # <-- %s\n\n", e.Field, e.Value, e.Problem))
    
    if e.Suggestion != "" {
        msg.WriteString(fmt.Sprintf("Did you mean '%s'?\n\n", e.Suggestion))
    }
    
    if len(e.ValidValues) > 0 {
        msg.WriteString("Valid options:\n")
        for _, v := range e.ValidValues {
            msg.WriteString(fmt.Sprintf("  - %s\n", v))
        }
    }
    
    return msg.String()
}
```

### 6. Environment Variable Support

#### Built-in Expansion
```go
func expandEnvVars(config interface{}) interface{} {
    switch v := config.(type) {
    case string:
        return os.ExpandEnv(v)
    case map[string]interface{}:
        result := make(map[string]interface{})
        for k, val := range v {
            result[k] = expandEnvVars(val)
        }
        return result
    case []interface{}:
        result := make([]interface{}, len(v))
        for i, val := range v {
            result[i] = expandEnvVars(val)
        }
        return result
    default:
        return v
    }
}
```

### 7. Alias Usage Examples

With the comprehensive alias system, users have many intuitive options:

#### Payment Processing Pipeline
```yaml
# Using various aliases - all equivalent
process:
  - payments          # or payment_filter, filter_payments
  - app_payment       # or payment_transform, transform_payment

save_to:
  - postgres          # or postgresql, pg
  - cache             # or redis
```

#### Contract Analytics Pipeline
```yaml
source: stellar       # or stellar_core, captive_core, core

process:
  - contracts         # or contract_data
  - invocations       # or contract_invocation
  - contract_events   # or contract_event

save_to:
  - parquet          # or parquet_archive
  - contract_duckdb  # specialized contract storage
```

#### Market Data Pipeline
```yaml
process:
  - trades           # or trade_transform, app_trade
  - market_metrics   # or market_stats
  - market_cap

save_to:
  - clickhouse       # or click
  - ws               # or websocket
```

#### Account Monitoring
```yaml
process:
  - account_data     # or account_info
  - account_tx       # or account_transaction
  - account_effects  # or account_effect

save_to:
  - account_postgres # specialized account storage
  - notification     # or notify, alerts
```

## Configuration Examples

### Example 1: Minimal Configuration

#### Before (Legacy)
```yaml
pipelines:
  MyPipeline:
    source:
      type: BufferedStorageSourceAdapter
      config:
        bucket_name: "stellar-mainnet-data"
        network: "mainnet"
        num_workers: 20
        retry_limit: 3
        retry_wait: 5
        ledgers_per_file: 1
        files_per_partition: 64000
    processors:
      - type: ContractData
        config:
          network_passphrase: "Public Global Stellar Network ; September 2015"
    consumers:
      - type: SaveToParquet
        config:
          storage_type: "GCS"
          bucket_name: "output-bucket"
          path_prefix: "contracts"
          compression: "snappy"
          buffer_size: 10000
          schema_evolution: true
          include_metadata: true
```

#### After (Simplified)
```yaml
source:
  bucket: "stellar-mainnet-data"
  network: mainnet

process: contract_data

save_to:
  parquet: "gs://output-bucket/contracts"
```

### Example 2: Multi-Stage Pipeline

#### Before (Legacy)
```yaml
pipelines:
  PaymentAnalytics:
    source:
      type: CaptiveCoreInboundAdapter
      config:
        network: "testnet"
        history_archive_urls:
          - "https://history.stellar.org/prd/core-testnet-001"
        binary_path: "/usr/local/bin/stellar-core"
        start_ledger: 1000000
        end_ledger: 2000000
    processors:
      - type: FilterPayments
        config:
          include_failed: false
          min_amount: "100"
      - type: TransformToAppPayment
        config:
          include_muxed_accounts: true
      - type: TransformToAppMetrics
        config:
          aggregation_window: "1h"
    consumers:
      - type: SaveToPostgreSQL
        config:
          connection_string: "${DATABASE_URL}"
          table_name: "payment_metrics"
          batch_size: 1000
      - type: SaveToRedis
        config:
          address: "redis://localhost:6379"
          key_prefix: "payments:"
          ttl: 86400
```

#### After (Simplified)
```yaml
source:
  stellar: testnet
  ledgers: 1000000-2000000

process:
  - payment_filter:
      min: 100
  - payment_transform:
      include_muxed: true
  - metrics:
      window: 1h

save_to:
  - postgres: payment_metrics
  - redis:
      prefix: "payments:"
      ttl: 1d
```

### Example 3: Environment-Based Configuration

```yaml
# Single file works for all environments
source:
  bucket: "${STELLAR_BUCKET}"
  network: "${NETWORK:-testnet}"

process:
  - contract_data
  - filter:
      contract_ids: "${FILTER_CONTRACTS}"

save_to:
  postgres: "${DATABASE_URL}"

# Development: NETWORK=testnet STELLAR_BUCKET=test-data
# Production: NETWORK=mainnet STELLAR_BUCKET=prod-data
```

## Development Timeline

### Week 1: Core Infrastructure
- [ ] Create v2 config package structure
- [ ] Implement format detection
- [ ] Build compatibility layer
- [ ] Set up testing framework

### Week 2: Type System
- [ ] Implement type inference engine
- [ ] Create alias mapping system
- [ ] Build inference rules for all components
- [ ] Add comprehensive tests

### Week 3: Defaults and Validation
- [ ] Implement smart defaults engine
- [ ] Create network-aware defaults
- [ ] Build enhanced validator
- [ ] Implement helpful error messages

### Week 4: Loader and Transformer
- [ ] Complete configuration loader
- [ ] Implement environment variable expansion
- [ ] Build config transformer
- [ ] Integration testing

### Week 5: CLI Commands
- [ ] `flowctl config validate` command
- [ ] `flowctl config explain` command
- [ ] `flowctl config upgrade` command
- [ ] `flowctl config diff` command

### Week 6: Migration Tools
- [ ] Config analyzer tool
- [ ] Automated upgrade tool
- [ ] Migration guide
- [ ] Example repository

### Week 7-8: Testing and Documentation
- [ ] Comprehensive test coverage
- [ ] Performance testing
- [ ] Documentation update
- [ ] Video tutorials

## Testing Strategy

### 1. Unit Tests
```go
func TestTypeInference(t *testing.T) {
    tests := []struct {
        name     string
        input    map[string]interface{}
        expected string
    }{
        {
            name:     "infers storage from bucket",
            input:    map[string]interface{}{"bucket": "test"},
            expected: "BufferedStorageSourceAdapter",
        },
        {
            name:     "infers stellar from network",
            input:    map[string]interface{}{"stellar": "mainnet"},
            expected: "CaptiveCoreInboundAdapter",
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := inferSourceType(tt.input)
            assert.Equal(t, tt.expected, result)
        })
    }
}
```

### 2. Configuration Equivalence Tests
```go
func TestConfigEquivalence(t *testing.T) {
    legacy := loadConfig("testdata/legacy/payment_pipeline.yaml")
    simple := loadConfig("testdata/v2/payment_pipeline.yaml")
    
    // Ensure both produce identical pipeline configuration
    assert.Equal(t, legacy.Source.Type, simple.Source.Type)
    assert.Equal(t, legacy.Source.Config, simple.Source.Config)
    assert.Equal(t, len(legacy.Processors), len(simple.Processors))
}
```

### 3. Backward Compatibility Tests
- Test all existing example configurations
- Ensure zero breaking changes
- Verify warning messages for deprecated patterns

### 4. Error Message Tests
```go
func TestErrorMessages(t *testing.T) {
    config := `
source:
  buckt: "test"  # Typo
`
    _, err := LoadConfig(config)
    assert.Contains(t, err.Error(), "Did you mean 'bucket'?")
}
```

## Migration Plan

### Phase 1: Soft Launch (Week 1-2)
1. Release as experimental feature
2. Document both formats
3. Gather early feedback
4. No deprecation notices

### Phase 2: Recommended (Week 3-4)
1. Update documentation to prefer simplified format
2. Add migration suggestions to CLI
3. Create migration guide
4. Update example repository

### Phase 3: Transition (Month 2+)
1. Show info messages for legacy configs
2. Provide automated upgrade tool
3. Update all internal examples
4. Community outreach

### Migration Tools

#### Config Analyzer
```bash
$ flowctl config analyze ./configs/
Analyzing 23 configuration files...

Summary:
- 20 configs can be simplified (average 75% reduction)
- 3 configs use advanced features (manual review recommended)

Potential improvements:
- Remove 450 lines of boilerplate
- Eliminate 89 explicit type declarations
- Simplify 34 nested configurations

Run 'flowctl config upgrade --dry-run ./configs/' to preview changes
```

#### Automated Upgrader
```bash
$ flowctl config upgrade pipeline.yaml
Upgrading configuration...

Original: 68 lines
Simplified: 12 lines
Reduction: 82%

✅ Backup saved to: pipeline.yaml.backup
✅ Configuration upgraded successfully

Review changes:
$ diff pipeline.yaml.backup pipeline.yaml
```

## Success Metrics

### Adoption Metrics
- 50% of new configs use simplified format within 3 months
- 80% within 6 months
- <5% of users report issues

### Developer Experience
- Time to first pipeline: <5 minutes (from >15 minutes)
- Configuration errors: 50% reduction
- Support tickets: 30% reduction in config-related issues

### Technical Metrics
- Zero breaking changes
- Config parsing performance: <10ms overhead
- 100% backward compatibility

## Risk Mitigation

### Risk: User Confusion
**Mitigation:**
- Clear documentation showing both formats
- Automatic format detection
- No forced migration

### Risk: Hidden Complexity
**Mitigation:**
- `flowctl config explain` command
- Verbose mode showing inferred values
- Clear documentation of defaults

### Risk: Performance Impact
**Mitigation:**
- Lazy evaluation of defaults
- Caching of inference rules
- Benchmark testing

## Future Considerations

This implementation prepares for:
- Phase 3: Protocol handlers (`stellar://mainnet`)
- Phase 4: Pipeline DSL (`source | process | sink`)
- Component marketplace integration
- Visual pipeline builder

## Conclusion

Phase 2 represents a crucial step in making CDP Pipeline Workflow more accessible while maintaining its power. By focusing on developer experience through simplified configuration, we can dramatically reduce the barrier to entry while keeping all advanced features available for those who need them.