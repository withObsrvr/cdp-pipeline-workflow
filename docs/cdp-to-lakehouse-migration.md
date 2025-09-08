# CDP Pipeline to Lakehouse Migration Guide

## Overview

This document describes how the existing CDP Pipeline Workflow will be adapted to work with the new Obsrvr Lake architecture, transforming from direct database writes to lakehouse table population.

## Architecture Transformation

### Current CDP Architecture
```
Source → Processors → Consumers (PostgreSQL/ClickHouse/Redis)
```

### New Lakehouse Architecture
```
Galexie → Bronze → CDP Processors → Silver/Gold Iceberg Tables
                         ↓
                   Obsrvr Actions
```

## Processor Migration Strategy

### 1. Source Adapter Changes

**Current: BufferedStorageSourceAdapter**
```yaml
source:
  type: BufferedStorageSourceAdapter
  config:
    bucket_name: "stellar-ledger-data"
    start_ledger: 1000000
```

**New: IcebergSourceAdapter**
```yaml
source:
  type: IcebergSourceAdapter
  config:
    catalog: "nessie"
    table: "bronze.xdr_files"
    start_timestamp: "2024-01-01"
    streaming: true  # Enable CDC
```

### 2. Processor Modifications

Processors will output to Iceberg tables instead of forwarding to next processor:

```go
// Old approach - pass through messaging
func (p *TokenTransferProcessor) Process(ctx context.Context, msg Message) error {
    transfers := p.extractTransfers(msg)
    
    // Forward to next processor
    for _, processor := range p.processors {
        processor.Process(ctx, Message{Payload: transfers})
    }
}

// New approach - write to Iceberg
func (p *TokenTransferProcessor) Process(ctx context.Context, msg Message) error {
    transfers := p.extractTransfers(msg)
    
    // Write to Iceberg table
    writer := p.icebergWriter("silver.token_transfers")
    return writer.AppendRows(transfers)
}
```

### 3. Consumer Transformation

Consumers become Iceberg table writers or Action triggers:

```yaml
# Old consumer
consumers:
  - type: SaveToPostgreSQL
    config:
      table: "transactions"

# New consumer  
consumers:
  - type: IcebergTableWriter
    config:
      table: "silver.transactions"
      format: "parquet"
      compression: "snappy"
  
  - type: ActionTrigger
    config:
      event_type: "transaction_processed"
      condition: "amount > 10000"
```

## Processor Mapping to Lakehouse Tables

| Current Processor | Target Table | Layer | Schema |
|------------------|--------------|-------|---------|
| TransactionXDRExtractor | silver.transactions | Silver | hash, source, timestamp, operations |
| TokenTransferProcessor | silver.token_transfers | Silver | account, asset, amount, timestamp |
| ContractEventProcessor | silver.contract_events | Silver | contract_id, event_type, data |
| AccountDataProcessor | silver.account_states | Silver | account_id, balance, sequence |
| WalletBackendProcessor | gold.wallet_activities | Gold | Pre-joined for wallet queries |
| DEXTradeProcessor | silver.dex_trades | Silver | pair, price, volume, timestamp |
| MarketMetricsProcessor | gold.market_analytics | Gold | Aggregated metrics |

## New CDP Components

### 1. IcebergWriter Base Class

```go
type IcebergWriter struct {
    catalog    Catalog
    table      string
    batchSize  int
    schema     *arrow.Schema
}

func (w *IcebergWriter) AppendRows(rows []interface{}) error {
    // Convert to Arrow format
    batch := w.convertToArrow(rows)
    
    // Write to Iceberg
    return w.catalog.AppendToTable(w.table, batch)
}
```

### 2. Streaming Processor Adapter

```go
type StreamingProcessor struct {
    BaseProcessor
    materializedView string
    refreshInterval  time.Duration
}

func (p *StreamingProcessor) Process(ctx context.Context, msg Message) error {
    // Write to streaming engine instead of batch
    return p.streamingEngine.Insert(p.materializedView, msg.Payload)
}
```

### 3. Schema Registry Integration

```go
type SchemaEvolutionProcessor struct {
    registry SchemaRegistry
}

func (p *SchemaEvolutionProcessor) Process(ctx context.Context, msg Message) error {
    // Check if schema needs evolution
    currentSchema := p.registry.GetSchema(p.table)
    requiredSchema := p.inferSchema(msg.Payload)
    
    if !currentSchema.Compatible(requiredSchema) {
        p.registry.EvolveSchema(p.table, requiredSchema)
    }
    
    return p.BaseProcessor.Process(ctx, msg)
}
```

## Pipeline Configuration Examples

### 1. Basic Silver Layer Pipeline

```yaml
pipelines:
  TransactionProcessor:
    source:
      type: IcebergSourceAdapter
      config:
        table: "bronze.xdr_files"
        format: "streaming"  # CDC mode
    
    processors:
      - type: XDRDecoder
        config:
          output_format: "structured"
      
      - type: TransactionExtractor
        config:
          include_failed: false
    
    consumers:
      - type: IcebergTableWriter
        config:
          catalog: "nessie"
          table: "silver.transactions"
          write_mode: "append"
          partition_by: ["date(created_at)"]
```

### 2. Gold Layer Aggregation Pipeline

```yaml
pipelines:
  WalletAnalytics:
    source:
      type: IcebergSourceAdapter
      config:
        table: "silver.token_transfers"
        filter: "timestamp > current_date - 7"
    
    processors:
      - type: AccountAggregator
        config:
          group_by: ["account", "asset_code"]
          aggregations:
            - "sum(amount) as total_volume"
            - "count(*) as transfer_count"
    
    consumers:
      - type: IcebergTableWriter
        config:
          table: "gold.wallet_metrics"
          write_mode: "overwrite"  # Replace partition
          partition_by: ["report_date"]
```

### 3. Real-time Streaming Pipeline

```yaml
pipelines:
  LargePaymentMonitor:
    source:
      type: MaterializeSourceAdapter
      config:
        view: "silver.payments_stream"
        tail: true  # Continuous updates
    
    processors:
      - type: ThresholdFilter
        config:
          field: "amount"
          operator: ">"
          value: 10000
    
    consumers:
      - type: ActionTrigger
        config:
          action: "large_payment_alert"
          include_fields: ["account", "amount", "asset"]
```

## Migration Utilities

### 1. Historical Data Migrator

```go
// Migrate existing PostgreSQL data to Iceberg
func MigrateToLakehouse(sourceDB *sql.DB, catalog Catalog) error {
    tables := []string{"transactions", "state_changes", "accounts"}
    
    for _, table := range tables {
        rows, err := sourceDB.Query(fmt.Sprintf("SELECT * FROM %s", table))
        if err != nil {
            return err
        }
        
        writer := NewIcebergWriter(catalog, "silver."+table)
        return writer.ImportFromSQL(rows)
    }
}
```

### 2. Schema Converter

```python
# Convert PostgreSQL schema to Iceberg
def convert_schema(pg_schema):
    iceberg_schema = Schema()
    
    type_mapping = {
        'varchar': StringType.get(),
        'bigint': LongType.get(),
        'timestamp': TimestampType.with_timezone(),
        'jsonb': StringType.get(),  # Store as JSON string
        'numeric': DecimalType.of(38, 18)
    }
    
    for column in pg_schema.columns:
        iceberg_type = type_mapping.get(column.type, StringType.get())
        iceberg_schema.add_field(column.name, iceberg_type)
    
    return iceberg_schema
```

## Performance Optimizations

### 1. Batch Processing

```go
type BatchedIcebergWriter struct {
    *IcebergWriter
    buffer     []interface{}
    bufferSize int
    ticker     *time.Ticker
}

func (w *BatchedIcebergWriter) Write(record interface{}) error {
    w.buffer = append(w.buffer, record)
    
    if len(w.buffer) >= w.bufferSize {
        return w.flush()
    }
    
    return nil
}
```

### 2. Partition Pruning

```yaml
# Optimize queries with proper partitioning
processors:
  - type: IcebergTableWriter
    config:
      partition_by:
        - "date(timestamp)"     # Daily partitions
        - "bucket(16, account)" # Hash buckets for account queries
      sort_order:
        - "timestamp DESC"
```

### 3. Compaction Strategy

```sql
-- Periodic compaction job
CALL system.rewrite_data_files(
  table => 'silver.transactions',
  strategy => 'SORT',
  sort_order => 'timestamp DESC',
  where => 'date(timestamp) = current_date - 1'
);
```

## Benefits of Lakehouse Approach

1. **Unified Storage**: All data in one place (object storage)
2. **Cost Efficiency**: 95%+ reduction in storage costs
3. **Schema Evolution**: Add fields without breaking pipelines
4. **Time Travel**: Query data as of any point in time
5. **Multi-Engine**: Use different tools for different workloads
6. **ACID Guarantees**: Consistent data even with concurrent writes
7. **Incremental Processing**: Only process new data
8. **Open Format**: No vendor lock-in

## Timeline

### Phase 1 (Weeks 1-2): Infrastructure
- Set up Iceberg catalog
- Deploy object storage
- Create base tables

### Phase 2 (Weeks 3-4): Core Processors
- Migrate TransactionProcessor
- Migrate TokenTransferProcessor
- Test data quality

### Phase 3 (Weeks 5-6): Advanced Processors
- Migrate analytical processors
- Add streaming processors
- Implement Actions integration

### Phase 4 (Weeks 7-8): Optimization
- Add partitioning strategies
- Implement compaction
- Performance tuning

### Phase 5 (Weeks 9-10): Cutover
- Run parallel validation
- Migrate customers
- Decommission old infrastructure