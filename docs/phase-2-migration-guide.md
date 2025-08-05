# Phase 2: Simplified Configuration Migration Guide

## Overview

Phase 2 introduces a dramatically simplified configuration format for CDP Pipeline Workflow that reduces configuration verbosity by 60-80% while maintaining full backward compatibility. This guide will help you migrate from legacy configurations to the new v2 format.

## Key Benefits

- **80% less configuration** - Typical configs reduced from 50+ lines to ~10 lines
- **Zero breaking changes** - All existing configurations continue to work
- **Intuitive syntax** - New users productive in minutes
- **Smart defaults** - Sensible defaults for 90% of use cases
- **Better error messages** - Helpful suggestions when things go wrong

## Quick Start

### Check Your Current Configuration

```bash
# Validate your existing configuration
flowctl config validate config/my-pipeline.yaml

# See what your config does
flowctl config explain config/my-pipeline.yaml
```

### Automatic Migration

```bash
# Preview the simplified version
flowctl config upgrade --dry-run config/my-pipeline.yaml

# Create upgraded configuration
flowctl config upgrade config/my-pipeline.yaml
# Creates: config/my-pipeline.v2.yaml

# Upgrade in place (creates backup)
flowctl config upgrade -o config/my-pipeline.yaml config/my-pipeline.yaml
# Creates backup: config/my-pipeline.yaml.backup
```

## Migration Examples

### Example 1: Basic Storage Pipeline

**Before (Legacy):**
```yaml
pipelines:
  DataIngestion:
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
          rotation_interval_minutes: 60
          schema_evolution: true
          include_metadata: true
```

**After (Simplified):**
```yaml
source:
  bucket: "stellar-mainnet-data"
  network: mainnet

process: contract_data

save_to:
  parquet: "gs://output-bucket/contracts"
```

### Example 2: Payment Processing Pipeline

**Before (Legacy):**
```yaml
pipelines:
  PaymentAnalysis:
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
    consumers:
      - type: SaveToPostgreSQL
        config:
          connection_string: "${DATABASE_URL}"
          table_name: "payments"
          batch_size: 1000
          create_table: true
      - type: SaveToRedis
        config:
          address: "redis://localhost:6379"
          key_prefix: "payments:"
          ttl: 86400
```

**After (Simplified):**
```yaml
source:
  stellar: testnet
  ledgers: 1000000-2000000

process:
  - payment_filter:
      min: 100
  - payment_transform

save_to:
  - postgres: "${DATABASE_URL}"
  - redis:
      prefix: "payments:"
      ttl: 1d
```

### Example 3: Multi-Pipeline Configuration

**Before (Legacy):**
```yaml
pipelines:
  ContractEvents:
    source:
      type: BufferedStorageSourceAdapter
      config:
        bucket_name: "stellar-data"
        network: "mainnet"
    processors:
      - type: ContractEvent
        config: {}
    consumers:
      - type: SaveToClickHouse
        config:
          connection_string: "tcp://localhost:9000"
          
  MarketData:
    source:
      type: BufferedStorageSourceAdapter
      config:
        bucket_name: "stellar-data"
        network: "mainnet"
    processors:
      - type: TransformToAppTrade
        config: {}
      - type: MarketMetricsProcessor
        config: {}
    consumers:
      - type: SaveToWebSocket
        config:
          port: 8080
```

**After (Simplified):**
```yaml
pipelines:
  ContractEvents:
    source:
      bucket: "stellar-data"
      network: mainnet
    process: contract_events
    save_to: clickhouse
    
  MarketData:
    source:
      bucket: "stellar-data"
      network: mainnet
    process:
      - trades
      - market_metrics
    save_to:
      ws:
        port: 8080
```

## Key Changes

### 1. Aliases Everywhere

Instead of verbose type names, use intuitive aliases:

| Legacy Type | v2 Alias |
|-------------|----------|
| `BufferedStorageSourceAdapter` | `bucket`, `storage`, `gcs` |
| `CaptiveCoreInboundAdapter` | `stellar`, `core` |
| `FilterPayments` | `payment_filter`, `payments` |
| `SaveToPostgreSQL` | `postgres`, `pg` |
| `SaveToParquet` | `parquet` |

See full alias reference: `flowctl config examples`

### 2. Smart Field Names

Field names are simplified:

| Legacy Field | v2 Field |
|--------------|----------|
| `bucket_name` | `bucket` |
| `num_workers` | `workers` |
| `min_amount` | `min` |
| `connection_string` | `connection` |
| `batch_size` | `batch` |

### 3. Protocol URLs

Use intuitive protocol-style URLs:

```yaml
# Sources
source: stellar://mainnet
source: s3://my-bucket/path

# Consumers  
save_to: postgres://user:pass@host/db
save_to: gs://bucket/path/to/files
```

### 4. Simplified Structure

For single-pipeline configs, omit the `pipelines` wrapper:

```yaml
# No need for pipelines -> MyPipeline -> ...
source: ...
process: ...
save_to: ...
```

### 5. Smart Defaults

Most fields have sensible defaults and can be omitted:

- `num_workers: 20` (default)
- `retry_limit: 3` (default)
- `compression: snappy` (default for Parquet)
- `create_table: true` (default for databases)
- Network passphrases (inferred from network name)

## Environment Variables

Built-in support with defaults:

```yaml
source:
  bucket: "${DATA_BUCKET}"
  network: "${NETWORK:-testnet}"  # Default to testnet

save_to: "${DATABASE_URL}"
```

## Testing Your Migration

### 1. Validate Both Configs Work

```bash
# Test original
flowctl run config/original.yaml

# Test migrated
flowctl run config/migrated.yaml
```

### 2. Compare Configurations

```bash
# See what each does
flowctl config explain config/original.yaml > original.txt
flowctl config explain config/migrated.yaml > migrated.txt
diff original.txt migrated.txt
```

### 3. Use Equivalence Testing

The v2 loader ensures functional equivalence - your pipelines will behave identically.

## Common Patterns

### Pattern: Multiple Outputs

```yaml
save_to:
  - parquet: "gs://archive/data"
  - postgres: "${DATABASE_URL}"
  - redis:
      ttl: 1h
```

### Pattern: Conditional Processing

```yaml
process:
  - payment_filter:
      min: "${MIN_AMOUNT:-100}"
  - contract_filter:
      contract_ids: "${CONTRACTS}"
```

### Pattern: Override Defaults

```yaml
save_to:
  parquet:
    path: "output"
    compression: zstd      # Override default
    buffer: 50000          # Override default
```

## Troubleshooting

### "Unknown field" Errors

The validator suggests corrections:

```
Error: Unknown field 'buckt'
Did you mean 'bucket'?
```

### Missing Aliases

Check available aliases:
```bash
flowctl config examples
```

### Legacy Features

Some advanced features may require legacy format. The v2 loader handles both seamlessly.

## Best Practices

1. **Start Simple** - Migrate basic configs first
2. **Test Thoroughly** - Ensure pipelines behave identically
3. **Use Validation** - Always validate before running
4. **Keep Backups** - The upgrade tool creates backups automatically
5. **Gradual Migration** - No need to migrate everything at once

## Getting Help

- Run `flowctl config --help` for CLI options
- Check examples: `flowctl config examples`
- Validate configs: `flowctl config validate <file>`
- Explain configs: `flowctl config explain <file>`

## Summary

Phase 2 simplification makes CDP Pipeline Workflow more accessible while maintaining all the power of the legacy format. Migration is optional, gradual, and risk-free. Start with one pipeline and experience the simplicity!