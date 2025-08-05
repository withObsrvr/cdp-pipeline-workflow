# Phase 2: Complete Alias Reference

## Overview

This document provides a complete reference of all available aliases in the simplified configuration system. Users can use any of these aliases interchangeably to create more readable and intuitive pipeline configurations.

## Source Aliases

### Storage Sources
| Alias | Maps To | Description |
|-------|---------|-------------|
| `file`, `fs`, `local` | FSBufferedStorageSourceAdapter | Local filesystem |
| `s3`, `aws` | S3BufferedStorageSourceAdapter | Amazon S3 |
| `gcs`, `gcp`, `google`, `bucket`, `storage` | BufferedStorageSourceAdapter | Google Cloud Storage |

### Stellar Sources
| Alias | Maps To | Description |
|-------|---------|-------------|
| `stellar`, `stellar_core`, `captive_core`, `core` | CaptiveCoreInboundAdapter | Stellar Core connection |
| `rpc`, `stellar_rpc` | RPCSourceAdapter | Stellar RPC endpoint |
| `soroban`, `soroban_rpc` | SorobanSourceAdapter | Soroban RPC endpoint |

## Processor Aliases

### Account Processors
| Alias | Maps To | Description |
|-------|---------|-------------|
| `account_create`, `create_account` | CreateAccount | Account creation events |
| `account_data` | AccountData | Account data changes |
| `account_info`, `account_data_full` | ProcessAccountDataFull | Full account information |
| `account_tx`, `account_transaction` | AccountTransaction | Account transactions |
| `account_filter`, `account_data_filter` | AccountDataFilter | Filter account data |
| `account_year`, `account_analytics` | AccountYearAnalytics | Yearly analytics |
| `account_effect`, `account_effects` | AccountEffect | Account effects |

### Payment Processors
| Alias | Maps To | Description |
|-------|---------|-------------|
| `payments`, `payment_filter`, `filter_payments` | FilterPayments | Filter payment operations |
| `payment_transform`, `transform_payment`, `app_payment` | TransformToAppPayment | Transform to app format |

### Trade/Market Processors
| Alias | Maps To | Description |
|-------|---------|-------------|
| `trades`, `trade_transform`, `app_trade` | TransformToAppTrade | Trade transformations |
| `market_metrics`, `market_stats` | MarketMetricsProcessor | Market statistics |
| `market_cap` | TransformToMarketCapProcessor | Market cap calculations |
| `market_analytics` | TransformToMarketAnalytics | Market analytics |

### Asset Processors
| Alias | Maps To | Description |
|-------|---------|-------------|
| `trustline`, `trustline_transform`, `app_trustline` | TransformToAppTrustline | Trustline operations |
| `asset_stats` | TransformToAssetStats | Asset statistics |
| `token_price` | TransformToTokenPrice | Token pricing |
| `ticker_asset` | TransformToTickerAsset | Ticker asset data |
| `ticker_orderbook` | TransformToTickerOrderbook | Orderbook data |
| `asset_enrichment`, `enrich_asset` | AssetEnrichment | Enrich asset metadata |

### Contract Processors
| Alias | Maps To | Description |
|-------|---------|-------------|
| `contract_data`, `contracts` | ContractData | Contract data changes |
| `contract_ledger` | ContractLedgerReader | Contract ledger reader |
| `contract_invocation`, `invocations` | ContractInvocation | Contract invocations |
| `contract_creation` | ContractCreation | Contract creation events |
| `contract_event`, `contract_events` | ContractEvent | Contract events |
| `contract_filter`, `filter_contracts` | ContractFilter | Filter contracts |
| `filtered_invocation` | FilteredContractInvocation | Filtered invocations |
| `contract_extractor` | ContractInvocationExtractor | Extract invocation data |

### Ledger Processors
| Alias | Maps To | Description |
|-------|---------|-------------|
| `ledger`, `ledger_reader` | LedgerReader | Read ledger data |
| `latest_ledger` | LatestLedger | Latest ledger info |
| `ledger_changes` | LedgerChanges | Ledger changes |
| `ledger_rpc`, `latest_ledger_rpc` | LatestLedgerRPC | Latest ledger via RPC |

### Event Processors
| Alias | Maps To | Description |
|-------|---------|-------------|
| `events`, `event_filter`, `filter_events` | FilterEventsProcessor | Filter events |
| `events_rpc`, `get_events` | GetEventsRPC | Get events via RPC |

### Other Processors
| Alias | Maps To | Description |
|-------|---------|-------------|
| `metrics`, `app_metrics` | TransformToAppMetrics | App metrics |
| `app_account`, `transform_account` | TransformToAppAccount | App account format |
| `blank`, `passthrough` | BlankProcessor | Pass-through processor |
| `stdout`, `debug` | StdoutSink | Debug output |
| `soroswap` | Soroswap | Soroswap DEX |
| `soroswap_router` | SoroswapRouter | Soroswap router |
| `kale` | Kale | Kale DEX |

## Consumer Aliases

### File/Archive Consumers
| Alias | Maps To | Description |
|-------|---------|-------------|
| `parquet`, `parquet_archive` | SaveToParquet | Parquet file storage |
| `excel`, `xlsx` | SaveToExcel | Excel file output |

### Database Consumers
| Alias | Maps To | Description |
|-------|---------|-------------|
| `postgres`, `postgresql`, `pg` | SaveToPostgreSQL | PostgreSQL database |
| `mongo`, `mongodb` | SaveToMongoDB | MongoDB database |
| `duckdb`, `duck` | SaveToDuckDB | DuckDB database |
| `clickhouse`, `click` | SaveToClickHouse | ClickHouse database |
| `timescale`, `timescaledb` | SaveToTimescaleDB | TimescaleDB |

### Cache/Memory Consumers
| Alias | Maps To | Description |
|-------|---------|-------------|
| `redis`, `cache` | SaveToRedis | Redis cache |
| `redis_orderbook`, `orderbook_cache` | SaveToRedisOrderbook | Orderbook cache |

### Messaging Consumers
| Alias | Maps To | Description |
|-------|---------|-------------|
| `zeromq`, `zmq` | SaveToZeroMQ | ZeroMQ messaging |
| `websocket`, `ws` | SaveToWebSocket | WebSocket streaming |

### Cloud Storage Consumers
| Alias | Maps To | Description |
|-------|---------|-------------|
| `gcs`, `google_storage` | SaveToGCS | Google Cloud Storage |

### Specialized Consumers
| Alias | Maps To | Description |
|-------|---------|-------------|
| `contract_duckdb` | SaveContractToDuckDB | Contract-specific DuckDB |
| `asset_postgres` | SaveAssetToPostgreSQL | Asset-specific PostgreSQL |
| `payment_postgres` | SavePaymentToPostgreSQL | Payment-specific PostgreSQL |
| `account_postgres` | SaveAccountDataToPostgreSQL | Account-specific PostgreSQL |
| `market_analytics` | SaveToMarketAnalytics | Market analytics storage |
| `notification`, `notify`, `alerts` | NotificationDispatcher | Send notifications |
| `claude`, `anthropic` | AnthropicClaude | AI/ML processing |
| `stdout`, `console` | StdoutConsumer | Console output |
| `debug`, `logger` | DebugLogger | Debug logging |

## Field Aliases

### Common Field Mappings
| Alias | Maps To | Description |
|-------|---------|-------------|
| `bucket` | bucket_name | Storage bucket name |
| `path`, `prefix` | path_prefix | Path prefix |
| `workers`, `threads` | num_workers | Number of workers |
| `buffer` | buffer_size | Buffer size |
| `batch` | batch_size | Batch size |
| `min` | min_amount | Minimum amount |
| `max` | max_amount | Maximum amount |
| `compress`, `compression_type` | compression | Compression type |
| `partition`, `partitioning` | partition_by | Partitioning strategy |
| `connection`, `conn`, `db`, `database` | connection_string | Database connection |
| `net` | network | Network name |
| `ttl`, `expire` | time_to_live | Time to live |
| `rotation`, `interval` | rotation_interval_minutes | Rotation interval |

## Usage Examples

### Using Multiple Aliases
```yaml
# All of these are equivalent ways to specify PostgreSQL
save_to: postgres
save_to: postgresql  
save_to: pg

# All of these filter payments
process: payments
process: payment_filter
process: filter_payments
```

### Combining Aliases
```yaml
source:
  bucket: "my-data"      # Instead of bucket_name
  workers: 50            # Instead of num_workers
  net: mainnet           # Instead of network

process:
  - contracts            # Instead of ContractData
  - invocations          # Instead of ContractInvocation

save_to:
  - parquet              # Instead of SaveToParquet
  - pg                   # Instead of SaveToPostgreSQL
```

### Specialized vs Generic
```yaml
# Generic consumers
save_to:
  - postgres             # General PostgreSQL storage
  - duckdb               # General DuckDB storage

# Specialized consumers for optimized schemas
save_to:
  - payment_postgres     # Optimized for payments
  - contract_duckdb      # Optimized for contracts
```

## Best Practices

1. **Consistency**: Pick one alias style and use it throughout your config
2. **Clarity**: Choose aliases that make the pipeline's purpose clear
3. **Simplicity**: Use the shortest alias that's still descriptive
4. **Documentation**: Comment when using less common aliases

## Future Additions

The alias system is designed to be extensible. New aliases can be added without breaking existing configurations. Future additions might include:

- Protocol-style aliases: `kafka://`, `s3://`, `postgres://`
- Function-style aliases: `filter(amount > 100)`
- Pipeline composition: `@include common/stellar-base.yaml`