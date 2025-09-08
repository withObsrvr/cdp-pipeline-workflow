# Obsrvr Flow Hosted Data Service Architecture

## Overview

This document describes the architecture for Obsrvr Flow's hosted data service, which allows customers to select processors for indexing Stellar blockchain data and access it through a unified API without managing their own infrastructure.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Hybrid Storage Strategy](#hybrid-storage-strategy)
3. [Dynamic API Gateway](#dynamic-api-gateway)
4. [Customer Configuration](#customer-configuration)
5. [Implementation Guide](#implementation-guide)
6. [API Examples](#api-examples)
7. [Cost Optimization](#cost-optimization)

## Architecture Overview

The hosted data service provides a multi-tenant platform where customers can:
- Select specific processors to index blockchain data
- Access their data through REST APIs and webhooks
- Query both recent (hot) and historical (cold) data seamlessly
- Pay only for the processors and storage they use

```
┌─────────────────┐     ┌──────────────────┐     ┌────────────────┐
│  CDP Pipeline   │────▶│  Hybrid Storage  │────▶│  API Gateway   │
│   Processors    │     │  (Hot + Cold)    │     │   (Dynamic)    │
└─────────────────┘     └──────────────────┘     └────────────────┘
         │                       │                         │
         │                       │                         ▼
         │                       │                 ┌──────────────┐
         │                       │                 │   Customer   │
         └───────────────────────┴────────────────▶│  APIs/SDKs   │
                                                   └──────────────┘
```

## Hybrid Storage Strategy

### Hot Storage (Recent Data)
- **Purpose**: Store last 30-90 days of data for real-time queries
- **Technologies**: PostgreSQL (transactional) or ClickHouse (analytical)
- **Features**: Full indexing, sub-second queries, high availability

### Cold Storage (Historical Data)
- **Purpose**: Store older data cost-effectively
- **Technologies**: S3/GCS with Parquet format
- **Query Engines**: DuckDB (embedded) or AWS Athena (managed)
- **Cost Savings**: ~95% reduction compared to hot storage

### Storage Schema Example

```sql
-- PostgreSQL Hot Storage (Partitioned by time)
CREATE TABLE token_transfers (
    id BIGSERIAL,
    customer_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    account TEXT NOT NULL,
    asset_code TEXT,
    asset_issuer TEXT,
    amount NUMERIC(38,18),
    transaction_hash TEXT,
    metadata JSONB
) PARTITION BY RANGE (timestamp);

-- ClickHouse Alternative
CREATE TABLE token_transfers (
    customer_id String,
    timestamp DateTime64(3),
    account String,
    asset_code String,
    asset_issuer String,
    amount Decimal128(18),
    transaction_hash String,
    metadata String
) ENGINE = MergeTree()
PARTITION BY (customer_id, toYYYYMM(timestamp))
ORDER BY (customer_id, timestamp, account);
```

### Archival Process

```go
// Automated daily archival to cold storage
func ArchiveToParquet(cutoffDate time.Time) error {
    // 1. Query data older than cutoff from hot storage
    // 2. Write to Parquet files in S3
    // 3. Organize by: s3://bucket/customer_id=X/year=Y/month=M/data.parquet
    // 4. Delete from hot storage after verification
}
```

## Dynamic API Gateway

### Route Mapping

Each processor type exposes specific API endpoints:

| Processor | API Endpoints |
|-----------|--------------|
| TokenTransferProcessor | `/token-transfers`<br>`/accounts/{account}/token-balances` |
| ContractEventProcessor | `/contract-events`<br>`/contracts/{contract_id}/events` |
| AccountBalanceProcessor | `/accounts/{account}/balance-history`<br>`/accounts/{account}/current-balance` |
| DEXTradeProcessor | `/dex/trades`<br>`/dex/volume`<br>`/dex/pairs/{pair}/stats` |
| ContractDataProcessor | `/contracts/{contract_id}/data`<br>`/contracts/{contract_id}/storage` |

### Dynamic Route Generation

```go
// Routes are generated based on enabled processors
func GenerateCustomerRoutes(customerID string, config CustomerConfig) {
    baseURL := fmt.Sprintf("/api/v1/%s", customerID)
    
    for _, processor := range config.EnabledProcessors {
        routes := ProcessorRouteMap[processor.Type]
        for _, route := range routes {
            RegisterRoute(baseURL + route.Path, route.Handler)
        }
    }
}
```

### Authentication & Rate Limiting

```go
// Middleware stack for each customer
middleware := []Middleware{
    AuthenticateAPIKey(customerID),
    RateLimit(customer.RateLimitPerMinute),
    UsageTracking(customerID),
    ResponseCache(customer.CacheTTL),
}
```

## Customer Configuration

### Configuration Schema

```yaml
customer:
  id: "abc123"
  name: "Acme Wallet"
  
  # Selected processors and their configurations
  processors:
    - type: "TokenTransferProcessor"
      enabled: true
      config:
        track_memos: true
        include_failed_txs: false
    
    - type: "ContractEventProcessor"  
      enabled: true
      config:
        contracts: ["CCXYZ...", "CCABC..."]
        event_types: ["transfer", "mint", "burn"]
    
    - type: "AccountBalanceProcessor"
      enabled: true
      config:
        track_trustlines: true
        include_claimable_balances: true
  
  # Storage configuration
  storage:
    hot_retention_days: 90
    cold_retention_years: 2
    archive_schedule: "0 2 * * *"  # Daily at 2 AM
  
  # API configuration  
  api:
    rate_limit_per_minute: 1000
    allowed_ips: ["1.2.3.4/32"]
    webhook_url: "https://customer.com/webhook"
    webhook_events: ["token_transfer", "large_payment"]
```

### Processor Dependencies

Some processors may depend on others:

```yaml
processor_dependencies:
  DEXVolumeProcessor:
    requires: ["DEXTradeProcessor"]
  TokenBalanceProcessor:
    requires: ["TokenTransferProcessor"]
```

## Implementation Guide

### 1. Customer Onboarding Flow

```typescript
// 1. Customer selects processors through UI
const selectedProcessors = [
  { type: "TokenTransferProcessor", config: {...} },
  { type: "ContractEventProcessor", config: {...} }
];

// 2. System validates configuration
const validation = validateProcessorConfig(selectedProcessors);

// 3. Generate customer configuration
const customerConfig = {
  id: generateCustomerID(),
  processors: selectedProcessors,
  storage: defaultStorageConfig,
  api: generateAPIConfig()
};

// 4. Deploy pipeline
await deployCustomerPipeline(customerConfig);

// 5. Return API credentials
return {
  apiKey: generateAPIKey(),
  endpoints: generateEndpointList(customerConfig),
  documentation: generateOpenAPISpec(customerConfig)
};
```

### 2. Query Router Implementation

```go
type QueryRouter struct {
    hotStorage  StorageBackend
    coldStorage StorageBackend
    cache       CacheBackend
}

func (qr *QueryRouter) Query(params QueryParams) (*Results, error) {
    // Check cache first
    if cached := qr.cache.Get(params.Hash()); cached != nil {
        return cached, nil
    }
    
    // Determine storage based on time range
    results := &Results{}
    
    if params.IncludesRecent() {
        hotResults, err := qr.hotStorage.Query(params)
        results.Merge(hotResults)
    }
    
    if params.IncludesHistorical() {
        coldResults, err := qr.coldStorage.Query(params)
        results.Merge(coldResults)
    }
    
    // Cache results
    qr.cache.Set(params.Hash(), results, params.CacheTTL())
    
    return results, nil
}
```

### 3. Webhook Dispatcher

```go
type WebhookDispatcher struct {
    queue    MessageQueue
    clients  map[string]WebhookClient
}

func (wd *WebhookDispatcher) Process(event Event) error {
    // Find customers subscribed to this event type
    subscribers := wd.findSubscribers(event.Type)
    
    for _, customerID := range subscribers {
        webhook := WebhookPayload{
            EventType: event.Type,
            Data:      event.Data,
            Timestamp: event.Timestamp,
            Signature: wd.sign(event, customerID),
        }
        
        // Queue for reliable delivery
        wd.queue.Publish(customerID, webhook)
    }
    
    return nil
}
```

## API Examples

### Discovery Endpoint

```bash
GET /api/v1/{customer_id}/
```

Response:
```json
{
  "customer_id": "abc123",
  "available_endpoints": [
    {
      "path": "/api/v1/abc123/token-transfers",
      "method": "GET",
      "description": "Query token transfers",
      "parameters": ["account", "asset_code", "from", "to", "limit"]
    },
    {
      "path": "/api/v1/abc123/accounts/{account}/balance-history",
      "method": "GET",
      "description": "Get historical balance snapshots"
    }
  ],
  "rate_limit": {
    "requests_per_minute": 1000,
    "concurrent_requests": 50
  },
  "documentation": "https://api.obsrvr.io/api/v1/abc123/docs"
}
```

### Token Transfers Query

```bash
GET /api/v1/abc123/token-transfers?account=GDEXAMPLE&from=2024-01-01&limit=100
Authorization: Bearer YOUR_API_KEY
```

Response:
```json
{
  "data": [
    {
      "timestamp": "2024-01-15T10:30:00Z",
      "account": "GDEXAMPLE...",
      "asset_code": "USDC",
      "asset_issuer": "GA5ZSEJYB...",
      "amount": "100.50",
      "direction": "received",
      "counterparty": "GCOUNTERPARTY...",
      "transaction_hash": "abc123...",
      "memo": "Payment for invoice #1234"
    }
  ],
  "metadata": {
    "total_results": 150,
    "returned_results": 100,
    "query_time_ms": 45,
    "data_sources": ["hot"],
    "next_cursor": "eyJvZmZzZXQiOjEwMH0="
  }
}
```

### Webhook Payload Example

```json
{
  "event_id": "evt_123456",
  "event_type": "large_payment",
  "timestamp": "2024-01-15T10:30:00Z",
  "data": {
    "account": "GDEXAMPLE...",
    "amount": "10000.00",
    "asset_code": "USDC",
    "transaction_hash": "def456...",
    "threshold_exceeded": "10000"
  },
  "signature": "sha256=abcdef..."
}
```

## Cost Optimization

### Storage Cost Comparison

| Storage Type | Cost per GB/month | Query Cost | Best For |
|--------------|-------------------|------------|----------|
| PostgreSQL (Hot) | $0.50-1.00 | Included | Recent data, real-time queries |
| ClickHouse (Hot) | $0.30-0.60 | Included | Analytics, aggregations |
| S3 + DuckDB (Cold) | $0.023 | $0.004/1000 queries | Historical data |
| S3 + Athena (Cold) | $0.023 | $5/TB scanned | Large-scale analytics |

### Optimization Strategies

1. **Automatic Tiering**: Move data to cold storage based on age and access patterns
2. **Compression**: Use Parquet with Snappy compression for 70-90% size reduction
3. **Partitioning**: Organize data by customer_id and date for efficient queries
4. **Caching**: Redis cache for frequently accessed data
5. **Query Optimization**: Pre-aggregate common metrics during ingestion

### Example Cost Calculation

For a customer with:
- 1M transactions/day
- 90-day hot retention
- 2-year cold retention

Monthly costs:
- Hot storage: ~$50 (30GB compressed)
- Cold storage: ~$15 (650GB compressed)  
- API requests: ~$20 (10M requests)
- Total: ~$85/month

Compared to self-hosted:
- PostgreSQL instance: ~$200/month
- Maintenance: ~$500/month (engineer time)
- Savings: ~88%

## Security Considerations

1. **Data Isolation**: Each customer's data is logically separated
2. **Encryption**: At-rest and in-transit encryption
3. **Access Control**: API key authentication with scope limitations
4. **Audit Logging**: All queries and access attempts are logged
5. **Compliance**: GDPR-compliant data retention and deletion

## Future Enhancements

1. **GraphQL API**: For more flexible querying
2. **Real-time Subscriptions**: WebSocket connections for live data
3. **Custom Processors**: Allow customers to deploy custom processing logic
4. **Data Exports**: Scheduled exports to customer's own storage
5. **Advanced Analytics**: Pre-built dashboards and reports