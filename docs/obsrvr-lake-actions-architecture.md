# Obsrvr Lake + Actions Architecture

## Overview

Obsrvr is transitioning to a unified lakehouse architecture powered by **Obsrvr Lake** (AI-native data lakehouse) and **Obsrvr Actions** (event-driven automation engine). This replaces the traditional per-customer database approach with a modern, scalable, and cost-effective solution.

## Architecture Evolution

### From: Traditional Hosted Databases
```
CDP Pipeline → Customer PostgreSQL/ClickHouse → REST API
```

### To: Unified Lakehouse
```
Galexie (XDR) → Obsrvr Flow → Iceberg Lakehouse → Multi-Engine Access
                                    ↓
                            Obsrvr Actions (Wasm)
```

## Core Components

### 1. Obsrvr Lake - The Data Foundation

#### Medallion Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    BRONZE LAYER                         │
│  Raw XDR files from Galexie (compressed, immutable)    │
│  Location: s3://obsrvr-lake/bronze/xdr/...            │
└────────────────────────┬───────────────────────────────┘
                         │ Obsrvr Flow
┌────────────────────────▼───────────────────────────────┐
│                    SILVER LAYER                         │
│  Parsed & structured data (transactions, events)       │
│  Format: Iceberg tables with schema evolution          │
└────────────────────────┬───────────────────────────────┘
                         │ SQL Transforms
┌────────────────────────▼───────────────────────────────┐
│                     GOLD LAYER                          │
│  Business-ready views (wallets, analytics, vectors)    │
│  Optimized for specific use cases                      │
└─────────────────────────────────────────────────────────┘
```

#### Key Technologies

- **Storage**: Object storage (Backblaze B2, S3-compatible)
- **Table Format**: Apache Iceberg (ACID, time travel, schema evolution)
- **Catalog**: Nessie (Git-like versioning) or AWS Glue
- **Compute**: 
  - Batch: Apache Spark/Flink
  - Interactive: Trino, DuckDB
  - Streaming: Materialize, RisingWave
- **Vector Store**: LanceDB for embeddings

### 2. Obsrvr Actions - Event-Driven Automation

```
Lake Events → Event Router → Wasm Runtime → Actions
                                ↓
                          Temporal Orchestration
```

#### Components

- **Wasm Plugins**: Sandboxed, portable automation logic
- **Temporal**: Durable workflow orchestration
- **Event Sources**: 
  - Streaming SQL materialized views
  - Lake change data capture
  - External webhooks

## Implementation Guide

### Phase 1: Bronze Layer Setup

```python
# 1. Galexie Configuration
galexie_config = {
    "source": "stellar-core",
    "destination": "s3://obsrvr-lake/bronze/xdr/",
    "format": "xdr",
    "compression": "zstd",
    "partitioning": "hourly"
}

# 2. Iceberg Bronze Table
CREATE TABLE bronze.xdr_files (
    file_path STRING,
    file_size BIGINT,
    compression_type STRING,
    ledger_range STRUCT<start: BIGINT, end: BIGINT>,
    ingestion_timestamp TIMESTAMP,
    checksum STRING
) USING iceberg
PARTITIONED BY (date(ingestion_timestamp))
LOCATION 's3://obsrvr-lake/bronze/catalog'
```

### Phase 2: Silver Layer Processing

```python
# Obsrvr Flow Pipeline Configuration
pipeline:
  name: "XDR-to-Silver"
  source:
    type: "IcebergSource"
    table: "bronze.xdr_files"
  
  processors:
    - type: "XDRDecoder"
      config:
        output_format: "structured"
    
    - type: "SchemaMapper"
      config:
        target_tables:
          - "silver.ledgers"
          - "silver.transactions"
          - "silver.contract_events"
  
  sink:
    type: "IcebergSink"
    catalog: "nessie"
    branch: "main"
```

### Phase 3: Gold Layer Views

```sql
-- Example: Wallet-optimized view
CREATE OR REPLACE VIEW gold.wallet_balances AS
WITH latest_balances AS (
    SELECT 
        account_id,
        asset_code,
        asset_issuer,
        balance,
        last_modified_ledger,
        ROW_NUMBER() OVER (
            PARTITION BY account_id, asset_code, asset_issuer 
            ORDER BY last_modified_ledger DESC
        ) as rn
    FROM silver.account_entries
    WHERE entry_type = 'TRUSTLINE'
)
SELECT * FROM latest_balances WHERE rn = 1;

-- Vector embedding table for semantic search
CREATE TABLE gold.transaction_embeddings (
    transaction_hash STRING,
    embedding ARRAY<FLOAT>,
    metadata MAP<STRING, STRING>
) USING iceberg
TBLPROPERTIES ('write.format.default'='parquet');
```

### Phase 4: Streaming SQL & Triggers

```sql
-- Materialize view for real-time monitoring
CREATE MATERIALIZED VIEW gold.large_payments AS
SELECT 
    t.hash,
    t.source_account,
    t.created_at,
    p.amount,
    p.asset_code
FROM silver.transactions t
JOIN silver.payments p ON t.hash = p.transaction_hash
WHERE p.amount > 10000
WITH (
    refresh_interval = '1 second'
);

-- Trigger for Obsrvr Actions
CREATE TRIGGER large_payment_alert
AFTER INSERT ON gold.large_payments
FOR EACH ROW
EXECUTE PROCEDURE notify_actions('large_payment', NEW);
```

### Phase 5: Actions Implementation

```rust
// Wasm plugin example
#[no_mangle]
pub extern "C" fn handle_large_payment(event: &[u8]) -> Result<(), Error> {
    let payment: LargePayment = deserialize(event)?;
    
    // Business logic
    if payment.amount > 50000 && payment.asset_code == "USDC" {
        send_notification(NotificationChannel::Email, {
            to: "compliance@customer.com",
            subject: "Large USDC Transfer Alert",
            body: format!("Transfer of {} USDC detected", payment.amount)
        })?;
    }
    
    Ok(())
}
```

## Multi-Tenant Strategy

Instead of separate databases per customer, use:

### 1. Row-Level Security

```sql
-- Customer data isolation via policies
CREATE ROW ACCESS POLICY customer_isolation
ON gold.transactions
FOR SELECT
USING (customer_id = current_setting('app.customer_id'));
```

### 2. Virtual Views

```sql
-- Dynamic view generation per customer
CREATE OR REPLACE FUNCTION create_customer_view(customer_id TEXT)
RETURNS VOID AS $$
BEGIN
    EXECUTE format('
        CREATE OR REPLACE VIEW %I.transactions AS
        SELECT * FROM gold.transactions
        WHERE customer_id = %L
    ', 'customer_' || customer_id, customer_id);
END;
$$ LANGUAGE plpgsql;
```

### 3. Query Federation

```yaml
# Customer query configuration
customer_config:
  id: "abc123"
  allowed_tables:
    - "gold.transactions"
    - "gold.token_transfers"
    - "gold.contract_events"
  row_filters:
    account_whitelist: ["GDCUSTOMER1...", "GDCUSTOMER2..."]
  compute_limits:
    max_query_runtime: 30s
    max_memory: 2GB
```

## API Access Patterns

### 1. SQL-over-HTTP (Trino)

```bash
POST /v1/statement
Authorization: Bearer customer_token
X-Customer-ID: abc123

{
  "query": "SELECT * FROM gold.transactions WHERE created_at > CURRENT_DATE - INTERVAL '7' DAY",
  "schema": "customer_abc123"
}
```

### 2. Arrow Flight SQL

```python
# High-performance columnar data access
import pyarrow.flight as flight

client = flight.FlightClient("grpc://lake.obsrvr.io:8815")
client.authenticate(CustomerAuthHandler(api_key))

# Execute query returning Arrow batches
result = client.do_get(
    flight.Ticket(
        b"SELECT * FROM gold.token_transfers WHERE account = 'GD...'"
    )
)
```

### 3. GraphQL Federation

```graphql
# Unified GraphQL API over lakehouse
type Query {
  transactions(
    account: String!
    startDate: DateTime
    endDate: DateTime
    limit: Int = 100
  ): TransactionConnection!
  
  tokenBalances(
    account: String!
    assetCode: String
  ): [TokenBalance!]!
  
  # AI-powered semantic search
  searchTransactions(
    query: String!
    similarity: Float = 0.8
  ): [Transaction!]!
}
```

## Cost Optimization

### Storage Costs (Monthly)

| Layer | Storage Type | Size | Cost | Purpose |
|-------|-------------|------|------|---------|
| Bronze | Object Storage (zstd) | 1TB | $6 | Raw XDR files |
| Silver | Iceberg (snappy) | 300GB | $10 | Structured data |
| Gold | Iceberg (zstd) | 100GB | $3 | Optimized views |
| **Total** | | **1.4TB** | **$19** | Full lakehouse |

Compare to traditional approach:
- PostgreSQL cluster: ~$500/month
- ClickHouse cluster: ~$300/month
- **Savings: 97%**

### Query Costs

| Engine | Use Case | Cost Model |
|--------|----------|------------|
| Trino | Interactive queries | $0.10/GB scanned |
| DuckDB | Embedded analytics | Free (customer compute) |
| Materialize | Streaming SQL | $0.05/GB + compute |
| Flight SQL | Bulk exports | $0.01/GB transferred |

## Migration Path

### Week 1-2: Setup Infrastructure
- Deploy Iceberg catalog (Nessie)
- Configure object storage
- Setup Galexie → Bronze pipeline

### Week 3-4: Build Silver Layer
- Create XDR decoder processors
- Define Silver schema
- Implement CDC pipelines

### Week 5-6: Create Gold Views
- Design customer-specific views
- Build materialized aggregations
- Add vector embeddings

### Week 7-8: Enable Access
- Deploy query engines
- Implement row-level security
- Create API endpoints

### Week 9-10: Add Actions
- Deploy Wasm runtime
- Create example automations
- Setup Temporal workflows

### Week 11-12: Customer Migration
- Migrate pilot customers
- Monitor performance
- Gather feedback

## Benefits Summary

1. **Unified Architecture**: Single lakehouse serves all customers
2. **Cost Reduction**: 97% lower storage costs
3. **Multi-Engine**: Customers choose their preferred query engine
4. **AI-Native**: Built-in vector search and embeddings
5. **Event-Driven**: Real-time actions from streaming SQL
6. **Open Standards**: No vendor lock-in with Iceberg
7. **Infinite Scale**: Object storage scales without limits
8. **Time Travel**: Query historical data states
9. **Schema Evolution**: Add columns without breaking queries
10. **Git-like Data**: Branch, merge, and version data changes

## Next Steps

1. **Prototype Bronze Layer**: Start ingesting XDR files
2. **Choose Catalog**: Evaluate Nessie vs Glue
3. **Select Compute**: Test Spark vs Flink for Silver processing
4. **Design Gold Schema**: Work with customers on views
5. **Build Actions SDK**: Create Wasm development kit
6. **Create Migration Tools**: Automate customer transitions