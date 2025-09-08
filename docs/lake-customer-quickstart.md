# Obsrvr Lake Customer Quick Start Guide

## What's Changed?

Instead of custom REST APIs per customer, you now query a unified data lakehouse using SQL, with optional automation through Actions.

## Access Methods

### 1. SQL Interface (Recommended)

```bash
# Query via HTTP API
curl -X POST https://lake.obsrvr.io/v1/query \
  -H "Authorization: Bearer $OBSRVR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM gold.wallet_balances WHERE account = '\''GDEX...'\'' AND asset_code = '\''USDC'\''"
  }'
```

### 2. Python SDK

```python
from obsrvr import Lake

lake = Lake(api_key="your-key")

# Simple query
balances = lake.query("""
    SELECT account, asset_code, balance 
    FROM gold.wallet_balances 
    WHERE balance > 0
""")

# Streaming query
for event in lake.stream("SELECT * FROM silver.payments_stream"):
    print(f"New payment: {event.amount} {event.asset_code}")
```

### 3. JDBC/ODBC Drivers

```java
// Connect any BI tool or application
String url = "jdbc:trino://lake.obsrvr.io:443/gold";
Properties props = new Properties();
props.setProperty("user", "customer_id");
props.setProperty("password", "api_key");
props.setProperty("SSL", "true");

Connection conn = DriverManager.getConnection(url, props);
```

### 4. GraphQL API

```graphql
query GetWalletActivity($account: String!, $days: Int = 7) {
  wallet(account: $account) {
    current_balances {
      asset_code
      balance
      value_usd
    }
    recent_transactions(days: $days) {
      hash
      timestamp
      type
      amount
      asset_code
    }
  }
}
```

## Available Tables

### Silver Layer (Raw Data)
- `silver.transactions` - All Stellar transactions
- `silver.operations` - Individual operations
- `silver.effects` - Operation effects
- `silver.contract_events` - Soroban events
- `silver.token_transfers` - All token movements
- `silver.account_states` - Account balance snapshots

### Gold Layer (Optimized Views)
- `gold.wallet_balances` - Current token balances
- `gold.wallet_activity` - Enriched transaction history
- `gold.dex_trades` - DEX trading data
- `gold.market_metrics` - Price and volume analytics
- `gold.contract_analytics` - Smart contract insights

## Common Queries

### Get Current Balances
```sql
SELECT 
    asset_code,
    asset_issuer,
    balance / 10000000.0 as balance,
    balance * price_usd / 10000000.0 as value_usd
FROM gold.wallet_balances
WHERE account = 'GDEXAMPLE...'
  AND balance > 0
ORDER BY value_usd DESC;
```

### Transaction History
```sql
SELECT 
    timestamp,
    operation_type,
    asset_code,
    amount / 10000000.0 as amount,
    CASE 
        WHEN direction = 'in' THEN 'Received'
        ELSE 'Sent'
    END as direction,
    counterparty,
    transaction_hash
FROM gold.wallet_activity
WHERE account = 'GDEXAMPLE...'
  AND timestamp > CURRENT_DATE - INTERVAL '30' DAY
ORDER BY timestamp DESC
LIMIT 100;
```

### Real-time Payment Monitoring
```sql
-- Create a streaming query
CREATE CONTINUOUS VIEW my_payments AS
SELECT 
    timestamp,
    account,
    asset_code,
    amount,
    transaction_hash
FROM silver.payments_stream
WHERE account IN ('GDACCOUNT1...', 'GDACCOUNT2...')
  AND amount > 1000000000; -- 100 XLM
```

### Historical Balance Chart
```sql
WITH daily_balances AS (
    SELECT 
        DATE(timestamp) as date,
        asset_code,
        LAST_VALUE(balance) OVER (
            PARTITION BY asset_code 
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as end_balance
    FROM silver.account_states
    WHERE account = 'GDEXAMPLE...'
)
SELECT 
    date,
    asset_code,
    end_balance / 10000000.0 as balance
FROM daily_balances
WHERE date >= CURRENT_DATE - INTERVAL '90' DAY
ORDER BY date, asset_code;
```

## Setting Up Actions

### 1. Define Your Action
```yaml
# action-config.yaml
name: large_payment_alert
trigger:
  source: gold.wallet_activity
  condition: "amount > 1000000000 AND asset_code = 'USDC'"
action:
  type: webhook
  url: https://your-app.com/alerts
  headers:
    Authorization: "Bearer ${YOUR_WEBHOOK_SECRET}"
```

### 2. Deploy via CLI
```bash
obsrvr actions deploy action-config.yaml
```

### 3. Or Use Wasm for Complex Logic
```rust
use obsrvr_actions::prelude::*;

#[action]
fn process_large_payment(payment: Payment) -> Result<()> {
    if payment.amount > 1_000_000_000 {
        // Custom business logic
        send_email(EmailConfig {
            to: "alerts@company.com",
            subject: format!("Large {} payment", payment.asset_code),
            body: format!("Amount: {}", payment.amount / 10_000_000.0)
        })?;
        
        // Update risk score
        update_risk_score(payment.account, 0.8)?;
    }
    Ok(())
}
```

## Performance Tips

### 1. Use Time Filters
```sql
-- Good: Uses partition pruning
WHERE timestamp >= CURRENT_DATE - INTERVAL '7' DAY

-- Bad: Scans entire table
WHERE EXTRACT(DOW FROM timestamp) = 1
```

### 2. Leverage Materialized Views
```sql
-- Instead of complex joins, use pre-computed views
SELECT * FROM gold.account_summary 
WHERE account = 'GDEXAMPLE...'
```

### 3. Async Queries for Large Datasets
```python
# For large exports
query_id = lake.submit_async_query("""
    SELECT * FROM silver.transactions 
    WHERE timestamp >= '2024-01-01'
""")

# Check status
status = lake.get_query_status(query_id)
if status.state == "SUCCEEDED":
    results = lake.get_results(query_id, format="parquet")
```

## Cost Model

### Included in Base Tier
- 1TB storage (across all layers)
- 100GB monthly queries
- 1M Action executions
- 7-day data retention in streaming views

### Usage-Based Pricing
- Additional storage: $0.02/GB/month
- Query processing: $0.10/GB scanned
- Action executions: $0.10/1000 executions
- Streaming views: $0.05/GB/hour

### Cost Optimization
```sql
-- Use aggregated tables when possible
SELECT * FROM gold.daily_account_metrics  -- Scans 1MB
-- Instead of
SELECT DATE(timestamp), COUNT(*) FROM silver.transactions GROUP BY 1  -- Scans 100GB
```

## Migration Checklist

- [ ] Obtain Lake API credentials
- [ ] Update queries to use SQL instead of REST endpoints
- [ ] Test queries in Lake Explorer UI
- [ ] Set up Actions for real-time needs
- [ ] Configure data retention preferences
- [ ] Update client applications to use new SDK
- [ ] Schedule training for SQL access

## Support Resources

- **Lake Explorer**: https://lake.obsrvr.io/explorer
- **SQL Reference**: https://docs.obsrvr.io/lake/sql
- **Actions SDK**: https://github.com/obsrvr/actions-sdk
- **Support**: support@obsrvr.io