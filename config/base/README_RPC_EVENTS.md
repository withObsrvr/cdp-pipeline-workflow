# RPC Contract Events Pipeline Guide

This guide shows how to set up a pipeline to fetch Soroban contract events using Stellar RPC.

## Quick Start

1. **Copy the template:**
   ```bash
   cp config/base/contract_events_rpc.yaml config/base/my_contract_events.yaml
   ```

2. **Configure your settings:**
   - Set your RPC endpoint (`rpc_url`)
   - Add authentication if needed (`auth_header`)
   - Specify contract IDs in `filters.contractIds`
   - Set `start_ledger` to where you want to begin
   - Choose your output consumer (PostgreSQL, MongoDB, DuckDB, etc.)

3. **Run the pipeline:**
   ```bash
   # Docker
   docker run --rm \
     -v $(pwd)/config:/app/config \
     withobsrvr/obsrvr-flow-pipeline:latest \
     /app/cdp-pipeline-workflow -config /app/config/base/my_contract_events.yaml

   # Or from source
   ./cdp-pipeline-workflow -config config/base/my_contract_events.yaml
   ```

---

## Configuration Options

### RPC Endpoints

**Mainnet (Public):**
```yaml
rpc_url: "https://rpc-mainnet.stellar.org"
auth_header: ""
```

**Testnet (Public):**
```yaml
rpc_url: "https://soroban-testnet.stellar.org"
auth_header: ""
```

**OBSRVR Nodes (Requires API Key):**
```yaml
rpc_url: "https://rpc-mainnet.nodeswithobsrvr.co"
auth_header: "Api-Key YOUR_API_KEY_HERE"
```

---

## Event Filtering Examples

### 1. All Events from a Contract

Get every event emitted by a specific contract:

```yaml
filters:
  - type: "contract"
    contractIds:
      - "CBGTG7XFRY3L6OKAUTR6KGDKUXUQBX3YDJ3QFDYTGVMOM7VV4O7NCODG"
    topics:
      - ["*", "*", "*", "*"]
```

### 2. Specific Event Type (Transfer)

Filter for only transfer events:

```yaml
filters:
  - type: "contract"
    contractIds:
      - "YOUR_CONTRACT_ID"
    topics:
      - ["AAAADwAAAAh0cmFuc2Zlcg==", "*", "*", "*"]  # "transfer" in base64
```

To encode a topic name to base64:
```bash
echo -n "transfer" | base64
# Output: dHJhbnNmZXI=
# Full XDR encoding: AAAADwAAAAh0cmFuc2Zlcg==
```

### 3. Multiple Contracts

Monitor events from several contracts:

```yaml
filters:
  - type: "contract"
    contractIds:
      - "CONTRACT_ID_1"
      - "CONTRACT_ID_2"
      - "CONTRACT_ID_3"
    topics:
      - ["*"]
```

### 4. Multiple Event Types

Filter for multiple specific events from one contract:

```yaml
filters:
  # Transfer events
  - type: "contract"
    contractIds:
      - "YOUR_CONTRACT_ID"
    topics:
      - ["AAAADwAAAAh0cmFuc2Zlcg==", "*", "*", "*"]

  # Mint events
  - type: "contract"
    contractIds:
      - "YOUR_CONTRACT_ID"
    topics:
      - ["AAAADwAAAARtaW50", "*", "*"]
```

---

## Ledger Range Options

### Continuous Streaming (Start to Latest)

Start from a specific ledger and continue indefinitely:

```yaml
start_ledger: 1000000
# end_ledger not set - runs continuously
poll_interval: 5  # Check for new ledgers every 5 seconds
```

### Historical Range (One-Time)

Process a specific ledger range and exit:

```yaml
start_ledger: 1000000
end_ledger: 1001000  # Stops at ledger 1001000 (exclusive)
poll_interval: 1     # Fast polling for historical data
```

### Latest Only

Always get the most recent events:

```yaml
# Don't set start_ledger - will auto-start from latest
poll_interval: 10
```

---

## Output Destinations

### PostgreSQL

Best for: Queryable event storage, analytics

```yaml
consumers:
  - type: SaveToPostgreSQL
    config:
      host: "localhost"
      port: 5432
      database: "stellar_events"
      username: "postgres"
      password: "password"
      sslmode: "disable"
```

**Database Schema:** The processor will create tables automatically.

### MongoDB

Best for: Flexible schema, document storage

```yaml
consumers:
  - type: SaveToMongoDB
    config:
      uri: "mongodb://localhost:27017"
      database: "stellar"
      collection: "contract_events"
```

### DuckDB

Best for: Local analytics, embedded database

```yaml
consumers:
  - type: SaveToDuckDB
    config:
      db_path: "data/events.duckdb"
```

### ZeroMQ

Best for: Real-time streaming to other applications

```yaml
consumers:
  - type: SaveToZeroMQ
    config:
      address: "tcp://127.0.0.1:5555"
```

**Consuming events from ZeroMQ:**
```python
import zmq
import json

context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect("tcp://localhost:5555")
socket.setsockopt_string(zmq.SUBSCRIBE, "")

while True:
    message = socket.recv_string()
    event = json.loads(message)
    print(f"Event: {event}")
```

### Redis

Best for: Caching, pub/sub, temporary storage

```yaml
consumers:
  - type: SaveToRedis
    config:
      redis_address: "localhost:6379"
      redis_password: ""
      redis_db: 0
      key_prefix: "stellar:events:"
```

---

## Performance Tuning

### High-Volume Contracts

For contracts with many events:

```yaml
batch_size: 100       # Events per request
poll_interval: 1      # Fast polling
pagination:
  limit: 10000        # Max events per response
```

### Low-Volume Contracts

For contracts with few events:

```yaml
batch_size: 50
poll_interval: 30     # Slower polling saves resources
pagination:
  limit: 100
```

### Historical Backfill

For processing large ledger ranges quickly:

```yaml
start_ledger: 1000000
end_ledger: 2000000
poll_interval: 0      # No delay between requests
batch_size: 200
pagination:
  limit: 10000
```

---

## Common Use Cases

### 1. Token Transfer Monitoring

Monitor all transfers for a Stellar Asset Contract (SAC):

```yaml
pipelines:
  USDCTransferMonitor:
    source:
      type: RPCSourceAdapter
      config:
        rpc_url: "https://rpc-mainnet.stellar.org"
        auth_header: ""
        start_ledger: 1000000
        rpc_method: "getEvents"
        filters:
          - type: "contract"
            contractIds:
              - "CCW67TSZV3SSS2HXMBQ5JFGCKJNXKZM7UQUWUZPUTHXSTZLEO7SJMI75"  # USDC on Stellar
            topics:
              - ["AAAADwAAAAh0cmFuc2Zlcg==", "*", "*", "*"]
        pagination:
          limit: 5000
        xdrFormat: "json"
    processors:
      - type: GetEventsRPC
    consumers:
      - type: SaveToPostgreSQL
        config:
          host: "localhost"
          database: "usdc_transfers"
```

### 2. DEX Trade Monitoring

Track all trades on a DEX contract:

```yaml
pipelines:
  SoroswapTrades:
    source:
      type: RPCSourceAdapter
      config:
        rpc_url: "https://rpc-mainnet.stellar.org"
        start_ledger: 1500000
        rpc_method: "getEvents"
        filters:
          - type: "contract"
            contractIds:
              - "YOUR_SOROSWAP_ROUTER_ID"
            topics:
              - ["AAAADwAAAARzd2Fw", "*", "*", "*"]  # "swap" events
        xdrFormat: "json"
    consumers:
      - type: SaveToPostgreSQL
        config:
          database: "dex_trades"
```

### 3. Multi-Contract Dashboard

Monitor events from multiple related contracts:

```yaml
pipelines:
  DeFiDashboard:
    source:
      type: RPCSourceAdapter
      config:
        rpc_url: "https://rpc-mainnet.stellar.org"
        start_ledger: 2000000
        poll_interval: 5
        rpc_method: "getEvents"
        filters:
          - type: "contract"
            contractIds:
              - "LENDING_POOL_CONTRACT"
              - "LIQUIDITY_POOL_CONTRACT"
              - "REWARDS_CONTRACT"
            topics:
              - ["*"]
        pagination:
          limit: 10000
        xdrFormat: "json"
    consumers:
      - type: SaveToPostgreSQL
      - type: SaveToRedis  # For real-time dashboard
```

---

## Troubleshooting

### No Events Returned

**Check:**
1. Contract ID is correct (56-character C-address)
2. `start_ledger` is after contract deployment
3. Events actually exist in the ledger range
4. Topic filters are correctly encoded

**Test with:**
```bash
# Get latest ledger to verify RPC is working
curl -X POST https://rpc-mainnet.stellar.org \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getLatestLedger"}'
```

### Authentication Errors

**Error:** `401 Unauthorized`

**Fix:** Check your `auth_header` format:
- OBSRVR: `"Api-Key YOUR_KEY"`
- JWT: `"Bearer YOUR_TOKEN"`
- Public endpoints: `""`

### Rate Limiting

**Error:** `429 Too Many Requests`

**Fix:** Increase `poll_interval`:
```yaml
poll_interval: 30  # Slow down requests
```

### Memory Issues (Large Ledger Ranges)

**Fix:** Use smaller batches and pagination:
```yaml
batch_size: 50
pagination:
  limit: 1000
```

---

## Best Practices

1. **Start Recent:** Begin with recent ledgers to verify config works
2. **Test Filters:** Use small ledger ranges to test event filters
3. **Monitor Resources:** Watch memory/CPU usage for high-volume contracts
4. **Use Cursors:** For large historical ranges, use pagination cursors
5. **Handle Failures:** Pipeline will retry failed RPC calls automatically
6. **Backup Data:** Use persistent storage (PostgreSQL/MongoDB) not just ZeroMQ

---

## Next Steps

- **Process Events:** Add custom processors to transform event data
- **Analytics:** Query PostgreSQL for insights
- **Alerts:** Connect to notification services for specific events
- **Dashboards:** Visualize data with Grafana or similar tools

For more examples, see the `config/base/` directory.
