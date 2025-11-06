# CDP Pipeline Workflow

A modular data pipeline for processing Stellar blockchain data at scale. Process ledgers, transactions, operations, and contract events from multiple sources with flexible output destinations.

**Quick Start:** Visit [withobsrvr.github.io/cdp-pipeline-workflow](https://withobsrvr.github.io/cdp-pipeline-workflow) for installation instructions.

## Features

- **Multiple Data Sources:**
  - Amazon S3
  - Google Cloud Storage (OAuth or Service Account)
  - Local filesystem
  - Stellar RPC endpoints
  - Captive Core (direct connection)

- **Flexible Processing:**
  - Payment operations
  - Account creation and management
  - Contract invocations and events
  - DEX trades and market data
  - Validator analytics

- **Output Destinations:**
  - PostgreSQL, MongoDB, ClickHouse, DuckDB
  - Redis (caching and orderbooks)
  - Google Cloud Storage, Excel
  - ZeroMQ, WebSocket streaming

## Installation

### Option 1: Docker (Recommended)

```bash
docker pull withobsrvr/obsrvr-flow-pipeline:latest
docker run -v $(pwd)/config.yaml:/config.yaml \
  withobsrvr/obsrvr-flow-pipeline:latest \
  -config /config.yaml
```

### Option 2: Build from Source

**Prerequisites:**
- Go 1.24 or later
- GCC (for CGO support)
- System libraries: libzmq3-dev, libczmq-dev, libsodium-dev

**Install dependencies:**

```bash
# Ubuntu/Debian
sudo apt-get install -y libzmq3-dev libczmq-dev libsodium-dev build-essential

# macOS
brew install zeromq czmq libsodium
```

**Build:**

```bash
git clone https://github.com/withObsrvr/cdp-pipeline-workflow.git
cd cdp-pipeline-workflow
CGO_ENABLED=1 go build -o cdp-pipeline-workflow
./cdp-pipeline-workflow -config config.yaml
```

## Quick Start

Create a `config.yaml` file:

```yaml
pipeline:
  name: MyFirstPipeline
  source:
    type: BufferedStorageSourceAdapter
    config:
      bucket_name: "stellar-ledgers"
      network: "mainnet"
      start_ledger: 1000
      end_ledger: 2000
      num_workers: 4
  processors:
    - type: LedgerMetaDecoder
  consumers:
    - type: SaveToPostgreSQL
      config:
        host: "localhost"
        port: 5432
        database: "stellar"
        username: "postgres"
        password: "password"
```

Run the pipeline:

```bash
./cdp-pipeline-workflow -config config.yaml
```

## Environment Variables

**AWS S3:**
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1
```

**Google Cloud Storage:**
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
```

## Configuration

The pipeline is configured using YAML files. Example configurations:

### S3 Source Configuration

```yaml
pipeline:
  name: PaymentPipeline
  source:
    type: S3BufferedStorageSourceAdapter
    config:
      bucket_name: "your-bucket"
      region: "us-east-1"
      network: "testnet"
      buffer_size: 640
      num_workers: 10
      start_ledger: 2
      end_ledger: 1000  # optional
      ledgers_per_file: 64  # optional, default: 64
      files_per_partition: 10  # optional, default: 10
```

### GCS OAuth Configuration
```yaml
pipeline:
  name: PaymentPipeline
  source:
    type: GCSBufferedStorageSourceAdapter
    config:
      bucket_name: "your-bucket"
      network: "testnet"
      buffer_size: 640
      num_workers: 10
      start_ledger: 2
      access_token: "your-oauth-token"
      ledgers_per_file: 64
      files_per_partition: 10
```

### MongoDB Consumer Configuration
```yaml
pipelines:
  PaymentPipeline:
    source:
      # ... source configuration as above ...
    processors:
      - type: TransformToAppPayment
        config:
          network_passphrase: "Test SDF Network ; September 2015"
    consumers:
      - type: SaveToMongoDB
        config:
          uri: "mongodb-uri"
          database: "mongodb-db"
          collection: "mongodb-collection"
          connect_timeout: 10  # seconds
```

### PostgreSQL Account Data Consumer Configuration
```yaml
pipelines:
  AccountDataPostgreSQLPipeline:
    source:
      type: BufferedStorageSourceAdapter
      config:
        bucket_name: "your-bucket"
        network: "mainnet"
        num_workers: 4
        retry_limit: 3
        retry_wait: 5
        start_ledger: 55808000
        end_ledger: 55808350
        ledgers_per_file: 1
        files_per_partition: 64000
        buffer_size: 1
    processors:
      - type: AccountData
        config:
          network_passphrase: "Public Global Stellar Network ; September 2015"
    consumers:
      - type: SaveAccountDataToPostgreSQL
        config:
          host: "localhost"
          port: 5432
          database: "stellar_accounts"
          username: "postgres"
          password: "your-password"
          sslmode: "disable"
          max_open_conns: 10
          max_idle_conns: 5
```

### Connecting to Remote PostgreSQL Servers

When connecting to remote PostgreSQL servers, you may need to adjust your configuration:

1. **Use IP Address**: For remote servers, consider using the IP address directly instead of the hostname to avoid DNS resolution issues.
2. **Correct Port**: Make sure to use the correct port for your PostgreSQL server (default is 5432, but cloud providers often use different ports).
3. **SSL Mode**: For secure connections, set `sslmode` to "require" or "verify-full".
4. **Connection Timeout**: Add a `connect_timeout` parameter (in seconds) to prevent long waits during connection attempts.

Example configuration for a remote PostgreSQL server:

```yaml
consumers:
  - type: SaveSoroswapToPostgreSQL
    config:
      host: "157.245.248.243"  # IP address of the remote server
      port: 23548              # Custom port used by the cloud provider
      database: "defaultdb"
      username: "dbuser"
      password: "your-secure-password"
      sslmode: "require"       # Required for secure connections
      max_open_conns: 10
      max_idle_conns: 5
      connect_timeout: 30      # 30 seconds timeout
```

### Connecting to Secure Redis Servers

When connecting to managed Redis services that require TLS (rediss://), you have two options:

1. **Use Individual Connection Parameters with TLS**:
   ```yaml
   consumers:
     - type: SaveLatestLedgerToRedis
       config:
         redis_address: "your-redis-host.example.com:6380"
         redis_password: "your-redis-password"
         redis_db: 0
         key_prefix: "stellar:ledger:"
         use_tls: true  # Enable TLS for secure connections
   ```

2. **Use Redis URL**:
   ```yaml
   consumers:
     - type: SaveLatestLedgerToRedis
       config:
         redis_url: "rediss://user:password@your-redis-host.example.com:6380/0"
         key_prefix: "stellar:ledger:"
   ```

For local development without TLS, you can use:
```yaml
consumers:
  - type: SaveLatestLedgerToRedis
    config:
      redis_address: "localhost:6379"
      redis_password: ""
      redis_db: 0
      key_prefix: "stellar:ledger:"
      use_tls: false
```

### DuckDB Account Data Consumer Configuration
```yaml
pipelines:
  AccountDataDuckDBPipeline:
    source:
      type: BufferedStorageSourceAdapter
      config:
        bucket_name: "your-bucket"
        network: "mainnet"
        num_workers: 4
        retry_limit: 3
        retry_wait: 5
        start_ledger: 55808000
        end_ledger: 55808350
        ledgers_per_file: 1
        files_per_partition: 64000
        buffer_size: 1
    processors:
      - type: AccountData
        config:
          network_passphrase: "Public Global Stellar Network ; September 2015"
    consumers:
      - type: SaveAccountDataToDuckDB
        config:
          db_path: "data/stellar_accounts.duckdb"
```

## Additional Resources

- **Documentation:** [Full configuration guide](https://github.com/withObsrvr/cdp-pipeline-workflow/blob/main/README.md)
- **Examples:** Check the `config/` directory for sample configurations
- **Issues:** [Report bugs or request features](https://github.com/withObsrvr/cdp-pipeline-workflow/issues)
- **Contributing:** Pull requests are welcome!

## GCS OAuth Authentication (Advanced)

For OAuth-based Google Cloud Storage access:

1. Create a Google Cloud OAuth 2.0 Client ID:
   - Go to Google Cloud Console → APIs & Services → Credentials
   - Create a new OAuth 2.0 Client ID
   - Download the client configuration file

2. Get an OAuth token using the [Google OAuth 2.0 Playground](https://developers.google.com/oauthplayground/):
   - Configure OAuth 2.0 with your client ID and secret
   - Select and authorize the "Google Cloud Storage API v1"
   - Exchange authorization code for tokens
   - Copy the Access Token

3. Use the token in your configuration:

```yaml
pipeline:
  source:
    type: GCSBufferedStorageSourceAdapter
    config:
      access_token: "your-access-token"
```

**Note:** OAuth tokens are temporary and will expire. For production use, consider using service account authentication instead (via `GOOGLE_APPLICATION_CREDENTIALS`).
