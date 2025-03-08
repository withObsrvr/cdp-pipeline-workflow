# CDP Pipeline Workflow

A data pipeline for processing Stellar blockchain data, with support for payment and account creation operations(WIP). Many of the consumers and processors are experimental and may not perform as they should.

## Features

- Processes Stellar blockchain data from multiple sources:
  - Amazon S3
  - Google Cloud Storage (with OAuth or Service Account)
  - Local filesystem
- Transforms operations into standardized formats
- Supports both payment and create account operations(WIP)
- Outputs to multiple destinations (MongoDB, ZeroMQ, PostgreSQL, DuckDB)
- Processes account data and stores it in PostgreSQL or DuckDB

## Prerequisites

- Go 1.22 or later
- Access to one of:
  - Amazon S3
  - Google Cloud Storage
  - Local filesystem
- MongoDB instance
- ZeroMQ (optional)

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

## Installation

```bash
go get github.com/withObsrvr/cdp-pipeline-workflow
```

## Usage

For GCS authentication:
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
./cdp-pipeline-workflow -config /path/to/config.yaml
```

For S3 authentication:
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
./cdp-pipeline-workflow -config /path/to/config.yaml
```

For GCS OAuth authentication, you'll need to:

1. Create a Google Cloud OAuth 2.0 Client ID:
   - Go to Google Cloud Console -> APIs & Services -> Credentials
   - Create a new OAuth 2.0 Client ID
   - Download the client configuration file

2. Get an OAuth token using the Google OAuth 2.0 Playground:
   - Visit https://developers.google.com/oauthplayground/
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

Note: OAuth tokens are temporary and will expire. For production use, consider using service account authentication instead.
