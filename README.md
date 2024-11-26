# CDP Pipeline Workflow

A data pipeline for processing Stellar blockchain data, with support for payment and account creation operations(WIP). Many of the consumers and processors are experimental and may not perform as they should.

## Features

- Processes Stellar blockchain data from Google Cloud Storage
- Transforms operations into standardized formats
- Supports both payment and create account operations(WIP)
- Outputs to multiple destinations (MongoDB, ZeroMQ)

## Prerequisites

- Go 1.22 or later
- Access to Google Cloud Storage
- MongoDB instance
- ZeroMQ (optional)

## Configuration

The pipeline is configured using YAML files. Example configuration:

```yaml
pipeline:
  name: PaymentPipeline
  source:
    type: BufferedStorageSourceAdapter
    config:
      bucket_name: "your-bucket"
      network: "testnet"
      buffer_size: 640
      num_workers: 10
      start_ledger: 539328
```

Save to MongoDB example configuration. Needs Google Cloud Storage bucket name, Mongodb URI, mongodb database name and mongodb collection name.

```yaml
pipelines:
  PaymentPipeline:
    source:
      type: BufferedStorageSourceAdapter
      config:
        bucket_name: "bucket-name"
        network: "testnet"
        buffer_size: 640
        num_workers: 10
        retry_limit: 3
        retry_wait: 5
        start_ledger: 539328
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

## Installation

```bash
go get github.com/withObsrvr/cdp-pipeline-workflow
```

## Usage

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
./cdp-pipeline-workflow
```

or

```bash
GOOGLE_APPLICATION_CREDENTIALS=$HOME/.config/gcloud/application_default_credentials.json ./cdp-pipeline-workflow
```


