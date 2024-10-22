# CDP Pipeline Workflow

A data pipeline for processing Stellar blockchain data, with support for payment and account creation operations(WIP).

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

## Installation

```bash
go get github.com/withObsrvr/cdp-pipeline-workflow
```

## Usage

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
./cdp-pipeline-workflow
```

