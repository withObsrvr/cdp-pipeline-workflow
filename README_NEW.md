# CDP Pipeline Workflow

A flexible pipeline system for processing blockchain data, with a focus on Stellar and Soroban contracts.

## Overview

CDP Pipeline Workflow is a modular data processing system designed to ingest, process, and store blockchain data. The system uses a pipeline architecture with processors and consumers that can be configured to handle different types of data and operations.

## Features

- Modular design with pluggable processors and consumers
- Support for multiple blockchain data sources
- Built-in processors for Stellar and Soroban smart contracts
- Support for various data sinks (PostgreSQL, Redis, WebSockets, etc.)
- Flexible configuration via YAML
- Extensible plugin system

## Getting Started

### Prerequisites

- Go 1.23 or later
- Access to a Stellar/Soroban node or archive (for live data)

### Installation

```bash
# Clone the repository
git clone https://github.com/withObsrvr/cdp-pipeline-workflow.git
cd cdp-pipeline-workflow

# Build the main application
go build -o pipeline ./cmd/pipeline
```

### Basic Usage

1. Configure your pipeline in YAML:

```yaml
pipelines:
  kale:
    name: "Kale Processor Pipeline"
    source:
      type: "rpc"
      config:
        url: "https://your-soroban-rpc-endpoint"
    processors:
      - type: "contract_filter"
        config:
          contract_id: "CAKXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
      - type: "kale"
        config:
          contract_id: "CAKXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    consumers:
      - type: "postgresql"
        config:
          connection_string: "postgres://user:password@localhost:5432/database"
```

2. Run the pipeline:

```bash
./pipeline --config your_config.yaml
```

## Architecture

See [MONOREPO.md](MONOREPO.md) for details on the repository structure and architecture.

## Development

### Adding a New Processor

1. Create a new directory in `pkg/processor/` for your processor category
2. Implement the `base.Processor` interface
3. Register your processor in `pkg/processor/base/factory.go`

### Adding a New Consumer

1. Create a new directory in `pkg/consumer/` for your consumer category
2. Implement the `base.Consumer` interface
3. Register your consumer in `pkg/consumer/base/factory.go`

### Running Tests

```bash
go test ./...
```

## Examples

See the `examples/` directory for usage examples.

## License

[Specify your license here]

## Acknowledgments

- The Stellar Development Foundation
- [Other acknowledgments] 