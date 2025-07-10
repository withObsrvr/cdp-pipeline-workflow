# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

The CDP Pipeline Workflow is a Stellar blockchain data processing pipeline built in Go. The system processes Stellar ledger data through a modular architecture consisting of Sources, Processors, and Consumers that can be chained together via YAML configuration files.

## Development Commands

### Building the Application
```bash
# Build locally (requires CGO and ZeroMQ libraries)
CGO_ENABLED=1 go build -o cdp-pipeline-workflow

# Build production Docker image
docker build -t obsrvr-flow-pipeline -f dockerfile .

# Build development Docker image  
docker build -t cdp-pipeline-dev -f dockerfile.dev .
```

### Running the Application
```bash
# Run with default config
./cdp-pipeline-workflow -config pipeline_config.yaml

# Run with specific config file
./cdp-pipeline-workflow -config config/base/pipeline_config.yaml

# Run locally with Docker
./run-local.sh config/base/pipeline_config.yaml

# Run development ZeroMQ pipeline
./run-dev-zeromq.sh
```

### Testing
```bash
# Run Go tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Test specific package
go test ./pkg/processor/...
```

Note: The codebase currently has no test files (*_test.go). Testing relies on Docker-based integration testing and manual configuration testing.

## Architecture

### Core Components

1. **Sources** (`pkg/source/`, `source_adapter_*.go`): Data ingestion from various sources
   - `CaptiveCoreInboundAdapter`: Direct Stellar Core connection
   - `BufferedStorageSourceAdapter`: S3/GCS/filesystem storage
   - `RPCSourceAdapter`: Stellar RPC endpoints
   - `SorobanSourceAdapter`: Soroban smart contract events

2. **Processors** (`pkg/processor/`, `processor/`): Data transformation and filtering
   - Payment processors: `FilterPayments`, `TransformToAppPayment`
   - Account processors: `AccountData`, `CreateAccount`, `AccountTransaction`
   - Contract processors: `ContractInvocation`, `ContractEvent`, `ContractLedgerReader`
   - Market processors: `TransformToAppTrade`, `MarketMetricsProcessor`

3. **Consumers** (`pkg/consumer/`, `consumer/`): Data output and storage
   - Database: `SaveToPostgreSQL`, `SaveToMongoDB`, `SaveToDuckDB`, `SaveToClickHouse`
   - Cache: `SaveToRedis`, `SaveToRedisOrderbook`
   - Files: `SaveToExcel`, `SaveToGCS`
   - Messaging: `SaveToZeroMQ`, `SaveToWebSocket`

### Data Flow Pattern

```
Source -> Processor(s) -> Consumer(s)
```

- Sources capture blockchain events and emit `Message` objects
- Processors subscribe to sources/other processors and transform data
- Consumers subscribe to processors and persist/forward data
- All components implement the `Processor` interface for uniform chaining

### Configuration System

- Pipeline configurations are defined in YAML files under `config/base/`
- Each pipeline specifies a source, optional processors, and consumers
- Multiple pipelines can be defined in a single config file
- Factory pattern in `main.go` instantiates components based on config type strings

### Key Interfaces

- `types.Processor`: Core processing interface with `Process()` and `Subscribe()` methods
- `types.Message`: Wrapper for data payloads passed between components
- `SourceAdapter`: Interface for data sources with `Run()` and `Subscribe()` methods

## Development Notes

### Dependencies
- Requires CGO for DuckDB and ZeroMQ support
- Uses Stellar Go SDK for blockchain data structures
- Heavy use of YAML configuration with reflection-based component instantiation

### Code Organization
- Legacy code in root directory (`processor/`, `consumer/`, `source_adapter_*.go`)
- New modular code in `pkg/` directory with factory patterns
- Both entry points exist: `main.go` (legacy) and `cmd/pipeline/main.go` (new)

### Configuration Files
- Base configurations in `config/base/`
- Secret configurations (credentials) use `.secret.yaml` extension
- Template configurations available for common use cases
- Environment variables can override config values (e.g., AWS_ACCESS_KEY_ID, GOOGLE_APPLICATION_CREDENTIALS)

### Docker Development
- Development workflow uses `dockerfile.dev` with debugging tools
- Production builds use multi-stage `dockerfile` with minimal runtime
- `run-local.sh` script simplifies local Docker development
- Go version: 1.23 (production), 1.22.5 (development)
- Required system libraries: libzmq3-dev, libczmq-dev, libsodium-dev

### Common Pipeline Types
- **Ledger processing**: Process all ledger data from blockchain
- **Payment filtering**: Extract specific payment operations
- **Account monitoring**: Track account changes and transactions
- **Contract events**: Monitor Soroban smart contract events
- **Market data**: Process DEX trades and market metrics