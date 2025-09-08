# Wallet Backend Integration

This document describes the integration of Stellar wallet-backend indexing patterns into CDP Pipeline Workflow.

## Overview

We've successfully integrated key patterns from Stellar's wallet-backend indexer while maintaining CDP's modular architecture. The implementation provides comprehensive blockchain state tracking with efficient batch processing.

## Components Added

### 1. WalletBackendProcessor (`processor/processor_wallet_backend.go`)

Extracts comprehensive state changes from blockchain transactions:

- **State Change Types**: CREDIT, DEBIT, MINT, BURN, CLAWBACK, SIGNER, THRESHOLD, FLAGS, HOME_DOMAIN, DATA_ENTRY, SPONSORSHIP, CONTRACT_DATA, CONTRACT_EVENT, and more
- **Comprehensive Coverage**: Tracks all operation types and their effects
- **Contract Events**: Processes Soroban smart contract events
- **Metadata Changes**: Extracts changes from transaction metadata using `GetChanges()`

Usage in pipeline:
```yaml
processors:
  - type: WalletBackend
    config:
      extract_contract_events: true
      track_participants: true
```

### 2. ParticipantExtractor (`processor/processor_participant_extractor.go`)

Identifies all accounts involved in transactions:

- **Role-based Tracking**: Categorizes participants as source, destination, signer, etc.
- **Comprehensive Coverage**: Extracts participants from operations and metadata
- **Contract Addresses**: Includes Soroban contract addresses from events

Usage in pipeline:
```yaml
processors:
  - type: ParticipantExtractor
    config: {}
```

### 3. BufferedPostgreSQL Consumer (`consumer/consumer_buffered_postgresql.go`)

High-performance database consumer with batch processing:

- **Configurable Buffering**: Set buffer size and flush intervals
- **Batch Inserts**: Groups records by type for efficient database writes
- **Retry Logic**: Configurable retry attempts with exponential backoff
- **Graceful Shutdown**: Ensures all buffered data is flushed on shutdown

Usage in pipeline:
```yaml
consumers:
  - type: BufferedPostgreSQL
    config:
      host: "localhost"
      port: 5432
      database: "wallet_backend"
      username: "postgres"
      password: "password"
      buffer_size: 1000
      flush_interval_ms: 5000
      max_retries: 3
```

## Database Schema

The BufferedPostgreSQL consumer creates the following tables:

- `transactions`: Core transaction data
- `state_changes`: All blockchain state modifications
- `transaction_participants`: Account participation tracking
- `generic_events`: Fallback for unknown event types

## Pipeline Configurations

### Example: Complete Wallet Backend Pipeline

```yaml
pipelines:
  WalletBackendComplete:
    source:
      type: BufferedStorageSourceAdapter
      config:
        bucket_name: "stellar-ledgers"
        network: "pubnet"
        buffer_size: 1000
        num_workers: 20
    processors:
      - type: WalletBackend
        config:
          extract_contract_events: true
          track_participants: true
      - type: ParticipantExtractor
        config: {}
    consumers:
      - type: BufferedPostgreSQL
        config:
          host: "localhost"
          port: 5432
          database: "wallet_backend"
          buffer_size: 1000
          flush_interval_ms: 5000
```

## Key Improvements Over Standard CDP

1. **Comprehensive State Tracking**: Goes beyond payments to track ALL blockchain state changes
2. **Efficient Batch Processing**: Reduces database load with configurable buffering
3. **Participant Analysis**: Systematic identification of all involved accounts
4. **Unified Event Processing**: Handles both classic operations and Soroban events

## SDK Methods Used

The implementation leverages Stellar SDK helper methods for protocol abstraction:

- `GetChanges()`: Extract ledger entry changes from metadata
- `GetTransactionEvents()`: Extract Soroban contract events
- `Successful()`: Check transaction success
- Various operation getter methods (e.g., `GetPaymentOp()`)

## Building and Running

```bash
# Build with CGO enabled (required for DuckDB and ZeroMQ)
CGO_ENABLED=1 go build -o cdp-pipeline-workflow

# Run with wallet backend pipeline
./cdp-pipeline-workflow -config config/base/wallet_backend_pipeline.yaml
```

## Future Enhancements

1. **Enhanced Effects Processing**: Expand tracking of account configuration changes
2. **Trade Extraction**: Process DEX trades from offer changes
3. **Performance Metrics**: Add instrumentation for monitoring
4. **Schema Migrations**: Automated database schema management