
# Stellar Blockchain Data Processing Pipeline

A flexible and extensible data processing pipeline for the Stellar blockchain, designed to capture, transform, and store blockchain events in various formats.

---

## Overview

This pipeline system follows a modular architecture that processes Stellar blockchain data through three main components:
- **Sources**: Capture data from the Stellar network.
- **Processors**: Transform and filter the data.
- **Consumers**: Store or forward the processed data.

---

## Architecture

### Core Components

1. **Source Adapters**
   - `CaptiveCoreInboundAdapter`: Direct connection to Stellar Core.
   - `BufferedStorageSourceAdapter`: Buffered processing from storage.
   - `SorobanSourceAdapter`: Soroban smart contract events.
   - `GCSBufferedStorageSourceAdapter`: Google Cloud Storage integration.
   - `FSBufferedStorageSourceAdapter`: File system storage.

2. **Processors**
   - **Payment Processors**: `FilterPayments`, `TransformToAppPayment`.
   - **Account Processors**: `CreateAccountTransformer`, `ProcessAccountData`.
   - **Contract Processors**: `ContractLedgerReader`, `ContractInvocation`.
   - **Market Processors**: `TransformToAppTrade`, `MarketMetricsProcessor`.
   - **Analytics Processors**: `AccountYearAnalytics`, `TransformToMarketAnalytics`.

3. **Consumers**
   - **Database Consumers**: `SaveToMongoDB`, `SaveToPostgreSQL`, `SaveToClickHouse`.
   - **Cache Consumers**: `SaveToRedis`, `SaveToRedisOrderbook`.
   - **File Consumers**: `SaveToExcel`, `SaveToGCS`.
   - **Message Queue**: `SaveToZeroMQ`.
   - **Notification**: `NotificationDispatcher`.

---

### Data Flow

```
[Source] -> [Processor(s)] -> [Consumer(s)]
   ↓           ↓               ↓
Capture     Transform        Store/
Events      Filter           Forward
```

---

## Configuration

### Basic Pipeline Configuration

```yaml
pipeline:
  name: PaymentPipeline
  source:
    type: CaptiveCoreInboundAdapter
    config:
      network: testnet
      history_archive_urls:
        - https://history.stellar.org/prd/core-live/core_live_001
      core_binary_path: /usr/local/bin/stellar-core
  processors:
    - type: FilterPayments
      config:
        min_amount: "100.00"
        asset_code: "XLM"
    - type: TransformToAppPayment
      config:
        network_passphrase: "Test SDF Network ; September 2015"
  consumers:
    - type: SaveToMongoDB
      config:
        uri: "mongodb://localhost:27017"
        database: "stellar_events"
        collection: "payments"
```

---

### Advanced Configuration Example

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
    processors:
      - type: TransformToAppPayment
      - type: FilterPayments
        config:
          min_amount: "100.00"
          asset_code: "native"
    consumers:
      - type: SaveToPostgreSQL
        config:
          host: "localhost"
          port: 5432
          database: "stellar_events"
```

---

## Development Guide

### Creating a New Processor

```go
type CustomProcessor struct {
  networkPassphrase string
  processors        []Processor
}

func NewCustomProcessor(config map[string]interface{}) (CustomProcessor, error) {
  networkPassphrase, ok := config["network_passphrase"].(string)
  if !ok {
    return nil, fmt.Errorf("missing network_passphrase")
  }
  return &CustomProcessor{
    networkPassphrase: networkPassphrase,
  }, nil
}

func (p CustomProcessor) Process(ctx context.Context, msg Message) error {
  // Process the message
  return nil
}

func (p CustomProcessor) Subscribe(processor Processor) {
  p.processors = append(p.processors, processor)
}
```

---

### Creating a New Consumer

```go
type CustomConsumer struct {
  config     map[string]interface{}
  processors []Processor
}

func NewCustomConsumer(config map[string]interface{}) (CustomConsumer, error) {
  // Validate and process configuration
  return &CustomConsumer{
    config: config,
  }, nil
}

func (c CustomConsumer) Process(ctx context.Context, msg Message) error {
  // Store or forward the message
  return nil
}

func (c CustomConsumer) Subscribe(processor Processor) {
  c.processors = append(c.processors, processor)
}
```

---

## Best Practices

1. **Error Handling**
   - Always validate configuration parameters.
   - Implement graceful error recovery.
   - Use context for cancellation and timeouts.

2. **Performance**
   - Implement buffering for batch operations.
   - Use goroutines for parallel processing.
   - Monitor memory usage in processors.

3. **Data Validation**
   - Validate all incoming data.
   - Implement type assertions safely.
   - Handle missing or malformed data gracefully.

4. **Configuration**
   - Use clear, descriptive configuration keys.
   - Provide default values where appropriate.
   - Document all configuration options.

5. **Testing**
   - Write unit tests for processors and consumers.
   - Test with different network configurations.
   - Implement integration tests for full pipelines.

---

## Dependencies

Core dependencies from `go.mod`:

```go
require (
  github.com/stellar/go v0.0.0-20241213185123-981158ac730c
  github.com/redis/go-redis/v9 v9.7.0
  github.com/lib/pq v1.10.9
  github.com/gorilla/websocket v1.4.2
  go.mongodb.org/mongo-driver v1.17.1
  google.golang.org/api v0.214.0
  github.com/zeromq/goczmq v4.1.0
  github.com/xuri/excelize/v2 v2.8.1
)
```

---

## Contributing

1. Fork the repository.
2. Create a feature branch.
3. Implement your changes.
4. Add tests for new functionality.
5. Submit a pull request.

When contributing:
- Follow existing code style and patterns.
- Document new processors and consumers.
- Update configuration examples.
- Add unit tests for new components.

For questions or support, please open an issue in the repository.

