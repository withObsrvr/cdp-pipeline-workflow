# Parquet Consumer Processor Integration Guide

## Overview

This guide explains how to make your CDP pipeline processors compatible with the SaveToParquet consumer. The Parquet consumer can handle various message formats and automatically infers schemas, but following these guidelines will ensure optimal performance and proper data partitioning.

## Message Format Requirements

### 1. Basic Message Structure

The Parquet consumer accepts messages in two formats:

#### Option A: JSON Bytes (Recommended)
```go
// In your processor's forwardToProcessors method:
jsonBytes, err := json.Marshal(yourDataStructure)
if err != nil {
    return fmt.Errorf("error marshaling data: %w", err)
}

// Send as Message with byte payload
msg := processor.Message{
    Payload: jsonBytes,
}
```

#### Option B: Map Interface
```go
// Send as Message with map payload
msg := processor.Message{
    Payload: map[string]interface{}{
        "your_field": "value",
        // ... other fields
    },
}
```

### 2. Required Fields for Proper Partitioning

To enable proper time-based partitioning and file organization, include these fields:

#### Ledger Sequence (Required)
```json
{
    "ledger_sequence": 426007  // As a number, not string
}
```

#### Timestamp Fields (Recommended)
For accurate date partitioning, include a timestamp. The consumer looks for these in order:
1. `closed_at` in nested data structures
2. `timestamp` at the root level
3. Falls back to current time if none provided

```json
{
    "timestamp": "2025-08-05T09:15:27.905427888Z",
    // or within nested structure:
    "contract_data": {
        "closed_at": "2025-07-13T09:36:17Z"
    }
}
```

### 3. Message Type Identification

Include a `message_type` field to help with schema management:

```json
{
    "message_type": "contract_data",  // or "event", "asset", etc.
    "processor_name": "your_processor_name"
}
```

## Processor Implementation Examples

### Example 1: Contract Data Processor Pattern

```go
type YourProcessorMessage struct {
    YourData        YourDataStruct         `json:"your_data"`
    ProcessorName   string                 `json:"processor_name"`
    MessageType     string                 `json:"message_type"`
    Timestamp       time.Time              `json:"timestamp"`
    LedgerSequence  uint32                 `json:"ledger_sequence"`
}

func (p *YourProcessor) forwardToProcessors(ctx context.Context, data YourDataStruct, ledgerSeq uint32) error {
    msg := YourProcessorMessage{
        YourData:       data,
        ProcessorName:  p.name,
        MessageType:    "your_data_type",
        Timestamp:      time.Now(),
        LedgerSequence: ledgerSeq,
    }
    
    jsonBytes, err := json.Marshal(msg)
    if err != nil {
        return fmt.Errorf("error marshaling: %w", err)
    }
    
    for _, subscriber := range p.subscribers {
        if err := subscriber.Process(ctx, processor.Message{Payload: jsonBytes}); err != nil {
            return fmt.Errorf("error in processor chain: %w", err)
        }
    }
    
    return nil
}
```

### Example 2: Event Processor Pattern

```go
type EventMessage struct {
    Type            string    `json:"type"`
    Ledger          uint64    `json:"ledger"`
    LedgerClosedAt  string    `json:"ledger_closed_at"`
    ContractID      string    `json:"contract_id"`
    ID              string    `json:"id"`
    // ... other fields
}
```

### Example 3: Adding Archive Metadata

If your processor reads from archived sources, include metadata for data lineage:

```go
type MessageWithArchiveMetadata struct {
    // Your data fields...
    ArchiveMetadata *ArchiveSourceMetadata `json:"archive_metadata,omitempty"`
}

type ArchiveSourceMetadata struct {
    FileName    string `json:"file_name"`
    BucketName  string `json:"bucket_name"`
    FileSize    int64  `json:"file_size"`
    // ... other metadata
}
```

## Schema Inference

The Parquet consumer automatically infers schemas from your messages. To ensure consistent schemas:

### 1. Consistent Field Types
Always use the same type for a field across messages:
```go
// Good - consistent types
"amount": 12345,          // Always number
"is_active": true,        // Always boolean
"account_id": "GAB...",   // Always string

// Bad - inconsistent types
"amount": "12345",        // Sometimes string
"amount": 12345,          // Sometimes number
```

### 2. Null Handling
Use `nil` or omit fields rather than empty strings for missing values:
```go
// Good
if value == "" {
    // omit the field entirely
} else {
    data["field"] = value
}

// Or explicitly null
data["field"] = nil
```

### 3. Nested Structures
The consumer handles nested structures well:
```go
{
    "transaction": {
        "id": "abc123",
        "operations": [
            {"type": "payment", "amount": "1000"},
            {"type": "create_account", "starting_balance": "100"}
        ]
    }
}
```

## Known Message Types

The Parquet consumer recognizes these message types and optimizes schemas accordingly:

### 1. Contract Data Messages
```json
{
    "message_type": "contract_data",
    "contract_data": {
        "contract_id": "CC...",
        "closed_at": "2025-07-13T09:36:17Z",
        // ... stellar ContractDataOutput fields
    }
}
```

### 2. Event Messages
```json
{
    "message_type": "event",
    "type": "contract",
    "ledger": 426007,
    "ledger_closed_at": "2025-07-13T09:36:17Z"
}
```

### 3. Asset Messages
```json
{
    "message_type": "asset",
    "code": "USDC",
    "issuer": "GA...",
    "timestamp": "2025-07-13T09:36:17Z"
}
```

## Best Practices

### 1. Include Descriptive Metadata
```go
message := map[string]interface{}{
    "processor_name": "soroswap_router",
    "processor_version": "1.0.0",
    "message_type": "swap_event",
    "environment": "testnet",
    // ... your data
}
```

### 2. Use Appropriate Time Formats
Always use RFC3339 format for timestamps:
```go
timestamp := time.Now().Format(time.RFC3339)
// Results in: "2025-08-05T14:30:45Z"
```

### 3. Handle Large Numbers
For amounts and balances, consider using strings to avoid precision loss:
```go
"balance": "123456789012345678901234567890"  // String for large numbers
```

### 4. Batch Considerations
The Parquet consumer buffers messages. Structure your data to work well in batches:
- Keep message sizes reasonable (< 1MB per message)
- Ensure consistent schema within a ledger range
- Include all necessary fields for standalone processing

## Testing Your Processor

### 1. Test Configuration
```yaml
pipelines:
  TestPipeline:
    source:
      type: YourSource
      config:
        # ... source config
    processors:
      - type: "YourProcessor"
        name: "test-processor"
        config:
          # ... processor config
    consumers:
      - type: DebugLogger  # Use this first to inspect message format
        config:
          name: "message_inspector"
          log_prefix: "MSG_FORMAT"
          max_fields: 50
      - type: SaveToParquet
        config:
          storage_type: "FS"
          local_path: "./test-output"
          buffer_size: 1  # Immediate flush for testing
          debug: true
```

### 2. Verify Output
Check that your messages are properly formatted:
```bash
# Run pipeline
./cdp-pipeline-workflow -config test-config.yaml

# Check logs for message structure
grep "MSG_FORMAT" output.log

# Verify Parquet files created
find ./test-output -name "*.parquet" -ls
```

### 3. Validate Parquet Schema
Use parquet-tools to inspect the schema:
```bash
# Install parquet-tools
pip install pyarrow

# Inspect schema
python -c "import pyarrow.parquet as pq; print(pq.ParquetFile('output.parquet').schema)"
```

## Common Issues and Solutions

### Issue 1: Files Not Created
**Symptom**: No Parquet files appear
**Solution**: Ensure buffer is flushing:
- Set `buffer_size: 1` for testing
- Include required `ledger_sequence` field
- Check logs for schema inference errors

### Issue 2: Wrong Date Partitions
**Symptom**: Files in unexpected date folders
**Solution**: Include proper timestamp:
```go
// For contract data
"contract_data": {
    "closed_at": time.Now().Format(time.RFC3339)
}

// For other data
"timestamp": time.Now().Format(time.RFC3339)
```

### Issue 3: Schema Conflicts
**Symptom**: "Schema mismatch" errors
**Solution**: Ensure consistent types:
- Numbers as `float64` or `int`
- Booleans as `bool`
- Strings as `string`
- Don't mix types for the same field

### Issue 4: Memory Issues
**Symptom**: High memory usage
**Solution**: 
- Reduce `buffer_size` in consumer config
- Ensure messages are reasonable size
- Avoid including large binary data directly

## Migration Guide

### From Existing Processors
If you have an existing processor, add Parquet support:

1. **Add JSON marshaling**:
```go
// Existing
for _, subscriber := range p.subscribers {
    subscriber.Process(ctx, processor.Message{Payload: yourStruct})
}

// Updated
jsonBytes, _ := json.Marshal(yourStruct)
for _, subscriber := range p.subscribers {
    subscriber.Process(ctx, processor.Message{Payload: jsonBytes})
}
```

2. **Add required fields**:
```go
type EnhancedMessage struct {
    YourExistingStruct
    LedgerSequence uint32    `json:"ledger_sequence"`
    Timestamp      time.Time `json:"timestamp"`
    MessageType    string    `json:"message_type"`
}
```

## Performance Optimization

### 1. Buffer Size Tuning
- Development: `buffer_size: 10-100`
- Production: `buffer_size: 1000-10000`
- Adjust based on message rate and size

### 2. Compression Selection
- `snappy`: Fast compression, good for real-time (default)
- `gzip`: Better compression, slower
- `zstd`: Best compression ratio
- `none`: No compression, fastest write

### 3. Partitioning Strategy
- `ledger_day`: Best for time-series queries
- `ledger_range`: Best for ledger-based queries
- `hour`: Best for high-volume real-time data

## Example: Complete Processor Implementation

```go
package processor

import (
    "context"
    "encoding/json"
    "fmt"
    "sync"
    "time"
)

type MyCustomProcessor struct {
    name              string
    subscribers       []Processor
    networkPassphrase string
    mu                sync.Mutex
}

type MyCustomMessage struct {
    Data            MyDataStructure        `json:"data"`
    ProcessorName   string                 `json:"processor_name"`
    MessageType     string                 `json:"message_type"`
    Timestamp       time.Time              `json:"timestamp"`
    LedgerSequence  uint32                 `json:"ledger_sequence"`
    LedgerClosedAt  time.Time              `json:"ledger_closed_at"`
}

func (p *MyCustomProcessor) Process(ctx context.Context, msg Message) error {
    // Process your data...
    myData := processData(msg)
    
    // Create Parquet-compatible message
    outputMsg := MyCustomMessage{
        Data:           myData,
        ProcessorName:  p.name,
        MessageType:    "my_custom_type",
        Timestamp:      time.Now(),
        LedgerSequence: extractLedgerSequence(msg),
        LedgerClosedAt: extractLedgerCloseTime(msg),
    }
    
    // Marshal to JSON
    jsonBytes, err := json.Marshal(outputMsg)
    if err != nil {
        return fmt.Errorf("failed to marshal message: %w", err)
    }
    
    // Forward to subscribers (including Parquet consumer)
    p.mu.Lock()
    subscribers := make([]Processor, len(p.subscribers))
    copy(subscribers, p.subscribers)
    p.mu.Unlock()
    
    for _, subscriber := range subscribers {
        if err := subscriber.Process(ctx, Message{Payload: jsonBytes}); err != nil {
            log.Printf("Error in subscriber %T: %v", subscriber, err)
            // Continue processing other subscribers
        }
    }
    
    return nil
}

func (p *MyCustomProcessor) Subscribe(processor Processor) {
    p.mu.Lock()
    defer p.mu.Unlock()
    p.subscribers = append(p.subscribers, processor)
}
```

## Support and Debugging

If your processor's data isn't appearing in Parquet files:

1. **Enable debug mode** in the Parquet consumer
2. **Use DebugLogger** consumer to inspect message format
3. **Check logs** for schema inference errors
4. **Verify required fields** are present
5. **Test with buffer_size: 1** for immediate feedback

For additional help, check the example processors:
- `processor_contract_data.go` - Contract data with nested structures
- `processor_contract_event.go` - Event processing with timestamps
- `processor_transform_to_app_payment.go` - Payment data transformation