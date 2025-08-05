# Parquet Consumer Migration Example

## Converting an Existing Processor for Parquet Support

This example shows how to modify an existing processor to support the Parquet consumer.

### Before: Original Processor

```go
package processor

import (
    "context"
    "fmt"
    "github.com/stellar/go/xdr"
)

type MyOriginalProcessor struct {
    subscribers []Processor
}

type MyDataStruct struct {
    AccountID    string
    Balance      int64
    TrustLines   []TrustLine
    LastModified uint32
}

func (p *MyOriginalProcessor) Process(ctx context.Context, msg Message) error {
    // Process ledger data
    ledgerMeta := msg.Payload.(xdr.LedgerCloseMeta)
    
    // Extract your data
    myData := MyDataStruct{
        AccountID:    "GABC...",
        Balance:      1000000,
        LastModified: uint32(ledgerMeta.LedgerSequence()),
    }
    
    // Send to subscribers - ORIGINAL WAY
    for _, subscriber := range p.subscribers {
        err := subscriber.Process(ctx, Message{
            Payload: myData,  // Sending struct directly
        })
        if err != nil {
            return err
        }
    }
    
    return nil
}
```

### After: Parquet-Compatible Processor

```go
package processor

import (
    "context"
    "encoding/json"
    "fmt"
    "time"
    "github.com/stellar/go/xdr"
)

type MyEnhancedProcessor struct {
    name        string  // Add processor name
    subscribers []Processor
}

// Create a message wrapper for Parquet compatibility
type MyProcessorMessage struct {
    // Embed original data
    Data MyDataStruct `json:"data"`
    
    // Add required Parquet fields
    ProcessorName  string    `json:"processor_name"`
    MessageType    string    `json:"message_type"`
    LedgerSequence uint32    `json:"ledger_sequence"`
    Timestamp      time.Time `json:"timestamp"`
    LedgerClosedAt time.Time `json:"ledger_closed_at,omitempty"`
}

func (p *MyEnhancedProcessor) Process(ctx context.Context, msg Message) error {
    // Process ledger data
    ledgerMeta := msg.Payload.(xdr.LedgerCloseMeta)
    ledgerHeader := ledgerMeta.LedgerHeaderHistoryEntry().Header
    
    // Extract your data (same as before)
    myData := MyDataStruct{
        AccountID:    "GABC...",
        Balance:      1000000,
        LastModified: uint32(ledgerMeta.LedgerSequence()),
    }
    
    // Create Parquet-compatible message
    parquetMsg := MyProcessorMessage{
        Data:           myData,
        ProcessorName:  p.name,
        MessageType:    "account_balance",
        LedgerSequence: uint32(ledgerHeader.LedgerSeq),
        Timestamp:      time.Now(),
        LedgerClosedAt: time.Unix(int64(ledgerHeader.ScpValue.CloseTime), 0),
    }
    
    // Marshal to JSON for Parquet consumer
    jsonBytes, err := json.Marshal(parquetMsg)
    if err != nil {
        return fmt.Errorf("failed to marshal message: %w", err)
    }
    
    // Send to subscribers - ENHANCED WAY
    for _, subscriber := range p.subscribers {
        err := subscriber.Process(ctx, Message{
            Payload: jsonBytes,  // Sending JSON bytes
        })
        if err != nil {
            // Log but continue for other subscribers
            log.Printf("Error in subscriber %T: %v", subscriber, err)
        }
    }
    
    return nil
}
```

## Step-by-Step Migration Guide

### Step 1: Add Required Fields

Add fields that Parquet consumer needs:

```go
// Before
type YourData struct {
    Field1 string
    Field2 int
}

// After - Option A: Modify existing struct
type YourData struct {
    Field1         string    `json:"field1"`
    Field2         int       `json:"field2"`
    LedgerSequence uint32    `json:"ledger_sequence"`  // Add this
    Timestamp      time.Time `json:"timestamp"`        // Add this
}

// After - Option B: Create wrapper (recommended)
type YourDataMessage struct {
    YourData       YourData  `json:"data"`
    LedgerSequence uint32    `json:"ledger_sequence"`
    Timestamp      time.Time `json:"timestamp"`
    MessageType    string    `json:"message_type"`
}
```

### Step 2: Extract Ledger Information

```go
// From LedgerCloseMeta
ledgerSeq := uint32(ledgerMeta.LedgerSequence())
closeTime := time.Unix(int64(ledgerMeta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0)

// From transaction context
ledgerSeq := tx.Envelope.LedgerSequence
closeTime := tx.LedgerCloseTime

// From your existing data
ledgerSeq := myData.LastModifiedLedger
```

### Step 3: Convert to JSON

```go
// Change from direct struct passing
subscriber.Process(ctx, Message{Payload: myStruct})

// To JSON marshaling
jsonBytes, err := json.Marshal(myStruct)
if err != nil {
    return fmt.Errorf("marshal error: %w", err)
}
subscriber.Process(ctx, Message{Payload: jsonBytes})
```

### Step 4: Update Pipeline Configuration

```yaml
pipelines:
  YourPipeline:
    source:
      type: YourSource
      config:
        # ... your source config
    processors:
      - type: "YourProcessor"
        name: "my-processor"
        config:
          # ... your processor config
    consumers:
      # Keep existing consumers
      - type: SaveToMongoDB
        config:
          # ... existing config
      
      # Add Parquet consumer
      - type: SaveToParquet
        config:
          storage_type: "FS"
          local_path: "~/data/stellar"
          path_prefix: "your_data_type"
          buffer_size: 1000
          partition_by: "ledger_day"
```

## Common Patterns

### Pattern 1: Nested Data Preservation

If your processor has nested data that should be preserved:

```go
type ComplexData struct {
    Transaction TransactionInfo    `json:"transaction"`
    Operations  []OperationDetail  `json:"operations"`
    Effects     []EffectInfo       `json:"effects"`
}

// Wrap for Parquet
type ParquetMessage struct {
    // Flatten important fields for partitioning
    LedgerSequence uint32    `json:"ledger_sequence"`
    Timestamp      time.Time `json:"timestamp"`
    TransactionID  string    `json:"transaction_id"`
    
    // Keep nested data intact
    ComplexData ComplexData `json:"complex_data"`
}
```

### Pattern 2: Multiple Message Types

If your processor emits different types of messages:

```go
func (p *YourProcessor) forwardMessage(ctx context.Context, msgType string, data interface{}, ledgerSeq uint32) error {
    wrapper := map[string]interface{}{
        "message_type":    msgType,
        "ledger_sequence": ledgerSeq,
        "timestamp":       time.Now(),
        "processor_name":  p.name,
    }
    
    // Add type-specific data
    switch msgType {
    case "payment":
        wrapper["payment_data"] = data
    case "trade":
        wrapper["trade_data"] = data
    case "account_update":
        wrapper["account_data"] = data
    }
    
    jsonBytes, err := json.Marshal(wrapper)
    if err != nil {
        return err
    }
    
    // Forward to all subscribers
    for _, sub := range p.subscribers {
        sub.Process(ctx, Message{Payload: jsonBytes})
    }
    
    return nil
}
```

### Pattern 3: Backward Compatibility

Keep backward compatibility with existing consumers:

```go
func (p *YourProcessor) Process(ctx context.Context, msg Message) error {
    // Process your data
    myData := processYourData(msg)
    
    // Check if we should send legacy format
    if p.legacyMode {
        // Old way for existing consumers
        for _, sub := range p.subscribers {
            sub.Process(ctx, Message{Payload: myData})
        }
    } else {
        // New way for Parquet support
        enhanced := EnhanceForParquet(myData, msg)
        jsonBytes, _ := json.Marshal(enhanced)
        for _, sub := range p.subscribers {
            sub.Process(ctx, Message{Payload: jsonBytes})
        }
    }
    
    return nil
}
```

## Testing Your Migration

### 1. Unit Test Example

```go
func TestProcessorParquetCompatibility(t *testing.T) {
    processor := NewYourProcessor(config)
    mockConsumer := &MockParquetConsumer{}
    processor.Subscribe(mockConsumer)
    
    // Send test message
    testMsg := createTestMessage()
    err := processor.Process(context.Background(), testMsg)
    assert.NoError(t, err)
    
    // Verify output format
    assert.Equal(t, 1, len(mockConsumer.Messages))
    
    // Check it's JSON bytes
    payload := mockConsumer.Messages[0].Payload
    jsonBytes, ok := payload.([]byte)
    assert.True(t, ok, "Payload should be []byte")
    
    // Verify required fields
    var decoded map[string]interface{}
    err = json.Unmarshal(jsonBytes, &decoded)
    assert.NoError(t, err)
    
    assert.Contains(t, decoded, "ledger_sequence")
    assert.Contains(t, decoded, "timestamp")
    assert.Contains(t, decoded, "message_type")
}
```

### 2. Integration Test Config

```yaml
# test-parquet-integration.yaml
pipelines:
  TestPipeline:
    source:
      type: FSBufferedStorageSourceAdapter
      config:
        path: "./test-data"
        start_ledger: 1000
        end_ledger: 1010
    processors:
      - type: "YourProcessor"
        name: "test-processor"
    consumers:
      - type: DebugLogger
        config:
          log_prefix: "TEST"
      - type: SaveToParquet
        config:
          storage_type: "FS"
          local_path: "./test-output"
          buffer_size: 1
          debug: true
```

### 3. Verification Script

```bash
#!/bin/bash
# verify-parquet-output.sh

# Run test pipeline
./cdp-pipeline-workflow -config test-parquet-integration.yaml

# Check output
echo "Checking Parquet files..."
find ./test-output -name "*.parquet" -ls

# Verify with Python
python3 << EOF
import pyarrow.parquet as pq
import glob

files = glob.glob('./test-output/**/*.parquet', recursive=True)
for f in files:
    print(f"\nFile: {f}")
    table = pq.read_table(f)
    print(f"Schema: {table.schema}")
    print(f"Rows: {table.num_rows}")
    df = table.to_pandas()
    print(df.head())
    
    # Verify required columns
    assert 'ledger_sequence' in df.columns
    assert 'timestamp' in df.columns
    print("✓ Required fields present")
EOF
```

## Troubleshooting Migration Issues

### Issue: Type Conflicts
```
Error: cannot convert float64 to int64
```
**Solution**: Ensure consistent types in JSON:
```go
// Use explicit types
"amount": int64(12345)      // Not float64
"ledger": uint32(426007)    // Not int
```

### Issue: Missing Required Fields
```
Error: required field ledger_sequence not found
```
**Solution**: Always include required fields:
```go
msg["ledger_sequence"] = extractLedgerSeq(inputMsg)
msg["timestamp"] = time.Now().Format(time.RFC3339)
```

### Issue: Large Message Size
```
Error: message too large for buffer
```
**Solution**: Consider chunking or summarizing:
```go
// Instead of sending all operations at once
if len(operations) > 1000 {
    // Send in chunks
    for i := 0; i < len(operations); i += 100 {
        end := min(i+100, len(operations))
        sendChunk(operations[i:end])
    }
}
```

## Performance Considerations

### Before (Direct Struct)
- ✅ Lower memory usage
- ✅ Faster processing
- ❌ Not compatible with Parquet
- ❌ Limited to specific consumers

### After (JSON Marshaling)
- ❌ Slightly higher memory usage
- ❌ JSON marshaling overhead
- ✅ Compatible with Parquet
- ✅ Works with all consumers
- ✅ Better debugging/logging
- ✅ Schema evolution support

The trade-offs are generally worth it for the flexibility and storage efficiency of Parquet files.

## Next Steps

1. **Test Thoroughly**: Use the test configuration to verify output
2. **Monitor Performance**: Check processing time and memory usage
3. **Optimize Buffer Size**: Tune based on your data volume
4. **Add Metrics**: Track messages processed and files created
5. **Document Changes**: Update your processor's README

Remember: The goal is to make minimal changes while ensuring compatibility with the Parquet consumer. The wrapper pattern is often the safest approach.