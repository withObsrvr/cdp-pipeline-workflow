# Parquet Consumer Quick Reference

## Processor Checklist ✓

### Minimum Requirements
- [ ] Message sent as JSON bytes or map[string]interface{}
- [ ] Include `ledger_sequence` field (as number)
- [ ] Include timestamp field for date partitioning

### Recommended Fields
- [ ] `message_type` - Identifies your data type
- [ ] `processor_name` - Source processor identification  
- [ ] `timestamp` or nested `closed_at` - For partitioning
- [ ] Consistent field types across all messages

## Quick Implementation

### 1. Minimal Message Structure
```go
msg := map[string]interface{}{
    "ledger_sequence": 426007,                      // Required
    "timestamp": time.Now().Format(time.RFC3339),   // Required for partitioning
    "message_type": "your_type",                    // Recommended
    "your_data": yourDataHere,                      // Your actual data
}

jsonBytes, _ := json.Marshal(msg)
subscriber.Process(ctx, processor.Message{Payload: jsonBytes})
```

### 2. For Contract Data
```go
msg := map[string]interface{}{
    "message_type": "contract_data",
    "ledger_sequence": ledgerSeq,
    "contract_data": map[string]interface{}{
        "contract_id": contractID,
        "closed_at": closedAt.Format(time.RFC3339),  // Important!
        // ... other contract fields
    },
}
```

### 3. For Events
```go
msg := map[string]interface{}{
    "message_type": "event", 
    "ledger": ledgerNum,
    "ledger_closed_at": closedAt.Format(time.RFC3339),
    "type": "contract_event",
    // ... event data
}
```

## Common Pitfalls to Avoid

### ❌ DON'T: Inconsistent Types
```go
// Bad - type changes between messages
msg["amount"] = "12345"    // Sometimes string
msg["amount"] = 12345      // Sometimes number
```

### ✅ DO: Consistent Types
```go
// Good - always the same type
msg["amount"] = int64(12345)     // Always number
msg["amount_str"] = "12345"      // Always string
```

### ❌ DON'T: Missing Required Fields
```go
// Bad - no ledger info
msg := map[string]interface{}{
    "data": myData,
}
```

### ✅ DO: Include Required Fields
```go
// Good - includes required fields
msg := map[string]interface{}{
    "data": myData,
    "ledger_sequence": 426007,
    "timestamp": time.Now().Format(time.RFC3339),
}
```

### ❌ DON'T: Wrong Time Format
```go
// Bad - various time formats
msg["time"] = "2025-08-05 14:30:45"          // Wrong format
msg["time"] = time.Now().Unix()              // Unix timestamp
msg["time"] = "Aug 5, 2025"                  // Human readable
```

### ✅ DO: RFC3339 Format
```go
// Good - RFC3339 format
msg["timestamp"] = time.Now().Format(time.RFC3339)
// Result: "2025-08-05T14:30:45Z"
```

## Debug Commands

### Test Your Processor Output
```bash
# 1. Use DebugLogger to see message format
# Add to your pipeline config:
consumers:
  - type: DebugLogger
    config:
      log_prefix: "MY_MSG"

# 2. Run and check output
./cdp-pipeline-workflow -config test.yaml 2>&1 | grep "MY_MSG"

# 3. Check for Parquet files
find ./output -name "*.parquet" -ls
```

### Verify Parquet Schema
```python
# Check schema with Python
import pyarrow.parquet as pq
table = pq.read_table('output.parquet')
print(table.schema)
print(f"Rows: {table.num_rows}")
print(table.to_pandas().head())
```

## Configuration Examples

### Testing Configuration
```yaml
- type: SaveToParquet
  config:
    storage_type: "FS"
    local_path: "./test-output"
    buffer_size: 1          # Immediate flush
    debug: true             # Enable logging
    partition_by: "ledger_day"
```

### Production Configuration  
```yaml
- type: SaveToParquet
  config:
    storage_type: "GCS"     # or "S3"
    bucket_name: "my-data"
    path_prefix: "stellar"
    buffer_size: 10000      # Buffer for efficiency
    compression: "snappy"   # Fast compression
    rotation_interval_minutes: 60
    partition_by: "ledger_day"
```

## Field Reference

| Field | Type | Required | Purpose |
|-------|------|----------|----------|
| `ledger_sequence` | number | Yes | Identifies ledger, used in filenames |
| `timestamp` | string (RFC3339) | Recommended | Date partitioning |
| `message_type` | string | Recommended | Schema identification |
| `processor_name` | string | Recommended | Source tracking |
| `closed_at` | string (RFC3339) | For contract data | Ledger close time |
| `ledger_closed_at` | string (RFC3339) | For events | Ledger close time |

## Schema Type Mapping

| Go Type | Parquet Type | Notes |
|---------|--------------|-------|
| `string` | STRING | Use for text, IDs |
| `int`, `int64` | INT64 | Ledger numbers |
| `uint32` | UINT32 | Ledger sequences |
| `float64` | DOUBLE | JSON numbers |
| `bool` | BOOLEAN | True/false values |
| `time.Time` | TIMESTAMP | Use RFC3339 string |
| `[]byte` | BINARY | Avoid if possible |
| `nil` | NULL | Nullable fields |

## Contact & Support

- Check existing processors in `/processor` directory for examples
- Use DebugLogger consumer to inspect message formats
- Enable `debug: true` in Parquet consumer config for detailed logs
- Test with `buffer_size: 1` for immediate feedback during development