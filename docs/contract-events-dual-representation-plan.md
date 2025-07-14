# Contract Events Dual Representation Implementation Plan

## Overview

This document outlines the implementation plan for adding dual representation (raw and decoded) to contract events in the CDP Pipeline Workflow, similar to how contract invocations are currently handled and following Stellar's approach.

## Current State

### Contract Events Processing
- **Processor**: Stores raw `xdr.ScVal` arrays for topics and event data as `json.RawMessage`
- **Consumer**: Saves topics and data as JSONB in PostgreSQL without decoding
- **Storage**: Raw XDR values preserved, requiring client-side decoding for analysis

### Contract Invocations Processing (Reference)
- **Processor**: Provides both `arguments` (raw) and `arguments_decoded` (human-readable)
- **Decoding**: Uses `ConvertScValToJSON` function to decode all ScVal types
- **Storage**: Both representations stored in database for flexibility

## Implementation Goals

1. Add decoded representations for contract event topics and data
2. Maintain backward compatibility with existing raw storage
3. Provide human-readable event data for easier querying and analysis
4. Follow established patterns from contract invocations processor

## Technical Approach

### Phase 1: Update Contract Event Data Structure

#### 1.1 Modify `ContractEvent` struct in `processor/processor_contract_events.go`

```go
type ContractEvent struct {
    // Existing fields...
    Topic             []xdr.ScVal      `json:"topic"`
    TopicDecoded      []interface{}    `json:"topic_decoded"`      // NEW
    Data              json.RawMessage  `json:"data"`
    DataDecoded       interface{}      `json:"data_decoded"`       // NEW
    // Other existing fields...
}
```

#### 1.2 Add decoding logic to `processContractEvent` function

```go
// Decode topics
var topicDecoded []interface{}
for _, topic := range event.Body.V0.Topics {
    decoded, err := ConvertScValToJSON(topic)
    if err != nil {
        log.Printf("Failed to decode topic: %v", err)
        decoded = nil
    }
    topicDecoded = append(topicDecoded, decoded)
}

// Decode event data
var dataDecoded interface{}
if event.Body.V0.Data != nil {
    dataDecoded, err = ConvertScValToJSON(*event.Body.V0.Data)
    if err != nil {
        log.Printf("Failed to decode event data: %v", err)
        dataDecoded = nil
    }
}
```

### Phase 2: Update Database Schema

#### 2.1 Modify `contract_events` table in `consumer/consumer_save_contract_events_to_postgresql.go`

```sql
ALTER TABLE contract_events 
ADD COLUMN IF NOT EXISTS topic_decoded JSONB,
ADD COLUMN IF NOT EXISTS data_decoded JSONB;
```

#### 2.2 Update insert statement to include decoded fields

```sql
INSERT INTO contract_events (
    timestamp, ledger_sequence, transaction_hash, contract_id, 
    type, topic, topic_decoded, data, data_decoded, 
    in_successful_tx, event_index, operation_index, network_passphrase
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
```

### Phase 3: Add Event Type Detection (Optional Enhancement)

#### 3.1 Create event type detection utility

```go
func DetectEventType(topics []xdr.ScVal) string {
    if len(topics) > 0 {
        eventName, err := ConvertScValToJSON(topics[0])
        if err == nil {
            if str, ok := eventName.(string); ok {
                return str
            }
        }
    }
    return "unknown"
}
```

#### 3.2 Add common event decoders

Create specialized decoders for common event types:
- Transfer events
- Mint/Burn events
- Swap events
- Liquidity events

### Phase 4: Testing and Migration

#### 4.1 Unit Tests
- Test ScVal decoding for all supported types
- Test event topic and data decoding
- Test database insertion with decoded fields

#### 4.2 Integration Tests
- Process sample ledgers with known contract events
- Verify dual representation storage
- Test querying capabilities with decoded data

#### 4.3 Migration Strategy
- Add new columns without removing existing ones
- Deploy processor updates to populate decoded fields
- Backfill historical data if needed (separate script)

## Implementation Timeline

### Week 1: Core Implementation
- Update ContractEvent struct
- Implement decoding logic
- Update database schema

### Week 2: Testing and Refinement
- Write comprehensive tests
- Handle edge cases
- Performance optimization

### Week 3: Deployment
- Deploy to development environment
- Monitor and validate results
- Deploy to production

## Benefits

1. **Improved Queryability**: Query events by decoded values instead of raw XDR
2. **Better Analytics**: Easier to analyze contract behavior and patterns
3. **Developer Experience**: No need for client-side XDR decoding
4. **Flexibility**: Both raw and decoded data available for different use cases

## Example Output

### Before (Current)
```json
{
  "topic": [
    {"type": "ScvSymbol", "sym": "transfer"},
    {"type": "ScvAddress", "address": {...}}
  ],
  "data": {
    "V0": {
      "Data": {
        "I128": {"Hi": 0, "Lo": 1000000}
      }
    }
  }
}
```

### After (With Dual Representation)
```json
{
  "topic": [...],  // Raw format preserved
  "topic_decoded": [
    "transfer",
    "CCFZRNQVAY52P7LRY7GLA2R2XQMWMYYB6WZ7EKNO3BV3LP3QGXFB4VKJ"
  ],
  "data": {...},   // Raw format preserved
  "data_decoded": {
    "type": "i128",
    "value": "1000000"
  }
}
```

## Considerations

1. **Storage Impact**: Decoded fields will increase storage requirements
2. **Processing Time**: Decoding adds processing overhead
3. **Backwards Compatibility**: Existing queries using raw fields continue to work
4. **Null Handling**: Failed decoding should store null, not fail the entire event

## Future Enhancements

1. **Event Registry**: Maintain registry of known event signatures
2. **Custom Decoders**: Allow plugins for protocol-specific event decoding
3. **Event Streaming**: Real-time decoded event streams via WebSocket
4. **Event Analytics**: Pre-aggregated metrics based on decoded events

## References

- [Stellar Contract Events Processing](https://github.com/stellar/go/blob/master/processors/contract/contract_events.go)
- Existing Contract Invocations implementation in `processor/processor_contract_invocation.go`
- ScVal conversion utilities in `processor/scval_converter.go`