# Contract Invocation Processor Dual Representation Implementation Plan

## Overview

This document outlines the implementation plan for adding dual representation (raw XDR data + decoded human-readable format) to the Contract Invocation Processor. This follows the pattern established in the Contract Events Processor and ensures consistency across contract-related data processing.

## Background

The Contract Events Processor already implements dual representation for topics and data fields:
- `Topics []xdr.ScVal` (raw) + `TopicsDecoded []interface{}` (decoded)
- `Data json.RawMessage` (semi-raw) + `DataDecoded interface{}` (decoded)

The Contract Invocation Processor should follow this pattern for better data accessibility and precision.

## Current State Analysis

### Fields Already Supporting Dual Representation
1. **ContractInvocation.Arguments**
   - Raw: `Arguments []json.RawMessage`
   - Decoded: `ArgumentsDecoded map[string]interface{}`

### Fields Requiring Dual Representation

#### 1. DiagnosticEvent Structure
Current:
```go
type DiagnosticEvent struct {
    ContractID string          `json:"contract_id"`
    Topics     []string        `json:"topics"`        // Stores JSON strings
    Data       json.RawMessage `json:"data"`          // No decoded version
}
```

Required:
```go
type DiagnosticEvent struct {
    ContractID    string        `json:"contract_id"`
    Topics        []xdr.ScVal   `json:"topics"`           // Raw XDR
    TopicsDecoded []interface{} `json:"topics_decoded"`   // Decoded
    Data          xdr.ScVal     `json:"data"`             // Raw XDR
    DataDecoded   interface{}   `json:"data_decoded"`     // Decoded
}
```

#### 2. StateChange Structure
Current:
```go
type StateChange struct {
    ContractID string          `json:"contract_id"`
    Key        string          `json:"key"`
    OldValue   json.RawMessage `json:"old_value,omitempty"`
    NewValue   json.RawMessage `json:"new_value,omitempty"`
    Operation  string          `json:"operation"`
}
```

Required:
```go
type StateChange struct {
    ContractID    string      `json:"contract_id"`
    KeyRaw        xdr.ScVal   `json:"key_raw"`          // Raw XDR
    Key           string      `json:"key"`              // Decoded
    OldValueRaw   xdr.ScVal   `json:"old_value_raw"`    // Raw XDR
    OldValue      interface{} `json:"old_value"`        // Decoded
    NewValueRaw   xdr.ScVal   `json:"new_value_raw"`    // Raw XDR
    NewValue      interface{} `json:"new_value"`        // Decoded
    Operation     string      `json:"operation"`
}
```

## Implementation Phases

### Phase 1: Update Data Structures (Priority: High)

1. **Update struct definitions** in `processor/processor_contract_invocation.go`
2. **Maintain backward compatibility** by keeping existing field names for decoded versions
3. **Add new raw fields** with appropriate naming convention (`*Raw` suffix for clarity)

### Phase 2: Update Extraction Methods

#### 2.1 Modify `extractDiagnosticEvents()`
```go
func (p *ContractInvocationProcessor) extractDiagnosticEvents(tx ingest.LedgerTransaction, opIndex int) []DiagnosticEvent {
    var events []DiagnosticEvent
    
    if tx.UnsafeMeta.V == 3 {
        sorobanMeta := tx.UnsafeMeta.V3.SorobanMeta
        if sorobanMeta != nil && sorobanMeta.Events != nil {
            for _, event := range sorobanMeta.Events {
                // Store raw topics
                topics := event.Body.V0.Topics
                
                // Decode topics
                var topicsDecoded []interface{}
                for _, topic := range topics {
                    decoded, _ := ConvertScValToJSON(topic)
                    topicsDecoded = append(topicsDecoded, decoded)
                }
                
                // Store raw data
                data := event.Body.V0.Data
                
                // Decode data
                dataDecoded, _ := ConvertScValToJSON(data)
                
                events = append(events, DiagnosticEvent{
                    ContractID:    contractID,
                    Topics:        topics,
                    TopicsDecoded: topicsDecoded,
                    Data:          data,
                    DataDecoded:   dataDecoded,
                })
            }
        }
    }
    
    return events
}
```

#### 2.2 Enhance `extractStateChanges()`
- Parse ledger entry changes from transaction meta
- Extract contract data entries
- Store both raw XDR and decoded values

#### 2.3 Consider updating `extractArguments()`
- Add `ArgumentsRaw []xdr.ScVal` field to ContractInvocation
- Store original ScVal arguments alongside current implementation

### Phase 3: Testing Strategy

1. **Unit Tests**
   - Verify both raw and decoded fields are populated correctly
   - Test ConvertScValToJSON for all ScVal types
   - Ensure nil/empty values are handled properly

2. **Integration Tests**
   - Process sample ledgers with known contract invocations
   - Verify dual representation across different contract types
   - Test with complex nested data structures

3. **Performance Tests**
   - Measure memory impact of storing dual representation
   - Benchmark processing time with additional conversions
   - Monitor serialization/deserialization performance

4. **Backward Compatibility Tests**
   - Ensure existing consumers continue to work
   - Verify JSON output structure remains compatible
   - Test migration path for existing data

### Phase 4: Rollout Plan

1. **Development Environment**
   - Implement changes in feature branch
   - Test with development pipelines
   - Validate against testnet data

2. **Staging Environment**
   - Deploy to staging with limited data
   - Monitor performance metrics
   - Gather feedback from internal consumers

3. **Production Rollout**
   - Gradual rollout with feature flags if needed
   - Monitor system performance
   - Keep rollback plan ready

### Phase 5: Documentation and Communication

1. **Update Documentation**
   - Document new field structure
   - Provide migration guide for consumers
   - Include example JSON outputs

2. **Consumer Communication**
   - Notify downstream consumers of new fields
   - Provide timeline for adoption
   - Offer support during migration

## Benefits

1. **Data Precision**: Raw XDR preserves exact blockchain state
2. **Flexibility**: Consumers can choose appropriate format
3. **Consistency**: Matches pattern from Contract Events Processor
4. **Debugging**: Easier troubleshooting with both representations
5. **Future-proof**: Raw data enables reprocessing if decoding logic changes

## Use Cases

### Analytics Systems
- Use decoded data for queries and aggregations
- Human-readable format for dashboards

### Replay Systems
- Use raw XDR for exact transaction replay
- Cryptographic verification of data

### Integration APIs
- Provide both formats for maximum compatibility
- Let consumers choose based on their needs

### Audit Systems
- Raw data for compliance and verification
- Decoded data for reporting

## Migration Considerations

1. **No Breaking Changes**: Existing decoded fields retain same names and structure
2. **Additive Only**: New fields are added, nothing removed
3. **Gradual Adoption**: Consumers can migrate at their own pace
4. **Clear Documentation**: Migration path well documented

## Future Considerations

1. **Standardize Across Processors**: Apply pattern to other processors in `pkg/processor/`
2. **Common Types**: Update `pkg/processor/contract/common/types.go`
3. **Schema Versioning**: Consider versioning strategy for future changes
4. **Compression**: Evaluate compression for raw XDR data if size becomes concern

## Implementation Checklist

- [ ] Update DiagnosticEvent struct
- [ ] Update StateChange struct  
- [ ] Modify extractDiagnosticEvents method
- [ ] Implement extractStateChanges with dual representation
- [ ] Add unit tests for dual representation
- [ ] Add integration tests
- [ ] Update documentation
- [ ] Create migration guide
- [ ] Performance testing
- [ ] Staging deployment
- [ ] Production rollout
- [ ] Consumer communication

## Timeline Estimate

- Phase 1-2 (Implementation): 3-5 days
- Phase 3 (Testing): 2-3 days
- Phase 4 (Rollout): 1 week
- Phase 5 (Documentation): 2 days

Total: ~2-3 weeks for complete implementation and rollout

## Risks and Mitigation

1. **Risk**: Increased memory usage
   - **Mitigation**: Performance testing, optimization if needed

2. **Risk**: Breaking existing consumers
   - **Mitigation**: Backward compatibility, extensive testing

3. **Risk**: Complex migration
   - **Mitigation**: Clear documentation, phased rollout

4. **Risk**: Performance degradation
   - **Mitigation**: Benchmarking, optimization, caching strategies