# Contract Data Processor Implementation Plan

## Overview

This document outlines the implementation plan for creating a new `processor_contract_data.go` processor that leverages the stellar/go protocol-23 contract data processor while integrating with the CDP pipeline architecture.

## Objectives

1. Create a new contract data processor that uses stellar/go's official `ContractDataOutput` structure
2. Maintain compatibility with the existing CDP pipeline architecture
3. Ensure Protocol 23 compatibility from the start
4. Reduce maintenance overhead by leveraging official Stellar processors

## Analysis of Stellar/Go Contract Data Processor

Based on the stellar/go protocol-23 contract data processor analysis:

### Key Components

1. **ContractDataOutput Structure** (20+ fields):
   - `ContractId`: Contract identifier
   - `ContractKeyHash`: Unique hash for the contract key
   - `Key` / `KeyDecoded`: Contract data key (raw and decoded maps)
   - `Val` / `ValDecoded`: Contract data value (raw and decoded maps)
   - `ContractDataXDR`: **Base64-encoded XDR representation** of contract data
   - `ContractDurability`: Data persistence type
   - `ContractDataAssetCode`, `ContractDataAssetIssuer`, `ContractDataAssetType`: Asset information
   - `ContractDataBalanceHolder`: Account holding the balance
   - `ContractDataBalance`: Contract balance amount
   - `LedgerEntryChange`: Type of change (created/updated/removed)
   - `LastModifiedLedger`: When the entry was last changed
   - `LedgerSequence`: Current ledger number
   - `ClosedAt`: Ledger close timestamp
   - `Deleted`: Whether the entry was removed

2. **Core Transformation Method**:
   ```go
   func TransformContractData(ledgerChange ingest.Change, header xdr.LedgerHeader, passphrase string) (ContractDataOutput, error)
   ```

3. **Utility Functions**:
   - `AssetFromContractData()`: Extracts asset information
   - `ContractBalanceFromContractData()`: Retrieves balance data
   - `utils.ExtractEntryFromChange()`: Extracts entry details from changes

## Implementation Strategy

### Phase 1: Direct Integration Approach

**Recommended**: Use stellar/go processor directly within CDP pipeline wrapper

```go
// processor_contract_data.go structure
type ContractDataProcessor struct {
    name             string
    subscribers      []types.Processor
    networkPassphrase string
    mu              sync.Mutex
}

type ContractDataMessage struct {
    ContractData contract.ContractDataOutput `json:"contract_data"`
    Timestamp    time.Time                   `json:"timestamp"`
    LedgerSeq    uint32                      `json:"ledger_sequence"`
}
```

### Phase 2: Integration Points

1. **Import stellar/go processor**:
   ```go
   import "github.com/stellar/go/processors/contract"
   ```

2. **Use official transformation function**:
   ```go
   contractData, err := contract.TransformContractData(change, ledgerHeader, p.networkPassphrase)
   ```

3. **Wrap in CDP message format**:
   ```go
   message := types.Message{
       Payload: ContractDataMessage{
           ContractData: contractData,
           ContractId:   contractData.ContractId,  // Extract for easy access
           Timestamp:    time.Now(),
           LedgerSeq:    ledgerHeader.LedgerSeq,
       },
   }
   ```

## Detailed Implementation Plan

### File Structure

**New File**: `processor/processor_contract_data.go`

### Dependencies

```go
import (
    "context"
    "fmt"
    "io"
    "log"
    "sync"
    "time"

    "github.com/stellar/go/ingest"
    "github.com/stellar/go/processors/contract"
    "github.com/stellar/go/xdr"
    
    "github.com/withObsrvr/cdp-pipeline-workflow/types"
)
```

### Core Implementation

#### 1. Processor Structure

```go
type ContractDataProcessor struct {
    name             string
    subscribers      []types.Processor
    networkPassphrase string
    mu              sync.Mutex
}

func NewContractDataProcessor(name, networkPassphrase string) *ContractDataProcessor {
    return &ContractDataProcessor{
        name:             name,
        networkPassphrase: networkPassphrase,
        subscribers:      make([]types.Processor, 0),
    }
}
```

#### 2. Message Structure

```go
type ContractDataMessage struct {
    ContractData contract.ContractDataOutput `json:"contract_data"`
    ContractId   string                      `json:"contract_id"`    // Extracted for easy access
    Timestamp    time.Time                   `json:"timestamp"`
    LedgerSeq    uint32                      `json:"ledger_sequence"`
    
    // Additional CDP-specific fields if needed
    ProcessorName string `json:"processor_name"`
    MessageType   string `json:"message_type"`
}
```

#### 3. Core Processing Logic

```go
func (p *ContractDataProcessor) Process(ctx context.Context, msg types.Message) error {
    ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
    if !ok {
        return fmt.Errorf("expected LedgerCloseMeta, got %T", msg.Payload)
    }

    // Create transaction reader
    txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
        p.networkPassphrase, ledgerCloseMeta)
    if err != nil {
        return fmt.Errorf("error creating transaction reader: %w", err)
    }
    defer txReader.Close()

    ledgerHeader := ledgerCloseMeta.LedgerHeaderHistoryEntry().Header

    // Process each transaction
    for {
        tx, err := txReader.Read()
        if err == io.EOF {
            break
        }
        if err != nil {
            return fmt.Errorf("error reading transaction: %w", err)
        }

        // Get changes from transaction
        changes, err := tx.GetChanges()
        if err != nil {
            log.Printf("Error getting changes: %v", err)
            continue
        }

        // Process each change
        for _, change := range changes {
            // Filter for contract data entries only
            if !isContractDataChange(change) {
                continue
            }

            // Use stellar/go processor directly
            contractData, err := contract.TransformContractData(
                change, ledgerHeader, p.networkPassphrase)
            if err != nil {
                log.Printf("Error transforming contract data: %v", err)
                continue
            }

            // Create CDP message
            contractMsg := ContractDataMessage{
                ContractData:  contractData,
                ContractId:    contractData.ContractId,  // Extract for easy access
                Timestamp:     time.Now(),
                LedgerSeq:     uint32(ledgerHeader.LedgerSeq),
                ProcessorName: p.name,
                MessageType:   "contract_data",
            }

            // Send to subscribers
            p.mu.Lock()
            for _, subscriber := range p.subscribers {
                go func(sub types.Processor) {
                    if err := sub.Process(ctx, types.Message{Payload: contractMsg}); err != nil {
                        log.Printf("Error processing contract data message: %v", err)
                    }
                }(subscriber)
            }
            p.mu.Unlock()
        }
    }

    return nil
}
```

#### 4. Helper Functions

```go
func isContractDataChange(change ingest.Change) bool {
    // Check if this change involves contract data
    if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeContractData {
        return true
    }
    if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeContractData {
        return true
    }
    return false
}
```

#### 5. CDP Pipeline Integration

```go
func (p *ContractDataProcessor) Subscribe(processor types.Processor) {
    p.mu.Lock()
    defer p.mu.Unlock()
    p.subscribers = append(p.subscribers, processor)
}

func (p *ContractDataProcessor) GetName() string {
    return p.name
}
```

## Configuration Integration

### YAML Configuration

```yaml
processors:
  - type: "contract_data"
    name: "contract-data-processor"
    config:
      network_passphrase: "Test SDF Network ; September 2015"
```

### Factory Pattern Update

Add to the processor factory in `main.go`:

```go
case "contract_data":
    networkPassphrase := getNetworkPassphrase(processorConfig)
    return NewContractDataProcessor(processorConfig.Name, networkPassphrase), nil
```

## Benefits of This Approach

1. **Protocol Compatibility**: Automatically stays compatible with stellar/go updates
2. **Reduced Maintenance**: Leverages officially maintained transformation logic
3. **Consistency**: Ensures data matches stellar/go reference implementation
4. **Flexibility**: Can still add CDP-specific wrapping and routing logic
5. **Raw XDR Access**: The `ContractDataXDR` field provides base64-encoded XDR for custom processing
6. **Dual Format Support**: Both raw maps and decoded human-readable formats available

## Testing Strategy

1. **Unit Tests**: Test processor creation and configuration
2. **Integration Tests**: Test with sample contract data ledger entries
3. **Compatibility Tests**: Verify output matches stellar/go processor directly
4. **Pipeline Tests**: Ensure integration with existing CDP consumers

## Migration Path

1. **Phase 1**: Implement new processor alongside existing ones
2. **Phase 2**: Add configuration options for contract data processing
3. **Phase 3**: Test with real Protocol 23 data
4. **Phase 4**: Consider replacing custom contract processing logic

## Considerations

### Advantages
- Leverages official Stellar processor
- Automatically Protocol 23 compatible
- Reduced maintenance overhead
- Consistent with Stellar reference implementation

### Trade-offs
- Less control over output format
- Dependency on stellar/go processor API stability
- May need additional wrapping for CDP-specific needs

## Implementation Status

✅ **COMPLETED**: Contract data processor has been successfully implemented with the following features:

### Created Files
1. **`processor/processor_contract_data.go`** - Main processor implementation
2. **Updated `main.go`** - Added factory pattern support for `contract_data` type

### Current Implementation
- Uses stellar/go's `TransformContractDataStruct` API
- Protocol 23 compatible field usage where available  
- Provides ContractId as top-level field for easy filtering
- Includes full ContractDataXDR for raw XDR access
- Integrated with CDP pipeline architecture
- Follows CDP pattern for configuration extraction (same as ContractInvocationProcessor)
- **JSON serialization**: Properly serializes data to []byte before sending to consumers (matches ContractEventProcessor pattern)
- **Synchronous processing**: Uses synchronous subscriber processing to prevent goroutine leaks and mutex contention

### Configuration Example
```yaml
processors:
  - type: "contract_data"
    config:
      name: "my-contract-data-processor"
      network_passphrase: "Test SDF Network ; September 2015"
```

## Protocol 23 Migration Notes

The current implementation uses the available stellar/go API. When the official Protocol 23 branch becomes available with the `TransformContractData` function (as seen in https://github.com/stellar/go/blob/protocol-23/processors/contract/contract_data.go), the processor can be updated by:

1. Updating the API call from:
   ```go
   transformer := contract.NewTransformContractDataStruct(...)
   contractData, err, shouldContinue := transformer.TransformContractData(...)
   ```

2. To the simpler Protocol 23 API:
   ```go
   contractData, err := contract.TransformContractData(change, ledgerHeader, p.networkPassphrase)
   ```

## Next Steps

1. ✅ Create the `processor_contract_data.go` file
2. ✅ Implement core processor logic  
3. ✅ Add configuration support
4. 🔄 Create unit tests
5. 🔄 Test with real contract data
6. 🔄 Document usage and examples
7. 🔄 Update to Protocol 23 API when available

## Timeline

- **Week 1**: Core implementation and basic testing
- **Week 2**: Integration with CDP pipeline and advanced testing
- **Week 3**: Documentation and production readiness
- **Week 4**: Deployment and monitoring

This implementation plan provides a clear path to create a Protocol 23 compatible contract data processor that leverages Stellar's official implementation while maintaining integration with the CDP pipeline architecture.