# Cross-Contract Call Capture Implementation Plan

## Executive Summary

This document outlines the implementation plan for capturing cross-contract calls in the CDP Pipeline Workflow. Currently, the system only captures direct `InvokeHostFunction` operations, missing the complete execution tree of contract-to-contract calls. This plan details how to leverage existing Stellar XDR structures to capture the full call graph.

## Table of Contents

1. [Current State Analysis](#current-state-analysis)
2. [Technical Architecture](#technical-architecture)
3. [Implementation Strategy](#implementation-strategy)
4. [Code Changes Required](#code-changes-required)
5. [Data Structures](#data-structures)
6. [Testing Strategy](#testing-strategy)
7. [Migration Plan](#migration-plan)
8. [Performance Considerations](#performance-considerations)
9. [Future Enhancements](#future-enhancements)

## Current State Analysis

### What We Currently Capture

1. **Direct Invocations Only**
   - Operations with type `InvokeHostFunction` from transaction envelope
   - Top-level contract calls initiated by user accounts
   - Basic invocation metadata (contract ID, function name, arguments)

2. **Missing Data**
   - Contract-to-contract calls during execution
   - Complete call tree/graph
   - Authorization chains
   - Sub-invocation results and gas consumption

### Existing Infrastructure

The codebase already contains infrastructure for cross-contract calls:

```go
// ContractCall struct exists but is unused
type ContractCall struct {
    FromContract string `json:"from_contract"`
    ToContract   string `json:"to_contract"`
    Function     string `json:"function"`
    Successful   bool   `json:"successful"`
}

// processInvocation method exists but is marked as unused
func (p *ContractInvocationProcessor) processInvocation(
    invocation *xdr.SorobanAuthorizedInvocation,
    fromContract string,
    calls *[]ContractCall,
)
```

## Technical Architecture

### Available Data Sources

1. **Authorization Tree** (Primary Source)
   - Located in `InvokeHostFunctionOp.Auth[]`
   - Contains `SorobanAuthorizationEntry` with full invocation tree
   - Pre-execution authorization structure
   - Shows what contracts CAN be called

2. **Diagnostic Events** (Secondary Source)
   - Available via `tx.GetDiagnosticEvents()`
   - Runtime execution information
   - Shows what contracts WERE actually called
   - Contains execution status and errors

3. **Transaction Metadata**
   - `UnsafeMeta.V3.SorobanMeta`
   - Contains events and return values
   - Post-execution state changes

### XDR Structure Overview

```
InvokeHostFunctionOp
├── HostFunction
│   └── InvokeContract
│       ├── ContractAddress
│       ├── FunctionName
│       └── Args[]
└── Auth[] (SorobanAuthorizationEntry)
    ├── Credentials
    └── RootInvocation (SorobanAuthorizedInvocation)
        ├── Function
        │   └── ContractFn
        │       ├── ContractAddress
        │       ├── FunctionName
        │       └── Args[]
        └── SubInvocations[] (recursive)
```

## Implementation Strategy

### Phase 1: Basic Authorization Tree Capture

1. **Enable Authorization Processing**
   - Remove `//nolint:unused` from `processInvocation`
   - Extract auth entries in `processContractInvocation`
   - Build basic call tree from authorization data

2. **Update Data Structures**
   ```go
   type ContractCall struct {
       FromContract   string `json:"from_contract"`
       ToContract     string `json:"to_contract"`
       Function       string `json:"function"`
       CallDepth      int    `json:"call_depth"`
       AuthType       string `json:"auth_type"` // "source_account" or "contract"
       Successful     bool   `json:"successful"`
       ExecutionOrder int    `json:"execution_order"`
   }
   ```

### Phase 2: Diagnostic Event Correlation

1. **Parse Diagnostic Events**
   - Extract contract invocation patterns
   - Match with authorization tree
   - Determine actual execution vs authorized calls

2. **Event Pattern Recognition**
   ```go
   // Identify invocation patterns in diagnostic events
   type InvocationEvent struct {
       EventType string
       Contract  string
       Function  string
       Depth     int
       Success   bool
   }
   ```

### Phase 3: Complete Call Graph Construction

1. **Merge Authorization and Execution Data**
   - Combine auth tree with diagnostic events
   - Build complete call graph with execution status
   - Track failed/skipped calls

2. **Enhanced Metadata**
   ```go
   type EnhancedContractCall struct {
       ContractCall
       Arguments      []json.RawMessage `json:"arguments,omitempty"`
       ReturnValue    json.RawMessage   `json:"return_value,omitempty"`
       GasConsumed    uint64           `json:"gas_consumed,omitempty"`
       ExecutionTime  time.Duration    `json:"execution_time,omitempty"`
       ErrorMessage   string           `json:"error_message,omitempty"`
   }
   ```

## Code Changes Required

### 1. Update `processContractInvocation` Method

```go
func (p *ContractInvocationProcessor) processContractInvocation(
    tx ingest.LedgerTransaction,
    opIndex int,
    op xdr.Operation,
    meta xdr.LedgerCloseMeta,
    archiveMetadata *ArchiveSourceMetadata,
) (*ContractInvocation, error) {
    // ... existing code ...

    // NEW: Extract cross-contract calls from authorization
    invocation.ContractCalls = p.extractContractCallsFromAuth(invokeHostFunction)
    
    // NEW: Correlate with diagnostic events
    p.correlateWithDiagnosticEvents(tx, opIndex, invocation)
    
    // ... rest of existing code ...
}
```

### 2. Implement Authorization Extraction

```go
func (p *ContractInvocationProcessor) extractContractCallsFromAuth(
    invokeOp xdr.InvokeHostFunctionOp,
) []ContractCall {
    var calls []ContractCall
    
    // Get the main contract being invoked
    mainContract := ""
    if invokeOp.HostFunction.Type == xdr.HostFunctionTypeHostFunctionTypeInvokeContract {
        contractIDBytes := invokeOp.HostFunction.InvokeContract.ContractAddress.ContractId
        mainContract, _ = strkey.Encode(strkey.VersionByteContract, contractIDBytes[:])
    }
    
    // Process each authorization entry
    for _, authEntry := range invokeOp.Auth {
        // Process the authorization tree
        p.processAuthorizationTree(
            &authEntry.RootInvocation,
            mainContract,
            &calls,
            0, // depth
        )
    }
    
    return calls
}
```

### 3. Implement Authorization Tree Traversal

```go
func (p *ContractInvocationProcessor) processAuthorizationTree(
    invocation *xdr.SorobanAuthorizedInvocation,
    fromContract string,
    calls *[]ContractCall,
    depth int,
) {
    if invocation == nil {
        return
    }
    
    // Extract contract and function from this invocation
    if invocation.Function.Type == xdr.SorobanAuthorizedFunctionTypeSorobanAuthorizedFunctionTypeContractFn {
        contractFn := invocation.Function.ContractFn
        
        // Get contract ID
        contractIDBytes := contractFn.ContractAddress.ContractId
        toContract, _ := strkey.Encode(strkey.VersionByteContract, contractIDBytes[:])
        
        // Get function name
        functionName := string(contractFn.FunctionName)
        
        // Add to calls if it's a cross-contract call
        if fromContract != "" && toContract != fromContract {
            *calls = append(*calls, ContractCall{
                FromContract:   fromContract,
                ToContract:     toContract,
                Function:       functionName,
                CallDepth:      depth,
                AuthType:       "contract",
                ExecutionOrder: len(*calls),
            })
        }
        
        // Process sub-invocations recursively
        for _, subInvocation := range invocation.SubInvocations {
            p.processAuthorizationTree(
                &subInvocation,
                toContract,
                calls,
                depth + 1,
            )
        }
    }
}
```

### 4. Diagnostic Event Correlation

```go
func (p *ContractInvocationProcessor) correlateWithDiagnosticEvents(
    tx ingest.LedgerTransaction,
    opIndex int,
    invocation *ContractInvocation,
) {
    diagnosticEvents, err := tx.GetDiagnosticEvents()
    if err != nil {
        return
    }
    
    // Create a map of authorized calls for quick lookup
    authCallMap := make(map[string]*ContractCall)
    for i := range invocation.ContractCalls {
        call := &invocation.ContractCalls[i]
        key := fmt.Sprintf("%s->%s:%s", call.FromContract, call.ToContract, call.Function)
        authCallMap[key] = call
    }
    
    // Process diagnostic events to determine actual execution
    for _, event := range diagnosticEvents {
        if event.Event.Type == xdr.ContractEventTypeContract {
            // Parse event for invocation information
            contractID, _ := strkey.Encode(strkey.VersionByteContract, event.Event.ContractId[:])
            
            // TODO: Extract invocation details from event data
            // This requires parsing the event topics/data for invocation patterns
            
            // Mark corresponding authorized call as successful if found
            if event.InSuccessfulContractCall {
                // Update success status in authCallMap
            }
        }
    }
}
```

## Data Structures

### Enhanced ContractInvocation Structure

```go
type ContractInvocation struct {
    // ... existing fields ...
    
    // Enhanced contract calls with full metadata
    ContractCalls []EnhancedContractCall `json:"contract_calls,omitempty"`
    
    // Call graph visualization data
    CallGraph *CallGraphData `json:"call_graph,omitempty"`
}

type CallGraphData struct {
    Nodes []CallNode `json:"nodes"`
    Edges []CallEdge `json:"edges"`
    MaxDepth int `json:"max_depth"`
    TotalCalls int `json:"total_calls"`
}

type CallNode struct {
    ID string `json:"id"`
    ContractID string `json:"contract_id"`
    Label string `json:"label"`
    Type string `json:"type"` // "account", "contract"
}

type CallEdge struct {
    From string `json:"from"`
    To string `json:"to"`
    Function string `json:"function"`
    Order int `json:"order"`
    Success bool `json:"success"`
}
```

## Testing Strategy

### Unit Tests

1. **Authorization Tree Parsing**
   - Test nested invocation extraction
   - Test cycle detection
   - Test maximum depth handling

2. **Event Correlation**
   - Test matching auth entries with events
   - Test handling missing events
   - Test error cases

### Integration Tests

1. **End-to-End Call Tracking**
   - Deploy test contracts with known call patterns
   - Verify complete call graph capture
   - Test with various authorization scenarios

2. **Performance Tests**
   - Measure processing time with deep call trees
   - Test memory usage with large authorization sets
   - Benchmark against current implementation

### Test Contracts

```rust
// Contract A: Initiator
impl ContractA {
    pub fn call_b_and_c(env: Env, b_addr: Address, c_addr: Address) {
        // Call B which calls C
        let b_client = ContractBClient::new(&env, &b_addr);
        b_client.call_c(&c_addr);
        
        // Direct call to C
        let c_client = ContractCClient::new(&env, &c_addr);
        c_client.do_something();
    }
}
```

## Migration Plan

### Phase 1: Data Collection (No Breaking Changes)
1. Deploy updated processor alongside existing one
2. Collect cross-contract call data in new fields
3. Validate data accuracy
4. No changes to downstream consumers

### Phase 2: Consumer Updates
1. Update consumers to handle new fields
2. Add backward compatibility checks
3. Deploy consumer updates

### Phase 3: Full Rollout
1. Enable cross-contract tracking in production
2. Update documentation
3. Deprecate old fields (if any)

### Rollback Strategy
- Feature flag to disable cross-contract tracking
- Separate processor for testing
- Ability to filter out new fields in consumers

## Performance Considerations

### Processing Overhead
- Authorization tree traversal: O(n) where n = total invocations
- Diagnostic event correlation: O(m*n) where m = events
- Memory usage: Proportional to call tree depth

### Optimization Strategies

1. **Lazy Loading**
   - Only process auth tree if requested
   - Cache processed trees

2. **Depth Limiting**
   - Configure maximum recursion depth
   - Truncate very deep trees

3. **Parallel Processing**
   - Process auth entries concurrently
   - Batch event correlation

### Benchmarks

Expected performance impact:
- 10-20% increase in processing time for simple calls
- 50-100% increase for complex multi-contract interactions
- Negligible impact on transactions without contract calls

## Future Enhancements

### 1. Real-time Call Visualization
- WebSocket feed of call graphs
- Interactive call tree explorer
- Performance metrics per call

### 2. Smart Contract Analytics
- Most called contracts
- Call pattern analysis
- Gas optimization recommendations

### 3. Security Analysis
- Unauthorized call attempts
- Reentrancy detection
- Permission violation tracking

### 4. Enhanced Error Tracking
- Detailed error propagation
- Failed call analysis
- Root cause identification

## Conclusion

This implementation plan provides a comprehensive approach to capturing cross-contract calls in the CDP Pipeline. By leveraging existing XDR structures and combining authorization data with diagnostic events, we can build a complete picture of contract interactions on the Stellar network.

The phased approach ensures backward compatibility while gradually rolling out enhanced functionality. With proper testing and monitoring, this enhancement will provide valuable insights into smart contract behavior and interactions.

## Appendix: Example Output

```json
{
  "contract_invocation": {
    "contract_id": "CCFZRNQVVWMJGQE2IRNBCKBGXMJMQD2NAEIJII66P3H2OUGVTDXVWCGJ",
    "function_name": "transfer",
    "invoking_account": "GABC...",
    "contract_calls": [
      {
        "from_contract": "CCFZ...",
        "to_contract": "CDEF...",
        "function": "check_balance",
        "call_depth": 1,
        "auth_type": "contract",
        "successful": true,
        "execution_order": 0
      },
      {
        "from_contract": "CDEF...",
        "to_contract": "CXYZ...",
        "function": "validate_permission",
        "call_depth": 2,
        "auth_type": "contract",
        "successful": true,
        "execution_order": 1
      }
    ],
    "call_graph": {
      "nodes": [
        {"id": "CCFZ...", "label": "TokenContract", "type": "contract"},
        {"id": "CDEF...", "label": "BalanceChecker", "type": "contract"},
        {"id": "CXYZ...", "label": "PermissionValidator", "type": "contract"}
      ],
      "edges": [
        {"from": "CCFZ...", "to": "CDEF...", "function": "check_balance", "order": 0, "success": true},
        {"from": "CDEF...", "to": "CXYZ...", "function": "validate_permission", "order": 1, "success": true}
      ],
      "max_depth": 2,
      "total_calls": 2
    }
  }
}
```