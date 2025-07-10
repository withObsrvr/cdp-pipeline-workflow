# Contract Invocation Processor Enhancement Plan

## Executive Summary

The current `ContractInvocationProcessor` in the CDP Pipeline Workflow has significant limitations in extracting function names and arguments from Soroban contract invocations. This document outlines a comprehensive plan to enhance the processor to provide complete function call information, making it more useful for contract analysis and monitoring.

## Current State Analysis

### Limitations Identified

1. **Incomplete Function Name Extraction**
   - Current implementation only checks `args[0]` for a Symbol type
   - Fails when function name is stored in different formats or locations
   - No fallback mechanisms for edge cases

2. **Missing Argument Processing**
   - Arguments beyond the function name are completely ignored
   - No conversion from XDR ScVal types to human-readable formats
   - The `ContractInvocation` struct lacks an `Arguments` field

3. **Limited Data Structure**
   ```go
   type ContractInvocation struct {
       FunctionName     string            `json:"function_name,omitempty"` // Often empty
       // Missing: Arguments field
   }
   ```

### Current Implementation

```go
// Current function name extraction (lines 202-209)
if function := invokeHostFunction.HostFunction; function.Type == xdr.HostFunctionTypeHostFunctionTypeInvokeContract {
    if args := function.MustInvokeContract().Args; len(args) > 0 {
        if sym, ok := args[0].GetSym(); ok {
            invocation.FunctionName = string(sym)
        }
    }
}
```

## Proposed Enhancements

### 1. Enhanced Data Structure

Update the `ContractInvocation` struct to include complete function call information:

```go
type ContractInvocation struct {
    Timestamp        time.Time         `json:"timestamp"`
    LedgerSequence   uint32            `json:"ledger_sequence"`
    TransactionHash  string            `json:"transaction_hash"`
    ContractID       string            `json:"contract_id"`
    InvokingAccount  string            `json:"invoking_account"`
    FunctionName     string            `json:"function_name"`
    Arguments        []json.RawMessage `json:"arguments,omitempty"`        // NEW
    ArgumentsDecoded map[string]interface{} `json:"arguments_decoded,omitempty"` // NEW
    Successful       bool              `json:"successful"`
    DiagnosticEvents []DiagnosticEvent `json:"diagnostic_events,omitempty"`
    ContractCalls    []ContractCall    `json:"contract_calls,omitempty"`
    StateChanges     []StateChange     `json:"state_changes,omitempty"`
    TtlExtensions    []TtlExtension    `json:"ttl_extensions,omitempty"`
}
```

### 2. Robust Function Name Extraction

Implement a more comprehensive function name extraction:

```go
func extractFunctionName(invokeContract xdr.InvokeContractArgs) string {
    // Primary method: Use FunctionName field directly
    if len(invokeContract.FunctionName) > 0 {
        return string(invokeContract.FunctionName)
    }
    
    // Fallback: Check first argument
    if len(invokeContract.Args) > 0 {
        return getFunctionNameFromScVal(invokeContract.Args[0])
    }
    
    return "unknown"
}

func getFunctionNameFromScVal(val xdr.ScVal) string {
    switch val.Type {
    case xdr.ScValTypeScvSymbol:
        return string(*val.Sym)
    case xdr.ScValTypeScvString:
        return string(*val.Str)
    case xdr.ScValTypeScvBytes:
        return string(*val.Bytes)
    default:
        return ""
    }
}
```

### 3. Complete Argument Processing

Process all function arguments with proper type conversion:

```go
func extractArguments(args []xdr.ScVal) ([]json.RawMessage, map[string]interface{}, error) {
    rawArgs := make([]json.RawMessage, 0, len(args))
    decodedArgs := make(map[string]interface{})
    
    for i, arg := range args {
        // Convert ScVal to JSON-serializable format
        converted, err := ConvertScValToJSON(arg)
        if err != nil {
            log.Printf("Error converting argument %d: %v", i, err)
            converted = map[string]string{"error": err.Error(), "type": arg.Type.String()}
        }
        
        // Store raw JSON
        jsonBytes, err := json.Marshal(converted)
        if err != nil {
            continue
        }
        rawArgs = append(rawArgs, jsonBytes)
        
        // Store in decoded map with index
        decodedArgs[fmt.Sprintf("arg_%d", i)] = converted
    }
    
    return rawArgs, decodedArgs, nil
}
```

### 4. ScVal Type Conversion Utility

Create a comprehensive ScVal to JSON converter:

```go
func ConvertScValToJSON(val xdr.ScVal) (interface{}, error) {
    switch val.Type {
    case xdr.ScValTypeScvBool:
        return *val.B, nil
        
    case xdr.ScValTypeScvVoid:
        return nil, nil
        
    case xdr.ScValTypeScvU32:
        return *val.U32, nil
        
    case xdr.ScValTypeScvI32:
        return *val.I32, nil
        
    case xdr.ScValTypeScvU64:
        return *val.U64, nil
        
    case xdr.ScValTypeScvI64:
        return *val.I64, nil
        
    case xdr.ScValTypeScvU128:
        parts := *val.U128
        return map[string]interface{}{
            "type": "u128",
            "hi": parts.Hi,
            "lo": parts.Lo,
            "value": fmt.Sprintf("%d", uint128ToInt(parts)),
        }, nil
        
    case xdr.ScValTypeScvI128:
        parts := *val.I128
        return map[string]interface{}{
            "type": "i128",
            "hi": parts.Hi,
            "lo": parts.Lo,
            "value": fmt.Sprintf("%d", int128ToInt(parts)),
        }, nil
        
    case xdr.ScValTypeScvSymbol:
        return string(*val.Sym), nil
        
    case xdr.ScValTypeScvString:
        return string(*val.Str), nil
        
    case xdr.ScValTypeScvBytes:
        return map[string]interface{}{
            "type": "bytes",
            "hex": hex.EncodeToString(*val.Bytes),
            "base64": base64.StdEncoding.EncodeToString(*val.Bytes),
        }, nil
        
    case xdr.ScValTypeScvAddress:
        addr := val.Address
        switch addr.Type {
        case xdr.ScAddressTypeScAddressTypeAccount:
            accountID := addr.AccountId.Ed25519
            strkey, _ := strkey.Encode(strkey.VersionByteAccountID, accountID[:])
            return map[string]interface{}{
                "type": "account",
                "address": strkey,
            }, nil
        case xdr.ScAddressTypeScAddressTypeContract:
            contractID := addr.ContractId
            strkey, _ := strkey.Encode(strkey.VersionByteContract, contractID[:])
            return map[string]interface{}{
                "type": "contract",
                "address": strkey,
            }, nil
        }
        
    case xdr.ScValTypeScvVec:
        vec := *val.Vec
        result := make([]interface{}, len(*vec))
        for i, item := range *vec {
            converted, err := ConvertScValToJSON(item)
            if err != nil {
                result[i] = map[string]string{"error": err.Error()}
            } else {
                result[i] = converted
            }
        }
        return result, nil
        
    case xdr.ScValTypeScvMap:
        scMap := *val.Map
        result := make(map[string]interface{})
        for _, entry := range *scMap {
            key, _ := ConvertScValToJSON(entry.Key)
            value, _ := ConvertScValToJSON(entry.Val)
            keyStr := fmt.Sprintf("%v", key)
            result[keyStr] = value
        }
        return result, nil
        
    case xdr.ScValTypeScvContractInstance:
        return map[string]interface{}{
            "type": "contract_instance",
            "value": "complex_type",
        }, nil
        
    default:
        return nil, fmt.Errorf("unsupported ScVal type: %s", val.Type.String())
    }
}
```

## Implementation Roadmap

### Phase 1: Foundation (Week 1)
1. ✅ Create ScVal conversion utility module (`processor/scval_converter.go`)
2. ✅ Add comprehensive unit tests for all ScVal types
3. ✅ Update ContractInvocation struct

### Phase 2: Core Enhancement (Week 2)
1. ✅ Implement robust function name extraction
2. ✅ Add complete argument processing
3. ✅ Update processor logic to use new extraction methods
4. ✅ Add proper error handling and logging

### Phase 3: Integration & Testing (Week 3)
1. ✅ Integration testing with real Soroban transactions
2. ✅ Performance testing with large datasets
3. ✅ Update existing consumers to handle new fields
4. ✅ Documentation updates

### Phase 4: Advanced Features (Week 4)
1. ✅ Add function-specific argument parsing (e.g., swap, deposit, withdraw)
2. ✅ Create argument type inference
3. ✅ Add contract ABI support for known contracts
4. ✅ Create examples and best practices documentation

## Example Output

### Before Enhancement
```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "contract_id": "CCFZRNQVAY52P7LRY7GLA2R2XQMWMYYB6WZ7EKNO3BV3LP3QGXFB4VKJ",
  "function_name": "",  // Often empty
  "successful": true
}
```

### After Enhancement
```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "contract_id": "CCFZRNQVAY52P7LRY7GLA2R2XQMWMYYB6WZ7EKNO3BV3LP3QGXFB4VKJ",
  "function_name": "swap",
  "arguments": [
    "\"swap\"",
    "{\"type\":\"i128\",\"value\":\"1000000000\"}",
    "{\"type\":\"i128\",\"value\":\"950000000\"}",
    "{\"type\":\"vec\",\"value\":[\"GABC...\",\"GDEF...\"]}"
  ],
  "arguments_decoded": {
    "arg_0": "swap",
    "arg_1": {
      "type": "i128",
      "value": "1000000000"
    },
    "arg_2": {
      "type": "i128",
      "value": "950000000"
    },
    "arg_3": ["GABC...", "GDEF..."]
  },
  "successful": true
}
```

## Benefits

1. **Complete Visibility**: Full insight into Soroban contract function calls
2. **Better Analytics**: Ability to analyze contract usage patterns
3. **Improved Debugging**: Easier to trace contract interactions
4. **Enhanced Monitoring**: Better alerting on specific function calls
5. **Developer Friendly**: Human-readable argument formats

## Compatibility

- **Backwards Compatible**: Existing fields remain unchanged
- **Optional Fields**: New fields use `omitempty` tags
- **Graceful Degradation**: Falls back to current behavior on errors

## Success Metrics

1. **Function Name Extraction Rate**: Target >95% (up from ~30%)
2. **Argument Capture Rate**: Target 100% of available arguments
3. **Processing Performance**: <5% overhead increase
4. **Error Rate**: <1% parsing failures

## Next Steps

1. Review and approve this enhancement plan
2. Create feature branch for implementation
3. Begin Phase 1 implementation
4. Set up testing infrastructure
5. Schedule code reviews at each phase completion

## References

- [Stellar XDR Documentation](https://developers.stellar.org/docs/encyclopedia/xdr)
- [Soroban Contract Documentation](https://soroban.stellar.org/docs)
- [CDP Pipeline Workflow Architecture](../README.md)