# Stellar Go SDK Helper Methods Guide

## Overview

The Stellar Go SDK provides powerful helper methods in the `ingest` package that abstract away the complexity of handling different protocol versions and transaction metadata formats. This guide documents these methods to help developers build robust processors without manually handling version-specific logic.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Transaction Helper Methods](#transaction-helper-methods)
3. [Event Processing](#event-processing)
4. [Operation-Specific Helpers](#operation-specific-helpers)
5. [Asset and Account Helpers](#asset-and-account-helpers)
6. [Migration Guide](#migration-guide)
7. [Best Practices](#best-practices)
8. [Examples](#examples)

## Key Concepts

### LedgerTransaction

The `ingest.LedgerTransaction` type is the primary interface for processing transactions. It provides:
- Unified access to transaction data across protocol versions
- Helper methods that abstract V1, V2, V3, and V4 metadata differences
- Consistent error handling

### Protocol Version Abstraction

The SDK automatically handles differences between:
- **V1/V2**: Legacy transaction metadata
- **V3**: Introduced Soroban support with events in `SorobanMeta`
- **V4**: Protocol 23+ with events moved to `TransactionMetaV4`

## Transaction Helper Methods

### Core Transaction Methods

```go
// Check if transaction is Soroban-related
isSoroban := tx.IsSorobanTx()

// Get transaction result
successful := tx.Result.Successful()

// Get transaction hash
hash := tx.Result.TransactionHash.HexString()

// Get source account
sourceAccount := tx.Envelope.SourceAccount()

// Get sequence number
sequence := tx.Envelope.SeqNum()

// Get operations
operations := tx.Envelope.Operations()

// Get fee charged
feeCharged := tx.Result.Result.FeeCharged
```

### Metadata Access

```go
// Get operation results (safe access)
operationResults, ok := tx.Result.OperationResults()
if ok {
    for i, result := range operationResults {
        // Process each operation result
    }
}

// Get ledger sequence
ledgerSeq := tx.LedgerSequence()

// Get close time
closeTime := tx.CloseTime()
```

## Event Processing

### GetTransactionEvents() - Recommended Method

The most important helper for event processing. Handles all protocol versions automatically:

```go
// Get all events from a transaction (V3 and V4 compatible)
txEvents, err := tx.GetTransactionEvents()
if err != nil {
    // Not a Soroban transaction or no events
    return nil
}

// txEvents contains:
// - TransactionEvents: Transaction-level events (V4 only)
// - OperationEvents: Events organized by operation index
// - DiagnosticEvents: Diagnostic events for debugging

// Process operation events
for opIndex, opEvents := range txEvents.OperationEvents {
    for eventIdx, event := range opEvents {
        if event.Type == xdr.ContractEventTypeContract {
            // Process contract event
        }
    }
}

// Process transaction-level events (V4 only)
for _, txEvent := range txEvents.TransactionEvents {
    if txEvent.Event.Type == xdr.ContractEventTypeContract {
        // Process transaction-level event
    }
}
```

### GetContractEvents() - Contract Events Only

Get only contract events from Soroban transactions:

```go
contractEvents, err := tx.GetContractEvents()
if err != nil {
    // Handle error
}

for _, event := range contractEvents {
    // Process each contract event
}
```

### GetDiagnosticEvents()

Get diagnostic events (useful for debugging):

```go
diagnosticEvents, err := tx.GetDiagnosticEvents()
if err != nil {
    // Handle error
}

for _, diagEvent := range diagnosticEvents {
    if diagEvent.InSuccessfulContractCall {
        // Event from successful contract call
    }
}
```

## Operation-Specific Helpers

### Payment Operations

```go
for i, op := range tx.Envelope.Operations() {
    switch op.Body.Type {
    case xdr.OperationTypePayment:
        payment := op.Body.MustPayment()
        
        // Get payment details
        destination := payment.Destination.Address()
        amount := payment.Amount
        asset := payment.Asset
        
        // Get operation source account
        var source xdr.AccountId
        if op.SourceAccount != nil {
            source = op.SourceAccount.ToAccountId()
        } else {
            source = tx.Envelope.SourceAccount().ToAccountId()
        }
    }
}
```

### InvokeHostFunction Operations

```go
for i, op := range tx.Envelope.Operations() {
    if op.Body.Type == xdr.OperationTypeInvokeHostFunction {
        invokeOp := op.Body.MustInvokeHostFunctionOp()
        
        // Check function type
        switch invokeOp.HostFunction.Type {
        case xdr.HostFunctionTypeHostFunctionTypeInvokeContract:
            invokeContract := invokeOp.HostFunction.MustInvokeContract()
            
            // Get contract ID
            contractID := invokeContract.ContractAddress.ContractId
            contractIDStr, _ := strkey.Encode(strkey.VersionByteContract, contractID[:])
            
            // Get function name and arguments
            // (Note: Function name is typically the first argument)
            args := invokeContract.Args
            
        case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
            // Handle contract creation
            
        case xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm:
            // Handle WASM upload
        }
    }
}
```

### CreateAccount Operations

```go
for _, op := range tx.Envelope.Operations() {
    if op.Body.Type == xdr.OperationTypeCreateAccount {
        createAccount := op.Body.MustCreateAccount()
        
        destination := createAccount.Destination.Address()
        startingBalance := createAccount.StartingBalance
    }
}
```

## Asset and Account Helpers

### Asset Helpers

```go
// Convert XDR asset to string
assetString := asset.String()

// Check if native asset
isNative := asset.Type == xdr.AssetTypeAssetTypeNative

// Get asset code and issuer
if asset.Type == xdr.AssetTypeAssetTypeCreditAlphanum4 {
    code := string(asset.AlphaNum4.AssetCode[:])
    issuer := asset.AlphaNum4.Issuer.Address()
} else if asset.Type == xdr.AssetTypeAssetTypeCreditAlphanum12 {
    code := string(asset.AlphaNum12.AssetCode[:])
    issuer := asset.AlphaNum12.Issuer.Address()
}
```

### Account Helpers

```go
// Convert account ID to address
accountID := someAccountId
address := accountID.Address()

// Convert muxed account to address
muxedAccount := someMuxedAccount
address := muxedAccount.Address()

// Get account ID from muxed account
accountID := muxedAccount.ToAccountId()
```

### Strkey Encoding/Decoding

```go
// Encode contract ID
contractIDBytes := [32]byte{...}
contractIDStr, err := strkey.Encode(strkey.VersionByteContract, contractIDBytes[:])

// Decode contract ID
contractIDBytes, err := strkey.Decode(strkey.VersionByteContract, contractIDStr)

// Check address type
versionByte, err := strkey.Version(someAddress)
switch versionByte {
case strkey.VersionByteAccountID:
    // Regular account
case strkey.VersionByteContract:
    // Contract address
case strkey.VersionByteMuxedAccount:
    // Muxed account
}
```

## Migration Guide

### Old Pattern (Manual Version Checking)

```go
// DON'T DO THIS - Manual version checking
var events []xdr.ContractEvent
switch tx.UnsafeMeta.V {
case 3:
    if tx.UnsafeMeta.V3.SorobanMeta != nil {
        events = tx.UnsafeMeta.V3.SorobanMeta.Events
    }
case 4:
    // Complex V4 handling...
}
```

### New Pattern (SDK Helper Methods)

```go
// DO THIS - Use SDK helpers
txEvents, err := tx.GetTransactionEvents()
if err != nil {
    return nil // Not a Soroban transaction
}

for opIndex, opEvents := range txEvents.OperationEvents {
    for _, event := range opEvents {
        // Process event
    }
}
```

### Migration Checklist

1. **Replace manual metadata version checks** with helper methods
2. **Use GetTransactionEvents()** for event processing
3. **Use Result.Successful()** instead of checking result codes
4. **Use helper methods** for operation-specific data extraction
5. **Use strkey package** for address encoding/decoding
6. **Handle errors properly** - helpers return errors for invalid operations

## Best Practices

### 1. Always Check Transaction Success

```go
if !tx.Result.Successful() {
    // Skip failed transactions for most use cases
    return nil
}
```

### 2. Use Type-Safe Methods

```go
// Good - Type-safe with error handling
payment, ok := op.Body.GetPayment()
if !ok {
    return fmt.Errorf("not a payment operation")
}

// Better - Must methods panic if wrong type (use when type is certain)
payment := op.Body.MustPayment()
```

### 3. Handle Missing Optional Fields

```go
// Check if operation has source account
var source xdr.AccountId
if op.SourceAccount != nil {
    source = op.SourceAccount.ToAccountId()
} else {
    source = tx.Envelope.SourceAccount().ToAccountId()
}
```

### 4. Efficient Event Filtering

```go
// Filter events early
for _, event := range opEvents {
    if event.Type != xdr.ContractEventTypeContract {
        continue // Skip non-contract events
    }
    // Process only contract events
}
```

## Examples

### Complete Contract Event Processor

```go
func processLedger(ledgerCloseMeta xdr.LedgerCloseMeta) error {
    reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
        networkPassphrase, ledgerCloseMeta)
    if err != nil {
        return err
    }
    defer reader.Close()

    for {
        tx, err := reader.Read()
        if err == io.EOF {
            break
        }
        if err != nil {
            return err
        }

        // Skip failed transactions
        if !tx.Result.Successful() {
            continue
        }

        // Get all events using SDK helper
        txEvents, err := tx.GetTransactionEvents()
        if err != nil {
            continue // Not a Soroban transaction
        }

        // Process events
        for opIndex, opEvents := range txEvents.OperationEvents {
            for eventIdx, event := range opEvents {
                if event.Type == xdr.ContractEventTypeContract {
                    processContractEvent(tx, opIndex, eventIdx, event)
                }
            }
        }
    }
    return nil
}
```

### Payment Processor Using Helpers

```go
func processPayments(tx ingest.LedgerTransaction) error {
    // Check transaction success
    if !tx.Result.Successful() {
        return nil
    }

    // Get operation results
    opResults, ok := tx.Result.OperationResults()
    if !ok {
        return fmt.Errorf("no operation results")
    }

    // Process each operation
    for i, op := range tx.Envelope.Operations() {
        if op.Body.Type != xdr.OperationTypePayment {
            continue
        }

        payment := op.Body.MustPayment()
        
        // Check if operation was successful
        if opResults[i].Code != xdr.OperationResultCodeOpInner {
            continue
        }

        // Extract payment details
        paymentData := PaymentData{
            From:      getOperationSource(op, tx),
            To:        payment.Destination.Address(),
            Asset:     payment.Asset.String(),
            Amount:    amount.String(payment.Amount),
            Timestamp: time.Unix(tx.CloseTime(), 0),
        }

        // Process payment
        processPayment(paymentData)
    }
    return nil
}
```

## Processor Implementation Checklist

When implementing a new processor:

- [ ] Use `tx.GetTransactionEvents()` for event processing
- [ ] Use `tx.Result.Successful()` to check transaction status
- [ ] Use operation-specific helpers (MustPayment(), etc.)
- [ ] Use strkey package for address encoding
- [ ] Handle all error cases from helper methods
- [ ] Don't access UnsafeMeta directly unless absolutely necessary
- [ ] Use `tx.CloseTime()` for timestamps
- [ ] Use `tx.LedgerSequence()` for ledger numbers

## CDP Helper Packages

The CDP pipeline provides additional helper packages that complement the SDK:

### Asset Helpers (`pkg/helpers/stellar/assets.go`)
- `AssetToString()` - Convert asset to string representation
- `CreateAsset()` - Create asset from code and issuer
- `GetAssetCode()` - Extract asset code handling all types
- `GetAssetIssuer()` - Get issuer address or empty for native
- `CompareAssets()` - Check if two assets are equal

### Address Helpers (`pkg/helpers/stellar/addresses.go`)
- `ValidateAddress()` - Validate any Stellar address type
- `IsAccountAddress()` - Check if G... address
- `IsContractAddress()` - Check if C... address
- `ExtractAddressFromScVal()` - Extract from contract values
- `GetOperationSourceAccount()` - Get source with fallback

### Amount Helpers (`pkg/helpers/stellar/amounts.go`)
- `FormatAmount()` - Convert int64 to decimal string
- `ParseAmount()` - Parse decimal string to int64
- `AddAmounts()` - Safe addition with overflow check
- `SubtractAmounts()` - Safe subtraction
- `AmountToFloat()` - Convert for calculations

### Event Helpers (`pkg/helpers/stellar/events.go`)
- `DetermineTokenEventType()` - Detect mint/burn/transfer
- `ExtractEventTopic()` - Get specific event topic
- `ExtractI128Amount()` - Extract amounts from events
- `GetEventData()` - Access event data field

### Error Helpers (`pkg/helpers/stellar/errors.go`)
- `ProcessingError` - Rich error context
- `ErrorContext` - Fluent error building
- `ValidationError` - Input validation errors

## Additional Resources

- [Stellar Go SDK Documentation](https://pkg.go.dev/github.com/stellar/go)
- [Stellar Laboratory](https://laboratory.stellar.org) - Test XDR encoding/decoding
- [Protocol Documentation](https://developers.stellar.org/docs)
- [Horizon Ingestion System](https://github.com/stellar/go/tree/master/ingest)
- [Token Transfer Processor Patterns](./stellar-token-processor-patterns.md)

## Future Protocol Changes

The SDK helper methods are designed to abstract future protocol changes. When new versions are released:

1. Update to the latest Stellar Go SDK
2. Helper methods will handle new metadata versions automatically
3. New features may require new helper methods
4. Check SDK release notes for new capabilities

By using these helper methods, your processors will be more maintainable, readable, and resilient to protocol changes.