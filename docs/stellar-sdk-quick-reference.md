# Stellar SDK Quick Reference

## 🚀 Quick Conversions: Old Pattern → New Pattern

### Event Processing

❌ **OLD**
```go
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

✅ **NEW**
```go
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

### Transaction Success Check

❌ **OLD**
```go
if tx.Result.Result.Code == xdr.TransactionResultCodeTxSuccess {
    // Process
}
```

✅ **NEW**
```go
if tx.Result.Successful() {
    // Process
}
```

### Operation Results

❌ **OLD**
```go
if tx.Result.Result.Results != nil {
    results := *tx.Result.Result.Results
    if len(results) > opIndex {
        // Use results[opIndex]
    }
}
```

✅ **NEW**
```go
results, ok := tx.Result.OperationResults()
if ok && len(results) > opIndex {
    // Use results[opIndex]
}
```

### Contract ID Encoding

❌ **OLD**
```go
contractIDStr := base64.StdEncoding.EncodeToString(contractID[:])
```

✅ **NEW**
```go
contractIDStr, err := strkey.Encode(strkey.VersionByteContract, contractID[:])
```

### Getting Operation Source

❌ **OLD**
```go
var source xdr.AccountId
if op.SourceAccount != nil {
    source = *op.SourceAccount
} else {
    source = tx.Envelope.Tx.SourceAccount
}
```

✅ **NEW**
```go
var source xdr.AccountId
if op.SourceAccount != nil {
    source = op.SourceAccount.ToAccountId()
} else {
    source = tx.Envelope.SourceAccount().ToAccountId()
}
```

### Diagnostic Events

❌ **OLD**
```go
diagnosticEvents := tx.Result.Result.Ext.V1.TxEvents
```

✅ **NEW**
```go
diagnosticEvents, err := tx.GetDiagnosticEvents()
```

## 🔧 Common SDK Helper Methods

### Transaction Level
```go
tx.Result.Successful()              // Check if transaction succeeded
tx.Result.TransactionHash           // Get transaction hash
tx.Envelope.SourceAccount()         // Get source account
tx.Envelope.SeqNum()               // Get sequence number
tx.Envelope.Operations()           // Get all operations
tx.LedgerSequence()                // Get ledger sequence
tx.CloseTime()                     // Get close time
tx.IsSorobanTx()                   // Check if Soroban transaction
```

### Event Methods
```go
tx.GetTransactionEvents()          // Get all events (V3/V4 compatible)
tx.GetContractEvents()             // Get only contract events
tx.GetDiagnosticEvents()           // Get diagnostic events
```

### Operation Type Checks
```go
op.Body.Type == xdr.OperationTypePayment
op.Body.Type == xdr.OperationTypeCreateAccount
op.Body.Type == xdr.OperationTypeInvokeHostFunction
// ... etc
```

### Operation Data Access
```go
op.Body.MustPayment()                    // Panics if not payment
op.Body.GetPayment()                     // Returns (payment, ok)
op.Body.MustCreateAccount()              // Panics if not create account
op.Body.MustInvokeHostFunctionOp()       // Panics if not invoke
// ... etc for all operation types
```

### Asset Helpers
```go
asset.String()                           // Convert to string representation
asset.Equals(other)                      // Compare assets
asset.IsNative()                         // Check if XLM
asset.GetCode()                          // Get asset code
asset.GetIssuer()                        // Get issuer
```

### Account/Address Helpers
```go
accountID.Address()                      // Convert to string address
muxedAccount.Address()                   // Get address from muxed
muxedAccount.ToAccountId()               // Extract account ID
strkey.Encode(version, data)             // Encode to strkey format
strkey.Decode(version, address)          // Decode from strkey
```

## 📋 Processing Patterns

### Basic Transaction Processing
```go
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
    
    if !tx.Result.Successful() {
        continue
    }
    
    // Process transaction
}
```

### Operation Processing
```go
for i, op := range tx.Envelope.Operations() {
    switch op.Body.Type {
    case xdr.OperationTypePayment:
        payment := op.Body.MustPayment()
        // Process payment
    case xdr.OperationTypeCreateAccount:
        createAccount := op.Body.MustCreateAccount()
        // Process account creation
    // ... other operation types
    }
}
```

### Event Processing
```go
txEvents, err := tx.GetTransactionEvents()
if err != nil {
    continue // Not a Soroban transaction
}

// Operation events
for opIndex, opEvents := range txEvents.OperationEvents {
    for _, event := range opEvents {
        if event.Type == xdr.ContractEventTypeContract {
            // Process contract event
        }
    }
}

// Transaction events (V4 only)
for _, txEvent := range txEvents.TransactionEvents {
    // Process transaction-level event
}
```

## ⚠️ Common Pitfalls

### 1. Not Checking Transaction Success
```go
// Always check!
if !tx.Result.Successful() {
    continue
}
```

### 2. Assuming Operation Source
```go
// Always check for operation-specific source
var source xdr.AccountId
if op.SourceAccount != nil {
    source = op.SourceAccount.ToAccountId()
} else {
    source = tx.Envelope.SourceAccount().ToAccountId()
}
```

### 3. Not Handling Errors from Helpers
```go
// Helpers can return errors
events, err := tx.GetTransactionEvents()
if err != nil {
    // Handle: not a Soroban transaction
}
```

### 4. Direct UnsafeMeta Access
```go
// DON'T use UnsafeMeta directly
// tx.UnsafeMeta.V3... ❌
// Use helper methods instead ✅
```

## 🎯 Quick Decision Tree

**Need events?**
- Contract events only → `GetContractEvents()`
- All events → `GetTransactionEvents()`
- Diagnostic → `GetDiagnosticEvents()`

**Need operation data?**
- Know the type → `MustOperationType()`
- Might be wrong type → `GetOperationType()`

**Need addresses?**
- Account ID → `.Address()`
- Contract → `strkey.Encode(strkey.VersionByteContract, ...)`
- Muxed → `.ToAccountId()` then `.Address()`

**Need transaction data?**
- Success → `Result.Successful()`
- Hash → `Result.TransactionHash.HexString()`
- Time → `tx.CloseTime()`

## 📚 Resources

- [Full Guide](./stellar-go-sdk-helper-methods.md)
- [Migration Plan](./stellar-processor-migration-plan.md)
- [Stellar Go SDK Docs](https://pkg.go.dev/github.com/stellar/go)