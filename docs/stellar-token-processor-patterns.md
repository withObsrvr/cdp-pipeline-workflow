# Stellar Token Processor Patterns & Best Practices

## Overview

This document captures valuable patterns and helpers from Stellar's official token_transfer processor that can enhance CDP pipeline development.

## 1. Additional SDK Helper Methods

### Amount Handling

```go
import "github.com/stellar/go/amount"

// Parse amount from int64 to string with proper decimal places
amountStr := amount.String(payment.Amount) // "100.0000000" for 1000000000

// Parse string amount to int64
amountInt, err := amount.ParseInt64("100.50")

// Get raw string representation
rawAmount := amount.StringFromInt64(payment.Amount)
```

### Asset Creation Helpers

```go
// Create native asset (XLM)
xlm := xdr.MustNewNativeAsset()

// Create credit asset
usdc := xdr.MustNewCreditAsset("USDC", "GCKFBEIYV2U22IO2BJ4KVJOIP7XPWQGQFKKWXR6DOSJBV7STMAQSMTMA")

// Dynamic asset creation
func CreateAsset(code, issuer string) xdr.Asset {
    if issuer == "" {
        return xdr.MustNewNativeAsset()
    }
    return xdr.MustNewCreditAsset(code, issuer)
}
```

### Contract Value Helpers

```go
import "github.com/stellar/go/xdr"

// Extract address from ScVal
func ExtractAddress(val xdr.ScVal) (string, error) {
    switch val.Type {
    case xdr.ScValTypeScvAddress:
        addr := val.MustAddress()
        switch addr.Type {
        case xdr.ScAddressTypeScAddressTypeAccount:
            return addr.AccountId.Address(), nil
        case xdr.ScAddressTypeScAddressTypeContract:
            return strkey.Encode(strkey.VersionByteContract, addr.ContractId[:])
        }
    }
    return "", fmt.Errorf("not an address type")
}

// Convert ScVal to native Go types
func ScValToInterface(val xdr.ScVal) (interface{}, error) {
    switch val.Type {
    case xdr.ScValTypeScvI64:
        return val.MustI64(), nil
    case xdr.ScValTypeScvU64:
        return val.MustU64(), nil
    case xdr.ScValTypeScvSymbol:
        return string(val.MustSym()), nil
    case xdr.ScValTypeScvString:
        return val.MustStr(), nil
    // ... handle other types
    }
}
```

## 2. Error Handling Patterns

### Rich Error Context

```go
// Pattern: Always include context in errors
func processOperation(tx ingest.LedgerTransaction, opIdx int) error {
    op := tx.Envelope.Operations()[opIdx]
    
    result, err := processPayment(op)
    if err != nil {
        return fmt.Errorf("processing operation %d for transaction %s in ledger %d: %w",
            opIdx, 
            tx.Result.TransactionHash.HexString(),
            tx.LedgerSequence(),
            err)
    }
    return nil
}
```

### Custom Error Types

```go
// Define domain-specific errors
type ProcessingError struct {
    Ledger      uint32
    Transaction string
    Operation   int
    Type        string
    Cause       error
}

func (e ProcessingError) Error() string {
    return fmt.Sprintf("processing error in %s at ledger %d, tx %s, op %d: %v",
        e.Type, e.Ledger, e.Transaction, e.Operation, e.Cause)
}

func (e ProcessingError) Unwrap() error {
    return e.Cause
}
```

### Error Validation Pattern

```go
// Early validation with descriptive errors
func validateTokenTransfer(op xdr.Operation) error {
    if op.Body.Type != xdr.OperationTypePayment {
        return fmt.Errorf("expected payment operation, got %s", op.Body.Type)
    }
    
    payment := op.Body.MustPayment()
    if payment.Amount <= 0 {
        return fmt.Errorf("invalid payment amount: %d", payment.Amount)
    }
    
    return nil
}
```

## 3. Performance Patterns

### Early Return Pattern

```go
func processTransaction(tx ingest.LedgerTransaction) error {
    // Skip failed transactions immediately
    if !tx.Result.Successful() {
        return nil
    }
    
    // Skip if no relevant operations
    if !hasTokenOperations(tx) {
        return nil
    }
    
    // Process only if needed
    return processTokenOperations(tx)
}
```

### Efficient Lookups

```go
// Use maps for O(1) lookups
type AssetCache struct {
    assets map[string]AssetInfo // key: "CODE:ISSUER"
}

func (c *AssetCache) GetAsset(code, issuer string) (AssetInfo, bool) {
    key := fmt.Sprintf("%s:%s", code, issuer)
    info, ok := c.assets[key]
    return info, ok
}
```

### Batch Processing

```go
type EventBatcher struct {
    events    []TokenEvent
    batchSize int
}

func (b *EventBatcher) Add(event TokenEvent) error {
    b.events = append(b.events, event)
    
    if len(b.events) >= b.batchSize {
        return b.Flush()
    }
    return nil
}

func (b *EventBatcher) Flush() error {
    if len(b.events) == 0 {
        return nil
    }
    
    // Process batch
    err := processBatch(b.events)
    b.events = b.events[:0] // Reset slice
    return err
}
```

## 4. Token Transfer Patterns

### Unified Event Generation

```go
type TokenEventType string

const (
    EventTypeMint     TokenEventType = "mint"
    EventTypeBurn     TokenEventType = "burn"
    EventTypeTransfer TokenEventType = "transfer"
)

func DetermineEventType(from, to, issuer string) TokenEventType {
    // Mint: issuer sends to non-issuer
    if from == issuer && to != issuer {
        return EventTypeMint
    }
    
    // Burn: non-issuer sends to issuer
    if from != issuer && to == issuer {
        return EventTypeBurn
    }
    
    // Transfer: between non-issuers
    return EventTypeTransfer
}
```

### Asset Type Handling

```go
func ProcessAssetTransfer(asset xdr.Asset, amount int64) (TokenTransfer, error) {
    transfer := TokenTransfer{
        Amount: amount.String(amount),
    }
    
    switch asset.Type {
    case xdr.AssetTypeAssetTypeNative:
        transfer.AssetCode = "XLM"
        transfer.AssetType = "native"
        
    case xdr.AssetTypeAssetTypeCreditAlphanum4:
        transfer.AssetCode = strings.TrimRight(string(asset.AlphaNum4.AssetCode[:]), "\x00")
        transfer.AssetIssuer = asset.AlphaNum4.Issuer.Address()
        transfer.AssetType = "credit_alphanum4"
        
    case xdr.AssetTypeAssetTypeCreditAlphanum12:
        transfer.AssetCode = strings.TrimRight(string(asset.AlphaNum12.AssetCode[:]), "\x00")
        transfer.AssetIssuer = asset.AlphaNum12.Issuer.Address()
        transfer.AssetType = "credit_alphanum12"
        
    default:
        return transfer, fmt.Errorf("unsupported asset type: %v", asset.Type)
    }
    
    return transfer, nil
}
```

## 5. Testing Patterns

### Table-Driven Tests

```go
func TestTokenTransferProcessor(t *testing.T) {
    tests := []struct {
        name     string
        ledger   xdr.LedgerCloseMeta
        expected []TokenTransferEvent
        wantErr  bool
    }{
        {
            name:   "native payment",
            ledger: createTestLedgerWithPayment(xdr.MustNewNativeAsset(), 1000000000),
            expected: []TokenTransferEvent{{
                Type:   "transfer",
                Asset:  "XLM",
                Amount: "100.0000000",
            }},
        },
        {
            name:   "token mint",
            ledger: createTestLedgerWithMint("USDC", issuer, 5000000000),
            expected: []TokenTransferEvent{{
                Type:   "mint",
                Asset:  "USDC",
                Amount: "500.0000000",
            }},
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            processor := NewTokenTransferProcessor()
            events, err := processor.Process(tt.ledger)
            
            if tt.wantErr {
                require.Error(t, err)
                return
            }
            
            require.NoError(t, err)
            assert.Equal(t, tt.expected, events)
        })
    }
}
```

### Test Fixture Helpers

```go
// Create test fixtures for common scenarios
func CreateTestPayment(from, to string, asset xdr.Asset, amount int64) xdr.Operation {
    return xdr.Operation{
        SourceAccount: &xdr.MuxedAccount{
            Type: xdr.CryptoKeyTypeKeyTypeEd25519,
            Ed25519: &xdr.Uint256{}, // Set actual key
        },
        Body: xdr.OperationBody{
            Type: xdr.OperationTypePayment,
            PaymentOp: &xdr.PaymentOp{
                Destination: xdr.MustAddress(to),
                Asset:       asset,
                Amount:      amount,
            },
        },
    }
}
```

## 6. Verification Pattern

### Data Integrity Verification

```go
// Verify processor output matches source data
type ProcessorVerifier struct {
    processor Processor
}

func (v *ProcessorVerifier) VerifyEvents(events []TokenEvent, source xdr.LedgerCloseMeta) error {
    // Extract expected events from source
    expected := extractExpectedEvents(source)
    
    // Compare counts
    if len(events) != len(expected) {
        return fmt.Errorf("event count mismatch: got %d, expected %d", 
            len(events), len(expected))
    }
    
    // Verify each event
    for i, event := range events {
        if err := verifyEvent(event, expected[i]); err != nil {
            return fmt.Errorf("event %d verification failed: %w", i, err)
        }
    }
    
    return nil
}
```

## 7. Helper Package Structure

```go
// pkg/helpers/stellar/assets.go
package stellar

// Asset conversion helpers
func AssetToString(asset xdr.Asset) string
func StringToAsset(s string) (xdr.Asset, error)
func IsNativeAsset(asset xdr.Asset) bool

// pkg/helpers/stellar/addresses.go
package stellar

// Address validation and conversion
func ValidateAddress(addr string) error
func IsContractAddress(addr string) bool
func IsMuxedAddress(addr string) bool

// pkg/helpers/stellar/amounts.go
package stellar

// Amount parsing and formatting
func FormatAmount(raw int64) string
func ParseAmount(s string) (int64, error)
func AddAmounts(a, b int64) (int64, error)
```

## Key Takeaways

1. **Always provide rich error context** - Include ledger, transaction, and operation details
2. **Use early returns** for performance - Skip unnecessary processing ASAP
3. **Leverage SDK helpers** - Don't reinvent the wheel
4. **Test with fixtures** - Create reusable test data generators
5. **Verify outputs** - Implement verification logic to ensure correctness
6. **Handle all asset types** - Native, AlphaNum4, and AlphaNum12
7. **Batch operations** - Process in groups for efficiency

## Implementation Checklist

When implementing token processors:

- [ ] Use amount.String() for proper decimal formatting
- [ ] Handle all three asset types (native, credit4, credit12)
- [ ] Include rich error context in all errors
- [ ] Implement early return optimizations
- [ ] Create comprehensive test fixtures
- [ ] Add verification logic
- [ ] Use maps for efficient lookups
- [ ] Batch events when possible
- [ ] Document event types clearly
- [ ] Handle edge cases (issuer operations, etc.)