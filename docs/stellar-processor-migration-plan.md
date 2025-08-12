# Stellar Processor Migration Plan

## Overview

This document provides a comprehensive plan for implementing all Stellar processors from the Stellar Go repository and updating existing CDP processors to use SDK helper methods.

## Stellar Processors to Implement

### Core Transaction Processors

| Processor | Purpose | Key SDK Methods | Priority |
|-----------|---------|-----------------|----------|
| **TransactionProcessor** | Process all transactions | `tx.Result.Successful()`, `tx.Envelope.Operations()` | High |
| **OperationProcessor** | Process individual operations | Operation-specific helpers | High |
| **LedgerProcessor** | Process ledger metadata | `ledgerCloseMeta.LedgerSequence()` | High |
| **EffectProcessor** | Extract operation effects | `tx.Result.OperationResults()` | High |

### Operation-Specific Processors

| Operation Type | Processor Name | Key SDK Methods | Status |
|----------------|----------------|-----------------|--------|
| Payment | PaymentProcessor | `op.Body.MustPayment()` | Partial |
| CreateAccount | CreateAccountProcessor | `op.Body.MustCreateAccount()` | Exists |
| AccountMerge | AccountMergeProcessor | `op.Body.MustAccountMerge()` | TODO |
| PathPaymentStrictReceive | PathPaymentProcessor | `op.Body.MustPathPaymentStrictReceiveOp()` | TODO |
| PathPaymentStrictSend | PathPaymentStrictSendProcessor | `op.Body.MustPathPaymentStrictSendOp()` | TODO |
| ManageBuyOffer | ManageBuyOfferProcessor | `op.Body.MustManageBuyOfferOp()` | TODO |
| ManageSellOffer | ManageSellOfferProcessor | `op.Body.MustManageSellOfferOp()` | TODO |
| CreatePassiveSellOffer | PassiveOfferProcessor | `op.Body.MustCreatePassiveSellOfferOp()` | TODO |
| SetOptions | SetOptionsProcessor | `op.Body.MustSetOptionsOp()` | TODO |
| ChangeTrust | ChangeTrustProcessor | `op.Body.MustChangeTrustOp()` | TODO |
| AllowTrust | AllowTrustProcessor | `op.Body.MustAllowTrustOp()` | TODO |
| ManageData | ManageDataProcessor | `op.Body.MustManageDataOp()` | TODO |
| BumpSequence | BumpSequenceProcessor | `op.Body.MustBumpSequenceOp()` | TODO |
| CreateClaimableBalance | ClaimableBalanceProcessor | `op.Body.MustCreateClaimableBalanceOp()` | TODO |
| ClaimClaimableBalance | ClaimClaimableBalanceProcessor | `op.Body.MustClaimClaimableBalanceOp()` | TODO |
| BeginSponsoringFutureReserves | SponsorshipProcessor | `op.Body.MustBeginSponsoringFutureReservesOp()` | TODO |
| EndSponsoringFutureReserves | EndSponsorshipProcessor | `op.Body.MustEndSponsoringFutureReservesOp()` | TODO |
| RevokeSponsorship | RevokeSponsorshipProcessor | `op.Body.MustRevokeSponsorshipOp()` | TODO |
| Clawback | ClawbackProcessor | `op.Body.MustClawbackOp()` | TODO |
| ClawbackClaimableBalance | ClawbackClaimableBalanceProcessor | `op.Body.MustClawbackClaimableBalanceOp()` | TODO |
| SetTrustLineFlags | SetTrustLineFlagsProcessor | `op.Body.MustSetTrustLineFlagsOp()` | TODO |
| LiquidityPoolDeposit | LiquidityPoolDepositProcessor | `op.Body.MustLiquidityPoolDepositOp()` | TODO |
| LiquidityPoolWithdraw | LiquidityPoolWithdrawProcessor | `op.Body.MustLiquidityPoolWithdrawOp()` | TODO |

### Soroban Processors

| Processor | Purpose | Key SDK Methods | Status |
|-----------|---------|-----------------|--------|
| **InvokeHostFunctionProcessor** | Process contract invocations | `op.Body.MustInvokeHostFunctionOp()` | Partial |
| **ExtendFootprintTTLProcessor** | Process TTL extensions | `op.Body.MustExtendFootprintTtlOp()` | TODO |
| **RestoreFootprintProcessor** | Process footprint restorations | `op.Body.MustRestoreFootprintOp()` | TODO |
| **ContractEventProcessor** | Process contract events | `tx.GetTransactionEvents()` | Updated ✓ |
| **ContractDataProcessor** | Process contract data changes | `tx.GetTransactionEvents()` | Exists |
| **DiagnosticEventProcessor** | Process diagnostic events | `tx.GetDiagnosticEvents()` | TODO |

### State Change Processors

| Processor | Purpose | Key SDK Methods | Status |
|-----------|---------|-----------------|--------|
| **LedgerEntryChangeProcessor** | Track ledger entry changes | `changes.LedgerEntryChanges()` | Partial |
| **AccountStateProcessor** | Track account state changes | State change helpers | TODO |
| **TrustlineStateProcessor** | Track trustline changes | State change helpers | TODO |
| **OfferStateProcessor** | Track offer changes | State change helpers | TODO |
| **ContractCodeProcessor** | Track contract code changes | State change helpers | TODO |
| **ContractDataStateProcessor** | Track contract data changes | State change helpers | TODO |

## Existing Processors to Update

### High Priority Updates

1. **ContractEventProcessor** ✓ - Already updated to use `GetTransactionEvents()`
2. **ContractInvocationProcessor** - Needs update for V4 support
3. **PaymentProcessor** - Partially uses helpers, needs full migration
4. **AccountDataProcessor** - Direct metadata access needs migration

### Medium Priority Updates

5. **AssetStatsProcessor** - Update to use asset helpers
6. **MarketMetricsProcessor** - Update for offer operations
7. **LedgerChangeProcessor** - Migrate to state change helpers
8. **AccountEffectProcessor** - Use effect extraction helpers

### Low Priority Updates

9. **LatestLedgerProcessor** - Simple updates needed
10. **BlankProcessor** - No changes needed
11. **DebugProcessors** - Optional updates

## Implementation Template

### Basic Processor Structure

```go
package processor

import (
    "context"
    "fmt"
    "github.com/stellar/go/ingest"
    "github.com/stellar/go/xdr"
)

type MyOperationProcessor struct {
    processors []Processor
    stats      ProcessorStats
}

func NewMyOperationProcessor(config map[string]interface{}) (*MyOperationProcessor, error) {
    return &MyOperationProcessor{}, nil
}

func (p *MyOperationProcessor) Process(ctx context.Context, msg Message) error {
    ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
    if !ok {
        return fmt.Errorf("expected LedgerCloseMeta")
    }

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

        // Use SDK helpers instead of manual processing
        if !tx.Result.Successful() {
            continue
        }

        for i, op := range tx.Envelope.Operations() {
            if op.Body.Type == xdr.OperationTypeMyOperation {
                p.processOperation(tx, i, op)
            }
        }
    }
    return nil
}
```

## Migration Steps for Each Processor

### Step 1: Identify Direct Metadata Access

Search for patterns like:
- `tx.UnsafeMeta.V3`
- `tx.UnsafeMeta.V4`
- Manual version checking with `switch tx.UnsafeMeta.V`

### Step 2: Replace with SDK Helpers

Map old patterns to new helpers:
- Events: Use `GetTransactionEvents()`
- Success check: Use `Result.Successful()`
- Operations: Use type-specific `Must*()` methods
- Time: Use `tx.CloseTime()`

### Step 3: Update Error Handling

SDK helpers return errors for invalid operations:
```go
events, err := tx.GetTransactionEvents()
if err != nil {
    // Not a Soroban transaction - handle appropriately
}
```

### Step 4: Test with Multiple Protocol Versions

Ensure processors work with:
- Protocol 22 (V3 metadata)
- Protocol 23 (V4 metadata)
- Future protocols (via SDK abstraction)

## Testing Strategy

### Unit Tests

Create test fixtures for each processor:
```go
func TestProcessorWithV3Metadata(t *testing.T) {
    // Test with V3 transaction metadata
}

func TestProcessorWithV4Metadata(t *testing.T) {
    // Test with V4 transaction metadata
}
```

### Integration Tests

Test against real ledger data:
1. Mainnet data (primarily V3)
2. Testnet data (mix of V3 and V4)
3. Futurenet data (latest protocol)

### Performance Tests

Benchmark before and after migration:
```go
func BenchmarkProcessorOld(b *testing.B) {
    // Benchmark with manual processing
}

func BenchmarkProcessorNew(b *testing.B) {
    // Benchmark with SDK helpers
}
```

## Priority Implementation Order

### Phase 1: Core Infrastructure (Week 1-2)
1. TransactionProcessor (base for all others)
2. OperationProcessor (operation routing)
3. EffectProcessor (effect extraction)
4. Update existing ContractInvocationProcessor

### Phase 2: Payment Operations (Week 3-4)
1. PaymentProcessor (update existing)
2. PathPaymentProcessor (both strict variants)
3. CreateAccountProcessor (update existing)
4. AccountMergeProcessor

### Phase 3: Asset/Offer Operations (Week 5-6)
1. ManageBuyOfferProcessor
2. ManageSellOfferProcessor
3. ChangeTrustProcessor
4. SetTrustLineFlagsProcessor

### Phase 4: Advanced Operations (Week 7-8)
1. ClaimableBalanceProcessor (create & claim)
2. SponsorshipProcessors (all variants)
3. ClawbackProcessors
4. LiquidityPoolProcessors

### Phase 5: Soroban Operations (Week 9-10)
1. ExtendFootprintTTLProcessor
2. RestoreFootprintProcessor
3. DiagnosticEventProcessor
4. Update all Soroban processors for V4

### Phase 6: State & Analytics (Week 11-12)
1. All state change processors
2. Update analytics processors
3. Performance optimization
4. Documentation updates

## Code Organization

```
processor/
├── base/
│   ├── transaction_processor.go
│   ├── operation_processor.go
│   └── effect_processor.go
├── operations/
│   ├── payment_processor.go
│   ├── create_account_processor.go
│   ├── manage_offer_processor.go
│   └── ... (one file per operation type)
├── soroban/
│   ├── invoke_host_function_processor.go
│   ├── contract_event_processor.go
│   └── diagnostic_event_processor.go
├── state/
│   ├── ledger_entry_processor.go
│   ├── account_state_processor.go
│   └── trustline_state_processor.go
└── analytics/
    ├── market_metrics_processor.go
    └── asset_stats_processor.go
```

## Success Metrics

- [ ] All processors use SDK helper methods
- [ ] Zero direct access to UnsafeMeta
- [ ] 100% protocol version compatibility
- [ ] Performance maintained or improved
- [ ] Comprehensive test coverage
- [ ] Updated documentation

## Maintenance Plan

1. **SDK Updates**: Regular updates to latest Stellar Go SDK
2. **Protocol Changes**: Test new protocols on testnet first
3. **Performance Monitoring**: Track processing speed
4. **Error Tracking**: Monitor for new error patterns
5. **Documentation**: Keep examples current

This migration will make the CDP pipeline more maintainable, reliable, and future-proof against protocol changes.