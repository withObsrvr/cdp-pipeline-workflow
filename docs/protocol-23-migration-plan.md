# Protocol 23 Migration Implementation Plan

## Overview

This document outlines the implementation plan to address build errors encountered after updating the stellar/go dependency to the protocol-23 branch. The changes are primarily related to API modifications in the Stellar Go SDK that reflect Protocol 23's improvements to Soroban state management.

## Build Errors Summary

The following build errors were identified:

1. `utils/ledger.go:66` - `TotalByteSizeOfBucketList` undefined
2. `processor/processor.go:115,222` - `LedgerEntryChangeType` undefined
3. `processor/processor_contract_creation.go:145` - `LedgerEntryChangeType` undefined
4. `processor/processor_contract_invocation.go:413` - `LedgerEntryChangeType` undefined
5. `processor/processor_latest_ledger.go:169` - `ReadBytes` undefined
6. `processor/processor_phoenix_amm.go:343` - `LedgerEntryChangeType` undefined

## Required Changes

### 1. Update TotalByteSizeOfBucketList Field Reference

**File**: `utils/ledger.go:66`

**Change**: Replace `TotalByteSizeOfBucketList` with `TotalByteSizeOfLiveSorobanState`

```go
// Before
totalByteSizeOfBucketList := uint64(lcmV1.TotalByteSizeOfBucketList)

// After
totalByteSizeOfBucketList := uint64(lcmV1.TotalByteSizeOfLiveSorobanState)
```

**Rationale**: Protocol 23 renamed this field to better reflect that it specifically measures the size of live Soroban state data rather than the entire bucket list.

### 2. Update LedgerEntryChangeType Method to Field Access

**Files**:
- `processor/processor.go:115,222`
- `processor/processor_contract_creation.go:145`
- `processor/processor_contract_invocation.go:413`
- `processor/processor_phoenix_amm.go:343`

**Change**: Replace method call `LedgerEntryChangeType()` with direct field access `ChangeType`

```go
// Before
changeType := change.LedgerEntryChangeType()

// After
changeType := change.ChangeType
```

**Rationale**: The ingest.Change struct now exposes ChangeType as a direct field instead of through a method, simplifying the API.

**Alternative Approach (Based on Protocol-23 Processors)**:
The stellar/go protocol-23 processors use a utility function approach:
```go
// Using utils.ExtractEntryFromChange
ledgerEntry, changeType, outputDeleted, err := utils.ExtractEntryFromChange(change)
```
This utility function extracts the ledger entry, change type, and deletion status in one call. Consider adopting this pattern for cleaner code.

### 3. Update ReadBytes Field Reference

**File**: `processor/processor_latest_ledger.go:169`

**Change**: Replace `ReadBytes` with `DiskReadBytes`

```go
// Before
sMetrics.readBytes = uint32(sorobanData.Resources.ReadBytes)

// After
sMetrics.readBytes = uint32(sorobanData.Resources.DiskReadBytes)
```

**Rationale**: Protocol 23 renamed this field to be more specific about what type of read operations are being measured (disk reads).

## Implementation Steps

### Phase 1: Code Updates
1. **Update utils/ledger.go**
   - Locate line 66 and update the field reference
   - Verify the variable name still makes sense (consider renaming for clarity)

2. **Update processor package files**
   - Update all instances of `LedgerEntryChangeType()` to `ChangeType`
   - Files to update:
     - processor/processor.go (2 instances)
     - processor/processor_contract_creation.go (1 instance)
     - processor/processor_contract_invocation.go (1 instance)
     - processor/processor_phoenix_amm.go (1 instance)

3. **Update processor_latest_ledger.go**
   - Update the ReadBytes field reference to DiskReadBytes

### Phase 2: Verification
1. **Build verification**
   ```bash
   CGO_ENABLED=1 go build -o cdp-pipeline-workflow
   ```

2. **Run tests**
   ```bash
   go test ./...
   ```

3. **Integration testing**
   - Run development pipeline with test data
   - Verify all processors work correctly with Protocol 23 changes

### Phase 3: Documentation Updates
1. Update any documentation that references the changed fields
2. Add notes about Protocol 23 compatibility in CLAUDE.md if necessary
3. Update any configuration examples that might be affected

## Testing Strategy

1. **Unit Tests**: Ensure all existing tests pass after changes
2. **Integration Tests**: 
   - Test with Protocol 23 test data
   - Verify ledger processing works correctly
   - Confirm contract event processing handles new field names
3. **Performance Testing**: Ensure no performance regression from API changes

## Rollback Plan

If issues are discovered:
1. Revert go.mod changes to previous stellar/go version
2. Revert code changes made for Protocol 23
3. Document any incompatibilities found for future reference

## Timeline

- **Immediate**: Fix build errors (Phase 1)
- **Day 1**: Complete verification and testing (Phase 2)
- **Day 2**: Update documentation and finalize changes (Phase 3)

## Notes

- The renamed fields better reflect their actual purpose in Protocol 23
- These changes are backward-incompatible with previous protocol versions
- Ensure all team members are aware of these API changes before merging

## Insights from Protocol-23 Processors

After reviewing the stellar/go protocol-23 processors, the following patterns were observed:

1. **Change Extraction Pattern**: The protocol-23 processors use `utils.ExtractEntryFromChange()` which returns:
   - The ledger entry
   - The change type (as a value, not through a method)
   - A boolean indicating if the entry was deleted
   - Any extraction error

2. **Consistent Error Handling**: All processors check for extraction errors before processing changes

3. **Direct Field Access**: Change types and other properties are accessed directly as struct fields rather than through getter methods

4. **Soroban-Specific Updates**: 
   - Resource tracking fields have been renamed for clarity (e.g., `ReadBytes` → `DiskReadBytes`)
   - State size measurements are now more specific (e.g., `TotalByteSizeOfBucketList` → `TotalByteSizeOfLiveSorobanState`)

These patterns suggest that Protocol 23 emphasizes cleaner APIs with direct field access and more descriptive naming for Soroban-related functionality.

## Strategic Consideration: Using Stellar Processors Directly

After reviewing the ttp-processor-demo repository, we should consider aligning more closely with Stellar's official processors to maintain better compatibility:

### Current CDP Pipeline Architecture
The cdp-pipeline-workflow currently implements its own processor framework with custom implementations of utilities like `ExtractEntryFromChange()`. While this provides flexibility, it requires manual maintenance of Protocol compatibility.

### Benefits of Using Stellar Processors Directly

1. **Automatic Protocol Updates**: Official processors are maintained by Stellar and updated with each protocol version
2. **Consistency**: Ensures our data processing matches Stellar's reference implementations
3. **Reduced Maintenance**: Less custom code to maintain when protocols change
4. **Better Testing**: Stellar processors are thoroughly tested by the Stellar team

### Recommended Approach

**Phase 4: Evaluate Stellar Processor Integration** (Post Protocol-23 Migration)

1. **Audit Current Processors**: Identify which of our custom processors could be replaced with official Stellar processors:
   - Contract event processing
   - Account change tracking
   - Payment filtering
   - Market data extraction

2. **Gradual Migration**: Replace custom processors with Stellar equivalents where possible:
   ```go
   // Instead of custom processor
   import "github.com/stellar/go/processors/contract"
   
   // Use official processors
   contractProcessor := contract.NewContractEventProcessor()
   ```

3. **Hybrid Approach**: Keep custom business logic while using Stellar utilities:
   ```go
   // Use Stellar utilities but keep custom pipeline architecture
   import "github.com/stellar/go/processors/utils"
   
   ledgerEntry, changeType, deleted, err := utils.ExtractEntryFromChange(change)
   ```

This strategic approach would reduce the impact of future protocol changes and ensure better alignment with Stellar's official implementations.