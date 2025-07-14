# Contract Invocation Dual Representation Bug Fixes

## Overview

This document summarizes the bug fixes applied to resolve PostgreSQL insertion errors that occurred after implementing dual representation for contract invocations.

## Issues Identified

### 1. **Primary Issue: SQL Type Conversion Error**
**Error:** `sql: converting argument $4 type: unsupported type xdr.ScVal, a struct`

**Root Cause:** The PostgreSQL consumer was trying to insert raw `xdr.ScVal` structures directly into JSONB columns, but PostgreSQL's driver doesn't know how to serialize XDR struct types.

**Location:** `consumer/consumer_save_contract_invocations_to_postgresql.go:329`

### 2. **Secondary Issue: Nil Pointer Dereference**
**Error:** `panic: runtime error: invalid memory address or nil pointer dereference`

**Root Cause:** Accessing `contractData.Contract.ContractId` when `ContractId` was nil in state change processing.

**Location:** `processor/processor_contract_invocation.go:456`

## Fixes Applied

### 1. **PostgreSQL Consumer Updates**

#### **1.1 Fixed Diagnostic Events Insertion**
**Problem:** Trying to insert raw `xdr.ScVal` directly into database.

**Before:**
```go
_, err = stmt.ExecContext(
    ctx,
    invocationID,
    event.ContractID,
    topicsJSON,
    event.Data,  // xdr.ScVal - unsupported!
)
```

**After:**
```go
// Use decoded data instead of raw XDR
dataJSON, err := json.Marshal(event.DataDecoded)
if err != nil {
    return fmt.Errorf("failed to marshal data: %w", err)
}

_, err = stmt.ExecContext(
    ctx,
    invocationID,
    event.ContractID,
    topicsJSON,
    dataJSON,  // Now properly serialized JSON
)
```

#### **1.2 Enhanced Database Schema**
**Added dual representation support with separate columns:**

```sql
CREATE TABLE contract_diagnostic_events (
    -- ... existing columns ...
    topics JSONB NOT NULL,              -- Raw XDR as JSON
    topics_decoded JSONB,               -- Human-readable decoded
    data JSONB,                         -- Raw XDR as JSON
    data_decoded JSONB,                 -- Human-readable decoded
    -- ... other columns ...
);

CREATE TABLE contract_invocations (
    -- ... existing columns ...
    arguments_raw JSONB,                -- Raw XDR arguments
    arguments JSONB,                    -- JSON messages
    arguments_decoded JSONB,            -- Human-readable decoded
    -- ... other columns ...
);
```

#### **1.3 Updated Insertion Logic**
**Now properly handles all dual representation fields:**

```go
// Store both raw and decoded versions
topicsRawJSON, _ := json.Marshal(event.Topics)          // Raw XDR
topicsDecodedJSON, _ := json.Marshal(event.TopicsDecoded) // Decoded
dataRawJSON, _ := json.Marshal(event.Data)              // Raw XDR  
dataDecodedJSON, _ := json.Marshal(event.DataDecoded)   // Decoded

stmt.ExecContext(ctx, invocationID, event.ContractID,
    topicsRawJSON, topicsDecodedJSON, dataRawJSON, dataDecodedJSON)
```

### 2. **Processor Nil Pointer Fixes**

#### **2.1 Added Nil Checks in State Change Extraction**
**Added defensive programming for all contract data access:**

```go
// Before: Could panic if contractData is nil
contractData := change.Post.Data.ContractData
if stateChange := p.extractStateChangeFromContractData(*contractData, ...) {

// After: Safe nil check
contractData := change.Post.Data.ContractData
if contractData != nil {
    if stateChange := p.extractStateChangeFromContractData(*contractData, ...) {
```

#### **2.2 Added ContractId Nil Check**
**Protected against nil ContractId access:**

```go
// Before: Could panic if ContractId is nil
contractIDBytes := contractData.Contract.ContractId
contractID, err := strkey.Encode(strkey.VersionByteContract, contractIDBytes[:])

// After: Safe nil check with graceful handling
contractIDBytes := contractData.Contract.ContractId
if contractIDBytes == nil {
    log.Printf("Contract ID is nil in state change data")
    return nil
}
contractID, err := strkey.Encode(strkey.VersionByteContract, contractIDBytes[:])
```

### 3. **Schema Migration Support**

#### **3.1 Backward Compatibility**
**Added automatic column migration:**

```sql
DO $$ 
BEGIN 
    -- Add new columns if they don't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'contract_diagnostic_events' 
                   AND column_name = 'topics_decoded') THEN
        ALTER TABLE contract_diagnostic_events ADD COLUMN topics_decoded JSONB;
    END IF;
    -- ... similar for other columns ...
END $$;
```

#### **3.2 Enhanced Indexing**
**Added GIN indexes for efficient JSON querying:**

```sql
CREATE INDEX IF NOT EXISTS idx_diagnostic_events_topics_decoded_gin 
    ON contract_diagnostic_events USING gin(topics_decoded);
CREATE INDEX IF NOT EXISTS idx_diagnostic_events_data_decoded_gin 
    ON contract_diagnostic_events USING gin(data_decoded);
```

### 4. **Comprehensive Testing**

#### **4.1 Added Nil Handling Test**
**Prevents regression of nil pointer issues:**

```go
func TestExtractStateChangeFromContractDataNilChecks(t *testing.T) {
    // Test with nil ContractId - should return nil gracefully
    contractDataWithNilID := xdr.ContractDataEntry{
        Contract: xdr.ScAddress{
            ContractId: nil, // This should be handled gracefully
        },
        // ... other fields ...
    }
    
    stateChange := processor.extractStateChangeFromContractData(contractDataWithNilID, ...)
    if stateChange != nil {
        t.Error("Expected nil state change when ContractId is nil")
    }
}
```

## Results

### ✅ **Issues Resolved:**
1. **No more PostgreSQL type conversion errors** - Raw XDR data properly serialized as JSON
2. **No more segmentation faults** - Nil pointers handled gracefully with logging
3. **Enhanced data storage** - Both raw and decoded representations stored for maximum utility
4. **Backward compatibility maintained** - Existing databases automatically migrated
5. **Improved observability** - Better logging for debugging issues

### ✅ **Performance Benefits:**
- **Efficient querying** - GIN indexes on decoded JSONB fields for fast searches
- **Data flexibility** - Consumers can choose raw or decoded data based on needs
- **Reduced processing** - Pre-computed decoded representations stored in database

### ✅ **Error Handling Improvements:**
- **Graceful degradation** - Nil data logged instead of causing crashes
- **Comprehensive logging** - Clear error messages for debugging
- **Robust validation** - Multiple safety checks prevent data corruption

## Database Schema Changes

### **Before (Single Representation):**
```sql
CREATE TABLE contract_diagnostic_events (
    topics JSONB NOT NULL,
    data JSONB
);
```

### **After (Dual Representation):**
```sql
CREATE TABLE contract_diagnostic_events (
    topics JSONB NOT NULL,              -- Raw XDR as JSON
    topics_decoded JSONB,               -- Human-readable
    data JSONB,                         -- Raw XDR as JSON  
    data_decoded JSONB                  -- Human-readable
);
```

## Migration Path

1. **Automatic Detection** - Schema checks existing columns
2. **Safe Addition** - New columns added only if missing
3. **Index Creation** - Performance indexes created automatically
4. **Data Preservation** - Existing data remains intact
5. **Graceful Handling** - NULL values handled for new columns until populated

## Testing Strategy

1. **Unit Tests** - Nil pointer protection verified
2. **Integration Tests** - Full pipeline tested with dual representation
3. **Schema Tests** - Migration logic validated
4. **Error Injection** - Nil data scenarios tested

The fixes ensure the contract invocations pipeline can handle all edge cases robustly while maintaining high performance and data accessibility through dual representation.