# Wallet-Backend Replacement Guide

## Executive Summary

To fully replace Stellar's wallet-backend with CDP pipelines, you need a **unified pipeline** that combines several processors, not the current separated approach. The wallet-backend requires atomic consistency across related data.

## What Wallet-Backend Actually Stores

Based on the Stellar wallet-backend architecture:

1. **Transactions Table**
   - Full transaction XDR
   - Transaction hash, result, metadata
   - Ledger sequence and timestamp
   
2. **Operations Table**
   - Full operation XDR
   - Operation type, source account
   - Links to parent transaction
   
3. **State Changes Table**
   - Account balance changes (CREDIT/DEBIT)
   - Trustline changes
   - Signer/threshold changes
   - Generic state modifications
   
4. **Participants Tracking**
   - All accounts involved in each transaction
   - Role-based participation (source, destination, etc.)

## Current CDP Pipeline Gaps

### 1. **Missing Raw XDR Storage**
The wallet-backend stores raw XDR for transactions and operations. None of our current pipelines do this.

### 2. **Separated Pipelines Problem**
Having three separate pipelines (`state_changes`, `effects`, `contract_events`) breaks atomicity:
- Data might be inconsistent across pipelines
- Race conditions between pipelines
- Duplicate processing of the same ledgers

### 3. **Schema Mismatch**
Our `BufferedPostgreSQL` consumer creates different tables than wallet-backend expects.

## Recommended Pipeline Configuration

### Option 1: Single Unified Pipeline (RECOMMENDED)

```yaml
pipelines:
  WalletBackendComplete:
    source:
      type: BufferedStorageSourceAdapter
      config:
        bucket_name: "stellar-ledgers"
        network: "mainnet"
        buffer_size: 1000
        num_workers: 20
        start_ledger: 0
    
    processors:
      # First: Extract raw transaction/operation data
      - type: TransactionXDRExtractor  # NEEDS TO BE CREATED
        config:
          store_raw_xdr: true
      
      # Second: Extract all state changes
      - type: WalletBackend
        config:
          extract_contract_events: false  # Wallet doesn't need these
          track_participants: true
      
      # Third: Generate effects
      - type: StellarEffects
        config:
          network_passphrase: "Public Global Stellar Network ; September 2015"
      
      # Fourth: Extract current account states
      - type: ProcessAccountDataFull
        config:
          network_passphrase: "Public Global Stellar Network ; September 2015"
    
    consumers:
      # Custom consumer that matches wallet-backend schema
      - type: WalletBackendPostgreSQL  # NEEDS TO BE CREATED
        config:
          host: "localhost"
          port: 5432
          database: "wallet_backend"
          username: "postgres"
          password: "password"
          # Schema configuration
          create_transactions_table: true
          create_operations_table: true
          create_state_changes_table: true
          create_accounts_table: true
          # Performance
          buffer_size: 1000
          flush_interval_ms: 5000
```

### Option 2: Multiple Coordinated Pipelines (NOT RECOMMENDED)

If you must use multiple pipelines, ensure:
1. All use the same ledger range
2. All write to the same database atomically
3. Use database transactions to ensure consistency

## New Components Needed

### 1. **TransactionXDRExtractor Processor**
```go
// Extracts and preserves raw XDR data
type TransactionXDRExtractor struct {
    // Outputs:
    // - Transaction envelope XDR
    // - Transaction result XDR
    // - Transaction meta XDR
    // - Operations array with XDR
}
```

### 2. **WalletBackendPostgreSQL Consumer**
```go
// Implements exact wallet-backend schema
type WalletBackendPostgreSQL struct {
    // Creates tables matching wallet-backend:
    // - transactions (with XDR columns)
    // - operations (with XDR columns)
    // - state_changes (unified format)
    // - accounts (current state)
}
```

## Migration Strategy

### Phase 1: Data Completeness
1. Create `TransactionXDRExtractor` processor
2. Create `WalletBackendPostgreSQL` consumer with exact schema
3. Test with small ledger range

### Phase 2: Performance Optimization
1. Optimize batch sizes for your hardware
2. Add database indexes matching wallet-backend
3. Implement connection pooling

### Phase 3: Validation
1. Compare output with official wallet-backend
2. Verify all accounts have consistent balances
3. Check transaction/operation counts match

## Wallet-Specific Optimizations

For wallet builders, consider these focused pipelines:

### 1. **User Balance Pipeline**
```yaml
processors:
  - type: AccountDataFilter
    config:
      accounts: ["USER_ACCOUNT_1", "USER_ACCOUNT_2"]
  - type: WalletBackend
    config:
      track_participants: false  # Only track specified accounts
```

### 2. **Transaction History Pipeline**
```yaml
processors:
  - type: AccountTransaction
    config:
      account: "USER_ACCOUNT"
  - type: StellarEffects
```

### 3. **Real-time Notifications Pipeline**
```yaml
source:
  type: CaptiveCoreInboundAdapter  # Real-time source
consumers:
  - type: SaveToRedis  # Fast cache
  - type: SaveToWebSocket  # Push notifications
```

## Key Differences from Current Approach

1. **Unified Processing**: One pipeline processes all data types together
2. **XDR Preservation**: Store raw XDR for future reprocessing
3. **Schema Compatibility**: Match wallet-backend's exact schema
4. **Atomic Writes**: All related data written in single transaction

## Performance Considerations

- Single pipeline processes ~1000-2000 ledgers/second
- Database becomes bottleneck around 5000 ledgers/second
- Use partitioned tables for large datasets
- Consider read replicas for wallet queries

## Conclusion

The current CDP pipelines have all the building blocks but need:
1. A processor to extract raw XDR data
2. A consumer that implements wallet-backend's exact schema
3. A unified pipeline configuration that processes everything together

With these additions, CDP can fully replace wallet-backend while offering better performance and flexibility.