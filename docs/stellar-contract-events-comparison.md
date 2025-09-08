# Contract Events Processor Comparison

This document compares the two contract event processors available in the CDP Pipeline.

## 1. Custom Contract Event Processor (`ContractEvent`)

**File**: `processor_contract_events.go`

### Features:
- Custom event type detection (transfer, mint, burn, etc.)
- Uses `ConvertScValToJSON` for decoding with custom formatting
- Tracks detailed statistics (successful vs failed events)
- Supports custom event categorization
- Includes diagnostic events
- More granular control over output format

### Output Structure:
```json
{
  "timestamp": "2024-01-01T00:00:00Z",
  "ledger_sequence": 12345,
  "transaction_hash": "abc123...",
  "contract_id": "CBCRIITNA4O3PBZCM7J4WIAGEVXW7HEEX55QIBZC3CFSYK3LYJ65WZHL",
  "type": "contract",
  "event_type": "transfer",
  "topic": [...],
  "topic_decoded": ["transfer", {...}, {...}, "USDC:..."],
  "data": {...},
  "data_decoded": {...},
  "in_successful_tx": true,
  "event_index": 0,
  "operation_index": 2,
  "network_passphrase": "Test SDF Network ; September 2015"
}
```

### Use When:
- You need custom event type detection
- You want specific output formatting
- You need diagnostic events
- You require custom processing logic

## 2. Stellar SDK Contract Event Processor (`StellarContractEvent`)

**File**: `processor_stellar_contract_events.go`

### Features:
- Uses official Stellar SDK implementation
- Guaranteed compatibility with protocol updates
- Simpler implementation with less custom code
- Standard Stellar event format
- Built-in XDR encoding/decoding

### Output Structure:
```json
{
  "events": [
    {
      "TransactionHash": "abc123...",
      "TransactionID": 12345,
      "Successful": true,
      "LedgerSequence": 12345,
      "ClosedAt": "2024-01-01T00:00:00Z",
      "InSuccessfulContractCall": true,
      "ContractId": "CBCRIITNA4O3PBZCM7J4WIAGEVXW7HEEX55QIBZC3CFSYK3LYJ65WZHL",
      "Type": 0,
      "TypeString": "contract",
      "Topics": {...},
      "TopicsDecoded": {...},
      "Data": {...},
      "DataDecoded": {...},
      "ContractEventXDR": "base64..."
    }
  ],
  "transaction_hash": "abc123...",
  "ledger_sequence": 12345,
  "processor_name": "stellar_contract_event_processor",
  "message_type": "stellar_contract_events",
  "timestamp": "2024-01-01T00:00:00Z"
}
```

### Use When:
- You want to use the official Stellar implementation
- You need guaranteed protocol compatibility
- You prefer standard Stellar formats
- You want minimal maintenance overhead

## Configuration Examples

### Using Custom Processor:
```yaml
processors:
  - type: ContractEvent
    config:
      name: custom_event_processor
      network_passphrase: "Test SDF Network ; September 2015"
```

### Using Stellar SDK Processor:
```yaml
processors:
  - type: StellarContractEvent
    config:
      name: stellar_event_processor
      network_passphrase: "Test SDF Network ; September 2015"
```

## Key Differences

1. **Event Type Detection**:
   - Custom: Has `DetectEventType()` for identifying transfer, mint, burn events
   - SDK: Returns raw event types as integers

2. **Data Format**:
   - Custom: Uses custom `ConvertScValToJSON` with enhanced ScAddress support
   - SDK: Uses Stellar's standard format with maps

3. **Output Grouping**:
   - Custom: One message per event
   - SDK: Groups all events from a transaction

4. **Maintenance**:
   - Custom: Requires updates for protocol changes
   - SDK: Automatically updated with Stellar SDK

## Recommendation

- Use **StellarContractEvent** for production systems that need stability and standard compliance
- Use **ContractEvent** when you need custom event processing, type detection, or specific output formats

Both processors handle all ScAddress types (Account, Contract, Muxed, Claimable Balance, Liquidity Pool) correctly.