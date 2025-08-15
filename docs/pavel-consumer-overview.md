# PAVEL Consumer Overview

## Public, Append-Only, Verifiable Event Log for StellarCarbon

### What is PAVEL?

PAVEL (Public, Append-Only, Verifiable Event Log) is a specialized data consumer for the CDP (Composable Data Platform) pipeline that creates a tamper-evident, publicly accessible audit trail of carbon credit transactions on the Stellar blockchain. It's designed specifically for StellarCarbon's sink_carbon contract invocations, providing transparency and verifiability for carbon offset activities.

### Purpose and Goals

The PAVEL consumer addresses several critical needs for StellarCarbon and the broader carbon credit ecosystem:

1. **Transparency**: Creates a public record of all carbon credit retirements that anyone can access without authentication
2. **Verifiability**: Provides cryptographic proofs that data hasn't been tampered with
3. **Accessibility**: Enables simple HTTP/HTTPS access to event data without requiring blockchain expertise
4. **Permanence**: Archives data in content-addressed storage (IPFS) for long-term preservation
5. **Compliance**: Supports regulatory requirements for transparent carbon credit tracking

### How It Works

#### Data Flow

```
Stellar Blockchain → CDP Pipeline → PAVEL Consumer → Public Storage
         ↓                ↓                ↓              ↓
    Ledger Data    Contract Events   Verified Log    HTTP Access
```

1. **Input**: The consumer receives `ExtractedContractInvocation` messages from the CDP pipeline containing:
   - Carbon credit retirement details (funder, recipient, amount, project)
   - Transaction metadata (timestamp, ledger number, transaction hash)
   - Contract execution status

2. **Processing**: Each event is:
   - Validated and filtered for sink_carbon invocations
   - Assigned a unique TOID (Transaction Operation ID)
   - Linked to the previous event via CID (Content Identifier)
   - Timestamped and enriched with metadata

3. **Storage**: Events are stored in multiple layers:
   - **Hot Storage**: Recent 24 hours in NDJSON format for real-time access
   - **Cold Storage**: Historical segments in compressed format
   - **IPFS Archive**: Immutable CAR files with content addressing
   - **Blockchain Anchor**: Optional Merkle root anchoring on Stellar

### Key Features

#### 1. Append-Only Design
- Events can only be added, never modified or deleted
- Each event references the previous event's CID
- Creates an immutable chain of events

#### 2. Cryptographic Verification
- Each segment is signed with Ed25519 signatures
- Merkle trees provide efficient bulk verification
- Optional on-chain anchoring adds blockchain-level trust

#### 3. Public Access Pattern
```
https://storage.googleapis.com/stellarcarbon-public-data/
├── stellarcarbon/sink_carbon/mainnet/
│   ├── stream.json                    # Stream metadata
│   ├── HEAD.json                      # Latest event pointer
│   ├── hot/
│   │   ├── today.ndjson              # Recent events (24h)
│   │   └── today.ndx                  # Byte-offset index
│   ├── events/
│   │   └── 2024/01/15/
│   │       ├── 0001.ndjson.gz        # Historical segments
│   │       └── 0002.ndjson.gz
│   └── ipfs/
│       ├── 0001.car                   # IPFS archives
│       └── 0002.car
```

#### 4. Efficient Querying
- Byte-offset indexes enable range queries without downloading entire files
- TOID-based cursoring for incremental updates
- No API keys or authentication required

### Event Schema

Each event in the log contains:

```json
{
  "toid": 250709376516096,
  "ledger": 58466921,
  "timestamp": "2024-01-15T10:30:45Z",
  "contract_id": "CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O",
  "transaction_hash": "5f8a3b2c1d4e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1",
  "function": "sink_carbon",
  "schema_version": 1,
  "contract_version": 1,
  "data": {
    "funder": "GABC...XYZ",
    "recipient": "GDEF...UVW",
    "amount": 1000000,
    "project_id": "PROJ_AMAZON_001",
    "memo_text": "Carbon offset for flight emissions",
    "email": "user@example.com"
  },
  "prev_cid": "bafyreigdyr...previous"
}
```

### Security Model

#### Trust Assumptions
1. **Data Source**: Trust in the Stellar blockchain and CDP pipeline
2. **Storage Provider**: Trust that cloud storage preserves data integrity
3. **Signing Key**: Trust in the security of the Ed25519 private key

#### Verification Levels
1. **Basic**: Verify Ed25519 signatures on segments
2. **Intermediate**: Reconstruct and verify Merkle trees
3. **Advanced**: Verify on-chain anchors via Stellar RPC
4. **Complete**: Validate entire CID chain from genesis

### Use Cases

#### For StellarCarbon
- Provide transparent proof of carbon credit retirements
- Enable third-party auditing of offset activities
- Support regulatory compliance reporting
- Build trust with carbon credit buyers

#### For Developers
- Build dashboards showing carbon offset activity
- Create verification tools for carbon credits
- Integrate carbon data into existing applications
- Develop analytics on carbon market trends

#### For Auditors
- Verify specific carbon retirement transactions
- Analyze patterns in carbon credit usage
- Cross-reference with other data sources
- Generate compliance reports

### Client Implementation Example

```javascript
// Fetch recent carbon sink events
async function getRecentCarbonEvents() {
  const baseURL = 'https://storage.googleapis.com/stellarcarbon-public-data';
  const streamPath = 'stellarcarbon/sink_carbon/mainnet';
  
  // Get current head
  const head = await fetch(`${baseURL}/${streamPath}/HEAD.json`)
    .then(r => r.json());
  
  // Fetch today's events
  const events = await fetch(`${baseURL}/${streamPath}/hot/today.ndjson`)
    .then(r => r.text())
    .then(text => text.trim().split('\n').map(line => JSON.parse(line)));
  
  return events;
}

// Verify a specific event
async function verifyEvent(eventCID) {
  // 1. Fetch the segment containing this event
  // 2. Verify the segment signature
  // 3. Check the CID chain
  // 4. Optionally verify on-chain anchor
}
```

### Benefits Over Traditional Approaches

| Traditional API | PAVEL Consumer |
|----------------|----------------|
| Requires API keys | No authentication needed |
| Can modify historical data | Immutable append-only log |
| Single point of failure | Distributed across storage layers |
| Opaque data changes | Cryptographically verifiable |
| Rate limits apply | Unlimited public reads |
| Vendor lock-in | Standard file formats |

### Integration with CDP Pipeline

The PAVEL consumer integrates seamlessly with the existing CDP pipeline:

1. **BufferedStorageSourceAdapter** reads ledger data from cloud storage
2. **ContractInvocationProcessor** extracts all contract invocations
3. **ContractInvocationExtractor** filters and structures sink_carbon calls
4. **PAVELConsumer** creates the public, verifiable event log

### Operational Considerations

#### Storage Requirements
- Hot storage: ~1-10 MB/day (depending on activity)
- Cold storage: ~365 MB/year (compressed)
- IPFS archives: ~500 MB/year (CAR files)

#### Performance
- Real-time event processing (< 1s latency)
- Segment rotation every hour or 10,000 events
- Blockchain anchoring every 100 segments

#### Monitoring
- Track event processing rate
- Monitor storage usage
- Verify signature generation
- Check anchor transaction status

### Future Enhancements

1. **Multi-Contract Support**: Extend to other StellarCarbon contracts
2. **Cross-Chain Verification**: Add anchoring to other blockchains
3. **Advanced Analytics**: Built-in aggregation and reporting
4. **Decentralized Storage**: IPFS pinning across multiple providers
5. **Zero-Knowledge Proofs**: Privacy-preserving verification

### Conclusion

The PAVEL consumer transforms blockchain events into a public good - a transparent, verifiable record of carbon credit activities that anyone can access and verify. By combining the trust properties of blockchain with the accessibility of traditional web infrastructure, it bridges the gap between Web3 transparency and Web2 usability, making carbon credit data truly public and auditable.