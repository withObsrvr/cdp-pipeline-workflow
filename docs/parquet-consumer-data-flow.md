# Parquet Consumer Data Flow Diagram

## Overview Flow

```
┌─────────────────┐         ┌──────────────────┐         ┌─────────────────┐
│                 │         │                  │         │                 │
│  Your Processor ├────────►│ Message Format   ├────────►│ Parquet Consumer│
│                 │ JSON    │ Validation       │         │                 │
└─────────────────┘         └──────────────────┘         └────────┬────────┘
                                                                   │
                                                                   ▼
                                                         ┌─────────────────┐
                                                         │ Schema Inference│
                                                         └────────┬────────┘
                                                                   │
                                                                   ▼
                                                         ┌─────────────────┐
                                                         │ Buffer Messages │
                                                         └────────┬────────┘
                                                                   │
                                                                   ▼
                                                         ┌─────────────────┐
                                                         │ Convert to Arrow│
                                                         └────────┬────────┘
                                                                   │
                                                                   ▼
                                                         ┌─────────────────┐
                                                         │ Write Parquet   │
                                                         └────────┬────────┘
                                                                   │
                                                                   ▼
                                              ┌────────────────────┴────────┐
                                              │                             │
                                              ▼                             ▼
                                    ┌─────────────────┐           ┌─────────────────┐
                                    │ Local FS        │           │ Cloud Storage   │
                                    │ ~/data/stellar/ │           │ GCS/S3 Bucket   │
                                    └─────────────────┘           └─────────────────┘
```

## Message Processing Details

### 1. Processor Output Stage
```
Your Processor
│
├─► Extract data from source (ledger, RPC, etc.)
├─► Transform to your data structure
├─► Add required metadata:
│   ├─► ledger_sequence
│   ├─► timestamp/closed_at
│   └─► message_type
│
└─► Marshal to JSON → Send to subscribers
```

### 2. Parquet Consumer Input Stage
```
Parquet Consumer receives message
│
├─► Check payload type
│   ├─► []byte → Unmarshal JSON
│   └─► map[string]interface{} → Use directly
│
├─► Extract metadata:
│   ├─► ledger_sequence (for tracking)
│   ├─► closed_at/timestamp (for partitioning)
│   └─► message_type (for schema selection)
│
└─► Add to buffer
```

### 3. Schema Management
```
First message in buffer?
│
├─► YES → Infer schema
│   ├─► Check message_type
│   ├─► Known type? → Use predefined schema
│   └─► Unknown? → Build schema from fields
│
└─► NO → Use existing schema
```

### 4. File Generation Flow
```
Buffer reaches threshold OR timeout
│
├─► Convert messages to Arrow RecordBatch
├─► Apply compression (snappy/gzip/etc)
├─► Generate file path:
│   └─► {prefix}/year={Y}/month={M}/day={D}/stellar-data-{start}-{end}-{timestamp}.parquet
│
└─► Write to storage backend
```

## Example Message Transformations

### Input from ContractDataProcessor
```json
{
  "contract_data": {
    "contract_id": "CC...",
    "closed_at": "2025-07-13T09:36:17Z",
    "last_modified_ledger": 426007,
    // ... more fields
  },
  "ledger_sequence": 426007,
  "message_type": "contract_data",
  "processor_name": "contract_data_processor"
}
```

### Arrow Schema Generated
```
contract_id: string
closed_at: timestamp[us, tz=UTC]
last_modified_ledger: uint32
ledger_sequence: uint32
message_type: string
processor_name: string
// ... other fields
```

### Parquet File Structure
```
/home/user/data/stellar-archive/
└── contract_data/
    └── year=2025/
        └── month=07/
            └── day=13/
                ├── stellar-data-426000-426100-1754399884.parquet
                ├── stellar-data-426100-426200-1754399890.parquet
                └── stellar-data-426200-426300-1754399896.parquet
```

## Buffer and Flush Triggers

```
Message arrives → Add to buffer
                    │
                    ▼
         Check flush conditions:
         ┌──────────┬──────────┬──────────┐
         │ Buffer   │ Time     │ File     │
         │ Full?    │ Elapsed? │ Size?    │
         └────┬─────┴────┬─────┴────┬─────┘
              │          │          │
              ▼          ▼          ▼
             YES → FLUSH TO PARQUET FILE
              │
              ▼
         Clear buffer & reset state
```

## Configuration Impact on Flow

### Buffer Size Impact
```
buffer_size: 1      → Immediate write (testing)
buffer_size: 100    → Small batches (low latency)
buffer_size: 10000  → Large batches (high throughput)
```

### Partition Strategy Impact
```
partition_by: "ledger_day"   → /year=2025/month=07/day=13/
partition_by: "ledger_range" → /ledgers/420000-430000/
partition_by: "hour"         → /year=2025/month=07/day=13/hour=14/
```

### Compression Impact
```
compression: "none"    → Fastest write, largest files
compression: "snappy"  → Fast compression, good balance (default)
compression: "gzip"    → Slower write, smaller files
compression: "zstd"    → Slowest write, smallest files
```

## Troubleshooting Flow

```
No Parquet files created?
│
├─► Check: Are messages arriving?
│   └─► Add DebugLogger before Parquet consumer
│
├─► Check: Required fields present?
│   ├─► ledger_sequence?
│   └─► timestamp/closed_at?
│
├─► Check: Buffer flushing?
│   ├─► Set buffer_size: 1 for testing
│   └─► Check logs for "Flushing X records"
│
└─► Check: Storage permissions?
    ├─► Local FS: Directory writable?
    └─► Cloud: Credentials configured?
```

## Performance Optimization Path

```
Development → Testing → Production
│
├─► Dev: buffer_size=1, debug=true
│   └─► Verify message format and output
│
├─► Test: buffer_size=100, debug=true
│   └─► Verify performance and correctness
│
└─► Prod: buffer_size=10000, debug=false
    └─► Optimize for throughput
```