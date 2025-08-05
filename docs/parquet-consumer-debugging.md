# Parquet Consumer Debugging Guide

## Issue Summary
The Parquet consumer is not creating files when running the pipeline with `contract_data_testnet.secret.yaml` configuration.

## Fixes Applied

### 1. **JSON Message Handling**
- Added JSON unmarshaling for byte payloads in `Process()` method
- The ContractDataProcessor sends messages as JSON bytes, not direct structs

### 2. **Field Name Mapping**
- Fixed field name mapping to use lowercase JSON field names
- ContractDataProcessor JSON marshaling converts field names to lowercase based on json tags:
  - `LedgerSeq` → `ledger_sequence`  
  - `MessageType` → `message_type`
  - `ContractData` → `contract_data`
  - `ContractId` → `contract_id`
  - `ProcessorName` → `processor_name`
  - `Timestamp` → `timestamp`
  - `ArchiveMetadata` → `archive_metadata`

### 3. **Schema Recognition**
- Updated schema registry to recognize ContractDataMessage format
- Checks for `message_type == "contract_data"` to identify messages from ContractDataProcessor
- Created dedicated schema for ContractDataMessage with flattened fields

### 4. **Home Directory Expansion**
- Added tilde (~) expansion for local filesystem paths
- Config uses `~/Documents/data/stellar-archive` which needs proper expansion

### 5. **Enhanced Logging**
Added comprehensive logging at key points:
- Consumer initialization with full config
- Message processing with payload type
- Ledger sequence extraction
- Local file writes with full paths
- Buffer status and flush triggers

## Debugging Steps

### 1. Run with Test Script
```bash
./test-parquet.sh
```

This script:
- Runs the pipeline with grep filter for relevant logs
- Checks for created Parquet files
- Shows output directory contents

### 2. Check Logs for:
- "SaveToParquet: Initializing with config" - Verify configuration
- "SaveToParquet: Processing message" - Confirm messages are received
- "SaveToParquet: Extracted ledger sequence" - Verify ledger tracking
- "Buffered message. Buffer size: X/100" - Monitor buffering
- "Flushing X records to Parquet" - Confirm flush triggers
- "LocalFSClient: Successfully wrote" - Verify file writes

### 3. Common Issues to Check:
- **No messages received**: Check if ContractDataProcessor is processing data
- **Buffer not filling**: Buffer size is 100, check if enough messages arrive
- **No flush triggered**: Check rotation interval (60 minutes) or buffer size
- **File write errors**: Check directory permissions and disk space
- **Schema inference failed**: First message must have valid structure

### 4. Manual Flush Trigger
The consumer flushes when:
- Buffer reaches configured size (100 messages)
- Rotation interval expires (60 minutes)
- Consumer is closed (Ctrl+C)

### 5. Verify Output Directory
```bash
# Check if directory was created
ls -la ~/Documents/data/stellar-archive/contract_data/

# Monitor for new files
watch -n 1 'find ~/Documents/data/stellar-archive/contract_data -name "*.parquet" -type f -ls'
```

## Configuration Used
```yaml
- type: SaveToParquet
  config:
    storage_type: "FS"
    local_path: "~/Documents/data/stellar-archive"
    path_prefix: "contract_data"
    compression: "snappy"
    buffer_size: 100                  # Reduced from 10000 for testing
    max_file_size_mb: 128
    rotation_interval_minutes: 60
    partition_by: "ledger_day"
    include_metadata: true
    debug: false                      # Set to true for more logging
```

## Next Steps if Files Still Not Created

1. **Enable debug mode**: Set `debug: true` in config
2. **Reduce buffer size further**: Try `buffer_size: 10` or even `1`
3. **Add forced flush**: Modify consumer to flush after first message for testing
4. **Check processor output**: Add logging to ContractDataProcessor to verify it's sending messages
5. **Test with simpler consumer**: Try SaveToMongoDB or SaveToPostgreSQL to verify data flow
6. **Check ledger range**: Verify ledgers 426000-427000 contain contract data on testnet

## Test Data Verification
To verify the source has contract data:
```bash
# Check if BufferedStorageSourceAdapter is finding files
./cdp-pipeline-workflow -config config/base/contract_data_testnet.secret.yaml 2>&1 | grep -i "source\|buffer\|ledger"
```

The issue is likely one of:
1. No contract data in the specified ledger range
2. Messages not reaching the consumer
3. Buffer not filling to trigger flush
4. Schema inference failing on message format