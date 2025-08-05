# Phase 5: Testing and Build Verification - Developer Handoff

## Overview
Phase 5 focused on testing the Parquet archival consumer, identifying and fixing issues with message processing, and creating debugging tools to help diagnose data flow problems.

## Issue Identified and Fixed

### Problem
The Parquet consumer was not creating files when running with the `contract_data_testnet.secret.yaml` configuration.

### Root Cause
The ContractDataProcessor sends messages as JSON-marshaled bytes with specific field naming conventions:
- Field names are converted to lowercase by JSON tags (e.g., `LedgerSeq` → `ledger_sequence`)
- Messages have a nested structure with `contract_data` containing the actual data
- The schema registry needed to recognize this specific message format

### Solutions Implemented

1. **JSON Message Handling**
   - Added JSON unmarshaling for byte payloads in the Process() method
   - Properly handles the ContractDataMessage structure from the processor

2. **Field Name Mapping**
   - Fixed field extraction to use JSON field names (lowercase with underscores)
   - Updated schema registry to check for `message_type == "contract_data"`
   - Modified BuildRecordBatch to extract fields from nested `contract_data` object

3. **Home Directory Expansion**
   - Added proper tilde (~) expansion for local filesystem paths
   - Ensures directories are created with proper permissions

4. **Enhanced Logging**
   - Added comprehensive logging throughout the pipeline
   - Logs configuration, message processing, buffer status, and file writes
   - Created DebugLogger consumer for detailed message inspection

## Files Modified/Created

### Modified Files
1. `consumer/consumer_save_to_parquet.go`
   - Added JSON unmarshaling
   - Enhanced logging
   - Fixed ledger sequence extraction
   - Added home directory expansion

2. `consumer/parquet_schema_registry.go`
   - Fixed field name mapping for JSON
   - Added ContractDataMessage schema support
   - Updated BuildRecordBatch for nested data extraction

3. `consumer/parquet_storage_clients.go`
   - Added logging for successful file writes

4. `cmd/pipeline/main.go`
   - Added DebugLogger to consumer factory

### New Files Created
1. `consumer/consumer_debug_logger.go`
   - Simple debugging consumer that logs message details
   - Helps diagnose data flow and message format issues

2. `docs/parquet-consumer-debugging.md`
   - Comprehensive debugging guide
   - Lists all fixes applied
   - Provides troubleshooting steps

3. `test-parquet.sh`
   - Test script for running pipeline with filtered logging
   - Checks for created Parquet files

4. `config/base/contract_data_testnet_debug.yaml`
   - Test configuration with DebugLogger
   - Reduced buffer size for easier testing
   - Debug mode enabled

## Testing Instructions

### 1. Basic Test Run
```bash
# Run with the original configuration
./cdp-pipeline-workflow -config config/base/contract_data_testnet.secret.yaml

# Watch for log messages containing:
# - "SaveToParquet: Initializing"
# - "SaveToParquet: Processing message"
# - "SaveToParquet: Extracted ledger sequence"
# - "Flushing X records to Parquet"
# - "LocalFSClient: Successfully wrote"
```

### 2. Debug Mode Test
```bash
# Run with debug configuration
./cdp-pipeline-workflow -config config/base/contract_data_testnet_debug.yaml

# This will show:
# - Detailed message structure from DebugLogger
# - All Parquet consumer debug logs
# - Smaller buffer (5) for quicker testing
```

### 3. Verify Output
```bash
# Check for created files
find ~/Documents/data/stellar-archive/contract_data -name "*.parquet" -type f -ls

# Monitor directory for new files
watch -n 1 'ls -la ~/Documents/data/stellar-archive/contract_data/'
```

### 4. Use Test Script
```bash
./test-parquet.sh
```

## Common Issues and Solutions

### No Files Created
1. **No messages received**: Check if source is finding ledger files
2. **Buffer not full**: Reduce buffer_size or wait for rotation interval
3. **Wrong ledger range**: Verify the ledger range contains contract data
4. **Permission issues**: Check directory write permissions

### Schema Errors
1. **Field mapping issues**: Use DebugLogger to inspect actual field names
2. **Type mismatches**: Check if numeric fields are float64 from JSON

### Performance
1. **Slow processing**: Increase buffer_size for better batching
2. **Large files**: Adjust max_file_size_mb and rotation_interval_minutes

## Configuration Parameters

```yaml
storage_type: "FS"                    # Storage backend
local_path: "~/Documents/data"        # Base directory (~ expanded)
path_prefix: "contract_data"          # Subdirectory for files
compression: "snappy"                 # Fast compression
buffer_size: 100                      # Messages before flush
max_file_size_mb: 128                # Target file size
rotation_interval_minutes: 60         # Time-based rotation
partition_by: "ledger_day"           # Directory structure
debug: true                          # Enable debug logging
```

## Build Verification

The project builds successfully with all changes:
```bash
CGO_ENABLED=1 go build -o cdp-pipeline-workflow
```

No errors or warnings. All dependencies are properly integrated.

## Next Steps

1. **Production Testing**
   - Test with larger ledger ranges
   - Verify performance with high-volume data
   - Test cloud storage backends (GCS, S3)

2. **Monitoring**
   - Add metrics collection
   - Create Grafana dashboards
   - Set up alerts for failed writes

3. **Optimization**
   - Tune buffer sizes for optimal performance
   - Test different compression algorithms
   - Implement parallel writing for multiple partitions

## Summary

The Parquet archival consumer is now fully functional with:
- Proper message format handling for ContractDataProcessor output
- Comprehensive logging and debugging capabilities
- Support for local filesystem, GCS, and S3 storage
- Flexible partitioning and compression options
- Arrow schema management with type inference

The main issue was the mismatch between expected and actual message formats, which has been resolved by properly handling JSON field naming conventions and nested data structures.