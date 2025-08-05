#!/bin/bash

# Test script for Parquet consumer with enhanced logging

echo "Starting Parquet consumer test..."
echo "Configuration: config/base/contract_data_testnet.secret.yaml"
echo "Output directory: ~/Documents/data/stellar-archive/contract_data"
echo ""

# Create output directory if it doesn't exist
mkdir -p ~/Documents/data/stellar-archive/contract_data

# Create log file with timestamp
LOG_FILE="parquet_test_$(date +%Y%m%d_%H%M%S).log"

echo "Running pipeline (logging to $LOG_FILE)..."

# Run the pipeline with the test configuration and capture all output
./cdp-pipeline-workflow -config config/base/contract_data_testnet.secret.yaml 2>&1 | tee "$LOG_FILE"

# Extract relevant logs from the saved file
echo ""
echo "=== Relevant Parquet Consumer Logs ==="
grep -E "(SaveToParquet|Parquet|Processing message|Buffered message|Flushing|wrote|Written|LocalFS)" "$LOG_FILE" || echo "No matching log entries found"

# Check for output files
echo ""
echo "=== Checking for output files ==="
find ~/Documents/data/stellar-archive/contract_data -name "*.parquet" -type f -ls

echo ""
echo "Test complete. Full logs saved to: $LOG_FILE"