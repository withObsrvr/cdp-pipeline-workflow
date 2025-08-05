#!/bin/bash

# Test script for Parquet consumer with enhanced logging

echo "Starting Parquet consumer test..."
echo "Configuration: config/base/contract_data_testnet.secret.yaml"
echo "Output directory: ~/Documents/data/stellar-archive/contract_data"
echo ""

# Create output directory if it doesn't exist
mkdir -p ~/Documents/data/stellar-archive/contract_data

# Run the pipeline with the test configuration
echo "Running pipeline..."
./cdp-pipeline-workflow -config config/base/contract_data_testnet.secret.yaml 2>&1 | grep -E "(SaveToParquet|Parquet|Processing message|Buffered message|Flushing|wrote|Written|LocalFS)"

# Check for output files
echo ""
echo "Checking for output files..."
find ~/Documents/data/stellar-archive/contract_data -name "*.parquet" -type f -ls

echo ""
echo "Test complete."