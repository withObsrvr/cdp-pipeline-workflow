#!/bin/bash

# Example script to run CDP pipeline with flowctl control plane integration

echo "CDP Pipeline with Flowctl Control Plane Example"
echo "=============================================="
echo ""

# Check if flowctl is running
if ! command -v flowctl &> /dev/null; then
    echo "✗ flowctl is not installed or not in PATH"
    echo "  Please install flowctl first"
    exit 1
fi

# Check if flowctl server is running
if ! nc -z localhost 8080 2>/dev/null; then
    echo "✗ flowctl server is not running on localhost:8080"
    echo "  Start it with: flowctl server --storage-type boltdb"
    exit 1
fi

echo "✓ flowctl server is running on localhost:8080"

# Set control plane endpoint
export FLOWCTL_ENDPOINT=localhost:8080
echo "✓ FLOWCTL_ENDPOINT set to: $FLOWCTL_ENDPOINT"

# Choose a config file
CONFIG_FILE="config/base/pipeline_config.yaml"
if [ ! -f "$CONFIG_FILE" ]; then
    echo "✗ Config file not found: $CONFIG_FILE"
    exit 1
fi
echo "✓ Using config: $CONFIG_FILE"

# Run the pipeline
echo ""
echo "Starting CDP pipeline with control plane integration..."
echo "Press Ctrl+C to stop"
echo ""

./cdp-pipeline-workflow -config "$CONFIG_FILE"