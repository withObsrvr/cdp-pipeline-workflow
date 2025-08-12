#!/bin/bash
# Test script for control plane integration

echo "Testing CDP Pipeline Control Plane Integration"
echo "============================================"

# Check if FLOWCTL_ENDPOINT is set
if [ -z "$FLOWCTL_ENDPOINT" ]; then
    echo "✓ FLOWCTL_ENDPOINT is not set - pipeline will run without control plane"
else
    echo "✓ FLOWCTL_ENDPOINT is set to: $FLOWCTL_ENDPOINT"
fi

# Build the CDP pipeline
echo ""
echo "Building CDP pipeline..."
CGO_ENABLED=1 go build -o cdp-pipeline-workflow
if [ $? -eq 0 ]; then
    echo "✓ Build successful"
else
    echo "✗ Build failed"
    exit 1
fi

# Check for a test config file
TEST_CONFIG="config/base/contract_events.secret.yaml"
if [ ! -f "$TEST_CONFIG" ]; then
    echo "✗ Test config file not found: $TEST_CONFIG"
    echo "  Please create a test configuration file"
    exit 1
fi
echo "✓ Found test config: $TEST_CONFIG"

echo ""
echo "To test with control plane integration:"
echo "1. Start flowctl server: flowctl server --storage-type boltdb"
echo "2. Set environment: export FLOWCTL_ENDPOINT=localhost:8080"
echo "3. Run pipeline: ./cdp-pipeline-workflow -config $TEST_CONFIG"
echo ""
echo "The pipeline will:"
echo "- Register with flowctl control plane"
echo "- Send heartbeats every 10 seconds"
echo "- Report metrics from components that implement StatsProvider"
echo ""
echo "To check status in flowctl:"
echo "- flowctl status                    # List all services"
echo "- flowctl status <pipeline-id>      # Get detailed pipeline status"