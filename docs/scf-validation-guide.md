# CDP Pipeline Workflow - SCF Tranche Validation Guide

This guide provides step-by-step instructions to validate the CDP (Composable Data Platform) pipeline workflow for the Stellar Community Fund (SCF) project tranche validation.

## Overview

The CDP pipeline processes Stellar blockchain data in real-time, enabling data extraction, transformation, and loading (ETL) for various use cases including contract event monitoring, payment tracking, and ledger analysis.

## Prerequisites

### For Docker Testing
- Docker installed and running
- Google Cloud credentials (for GCS access)
- Network connectivity to Stellar infrastructure

### For Local Testing
- Go 1.23 or higher
- CGO dependencies: `libzmq3-dev libczmq-dev libsodium-dev`
- Google Cloud SDK configured

## Quick Validation Test

### 1. Using Docker (Recommended)

```bash
# Pull the latest image
docker pull docker.io/withobsrvr/obsrvr-flow-pipeline:0.1.20

# Run a simple test to verify the latest ledger

docker run --rm --network host \
 -v "$HOME/.config/gcloud/application_default_credentials.json":/.config/gcp/credentials.json:ro   \
 -e GOOGLE_APPLICATION_CREDENTIALS=/.config/gcp/credentials.json  \
 -v $(pwd)/config:/app/config  \
 docker.io/withobsrvr/obsrvr-flow-pipeline:0.1.20   \
 /app/cdp-pipeline-workflow -config /app/config/base/latest_ledger_gcs.secret.yaml
```

### 2. Create Test Configuration

Create a file `config/base/latest_ledger_test.yaml`:

```yaml
pipelines:
 LatestLedgerPipeline:
   source:
     type: BufferedStorageSourceAdapter
     config:
       bucket_name: "obsrvr-stellar-ledger-data-testnet-data/landing/ledgers/testnet"
       network: "testnet"
       num_workers: 10
       retry_limit: 3
       retry_wait: 5
       start_ledger: 49018
       ledgers_per_file: 1
       files_per_partition: 64000
   processors:
     - type: LatestLedger
       config:
         network_passphrase: "Test SDF Network ; September 2015"
   consumers:
     - type: SaveToZeroMQ
       config:
         address: "tcp://127.0.0.1:5555"
```

### 3. Expected Output

A successful validation will show:

```
2025/08/17 10:13:29 Starting pipeline: LatestLedgerPipeline
2025/08/17 10:13:29 Enhanced config parsing failed, trying legacy mode: invalid configuration: unsupported configuration combination. Supported modes: 1) start_ledger + end_ledger, 2) start_time_ago + (end_time_ago OR continuous_mode), 3) start_ledger + end_time_ago
2025/08/17 10:13:29 Parsed configuration: start_ledger=49018, end_ledger=0, bucket=obsrvr-stellar-ledger-data-testnet-data/landing/ledgers/testnet, network=testnet
2025/08/17 10:13:29 Chained processor *processor.LatestLedgerProcessor -> consumer *consumer.SaveToZeroMQ
2025/08/17 10:13:29 Starting BufferedStorageSourceAdapter from ledger 49018
2025/08/17 10:13:29 Will process indefinitely from start ledger
2025/08/17 10:13:29 Starting ledger processing with range: [49018,latest)
2025/08/17 10:13:30 Starting to process ledger 49018 (took 659.803543ms since last ledger)
2025/08/17 10:13:30 Latest ledger: 49018 (Transactions: 0, Operations: 0, Success Rate: NaN%)
2025/08/17 10:13:30 Processing message in SaveToZeroMQ
```


## Local Testing (Without Docker)

### 1. Build the Binary

```bash
# Clone the repository
git clone https://github.com/withobsrvr/cdp-pipeline-workflow.git
cd cdp-pipeline-workflow

# Install dependencies (Ubuntu/Debian)
sudo apt-get update
sudo apt-get install -y libzmq3-dev libczmq-dev libsodium-dev

# Build the binary
CGO_ENABLED=1 go build -o cdp-pipeline-workflow

# Run the test
./cdp-pipeline-workflow -config config/base/latest_ledger_test.yaml
```

### 2. Verify Dependencies

```bash
# Check if binary is built correctly
ldd ./cdp-pipeline-workflow | grep -E "(zmq|sodium)"

# Expected output:
# libzmq.so.5 => /lib/x86_64-linux-gnu/libzmq.so.5
# libsodium.so.23 => /lib/x86_64-linux-gnu/libsodium.so.23
```


## Troubleshooting

### Common Issues

1. **Authentication Error**
   ```
   Error: failed to create storage client: google: could not find default credentials
   ```
   **Solution**: Ensure Google Cloud credentials are properly mounted:
   ```bash
   # Check if credentials exist
   ls -la "$HOME/.config/gcloud/application_default_credentials.json"
   
   # Authenticate if needed
   gcloud auth application-default login
   ```

2. **Binary Not Found**
   ```
   exec: "/app/bin/cdp-pipeline-workflow": stat /app/bin/cdp-pipeline-workflow: no such file or directory
   ```
   **Solution**: The binary is located at `/app/obsrvr-flow-pipeline`. Don't specify the full path:
   ```bash
   docker run ... docker.io/withobsrvr/obsrvr-flow-pipeline:0.1.20 -config /app/config/base/test.yaml
   ```

3. **Network Issues**
   ```
   Error: failed to read from GCS: context deadline exceeded
   ```
   **Solution**: Check network connectivity and increase timeouts in config:
   ```yaml
   retry_limit: 5
   retry_wait: 10
   timeout_seconds: 300
   ```




## Support

For issues or questions:
- GitHub Issues: https://github.com/withobsrvr/cdp-pipeline-workflow/issues
- Documentation: https://docs.obsrvr.com/cdp-pipeline

## Next Steps

After successful validation:
1. Test your specific use case configuration
2. Set up production monitoring
3. Configure appropriate resource limits
4. Implement error alerting
5. Plan for scaling based on data volume

---
*This validation guide is part of the Stellar Community Fund (SCF) project tranche validation process.*