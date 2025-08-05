# GCS Parquet Setup Guide

## Quick Start

To save contract data to Google Cloud Storage as Parquet files, use this minimal configuration:

```yaml
pipelines:
  ContractDataToGCS:
    source:
      type: BufferedStorageSourceAdapter
      config:
        bucket_name: "obsrvr-stellar-ledger-data-mainnet-data/landing/ledgers/mainnet"
        network: "mainnet"
        start_ledger: 50000000
        end_ledger: 50010000     # Small range for testing
    processors:
      - type: "ContractData"
        config:
          network_passphrase: "Public Global Stellar Network ; September 2015"
    consumers:
      - type: SaveToParquet
        config:
          storage_type: "GCS"
          bucket_name: "your-bucket-name"    # CHANGE THIS
          path_prefix: "contract_data"
          compression: "snappy"
          buffer_size: 1000
```

## Prerequisites

### 1. Create a GCS Bucket

```bash
# Set your project ID
PROJECT_ID="your-gcp-project-id"

# Create bucket
gsutil mb -p $PROJECT_ID -c STANDARD -l US gs://your-bucket-name/
```

### 2. Set Up Authentication

#### Option A: Service Account (Recommended for Production)

```bash
# Create service account
gcloud iam service-accounts create stellar-pipeline \
  --display-name="Stellar Pipeline Service Account" \
  --project=$PROJECT_ID

# Grant storage permissions
gsutil iam ch \
  serviceAccount:stellar-pipeline@$PROJECT_ID.iam.gserviceaccount.com:objectCreator \
  gs://your-bucket-name

# Create key file
gcloud iam service-accounts keys create ~/stellar-pipeline-key.json \
  --iam-account=stellar-pipeline@$PROJECT_ID.iam.gserviceaccount.com \
  --project=$PROJECT_ID

# Set environment variable
export GOOGLE_APPLICATION_CREDENTIALS=~/stellar-pipeline-key.json
```

#### Option B: Local Development (gcloud auth)

```bash
# Authenticate with your Google account
gcloud auth application-default login

# Select your project
gcloud config set project $PROJECT_ID
```

## Configuration Options

### Basic Configuration

```yaml
- type: SaveToParquet
  config:
    storage_type: "GCS"              # Required: Use GCS backend
    bucket_name: "my-stellar-data"   # Required: Your GCS bucket
    path_prefix: "contract_data"     # Optional: Prefix for all files
```

### Performance Tuning

```yaml
- type: SaveToParquet
  config:
    storage_type: "GCS"
    bucket_name: "my-stellar-data"
    
    # Buffer more records for fewer, larger files
    buffer_size: 50000               # Default: 10000
    max_file_size_mb: 512           # Default: 128
    
    # Compression options
    compression: "zstd"             # Options: none, snappy, gzip, zstd
    
    # Rotation settings
    rotation_interval_minutes: 60    # Create new file every hour
```

### Partitioning Strategies

```yaml
# Option 1: Partition by ledger date (recommended)
partition_by: "ledger_day"
# Creates: gs://bucket/prefix/year=2025/month=07/day=13/

# Option 2: Partition by ledger range
partition_by: "ledger_range"
ledgers_per_file: 10000
# Creates: gs://bucket/prefix/ledgers/50000000-50010000/

# Option 3: Partition by hour
partition_by: "hour"
# Creates: gs://bucket/prefix/year=2025/month=07/day=13/hour=14/
```

## Running the Pipeline

### 1. Basic Execution

```bash
# Set credentials
export GOOGLE_APPLICATION_CREDENTIALS=~/stellar-pipeline-key.json

# Run pipeline
./cdp-pipeline-workflow -config contract_data_gcs.yaml
```

### 2. Docker Execution

```bash
# Run with Docker, mounting credentials
docker run -v ~/stellar-pipeline-key.json:/key.json \
  -e GOOGLE_APPLICATION_CREDENTIALS=/key.json \
  -v $(pwd)/config:/config \
  stellar-pipeline \
  -config /config/contract_data_gcs.yaml
```

### 3. Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: stellar-parquet-archiver
spec:
  template:
    spec:
      containers:
      - name: pipeline
        image: stellar-pipeline:latest
        env:
        - name: GOOGLE_APPLICATION_CREDENTIALS
          value: /var/secrets/gcs-key.json
        volumeMounts:
        - name: gcs-key
          mountPath: /var/secrets
        command: ["/app/cdp-pipeline-workflow"]
        args: ["-config", "/config/contract_data_gcs.yaml"]
      volumes:
      - name: gcs-key
        secret:
          secretName: gcs-credentials
```

## Verifying Output

### 1. List Created Files

```bash
# List all Parquet files
gsutil ls -r gs://your-bucket-name/contract_data/**/*.parquet

# Check specific date
gsutil ls gs://your-bucket-name/contract_data/year=2025/month=07/day=13/
```

### 2. Download and Inspect

```bash
# Download a file
gsutil cp gs://your-bucket-name/contract_data/year=2025/month=07/day=13/stellar-data-*.parquet .

# Inspect with Python
python3 -c "
import pyarrow.parquet as pq
table = pq.read_table('stellar-data-*.parquet')
print(table.schema)
print(f'Rows: {table.num_rows}')
print(table.to_pandas().head())
"
```

### 3. Query with BigQuery

```sql
-- Create external table
CREATE OR REPLACE EXTERNAL TABLE `project.dataset.contract_data`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://your-bucket-name/contract_data/**/*.parquet']
);

-- Query data
SELECT 
  DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', closed_at)) as date,
  COUNT(*) as contract_updates,
  COUNT(DISTINCT contract_id) as unique_contracts
FROM `project.dataset.contract_data`
WHERE partition_date >= '2025-07-01'
GROUP BY date
ORDER BY date DESC;
```

## Cost Optimization

### 1. Lifecycle Rules

```bash
# Create lifecycle configuration file
cat > lifecycle.json << EOF
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
        "condition": {"age": 30}
      },
      {
        "action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},
        "condition": {"age": 90}
      },
      {
        "action": {"type": "Delete"},
        "condition": {"age": 365}
      }
    ]
  }
}
EOF

# Apply to bucket
gsutil lifecycle set lifecycle.json gs://your-bucket-name
```

### 2. Regional vs Multi-Regional

```bash
# Create regional bucket (lower cost)
gsutil mb -p $PROJECT_ID -c STANDARD -l us-central1 gs://regional-bucket/

# Create multi-regional bucket (higher availability)
gsutil mb -p $PROJECT_ID -c STANDARD -l US gs://multi-regional-bucket/
```

## Monitoring

### 1. GCS Metrics

```bash
# Check bucket size
gsutil du -s gs://your-bucket-name/contract_data/

# Count files
gsutil ls -r gs://your-bucket-name/contract_data/**/*.parquet | wc -l

# Monitor upload bandwidth
# (Check GCP Console > Storage > Metrics)
```

### 2. Pipeline Metrics

Add monitoring consumer:

```yaml
consumers:
  - type: SaveToParquet
    config:
      storage_type: "GCS"
      bucket_name: "your-bucket-name"
      debug: true  # Enable detailed logging
  
  - type: SaveToRedis
    config:
      address: "localhost:6379"
      key: "pipeline:metrics:contract_data"
      # Tracks: messages_processed, files_written, bytes_written
```

## Troubleshooting

### Permission Denied

```bash
# Check service account permissions
gsutil iam get gs://your-bucket-name

# Add missing permissions
gsutil iam ch serviceAccount:EMAIL@PROJECT.iam.gserviceaccount.com:objectCreator gs://your-bucket-name
```

### No Files Created

1. Check credentials:
```bash
gcloud auth application-default print-access-token
```

2. Test bucket access:
```bash
echo "test" | gsutil cp - gs://your-bucket-name/test.txt
```

3. Enable debug logging:
```yaml
debug: true
buffer_size: 1  # Flush immediately
```

### Slow Uploads

1. Increase chunk size:
```yaml
gcs_options:
  chunk_size: 33554432  # 32MB chunks
```

2. Use regional bucket closer to pipeline:
```bash
# Find your pipeline location
curl ipinfo.io

# Create bucket in same region
gsutil mb -l us-central1 gs://regional-bucket/
```

## Production Checklist

- [ ] Service account created with minimal permissions
- [ ] Bucket lifecycle rules configured
- [ ] Monitoring and alerts set up
- [ ] Buffer size tuned for workload
- [ ] Compression type selected (snappy for speed, zstd for size)
- [ ] Partition strategy matches query patterns
- [ ] Backup/disaster recovery plan in place
- [ ] Cost alerts configured in GCP