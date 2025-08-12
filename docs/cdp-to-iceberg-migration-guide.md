# CDP Parquet to Apache Iceberg Migration Guide

## Table of Contents
1. [Introduction](#introduction)
2. [Understanding Apache Iceberg](#understanding-apache-iceberg)
3. [Prerequisites](#prerequisites)
4. [Method 1: Using Spark with Iceberg](#method-1-using-spark-with-iceberg)
5. [Method 2: Using PyIceberg](#method-2-using-pyiceberg)
6. [Method 3: Using Trino](#method-3-using-trino)
7. [Automation and Best Practices](#automation-and-best-practices)
8. [Monitoring and Maintenance](#monitoring-and-maintenance)
9. [Troubleshooting](#troubleshooting)

## Introduction

This guide teaches you how to post-process parquet files created by the CDP pipeline into Apache Iceberg tables. By the end, you'll understand:
- Why Iceberg adds value to your parquet files
- Multiple methods to create Iceberg tables from existing parquet data
- How to automate the conversion process
- Best practices for maintaining Iceberg tables

## Understanding Apache Iceberg

### What is Apache Iceberg?

Apache Iceberg is a table format that adds a metadata layer on top of data files (like Parquet). Think of it as a "smart index" for your data lake that provides:

```
Traditional Parquet Files:          Iceberg Table:
├── file1.parquet                  ├── data/
├── file2.parquet                  │   ├── file1.parquet
├── file3.parquet                  │   ├── file2.parquet
└── file4.parquet                  │   └── file3.parquet
                                   └── metadata/
                                       ├── table metadata (schema, partitions)
                                       ├── snapshots (table history)
                                       └── manifests (file statistics)
```

### Key Benefits for CDP Data

1. **Time Travel**: Query blockchain data as it existed at any point
   ```sql
   SELECT * FROM stellar_events 
   TIMESTAMP AS OF '2024-01-01 00:00:00'
   WHERE ledger_sequence > 50000000;
   ```

2. **Schema Evolution**: Add new fields without rewriting files
   ```sql
   ALTER TABLE stellar_events ADD COLUMN processed_at TIMESTAMP;
   ```

3. **Efficient Queries**: Skip irrelevant files using metadata
   - Query only specific ledger ranges
   - Filter by partition without scanning all files
   - Use column statistics to prune files

4. **ACID Guarantees**: Safe concurrent operations
   - Multiple CDP pipelines can write safely
   - Readers always see consistent data

## Prerequisites

### Required Software

1. **Java 11 or 17** (Spark requirement)
   ```bash
   # Ubuntu/Debian
   sudo apt update
   sudo apt install openjdk-17-jdk
   
   # macOS
   brew install openjdk@17
   
   # Verify installation
   java -version
   ```

2. **Apache Spark with Iceberg Support**
   ```bash
   # Download Spark 3.5.0 (includes Scala)
   wget https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
   tar -xzf spark-3.5.0-bin-hadoop3.tgz
   sudo mv spark-3.5.0-bin-hadoop3 /opt/spark
   
   # Set environment variables
   echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
   echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bashrc
   source ~/.bashrc
   ```

3. **Python with PyIceberg** (for Method 2)
   ```bash
   pip install "pyiceberg[s3fs,gcsfs,pyarrow]"
   ```

### Understanding Your CDP Parquet Structure

First, examine your CDP parquet files:
```bash
# Local filesystem
find /path/to/cdp/parquet -name "*.parquet" | head -20

# S3
aws s3 ls s3://your-bucket/stellar-data/ --recursive | grep ".parquet" | head -20

# GCS
gsutil ls -r gs://your-bucket/stellar-data/ | grep ".parquet" | head -20
```

Typical CDP structure:
```
stellar-data/
├── year=2024/
│   ├── month=01/
│   │   ├── day=15/
│   │   │   ├── stellar-data-50000000-50010000-1705334400.parquet
│   │   │   └── stellar-data-50010001-50020000-1705338000.parquet
│   │   └── day=16/
│   │       └── stellar-data-50020001-50030000-1705420800.parquet
```

## Method 1: Using Spark with Iceberg

### Step 1: Create Spark Configuration

Create `spark-iceberg-config.conf`:
```properties
# Iceberg configurations
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.stellar=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.stellar.type=hadoop
spark.sql.catalog.stellar.warehouse=/path/to/iceberg/warehouse

# For S3
spark.hadoop.fs.s3a.access.key=YOUR_ACCESS_KEY
spark.hadoop.fs.s3a.secret.key=YOUR_SECRET_KEY
spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com

# For GCS
spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
spark.hadoop.google.cloud.auth.service.account.json.keyfile=/path/to/key.json

# Performance tuning
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
```

### Step 2: Initial Table Creation

Create `create_iceberg_table.sql`:
```sql
-- Create database if not exists
CREATE DATABASE IF NOT EXISTS stellar.cdp;

-- Create Iceberg table from existing parquet files
CREATE TABLE IF NOT EXISTS stellar.cdp.contract_events
USING iceberg
PARTITIONED BY (year, month, day)
AS SELECT * FROM parquet.`/path/to/cdp/stellar-data/`;

-- Verify table creation
DESCRIBE TABLE EXTENDED stellar.cdp.contract_events;

-- Check row count
SELECT COUNT(*) FROM stellar.cdp.contract_events;
```

Run the script:
```bash
spark-sql \
  --master local[4] \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 \
  --properties-file spark-iceberg-config.conf \
  -f create_iceberg_table.sql
```

### Step 3: Incremental Updates

Create `update_iceberg_table.py` for ongoing updates:
```python
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import sys

def update_iceberg_table(source_path, table_name, last_processed_timestamp):
    spark = SparkSession.builder \
        .appName("CDP to Iceberg Update") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.stellar", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.stellar.type", "hadoop") \
        .config("spark.sql.catalog.stellar.warehouse", "/path/to/iceberg/warehouse") \
        .getOrCreate()
    
    # Read new parquet files
    new_data = spark.read.parquet(source_path) \
        .filter(f"file_modification_time > '{last_processed_timestamp}'")
    
    if new_data.count() > 0:
        # Append new data to Iceberg table
        new_data.writeTo(f"stellar.cdp.{table_name}") \
            .option("merge-schema", "true") \
            .append()
        
        print(f"Added {new_data.count()} new records to {table_name}")
        
        # Optimize table (compact small files)
        spark.sql(f"""
            CALL stellar.system.rewrite_data_files(
                table => 'cdp.{table_name}',
                strategy => 'binpack',
                options => map('target-file-size-bytes', '134217728')
            )
        """)
    else:
        print("No new data to process")
    
    spark.stop()

if __name__ == "__main__":
    source_path = sys.argv[1] if len(sys.argv) > 1 else "/path/to/cdp/stellar-data/"
    table_name = sys.argv[2] if len(sys.argv) > 2 else "contract_events"
    last_timestamp = sys.argv[3] if len(sys.argv) > 3 else "2024-01-01 00:00:00"
    
    update_iceberg_table(source_path, table_name, last_timestamp)
```

Run incremental update:
```bash
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 \
  update_iceberg_table.py \
  "/path/to/cdp/stellar-data/" \
  "contract_events" \
  "2024-01-15 00:00:00"
```

## Method 2: Using PyIceberg

### Step 1: Setup PyIceberg

Create `pyiceberg_setup.py`:
```python
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, LongType, TimestampType, 
    StructType, ListType, MapType, BooleanType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import YearTransform, MonthTransform, DayTransform
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime

# Initialize catalog
catalog_config = {
    "uri": "file:///path/to/iceberg/warehouse",
    "warehouse": "file:///path/to/iceberg/warehouse",
}

catalog = load_catalog("default", **catalog_config)

# Define schema matching CDP parquet structure
def create_stellar_schema():
    return Schema(
        NestedField(1, "ledger_sequence", LongType(), required=True),
        NestedField(2, "ledger_hash", StringType(), required=True),
        NestedField(3, "ledger_close_time", TimestampType(), required=True),
        NestedField(4, "transaction_hash", StringType(), required=False),
        NestedField(5, "operation_id", LongType(), required=False),
        NestedField(6, "operation_type", StringType(), required=False),
        NestedField(7, "source_account", StringType(), required=False),
        NestedField(8, "destination_account", StringType(), required=False),
        NestedField(9, "asset_code", StringType(), required=False),
        NestedField(10, "asset_issuer", StringType(), required=False),
        NestedField(11, "amount", StringType(), required=False),
        NestedField(12, "data", StringType(), required=False),
        # Partition columns
        NestedField(13, "year", LongType(), required=True),
        NestedField(14, "month", LongType(), required=True),
        NestedField(15, "day", LongType(), required=True),
    )

# Create partition spec
partition_spec = PartitionSpec(
    PartitionField(
        source_id=3,  # ledger_close_time
        field_id=1000,
        transform=YearTransform(),
        name="year"
    ),
    PartitionField(
        source_id=3,
        field_id=1001,
        transform=MonthTransform(),
        name="month"
    ),
    PartitionField(
        source_id=3,
        field_id=1002,
        transform=DayTransform(),
        name="day"
    ),
)

# Create table
try:
    table = catalog.create_table(
        identifier="default.stellar_events",
        schema=create_stellar_schema(),
        partition_spec=partition_spec,
        properties={
            "write.parquet.compression-codec": "snappy",
            "write.metadata.delete-after-commit.enabled": "true",
            "write.metadata.previous-versions-max": "10",
        }
    )
    print("Table created successfully")
except Exception as e:
    print(f"Table may already exist: {e}")
    table = catalog.load_table("default.stellar_events")
```

### Step 2: Import Existing Parquet Files

Create `import_parquet_files.py`:
```python
import os
import glob
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

def import_parquet_to_iceberg(
    catalog_name: str,
    table_name: str,
    source_pattern: str,
    batch_size: int = 100
):
    # Load catalog and table
    catalog = load_catalog(catalog_name)
    table = catalog.load_table(table_name)
    
    # Find all parquet files
    parquet_files = glob.glob(source_pattern, recursive=True)
    print(f"Found {len(parquet_files)} parquet files to import")
    
    # Process in batches
    def process_batch(file_batch):
        # Read and combine parquet files
        tables = []
        for file_path in file_batch:
            try:
                table_data = pq.read_table(file_path)
                tables.append(table_data)
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
                continue
        
        if tables:
            # Combine tables
            combined_table = pa.concat_tables(tables)
            
            # Append to Iceberg table
            table.append(combined_table)
            return len(tables)
        return 0
    
    # Process files in parallel batches
    processed = 0
    with ThreadPoolExecutor(max_workers=4) as executor:
        # Create batches
        batches = [parquet_files[i:i + batch_size] 
                  for i in range(0, len(parquet_files), batch_size)]
        
        # Submit batch processing
        futures = {executor.submit(process_batch, batch): batch 
                  for batch in batches}
        
        # Track progress
        with tqdm(total=len(parquet_files)) as pbar:
            for future in as_completed(futures):
                batch = futures[future]
                try:
                    count = future.result()
                    processed += count
                    pbar.update(len(batch))
                except Exception as e:
                    print(f"Error processing batch: {e}")
    
    print(f"Successfully imported {processed} files")
    
    # Show table statistics
    print("\nTable Statistics:")
    print(f"Total Records: {table.scan().to_arrow().num_rows}")
    print(f"Table Location: {table.location()}")

if __name__ == "__main__":
    import_parquet_to_iceberg(
        catalog_name="default",
        table_name="default.stellar_events",
        source_pattern="/path/to/cdp/stellar-data/**/*.parquet",
        batch_size=50
    )
```

## Method 3: Using Trino

### Step 1: Setup Trino with Iceberg

Create `docker-compose.yml` for Trino:
```yaml
version: '3.8'

services:
  trino:
    image: trinodb/trino:latest
    container_name: trino-iceberg
    ports:
      - "8080:8080"
    volumes:
      - ./trino/etc:/etc/trino
      - ./trino/catalog:/etc/trino/catalog
      - /path/to/cdp/parquet:/data/parquet
      - /path/to/iceberg/warehouse:/data/iceberg
    environment:
      - JAVA_HEAP_SIZE=4G

  hive-metastore:
    image: apache/hive:4.0.0-alpha-2
    container_name: hive-metastore
    ports:
      - "9083:9083"
    environment:
      - SERVICE_NAME=metastore
      - DB_DRIVER=postgres
      - IS_RESUME=true
    volumes:
      - ./hive/conf:/opt/hive/conf
      - /path/to/iceberg/warehouse:/data/iceberg
```

Create `trino/catalog/iceberg.properties`:
```properties
connector.name=iceberg
hive.metastore.uri=thrift://hive-metastore:9083
iceberg.catalog.type=hive_metastore
```

### Step 2: Create and Populate Tables via Trino

Connect to Trino:
```bash
docker exec -it trino-iceberg trino
```

Run SQL commands:
```sql
-- Create schema
CREATE SCHEMA IF NOT EXISTS iceberg.stellar;

-- Create external table for parquet files
CREATE TABLE IF NOT EXISTS iceberg.stellar.events_external (
    ledger_sequence BIGINT,
    ledger_hash VARCHAR,
    ledger_close_time TIMESTAMP(6),
    transaction_hash VARCHAR,
    operation_id BIGINT,
    operation_type VARCHAR,
    source_account VARCHAR,
    destination_account VARCHAR,
    asset_code VARCHAR,
    asset_issuer VARCHAR,
    amount VARCHAR,
    data VARCHAR
)
WITH (
    external_location = 'file:///data/parquet/stellar-data/',
    format = 'PARQUET'
);

-- Create Iceberg table
CREATE TABLE IF NOT EXISTS iceberg.stellar.events
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year(ledger_close_time)', 'month(ledger_close_time)', 'day(ledger_close_time)']
)
AS SELECT * FROM iceberg.stellar.events_external;

-- Verify data
SELECT COUNT(*) FROM iceberg.stellar.events;
SELECT * FROM iceberg.stellar.events LIMIT 10;
```

## Automation and Best Practices

### Automated Pipeline Script

Create `cdp_to_iceberg_pipeline.sh`:
```bash
#!/bin/bash

# Configuration
CDP_PARQUET_PATH="/path/to/cdp/stellar-data"
ICEBERG_WAREHOUSE="/path/to/iceberg/warehouse"
CATALOG_NAME="stellar"
TABLE_NAME="cdp.events"
LAST_RUN_FILE="/var/run/cdp_iceberg_last_run"
LOG_FILE="/var/log/cdp_iceberg_$(date +%Y%m%d_%H%M%S).log"

# Function to log messages
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Check if another instance is running
LOCK_FILE="/var/run/cdp_iceberg.lock"
if [ -f "$LOCK_FILE" ]; then
    log "ERROR: Another instance is already running"
    exit 1
fi

# Create lock file
echo $$ > "$LOCK_FILE"
trap "rm -f $LOCK_FILE" EXIT

log "Starting CDP to Iceberg pipeline"

# Get last run timestamp
if [ -f "$LAST_RUN_FILE" ]; then
    LAST_RUN=$(cat "$LAST_RUN_FILE")
else
    LAST_RUN="2024-01-01 00:00:00"
fi

log "Processing files newer than: $LAST_RUN"

# Find new parquet files
NEW_FILES=$(find "$CDP_PARQUET_PATH" -name "*.parquet" -newermt "$LAST_RUN" | wc -l)
log "Found $NEW_FILES new parquet files"

if [ "$NEW_FILES" -gt 0 ]; then
    # Run Spark job to update Iceberg table
    spark-submit \
        --master local[*] \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 \
        --conf spark.sql.catalog.${CATALOG_NAME}=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.${CATALOG_NAME}.type=hadoop \
        --conf spark.sql.catalog.${CATALOG_NAME}.warehouse=${ICEBERG_WAREHOUSE} \
        update_iceberg_table.py \
        "$CDP_PARQUET_PATH" \
        "$TABLE_NAME" \
        "$LAST_RUN" 2>&1 | tee -a "$LOG_FILE"
    
    SPARK_EXIT_CODE=${PIPESTATUS[0]}
    
    if [ $SPARK_EXIT_CODE -eq 0 ]; then
        # Update last run timestamp
        date '+%Y-%m-%d %H:%M:%S' > "$LAST_RUN_FILE"
        log "Pipeline completed successfully"
        
        # Run table maintenance
        log "Running table maintenance..."
        spark-sql \
            --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 \
            -e "
            -- Compact small files
            CALL ${CATALOG_NAME}.system.rewrite_data_files('${TABLE_NAME}');
            
            -- Expire old snapshots (keep last 7 days)
            CALL ${CATALOG_NAME}.system.expire_snapshots('${TABLE_NAME}', TIMESTAMP '$(date -d '7 days ago' '+%Y-%m-%d %H:%M:%S')');
            
            -- Remove orphan files
            CALL ${CATALOG_NAME}.system.remove_orphan_files('${TABLE_NAME}');
            " 2>&1 | tee -a "$LOG_FILE"
    else
        log "ERROR: Spark job failed with exit code $SPARK_EXIT_CODE"
        exit $SPARK_EXIT_CODE
    fi
else
    log "No new files to process"
fi

# Generate statistics report
log "Generating statistics report..."
spark-sql \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 \
    -e "
    SELECT 
        COUNT(*) as total_records,
        MIN(ledger_sequence) as min_ledger,
        MAX(ledger_sequence) as max_ledger,
        COUNT(DISTINCT DATE(ledger_close_time)) as days_of_data,
        CURRENT_TIMESTAMP as report_time
    FROM ${CATALOG_NAME}.${TABLE_NAME};
    " 2>&1 | tee -a "$LOG_FILE"

log "Pipeline execution completed"
```

### Schedule with Cron

Add to crontab:
```bash
# Run every hour
0 * * * * /path/to/cdp_to_iceberg_pipeline.sh

# Or run every 6 hours
0 */6 * * * /path/to/cdp_to_iceberg_pipeline.sh
```

### Best Practices

1. **Partitioning Strategy**
   - Use time-based partitioning (year/month/day) for time-series blockchain data
   - Consider adding ledger range partitions for large datasets
   - Avoid over-partitioning (aim for >100MB per partition)

2. **File Sizing**
   - Target 100-500MB per data file
   - Use table maintenance to compact small files
   - Configure `write.target-file-size-bytes`

3. **Schema Evolution**
   - Start with core fields, add others later
   - Use `merge-schema` option for flexibility
   - Document schema changes in table properties

4. **Performance Optimization**
   ```sql
   -- Analyze table for better query planning
   ANALYZE TABLE stellar.cdp.events COMPUTE STATISTICS;
   
   -- Sort data within files for better compression
   INSERT OVERWRITE stellar.cdp.events
   SELECT * FROM stellar.cdp.events
   ORDER BY ledger_sequence;
   ```

5. **Monitoring**
   - Track table size growth
   - Monitor query performance
   - Set up alerts for failed updates

## Monitoring and Maintenance

### Table Health Checks

Create `iceberg_health_check.py`:
```python
from pyiceberg.catalog import load_catalog
from datetime import datetime, timedelta
import json

def check_table_health(catalog_name, table_name):
    catalog = load_catalog(catalog_name)
    table = catalog.load_table(table_name)
    
    health_report = {
        "table": table_name,
        "timestamp": datetime.now().isoformat(),
        "checks": {}
    }
    
    # Check 1: Table size and file count
    files = list(table.scan().plan_files())
    total_size = sum(f.file.file_size_in_bytes for f in files)
    
    health_report["checks"]["storage"] = {
        "total_files": len(files),
        "total_size_gb": round(total_size / 1024**3, 2),
        "avg_file_size_mb": round((total_size / len(files)) / 1024**2, 2) if files else 0
    }
    
    # Check 2: Small files (< 50MB)
    small_files = [f for f in files if f.file.file_size_in_bytes < 50 * 1024**2]
    health_report["checks"]["small_files"] = {
        "count": len(small_files),
        "percentage": round(len(small_files) / len(files) * 100, 2) if files else 0,
        "recommendation": "Run compaction" if len(small_files) > len(files) * 0.2 else "OK"
    }
    
    # Check 3: Snapshot history
    snapshots = list(table.history())
    health_report["checks"]["snapshots"] = {
        "total": len(snapshots),
        "oldest": snapshots[0].timestamp_ms if snapshots else None,
        "newest": snapshots[-1].timestamp_ms if snapshots else None,
        "recommendation": "Expire old snapshots" if len(snapshots) > 100 else "OK"
    }
    
    # Check 4: Data freshness
    if files:
        scan = table.scan()
        latest_data = scan.select("ledger_close_time").to_arrow()
        if latest_data.num_rows > 0:
            max_time = latest_data["ledger_close_time"].max().as_py()
            age_hours = (datetime.now() - max_time).total_seconds() / 3600
            health_report["checks"]["data_freshness"] = {
                "latest_data": max_time.isoformat(),
                "age_hours": round(age_hours, 2),
                "status": "OK" if age_hours < 24 else "STALE"
            }
    
    return health_report

if __name__ == "__main__":
    report = check_table_health("default", "default.stellar_events")
    print(json.dumps(report, indent=2))
```

### Maintenance Operations

```sql
-- Compact files (reduce small files)
CALL stellar.system.rewrite_data_files(
    table => 'cdp.events',
    strategy => 'binpack',
    options => map(
        'target-file-size-bytes', '134217728',  -- 128MB
        'min-file-size-bytes', '33554432'       -- 32MB
    )
);

-- Rewrite files with better sorting
CALL stellar.system.rewrite_data_files(
    table => 'cdp.events',
    strategy => 'sort',
    sort_order => 'ledger_sequence ASC'
);

-- Expire snapshots older than 7 days
CALL stellar.system.expire_snapshots(
    table => 'cdp.events',
    older_than => TIMESTAMP '2024-01-08 00:00:00',
    retain_last => 10
);

-- Clean up orphan files
CALL stellar.system.remove_orphan_files(
    table => 'cdp.events',
    older_than => TIMESTAMP '2024-01-08 00:00:00'
);

-- View table history
SELECT 
    snapshot_id,
    parent_id,
    operation,
    manifest_list,
    summary
FROM stellar.cdp.events$history
ORDER BY committed_at DESC
LIMIT 10;
```

## Troubleshooting

### Common Issues and Solutions

#### 1. "Table already exists" Error
```sql
-- Drop and recreate
DROP TABLE IF EXISTS stellar.cdp.events;
-- Then recreate with CREATE TABLE
```

#### 2. Schema Mismatch
```python
# Use merge-schema option
df.writeTo("stellar.cdp.events") \
   .option("merge-schema", "true") \
   .option("check-ordering", "false") \
   .append()
```

#### 3. Memory Issues with Large Files
```bash
# Increase Spark memory
spark-submit \
  --driver-memory 8g \
  --executor-memory 8g \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  your_script.py
```

#### 4. Slow Queries
```sql
-- Check table statistics
SHOW STATS FOR stellar.cdp.events;

-- Update statistics
ANALYZE TABLE stellar.cdp.events COMPUTE STATISTICS;

-- Check file distribution
SELECT 
    DATE(ledger_close_time) as date,
    COUNT(*) as records,
    COUNT(DISTINCT "$file") as files
FROM stellar.cdp.events
GROUP BY 1
ORDER BY 1 DESC
LIMIT 30;
```

#### 5. Debugging File Import
```python
# Verbose logging for debugging
import logging
logging.basicConfig(level=logging.DEBUG)

# Test with single file first
test_file = "/path/to/single/parquet/file.parquet"
df = pq.read_table(test_file)
print(f"Schema: {df.schema}")
print(f"Rows: {df.num_rows}")
print(f"Sample: {df.to_pandas().head()}")
```

### Performance Tuning

1. **Parallel Processing**
   ```properties
   spark.sql.shuffle.partitions=200
   spark.default.parallelism=100
   spark.sql.files.maxPartitionBytes=134217728
   ```

2. **Compression Settings**
   ```sql
   ALTER TABLE stellar.cdp.events
   SET TBLPROPERTIES (
       'write.parquet.compression-codec'='zstd',
       'write.parquet.compression-level'='3'
   );
   ```

3. **Query Optimization**
   ```sql
   -- Use partition pruning
   SELECT * FROM stellar.cdp.events
   WHERE year = 2024 AND month = 1 AND day = 15
   AND ledger_sequence BETWEEN 50000000 AND 50100000;
   
   -- Create materialized views for common queries
   CREATE MATERIALIZED VIEW stellar.cdp.daily_stats AS
   SELECT 
       DATE(ledger_close_time) as date,
       COUNT(*) as transaction_count,
       COUNT(DISTINCT source_account) as unique_accounts
   FROM stellar.cdp.events
   GROUP BY 1;
   ```

## Conclusion

You now have multiple methods to convert CDP parquet files into Apache Iceberg tables. Start with Method 1 (Spark) for simplicity, then explore PyIceberg for programmatic control or Trino for SQL-based workflows. Remember to:

1. Start small - test with a subset of data first
2. Monitor table health regularly
3. Schedule maintenance operations
4. Document your schema and partitioning decisions
5. Set up monitoring and alerting

The combination of CDP's efficient data capture and Iceberg's table management provides a powerful solution for blockchain analytics at scale.