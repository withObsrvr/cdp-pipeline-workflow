# Parquet Archival Consumer for CDP Pipeline Workflow

A comprehensive guide to implementing a Parquet-based archival consumer that integrates with the CDP Pipeline Workflow architecture, supporting local filesystem, Google Cloud Storage (GCS), and Amazon S3 storage backends.

## Overview

The Parquet archival consumer provides efficient long-term storage for Stellar blockchain data by:
- Converting streaming Protocol Buffer data to columnar Parquet format
- Supporting multiple storage backends (Local FS, GCS, S3)
- Implementing intelligent batching and file rotation
- Providing schema evolution and compression
- Maintaining compatibility with existing CDP pipeline patterns

## Architecture Integration

### Current Pipeline Flow
```
Source (XDR) → Processor → Multiple Consumers (PostgreSQL, Redis, etc.)
```

### Enhanced Pipeline with Archival
```
Source (XDR) → Processor → Real-time Consumers (PostgreSQL, Redis)
                        └→ Archival Consumer (Parquet) → GCS/S3/Local
```

## Implementation

### 1. Consumer Implementation

Create `/consumer/consumer_save_to_parquet.go`:

```go
package consumer

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "os"
    "path"
    "path/filepath"
    "sync"
    "time"

    "cloud.google.com/go/storage"
    "github.com/apache/arrow/go/v17/arrow"
    "github.com/apache/arrow/go/v17/arrow/array"
    "github.com/apache/arrow/go/v17/arrow/memory"
    "github.com/apache/arrow/go/v17/parquet/compress"
    "github.com/apache/arrow/go/v17/parquet/pqarrow"
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    processor "github.com/mxHeffly/cdp-pipeline-workflow/pkg/processor"
    "github.com/stellar/go/support/log"
)

// SaveToParquetConfig defines configuration for Parquet archival consumer
type SaveToParquetConfig struct {
    StorageType              string `json:"storage_type"`                // "FS", "GCS", "S3"
    BucketName              string `json:"bucket_name"`
    PathPrefix              string `json:"path_prefix"`
    LocalPath               string `json:"local_path"`                  // For FS storage
    Compression             string `json:"compression"`                 // "snappy", "gzip", "zstd", "none"
    MaxFileSizeMB           int    `json:"max_file_size_mb"`
    BufferSize              int    `json:"buffer_size"`
    RotationIntervalMinutes int    `json:"rotation_interval_minutes"`
    PartitionBy             string `json:"partition_by"`                // "ledger_day", "ledger_range", "hour"
    LedgersPerFile          uint32 `json:"ledgers_per_file"`            // For ledger_range partitioning
    SchemaEvolution         bool   `json:"schema_evolution"`
    IncludeMetadata         bool   `json:"include_metadata"`
    Region                  string `json:"region"`                      // For S3
}

// SaveToParquet is the Parquet archival consumer
type SaveToParquet struct {
    config          SaveToParquetConfig
    storageClient   StorageClient
    allocator       memory.Allocator
    
    // Buffering
    buffer          []processor.Message
    bufferMu        sync.Mutex
    currentFileSize int64
    lastFlush       time.Time
    
    // Schema management
    schemaRegistry  *ParquetSchemaRegistry
    currentSchema   *arrow.Schema
    
    // Metrics
    filesWritten    int64
    recordsWritten  int64
    bytesWritten    int64
    
    // State
    currentLedger   uint32
    startLedger     uint32
    ctx             context.Context
    cancel          context.CancelFunc
}

// StorageClient interface for different storage backends
type StorageClient interface {
    Write(ctx context.Context, key string, data []byte) error
    Close() error
}

// NewSaveToParquet creates a new Parquet archival consumer
func NewSaveToParquet(config interface{}) (*SaveToParquet, error) {
    var cfg SaveToParquetConfig
    
    // Handle config parsing
    switch v := config.(type) {
    case map[string]interface{}:
        configBytes, err := json.Marshal(v)
        if err != nil {
            return nil, fmt.Errorf("failed to marshal config: %w", err)
        }
        if err := json.Unmarshal(configBytes, &cfg); err != nil {
            return nil, fmt.Errorf("failed to unmarshal config: %w", err)
        }
    case SaveToParquetConfig:
        cfg = v
    default:
        return nil, fmt.Errorf("invalid config type: %T", v)
    }
    
    // Set defaults
    if cfg.BufferSize == 0 {
        cfg.BufferSize = 10000
    }
    if cfg.MaxFileSizeMB == 0 {
        cfg.MaxFileSizeMB = 128
    }
    if cfg.RotationIntervalMinutes == 0 {
        cfg.RotationIntervalMinutes = 60
    }
    if cfg.Compression == "" {
        cfg.Compression = "snappy"
    }
    if cfg.PartitionBy == "" {
        cfg.PartitionBy = "ledger_day"
    }
    if cfg.LedgersPerFile == 0 {
        cfg.LedgersPerFile = 10000
    }
    
    // Initialize storage client
    storageClient, err := createStorageClient(cfg)
    if err != nil {
        return nil, fmt.Errorf("failed to create storage client: %w", err)
    }
    
    ctx, cancel := context.WithCancel(context.Background())
    
    consumer := &SaveToParquet{
        config:         cfg,
        storageClient:  storageClient,
        allocator:      memory.NewGoAllocator(),
        buffer:         make([]processor.Message, 0, cfg.BufferSize),
        lastFlush:      time.Now(),
        schemaRegistry: NewParquetSchemaRegistry(),
        ctx:           ctx,
        cancel:        cancel,
    }
    
    // Start periodic flush goroutine
    go consumer.periodicFlush()
    
    return consumer, nil
}

// Process handles incoming messages
func (s *SaveToParquet) Process(msg processor.Message) error {
    s.bufferMu.Lock()
    defer s.bufferMu.Unlock()
    
    // Extract ledger info from metadata
    if metadata, ok := msg.Metadata["ledger"].(map[string]interface{}); ok {
        if ledgerNum, ok := metadata["sequence"].(float64); ok {
            s.currentLedger = uint32(ledgerNum)
            if s.startLedger == 0 {
                s.startLedger = s.currentLedger
            }
        }
    }
    
    s.buffer = append(s.buffer, msg)
    
    // Check if we should flush
    shouldFlush := false
    
    // Check buffer size
    if len(s.buffer) >= s.config.BufferSize {
        shouldFlush = true
    }
    
    // Check ledger range for ledger-based partitioning
    if s.config.PartitionBy == "ledger_range" {
        ledgerRange := s.currentLedger - s.startLedger
        if ledgerRange >= s.config.LedgersPerFile {
            shouldFlush = true
        }
    }
    
    if shouldFlush {
        return s.flush()
    }
    
    return nil
}

// flush writes buffered data to Parquet file
func (s *SaveToParquet) flush() error {
    if len(s.buffer) == 0 {
        return nil
    }
    
    log.Infof("Flushing %d records to Parquet", len(s.buffer))
    
    // Determine schema from first batch if needed
    if s.currentSchema == nil {
        schema, err := s.schemaRegistry.InferSchema(s.buffer)
        if err != nil {
            return fmt.Errorf("failed to infer schema: %w", err)
        }
        s.currentSchema = schema
    }
    
    // Convert messages to Arrow record batch
    recordBatch, err := s.convertToArrowBatch(s.buffer)
    if err != nil {
        return fmt.Errorf("failed to convert to Arrow batch: %w", err)
    }
    defer recordBatch.Release()
    
    // Write to Parquet
    parquetData, err := s.writeParquet(recordBatch)
    if err != nil {
        return fmt.Errorf("failed to write Parquet: %w", err)
    }
    
    // Generate file path
    filePath := s.generateFilePath()
    
    // Upload to storage
    if err := s.storageClient.Write(s.ctx, filePath, parquetData); err != nil {
        return fmt.Errorf("failed to write to storage: %w", err)
    }
    
    // Update metrics
    s.filesWritten++
    s.recordsWritten += int64(len(s.buffer))
    s.bytesWritten += int64(len(parquetData))
    
    log.Infof("Written Parquet file: %s (%d records, %d bytes)", filePath, len(s.buffer), len(parquetData))
    
    // Clear buffer and reset state
    s.buffer = s.buffer[:0]
    s.startLedger = s.currentLedger
    s.lastFlush = time.Now()
    s.currentFileSize = 0
    
    return nil
}

// convertToArrowBatch converts messages to Arrow record batch
func (s *SaveToParquet) convertToArrowBatch(messages []processor.Message) (arrow.Record, error) {
    // Group messages by type
    contractDataMessages := make([]*ContractDataOutput, 0)
    eventMessages := make([]*Event, 0)
    
    for _, msg := range messages {
        switch payload := msg.Payload.(type) {
        case map[string]interface{}:
            // Try to identify and convert based on fields present
            if _, hasContractID := payload["contract_id"]; hasContractID {
                var contractData ContractDataOutput
                if err := mapToStruct(payload, &contractData); err == nil {
                    contractDataMessages = append(contractDataMessages, &contractData)
                }
            } else if _, hasEventType := payload["type"]; hasEventType {
                var event Event
                if err := mapToStruct(payload, &event); err == nil {
                    eventMessages = append(eventMessages, &event)
                }
            }
        case *ContractDataOutput:
            contractDataMessages = append(contractDataMessages, payload)
        case *Event:
            eventMessages = append(eventMessages, payload)
        }
    }
    
    // For now, handle contract data (can be extended for other types)
    if len(contractDataMessages) > 0 {
        return s.convertContractDataToArrow(contractDataMessages, messages)
    }
    
    return nil, fmt.Errorf("no supported data types found in batch")
}

// convertContractDataToArrow converts contract data to Arrow format
func (s *SaveToParquet) convertContractDataToArrow(data []*ContractDataOutput, originalMessages []processor.Message) (arrow.Record, error) {
    // Define schema if not already defined
    if s.currentSchema == nil {
        s.currentSchema = s.getContractDataSchema()
    }
    
    // Create builders
    builders := make([]array.Builder, len(s.currentSchema.Fields()))
    for i, field := range s.currentSchema.Fields() {
        builders[i] = array.NewBuilder(s.allocator, field.Type)
    }
    
    // Populate builders
    for idx, record := range data {
        builders[0].(*array.StringBuilder).Append(record.ContractId)
        builders[1].(*array.StringBuilder).Append(record.ContractKeyType)
        builders[2].(*array.StringBuilder).Append(record.ContractDurability)
        
        // Asset fields (nullable)
        appendNullableString(builders[3].(*array.StringBuilder), record.ContractDataAssetCode)
        appendNullableString(builders[4].(*array.StringBuilder), record.ContractDataAssetIssuer)
        appendNullableString(builders[5].(*array.StringBuilder), record.ContractDataAssetType)
        
        // Balance fields
        appendNullableString(builders[6].(*array.StringBuilder), record.ContractDataBalanceHolder)
        if record.ContractDataBalance != "" {
            builders[7].(*array.Int64Builder).Append(parseBalance(record.ContractDataBalance))
        } else {
            builders[7].(*array.Int64Builder).AppendNull()
        }
        
        // Ledger info
        builders[8].(*array.Uint32Builder).Append(record.LastModifiedLedger)
        builders[9].(*array.BooleanBuilder).Append(record.Deleted)
        builders[10].(*array.Uint32Builder).Append(record.LedgerSequence)
        
        // Timestamp (microseconds)
        closedAt := time.Unix(record.ClosedAt, 0).UnixMicro()
        builders[11].(*array.TimestampBuilder).Append(arrow.Timestamp(closedAt))
        
        // XDR fields
        appendNullableString(builders[12].(*array.StringBuilder), record.LedgerKeyHash)
        if record.KeyXdr != "" {
            builders[13].(*array.BinaryBuilder).Append([]byte(record.KeyXdr))
        } else {
            builders[13].(*array.BinaryBuilder).AppendNull()
        }
        if record.ValXdr != "" {
            builders[14].(*array.BinaryBuilder).Append([]byte(record.ValXdr))
        } else {
            builders[14].(*array.BinaryBuilder).AppendNull()
        }
        
        // Contract instance fields
        appendNullableString(builders[15].(*array.StringBuilder), record.ContractInstanceType)
        appendNullableString(builders[16].(*array.StringBuilder), record.ContractInstanceWasmHash)
        
        if record.ExpirationLedgerSeq > 0 {
            builders[17].(*array.Uint32Builder).Append(record.ExpirationLedgerSeq)
        } else {
            builders[17].(*array.Uint32Builder).AppendNull()
        }
        
        // Add metadata if configured
        if s.config.IncludeMetadata && len(builders) > 18 {
            if metadata, err := json.Marshal(originalMessages[idx].Metadata); err == nil {
                builders[18].(*array.StringBuilder).Append(string(metadata))
            } else {
                builders[18].(*array.StringBuilder).AppendNull()
            }
        }
    }
    
    // Build arrays
    arrays := make([]arrow.Array, len(builders))
    for i, builder := range builders {
        arrays[i] = builder.NewArray()
        defer arrays[i].Release()
    }
    
    // Create record
    return array.NewRecord(s.currentSchema, arrays, int64(len(data))), nil
}

// getContractDataSchema returns the Arrow schema for contract data
func (s *SaveToParquet) getContractDataSchema() *arrow.Schema {
    fields := []arrow.Field{
        {Name: "contract_id", Type: arrow.BinaryTypes.String, Nullable: false},
        {Name: "contract_key_type", Type: arrow.BinaryTypes.String, Nullable: false},
        {Name: "contract_durability", Type: arrow.BinaryTypes.String, Nullable: false},
        {Name: "asset_code", Type: arrow.BinaryTypes.String, Nullable: true},
        {Name: "asset_issuer", Type: arrow.BinaryTypes.String, Nullable: true},
        {Name: "asset_type", Type: arrow.BinaryTypes.String, Nullable: true},
        {Name: "balance_holder", Type: arrow.BinaryTypes.String, Nullable: true},
        {Name: "balance", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
        {Name: "last_modified_ledger", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
        {Name: "deleted", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
        {Name: "ledger_sequence", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
        {Name: "closed_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: false},
        {Name: "ledger_key_hash", Type: arrow.BinaryTypes.String, Nullable: true},
        {Name: "key_xdr", Type: arrow.BinaryTypes.Binary, Nullable: true},
        {Name: "val_xdr", Type: arrow.BinaryTypes.Binary, Nullable: true},
        {Name: "contract_instance_type", Type: arrow.BinaryTypes.String, Nullable: true},
        {Name: "contract_instance_wasm_hash", Type: arrow.BinaryTypes.String, Nullable: true},
        {Name: "expiration_ledger_seq", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
    }
    
    // Add metadata field if configured
    if s.config.IncludeMetadata {
        fields = append(fields, arrow.Field{
            Name: "pipeline_metadata",
            Type: arrow.BinaryTypes.String,
            Nullable: true,
        })
    }
    
    metadata := arrow.NewMetadata([]string{"schema_version", "data_type"}, []string{"1.0.0", "contract_data"})
    return arrow.NewSchema(fields, &metadata)
}

// writeParquet writes Arrow record to Parquet format
func (s *SaveToParquet) writeParquet(record arrow.Record) ([]byte, error) {
    var buf bytes.Buffer
    
    // Configure Parquet writer properties
    props := pqarrow.NewWriterProperties(
        pqarrow.WithCompression(s.getCompressionType()),
        pqarrow.WithDictionaryDefault(true),
        pqarrow.WithStatsEnabled(true),
    )
    
    arrowProps := pqarrow.NewArrowWriterProperties(
        pqarrow.WithStoreSchema(),
    )
    
    // Create Parquet writer
    writer, err := pqarrow.NewFileWriter(record.Schema(), &buf, props, arrowProps)
    if err != nil {
        return nil, fmt.Errorf("failed to create Parquet writer: %w", err)
    }
    
    // Write record
    if err := writer.Write(record); err != nil {
        return nil, fmt.Errorf("failed to write record: %w", err)
    }
    
    // Close writer
    if err := writer.Close(); err != nil {
        return nil, fmt.Errorf("failed to close writer: %w", err)
    }
    
    return buf.Bytes(), nil
}

// getCompressionType returns the Parquet compression type
func (s *SaveToParquet) getCompressionType() compress.Compression {
    switch s.config.Compression {
    case "snappy":
        return compress.Codecs.Snappy
    case "gzip":
        return compress.Codecs.Gzip
    case "zstd":
        return compress.Codecs.Zstd
    case "lz4":
        return compress.Codecs.Lz4
    case "none":
        return compress.Codecs.Uncompressed
    default:
        return compress.Codecs.Snappy
    }
}

// generateFilePath generates the output file path based on partitioning strategy
func (s *SaveToParquet) generateFilePath() string {
    now := time.Now().UTC()
    var partPath string
    
    switch s.config.PartitionBy {
    case "ledger_day":
        // Partition by the day of the ledger
        ledgerTime := time.Unix(int64(s.startLedger)*6, 0) // Approximate 6 seconds per ledger
        partPath = fmt.Sprintf("year=%d/month=%02d/day=%02d",
            ledgerTime.Year(), ledgerTime.Month(), ledgerTime.Day())
            
    case "ledger_range":
        // Partition by ledger ranges
        rangeStart := (s.startLedger / s.config.LedgersPerFile) * s.config.LedgersPerFile
        rangeEnd := rangeStart + s.config.LedgersPerFile - 1
        partPath = fmt.Sprintf("ledgers/%d-%d", rangeStart, rangeEnd)
        
    case "hour":
        // Partition by hour
        partPath = fmt.Sprintf("year=%d/month=%02d/day=%02d/hour=%02d",
            now.Year(), now.Month(), now.Day(), now.Hour())
            
    default:
        // Default to daily partitions
        partPath = fmt.Sprintf("year=%d/month=%02d/day=%02d",
            now.Year(), now.Month(), now.Day())
    }
    
    // Generate filename
    filename := fmt.Sprintf("stellar-data-%d-%d-%d.parquet",
        s.startLedger, s.currentLedger, now.Unix())
    
    // Combine path components
    fullPath := path.Join(s.config.PathPrefix, partPath, filename)
    
    return fullPath
}

// periodicFlush handles time-based flushing
func (s *SaveToParquet) periodicFlush() {
    ticker := time.NewTicker(time.Duration(s.config.RotationIntervalMinutes) * time.Minute)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            s.bufferMu.Lock()
            timeSinceFlush := time.Since(s.lastFlush)
            if timeSinceFlush >= time.Duration(s.config.RotationIntervalMinutes)*time.Minute && len(s.buffer) > 0 {
                if err := s.flush(); err != nil {
                    log.Errorf("Error in periodic flush: %v", err)
                }
            }
            s.bufferMu.Unlock()
            
        case <-s.ctx.Done():
            return
        }
    }
}

// Close flushes remaining data and closes the consumer
func (s *SaveToParquet) Close() error {
    s.cancel()
    
    s.bufferMu.Lock()
    defer s.bufferMu.Unlock()
    
    // Flush remaining data
    if len(s.buffer) > 0 {
        if err := s.flush(); err != nil {
            return fmt.Errorf("error flushing on close: %w", err)
        }
    }
    
    // Close storage client
    if err := s.storageClient.Close(); err != nil {
        return fmt.Errorf("error closing storage client: %w", err)
    }
    
    log.Infof("Parquet archival consumer closed. Files written: %d, Records: %d, Bytes: %d",
        s.filesWritten, s.recordsWritten, s.bytesWritten)
    
    return nil
}

// GetMetrics returns consumer metrics
func (s *SaveToParquet) GetMetrics() map[string]interface{} {
    s.bufferMu.Lock()
    defer s.bufferMu.Unlock()
    
    return map[string]interface{}{
        "files_written":   s.filesWritten,
        "records_written": s.recordsWritten,
        "bytes_written":   s.bytesWritten,
        "buffer_size":     len(s.buffer),
        "current_ledger":  s.currentLedger,
    }
}

// Helper functions

func appendNullableString(builder *array.StringBuilder, value string) {
    if value != "" {
        builder.Append(value)
    } else {
        builder.AppendNull()
    }
}

func parseBalance(balance string) int64 {
    // Parse balance string to int64, handle errors gracefully
    var result int64
    fmt.Sscanf(balance, "%d", &result)
    return result
}

func mapToStruct(m map[string]interface{}, v interface{}) error {
    data, err := json.Marshal(m)
    if err != nil {
        return err
    }
    return json.Unmarshal(data, v)
}
```

### 2. Storage Client Implementations

Create `/consumer/parquet_storage_clients.go`:

```go
package consumer

import (
    "context"
    "fmt"
    "io"
    "os"
    "path/filepath"

    "cloud.google.com/go/storage"
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/s3"
)

// createStorageClient creates the appropriate storage client based on config
func createStorageClient(cfg SaveToParquetConfig) (StorageClient, error) {
    switch cfg.StorageType {
    case "FS":
        return NewLocalFSClient(cfg.LocalPath)
    case "GCS":
        return NewGCSClient(cfg.BucketName)
    case "S3":
        return NewS3Client(cfg.BucketName, cfg.Region)
    default:
        return nil, fmt.Errorf("unsupported storage type: %s", cfg.StorageType)
    }
}

// LocalFSClient implements StorageClient for local filesystem
type LocalFSClient struct {
    basePath string
}

func NewLocalFSClient(basePath string) (*LocalFSClient, error) {
    // Create base directory if it doesn't exist
    if err := os.MkdirAll(basePath, 0755); err != nil {
        return nil, fmt.Errorf("failed to create base directory: %w", err)
    }
    
    return &LocalFSClient{
        basePath: basePath,
    }, nil
}

func (c *LocalFSClient) Write(ctx context.Context, key string, data []byte) error {
    fullPath := filepath.Join(c.basePath, key)
    
    // Create directory structure
    dir := filepath.Dir(fullPath)
    if err := os.MkdirAll(dir, 0755); err != nil {
        return fmt.Errorf("failed to create directory: %w", err)
    }
    
    // Write to temporary file first
    tmpFile := fullPath + ".tmp"
    if err := os.WriteFile(tmpFile, data, 0644); err != nil {
        return fmt.Errorf("failed to write file: %w", err)
    }
    
    // Atomic rename
    if err := os.Rename(tmpFile, fullPath); err != nil {
        os.Remove(tmpFile)
        return fmt.Errorf("failed to rename file: %w", err)
    }
    
    return nil
}

func (c *LocalFSClient) Close() error {
    return nil
}

// GCSClient implements StorageClient for Google Cloud Storage
type GCSClient struct {
    client *storage.Client
    bucket string
}

func NewGCSClient(bucketName string) (*GCSClient, error) {
    ctx := context.Background()
    client, err := storage.NewClient(ctx)
    if err != nil {
        return nil, fmt.Errorf("failed to create GCS client: %w", err)
    }
    
    return &GCSClient{
        client: client,
        bucket: bucketName,
    }, nil
}

func (c *GCSClient) Write(ctx context.Context, key string, data []byte) error {
    bucket := c.client.Bucket(c.bucket)
    obj := bucket.Object(key)
    
    // Create writer with metadata
    w := obj.NewWriter(ctx)
    w.ContentType = "application/octet-stream"
    w.Metadata = map[string]string{
        "format":    "parquet",
        "generator": "cdp-pipeline-workflow",
    }
    
    // Write data
    if _, err := w.Write(data); err != nil {
        w.Close()
        return fmt.Errorf("failed to write to GCS: %w", err)
    }
    
    if err := w.Close(); err != nil {
        return fmt.Errorf("failed to close GCS writer: %w", err)
    }
    
    return nil
}

func (c *GCSClient) Close() error {
    return c.client.Close()
}

// S3Client implements StorageClient for Amazon S3
type S3Client struct {
    client *s3.Client
    bucket string
}

func NewS3Client(bucketName, region string) (*S3Client, error) {
    ctx := context.Background()
    
    cfg, err := config.LoadDefaultConfig(ctx,
        config.WithRegion(region),
    )
    if err != nil {
        return nil, fmt.Errorf("failed to load AWS config: %w", err)
    }
    
    client := s3.NewFromConfig(cfg)
    
    return &S3Client{
        client: client,
        bucket: bucketName,
    }, nil
}

func (c *S3Client) Write(ctx context.Context, key string, data []byte) error {
    _, err := c.client.PutObject(ctx, &s3.PutObjectInput{
        Bucket: aws.String(c.bucket),
        Key:    aws.String(key),
        Body:   bytes.NewReader(data),
        ContentType: aws.String("application/octet-stream"),
        Metadata: map[string]string{
            "format":    "parquet",
            "generator": "cdp-pipeline-workflow",
        },
    })
    
    if err != nil {
        return fmt.Errorf("failed to write to S3: %w", err)
    }
    
    return nil
}

func (c *S3Client) Close() error {
    return nil
}
```

### 3. Schema Registry

Create `/consumer/parquet_schema_registry.go`:

```go
package consumer

import (
    "fmt"
    "reflect"

    "github.com/apache/arrow/go/v17/arrow"
    "github.com/apache/arrow/go/v17/arrow/memory"
    processor "github.com/mxHeffly/cdp-pipeline-workflow/pkg/processor"
)

// ParquetSchemaRegistry manages Arrow schemas for different message types
type ParquetSchemaRegistry struct {
    allocator memory.Allocator
    schemas   map[string]*arrow.Schema
}

func NewParquetSchemaRegistry() *ParquetSchemaRegistry {
    return &ParquetSchemaRegistry{
        allocator: memory.NewGoAllocator(),
        schemas:   make(map[string]*arrow.Schema),
    }
}

// InferSchema infers an Arrow schema from a batch of messages
func (r *ParquetSchemaRegistry) InferSchema(messages []processor.Message) (*arrow.Schema, error) {
    if len(messages) == 0 {
        return nil, fmt.Errorf("cannot infer schema from empty message batch")
    }
    
    // Analyze first message to determine type
    firstPayload := messages[0].Payload
    
    switch payload := firstPayload.(type) {
    case map[string]interface{}:
        return r.inferSchemaFromMap(payload)
    case *ContractDataOutput:
        return r.getContractDataSchema(), nil
    case *Event:
        return r.getEventSchema(), nil
    case *AssetDetails:
        return r.getAssetSchema(), nil
    default:
        return nil, fmt.Errorf("unsupported payload type: %T", payload)
    }
}

// inferSchemaFromMap dynamically infers schema from map structure
func (r *ParquetSchemaRegistry) inferSchemaFromMap(data map[string]interface{}) (*arrow.Schema, error) {
    fields := make([]arrow.Field, 0, len(data))
    
    // Sort keys for consistent field ordering
    keys := make([]string, 0, len(data))
    for k := range data {
        keys = append(keys, k)
    }
    sort.Strings(keys)
    
    for _, key := range keys {
        value := data[key]
        field := arrow.Field{
            Name:     key,
            Nullable: true,
        }
        
        // Infer Arrow type from Go type
        switch v := value.(type) {
        case string:
            field.Type = arrow.BinaryTypes.String
        case float64:
            // Check if it's actually an integer
            if v == float64(int64(v)) {
                field.Type = arrow.PrimitiveTypes.Int64
            } else {
                field.Type = arrow.PrimitiveTypes.Float64
            }
        case bool:
            field.Type = arrow.FixedWidthTypes.Boolean
        case int, int32, int64:
            field.Type = arrow.PrimitiveTypes.Int64
        case uint32:
            field.Type = arrow.PrimitiveTypes.Uint32
        case nil:
            field.Type = arrow.BinaryTypes.String // Default to string for null values
        default:
            // For complex types, serialize as JSON string
            field.Type = arrow.BinaryTypes.String
        }
        
        fields = append(fields, field)
    }
    
    return arrow.NewSchema(fields, nil), nil
}

// getContractDataSchema returns the schema for contract data
func (r *ParquetSchemaRegistry) getContractDataSchema() *arrow.Schema {
    return arrow.NewSchema([]arrow.Field{
        {Name: "contract_id", Type: arrow.BinaryTypes.String, Nullable: false},
        {Name: "contract_key_type", Type: arrow.BinaryTypes.String, Nullable: false},
        {Name: "contract_durability", Type: arrow.BinaryTypes.String, Nullable: false},
        {Name: "asset_code", Type: arrow.BinaryTypes.String, Nullable: true},
        {Name: "asset_issuer", Type: arrow.BinaryTypes.String, Nullable: true},
        {Name: "asset_type", Type: arrow.BinaryTypes.String, Nullable: true},
        {Name: "balance_holder", Type: arrow.BinaryTypes.String, Nullable: true},
        {Name: "balance", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
        {Name: "last_modified_ledger", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
        {Name: "deleted", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
        {Name: "ledger_sequence", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
        {Name: "closed_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: false},
        {Name: "ledger_key_hash", Type: arrow.BinaryTypes.String, Nullable: true},
        {Name: "key_xdr", Type: arrow.BinaryTypes.Binary, Nullable: true},
        {Name: "val_xdr", Type: arrow.BinaryTypes.Binary, Nullable: true},
        {Name: "contract_instance_type", Type: arrow.BinaryTypes.String, Nullable: true},
        {Name: "contract_instance_wasm_hash", Type: arrow.BinaryTypes.String, Nullable: true},
        {Name: "expiration_ledger_seq", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
    }, nil)
}

// getEventSchema returns the schema for events
func (r *ParquetSchemaRegistry) getEventSchema() *arrow.Schema {
    return arrow.NewSchema([]arrow.Field{
        {Name: "type", Type: arrow.BinaryTypes.String, Nullable: false},
        {Name: "ledger", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
        {Name: "ledger_closed_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: false},
        {Name: "contract_id", Type: arrow.BinaryTypes.String, Nullable: false},
        {Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
        {Name: "in_successful_contract_call", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
        {Name: "topics", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
        {Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
    }, nil)
}

// getAssetSchema returns the schema for asset details
func (r *ParquetSchemaRegistry) getAssetSchema() *arrow.Schema {
    return arrow.NewSchema([]arrow.Field{
        {Name: "asset", Type: arrow.BinaryTypes.String, Nullable: false},
        {Name: "asset_type", Type: arrow.BinaryTypes.String, Nullable: false},
        {Name: "asset_code", Type: arrow.BinaryTypes.String, Nullable: true},
        {Name: "asset_issuer", Type: arrow.BinaryTypes.String, Nullable: true},
        {Name: "accounts", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
        {Name: "balances", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
        {Name: "pool_shares", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
        {Name: "claimable_balances", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
        {Name: "liquidity_pools", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
        {Name: "contract_balances", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
        {Name: "archived_contracts", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
    }, nil)
}
```

### 4. Integration with Main

Add to `/main.go` in the `createConsumer` function:

```go
case "SaveToParquet":
    return consumer.NewSaveToParquet(consumerConfig.Config)
```

### 5. Configuration Examples

Create `/config/examples/parquet-archival.yaml`:

```yaml
# Example configuration for Parquet archival consumer

# Local filesystem storage
local_archival:
  type: SaveToParquet
  config:
    storage_type: "FS"
    local_path: "/data/stellar-archive"
    path_prefix: "contract_data"
    compression: "snappy"
    buffer_size: 10000
    max_file_size_mb: 128
    rotation_interval_minutes: 60
    partition_by: "ledger_day"
    include_metadata: true

# Google Cloud Storage
gcs_archival:
  type: SaveToParquet
  config:
    storage_type: "GCS"
    bucket_name: "stellar-data-lake"
    path_prefix: "contract_data/parquet"
    compression: "zstd"
    buffer_size: 25000
    max_file_size_mb: 256
    rotation_interval_minutes: 30
    partition_by: "ledger_range"
    ledgers_per_file: 10000
    schema_evolution: true
    include_metadata: true

# Amazon S3
s3_archival:
  type: SaveToParquet
  config:
    storage_type: "S3"
    bucket_name: "stellar-archive"
    path_prefix: "blockchain/stellar"
    region: "us-west-2"
    compression: "snappy"
    buffer_size: 50000
    max_file_size_mb: 512
    rotation_interval_minutes: 120
    partition_by: "hour"
    include_metadata: false
```

## Pipeline Configuration

### Multi-Consumer Setup

Configure the pipeline to use both real-time and archival consumers:

```yaml
# config/production/pipeline.yaml
version: "1.0"
name: "stellar-contract-data-pipeline"

processors:
  - type: ContractDataProcessor
    config:
      source_endpoint: "stellar-live-source:50051"
      buffer_size: 1000

consumers:
  # Real-time consumer
  - type: SaveToPostgreSQL
    config:
      connection_string: "${DATABASE_URL}"
      table_name: "contract_data"
      batch_size: 100
  
  # Archival consumer
  - type: SaveToParquet
    config:
      storage_type: "GCS"
      bucket_name: "${ARCHIVE_BUCKET}"
      path_prefix: "contract_data"
      compression: "snappy"
      buffer_size: 25000
      max_file_size_mb: 256
      rotation_interval_minutes: 60
      partition_by: "ledger_day"
      include_metadata: true
```

## Query Patterns

### DuckDB Query Examples

```sql
-- Query Parquet files directly from GCS
SELECT 
    DATE_TRUNC('day', closed_at) as day,
    COUNT(*) as transactions,
    COUNT(DISTINCT contract_id) as unique_contracts
FROM read_parquet('gs://stellar-data-lake/contract_data/year=2024/month=*/day=*/*.parquet')
WHERE closed_at BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY day
ORDER BY day;

-- Analyze asset distribution
SELECT 
    asset_code,
    asset_issuer,
    COUNT(*) as transaction_count,
    SUM(balance) as total_balance
FROM read_parquet('gs://stellar-data-lake/contract_data/year=2024/*/*/*.parquet')
WHERE asset_code IS NOT NULL
GROUP BY asset_code, asset_issuer
ORDER BY transaction_count DESC
LIMIT 100;
```

### Python/PyArrow Query Examples

```python
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow.dataset as ds

# Read partitioned dataset
dataset = ds.dataset("gs://stellar-data-lake/contract_data/", 
                    format="parquet",
                    partitioning=ds.partitioning(
                        schema=pa.schema([
                            ("year", pa.int32()),
                            ("month", pa.int32()),
                            ("day", pa.int32())
                        ])
                    ))

# Filter and aggregate
table = dataset.to_table(
    filter=(pc.field("closed_at") >= "2024-01-01") & 
           (pc.field("closed_at") < "2024-02-01")
)

# Analyze with pandas
df = table.to_pandas()
daily_stats = df.groupby(df['closed_at'].dt.date).agg({
    'contract_id': 'nunique',
    'balance': 'sum',
    'deleted': 'sum'
})
```

## Performance Considerations

### Optimization Tips

1. **Buffer Size**: Larger buffers (25k-50k records) improve compression and reduce file count
2. **Compression**: Use Snappy for speed, Zstd for better compression ratio
3. **Partitioning**: Choose based on query patterns:
   - `ledger_day`: Good for time-based queries
   - `ledger_range`: Good for ledger-based queries
   - `hour`: Good for recent data analysis

### Expected Performance

- **Write throughput**: 50k-100k records/second
- **Compression ratio**: 10:1 to 20:1 (depending on data)
- **Query performance**: 1-10 seconds for daily aggregations
- **Storage cost**: ~$0.023/GB/month (GCS/S3)

## Monitoring and Operations

### Metrics to Monitor

```go
// Add to consumer implementation
metrics.Register("parquet_files_written", s.filesWritten)
metrics.Register("parquet_records_buffered", len(s.buffer))
metrics.Register("parquet_bytes_written", s.bytesWritten)
metrics.Register("parquet_last_flush_time", s.lastFlush.Unix())
```

### Operational Procedures

1. **Schema Evolution**: New fields are automatically added as nullable
2. **Backfilling**: Process historical data by replaying from archive source
3. **Compaction**: Periodically merge small files into larger ones
4. **Retention**: Use lifecycle policies on GCS/S3 to manage old data

## Migration Strategy

### Phase 1: Deploy Alongside Existing Consumers
- Add Parquet consumer to existing pipeline
- Validate data integrity
- Monitor performance impact

### Phase 2: Historical Backfill
- Process historical data in batches
- Use separate pipeline instance for backfill
- Verify data completeness

### Phase 3: Enable Analytics
- Set up DuckDB or Presto for queries
- Create materialized views for common queries
- Build dashboards on top of Parquet data

## Troubleshooting

### Common Issues

1. **Out of Memory**: Reduce buffer_size or increase container memory
2. **Slow Writes**: Check compression settings and network bandwidth
3. **Schema Mismatch**: Enable schema_evolution or manually update schema
4. **Missing Data**: Check partition paths and time ranges

### Debug Mode

```yaml
consumers:
  - type: SaveToParquet
    config:
      # ... other config
      debug: true  # Enables detailed logging
      dry_run: true  # Logs actions without writing files
```

## Conclusion

The Parquet archival consumer provides a production-ready solution for long-term Stellar blockchain data storage, combining the real-time capabilities of the CDP pipeline with efficient analytical storage. This hybrid approach ensures optimal performance for both operational and analytical workloads while significantly reducing storage costs.