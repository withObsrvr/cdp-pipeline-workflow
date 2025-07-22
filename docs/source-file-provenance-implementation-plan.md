# Source File Provenance Implementation Plan

## Overview

This document outlines the implementation strategy for adding source file provenance to contract data, enabling tracking of which GCS/S3 archive file each piece of contract data originated from.

## Current Architecture Analysis

### Message Flow
```
Archive File (S3/GCS) → stellar-cdp library → LedgerCloseMeta → processor.Message → ContractDataProcessor → Consumer
```

### Current Limitations

1. **No metadata preservation**: The `Message` structure only contains payload data
2. **Lost file context**: Archive file information is available during ingestion but not propagated
3. **External library abstraction**: The `stellar-cdp` library handles file reading internally
4. **Simple message structure**:
   ```go
   type Message struct {
       Payload interface{}  // Only contains xdr.LedgerCloseMeta
   }
   ```

### Source Adapters That Need Enhancement

- `S3BufferedStorageSourceAdapter` 
- `GCSBufferedStorageSourceAdapter`
- `FSBufferedStorageSourceAdapter`
- `BufferedStorageSourceAdapter` (generic)

## Proposed Solution

### Enhanced Message Structure

Extend the core message structure to include metadata while maintaining backward compatibility:

```go
type Message struct {
    Payload  interface{}            `json:"payload"`
    Metadata map[string]interface{} `json:"metadata,omitempty"`
}

type ArchiveSourceMetadata struct {
    SourceType    string    `json:"source_type"`     // "S3", "GCS", "FS"
    BucketName    string    `json:"bucket_name"`     // for cloud storage
    FilePath      string    `json:"file_path"`       // full file path
    FileName      string    `json:"file_name"`       // just filename
    StartLedger   uint32    `json:"start_ledger"`    // first ledger in file
    EndLedger     uint32    `json:"end_ledger"`      // last ledger in file
    ProcessedAt   time.Time `json:"processed_at"`
    FileSize      int64     `json:"file_size,omitempty"`
    Partition     uint32    `json:"partition,omitempty"`
}
```

### File Path Calculation Logic

Archive files follow Stellar's naming convention:
- File pattern: `ledger-{start_ledger}-{end_ledger}.xdr`
- Partition structure: `partition-{number}/ledger-{start}-{end}.xdr`
- Configuration-based: Uses `LedgersPerFile` and `FilesPerPartition` schema

## Implementation Plan

### Phase 1: Core Infrastructure Changes

#### 1.1 Update Message Structure

**File**: `processor/processor.go`

```go
// Extend existing Message struct
type Message struct {
    Payload  interface{}            `json:"payload"`
    Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// Helper function to extract archive metadata
func (m *Message) GetArchiveMetadata() (*ArchiveSourceMetadata, bool) {
    if m.Metadata == nil {
        return nil, false
    }
    
    archiveData, exists := m.Metadata["archive_source"]
    if !exists {
        return nil, false
    }
    
    // Type assertion and conversion logic
    // Implementation depends on how metadata is stored
    return convertToArchiveMetadata(archiveData), true
}
```

#### 1.2 Create Metadata Utils

**File**: `utils/metadata.go` (new file)

```go
package utils

import (
    "fmt"
    "time"
)

type ArchiveSourceMetadata struct {
    SourceType    string    `json:"source_type"`
    BucketName    string    `json:"bucket_name"`
    FilePath      string    `json:"file_path"`
    FileName      string    `json:"file_name"`
    StartLedger   uint32    `json:"start_ledger"`
    EndLedger     uint32    `json:"end_ledger"`
    ProcessedAt   time.Time `json:"processed_at"`
    FileSize      int64     `json:"file_size,omitempty"`
    Partition     uint32    `json:"partition,omitempty"`
}

// CalculateArchiveFilePath determines the archive file path for a given ledger
func CalculateArchiveFilePath(ledgerSeq uint32, ledgersPerFile, filesPerPartition uint32) (string, uint32, uint32) {
    startLedger := (ledgerSeq / ledgersPerFile) * ledgersPerFile
    endLedger := startLedger + ledgersPerFile - 1
    partitionNum := startLedger / (ledgersPerFile * filesPerPartition)
    
    fileName := fmt.Sprintf("ledger-%d-%d.xdr", startLedger, endLedger)
    filePath := fmt.Sprintf("partition-%d/%s", partitionNum, fileName)
    
    return filePath, startLedger, endLedger
}

// CreateArchiveMetadata creates standardized archive metadata
func CreateArchiveMetadata(sourceType, bucketName string, ledgerSeq uint32, schema datastore.DataStoreSchema) *ArchiveSourceMetadata {
    filePath, startLedger, endLedger := CalculateArchiveFilePath(
        ledgerSeq, 
        uint32(schema.LedgersPerFile), 
        uint32(schema.FilesPerPartition))
    
    fileName := fmt.Sprintf("ledger-%d-%d.xdr", startLedger, endLedger)
    partitionNum := startLedger / (uint32(schema.LedgersPerFile) * uint32(schema.FilesPerPartition))
    
    return &ArchiveSourceMetadata{
        SourceType:   sourceType,
        BucketName:   bucketName,
        FilePath:     filePath,
        FileName:     fileName,
        StartLedger:  startLedger,
        EndLedger:    endLedger,
        ProcessedAt:  time.Now(),
        Partition:    partitionNum,
    }
}
```

### Phase 2: Source Adapter Enhancements

#### 2.1 Update S3BufferedStorageSourceAdapter

**File**: `source_adapter_s3.go`

```go
// Add metadata injection to the processing callback
func (adapter *S3BufferedStorageSourceAdapter) Run(ctx context.Context) error {
    // ... existing initialization ...
    
    // Enhanced callback that injects metadata
    callback := func(ledger xdr.LedgerCloseMeta) error {
        // Calculate archive metadata
        archiveMetadata := utils.CreateArchiveMetadata(
            "S3",
            adapter.config.BucketName,
            ledger.LedgerSequence(),
            adapter.schema)
        
        // Create enhanced message with metadata
        message := processor.Message{
            Payload: ledger,
            Metadata: map[string]interface{}{
                "archive_source": archiveMetadata,
            },
        }
        
        // Forward to all subscribed processors
        adapter.mu.Lock()
        defer adapter.mu.Unlock()
        
        for _, proc := range adapter.processors {
            if err := proc.Process(ctx, message); err != nil {
                return fmt.Errorf("error in processor %T: %w", proc, err)
            }
        }
        
        return nil
    }
    
    // Use enhanced callback with cdp.ApplyLedgerMetadata
    return cdp.ApplyLedgerMetadata(ctx, adapter.datastore, adapter.config, callback)
}
```

#### 2.2 Update Other Source Adapters

Apply similar changes to:
- `GCSBufferedStorageSourceAdapter`
- `FSBufferedStorageSourceAdapter` 
- `BufferedStorageSourceAdapter`

Each adapter follows the same pattern but with appropriate `SourceType` values.

### Phase 3: Processor Updates

#### 3.1 Update ContractDataProcessor

**File**: `processor/processor_contract_data.go`

```go
// Update ContractDataMessage to include source metadata
type ContractDataMessage struct {
    ContractData  contract.ContractDataOutput `json:"contract_data"`
    ContractId    string                      `json:"contract_id"`
    Timestamp     time.Time                   `json:"timestamp"`
    LedgerSeq     uint32                      `json:"ledger_sequence"`
    ProcessorName string                      `json:"processor_name"`
    MessageType   string                      `json:"message_type"`
    
    // New source provenance fields
    SourceFile    *utils.ArchiveSourceMetadata `json:"source_file,omitempty"`
}

// Update Process method to extract and preserve metadata
func (p *ContractDataProcessor) Process(ctx context.Context, msg Message) error {
    ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
    if !ok {
        return fmt.Errorf("expected LedgerCloseMeta, got %T", msg.Payload)
    }

    // Extract source metadata if available
    var sourceMetadata *utils.ArchiveSourceMetadata
    if archiveMeta, exists := msg.GetArchiveMetadata(); exists {
        sourceMetadata = archiveMeta
    }

    // ... existing processing logic ...

    // Include source metadata in output message
    contractMsg := ContractDataMessage{
        ContractData:  contractData,
        ContractId:    contractData.ContractId,
        Timestamp:     time.Now(),
        LedgerSeq:     uint32(ledgerHeader.LedgerSeq),
        ProcessorName: p.name,
        MessageType:   "contract_data",
        SourceFile:    sourceMetadata,  // Add source provenance
    }

    // ... rest of processing ...
}
```

#### 3.2 Update Other Contract Processors

Apply similar metadata preservation to:
- `ContractEventProcessor`
- `ContractInvocationProcessor` 
- `ContractCreationProcessor`

### Phase 4: Consumer Integration

#### 4.1 Database Consumer Updates

**Example for PostgreSQL consumer** (`consumer/consumer_save_to_postgresql.go`):

```go
// Add source file columns to database schema
CREATE TABLE contract_data (
    -- existing columns --
    source_file_type VARCHAR(10),
    source_bucket_name VARCHAR(255),
    source_file_path VARCHAR(500),
    source_file_name VARCHAR(255),
    source_start_ledger INTEGER,
    source_end_ledger INTEGER,
    source_processed_at TIMESTAMP,
    source_partition INTEGER
);

// Update insert logic to include source metadata
func (consumer *SaveToPostgreSQL) insertContractData(data ContractDataMessage) error {
    query := `INSERT INTO contract_data (
        contract_id, ledger_sequence, ..., 
        source_file_type, source_bucket_name, source_file_path, 
        source_file_name, source_start_ledger, source_end_ledger,
        source_processed_at, source_partition
    ) VALUES ($1, $2, ..., $n)`
    
    var sourceType, bucketName, filePath, fileName sql.NullString
    var startLedger, endLedger, partition sql.NullInt32
    var processedAt sql.NullTime
    
    if data.SourceFile != nil {
        sourceType = sql.NullString{String: data.SourceFile.SourceType, Valid: true}
        bucketName = sql.NullString{String: data.SourceFile.BucketName, Valid: true}
        // ... populate other fields
    }
    
    _, err := consumer.db.Exec(query, 
        data.ContractId, data.LedgerSeq, ...,
        sourceType, bucketName, filePath, fileName,
        startLedger, endLedger, processedAt, partition)
    
    return err
}
```

### Phase 5: Configuration and Testing

#### 5.1 Configuration Updates

Update YAML configs to ensure metadata is preserved:

```yaml
processors:
  - type: "contract_data"
    config:
      name: "contract_data_with_source"
      network_passphrase: "Test SDF Network ; September 2015"
      preserve_source_metadata: true  # Optional flag

consumers:
  - type: "SaveToPostgreSQL"
    config:
      connection_string: "..."
      table_name: "contract_data"
      include_source_metadata: true  # Optional flag
```

#### 5.2 Testing Strategy

1. **Unit Tests**:
   - Test metadata calculation functions
   - Test message structure changes
   - Test processor metadata preservation

2. **Integration Tests**:
   - Test full pipeline with source metadata
   - Verify database storage of source information
   - Test backward compatibility with non-metadata messages

3. **Performance Tests**:
   - Measure impact of metadata processing
   - Test with large archive files
   - Verify no significant performance regression

## Benefits and Use Cases

### 1. Data Lineage Tracking
```sql
-- Find all contract data from a specific archive file
SELECT * FROM contract_data 
WHERE source_file_path = 'partition-123/ledger-456000-456063.xdr';

-- Track contract evolution across multiple files
SELECT source_file_name, COUNT(*) 
FROM contract_data 
WHERE contract_id = 'CXYZ...' 
GROUP BY source_file_name 
ORDER BY source_start_ledger;
```

### 2. Debugging and Data Quality
- Trace data issues back to specific archive files
- Identify processing gaps or file-specific problems
- Validate data consistency across file boundaries

### 3. Operational Monitoring
- Monitor processing progress by file/partition
- Identify slow or problematic archive files
- Support incremental processing strategies

## Backward Compatibility

### Message Structure
- New `Metadata` field is optional and nullable
- Existing processors continue to work without modification
- Gradual rollout possible processor by processor

### Database Schema
- Source metadata columns can be nullable
- Existing data remains unaffected
- Can be applied incrementally

## Implementation Timeline

- **Phase 1**: Core infrastructure (1 week)
- **Phase 2**: Source adapter updates (1 week) 
- **Phase 3**: Processor updates (1 week)
- **Phase 4**: Consumer integration (1 week)
- **Phase 5**: Testing and validation (1 week)

**Total: ~5 weeks for complete implementation**

## Considerations

### Performance Impact
- Minimal: Metadata calculation is O(1) arithmetic
- Small storage overhead: ~100-200 bytes per message
- Network impact: JSON serialization of additional fields

### Storage Requirements  
- Additional database columns for source tracking
- Consider archiving old source metadata if storage is constrained

### Monitoring
- Add metrics for metadata preservation success rates
- Monitor processor performance with enhanced messages
- Track source file processing completeness

This implementation provides complete source file provenance while maintaining backward compatibility and minimal performance impact.