package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// SaveToParquetConfig defines configuration for Parquet archival consumer
type SaveToParquetConfig struct {
	StorageType              string `json:"storage_type"`              // "FS", "GCS", "S3"
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
	Debug                   bool   `json:"debug"`                       // Enable debug logging
	DryRun                  bool   `json:"dry_run"`                     // Log actions without writing files
}

// StorageClient interface for different storage backends
type StorageClient interface {
	Write(ctx context.Context, key string, data []byte) error
	Close() error
}

// SaveToParquet is the Parquet archival consumer
type SaveToParquet struct {
	config          SaveToParquetConfig
	storageClient   StorageClient
	processors      []processor.Processor
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
	ledgerCloseTime time.Time
	ctx             context.Context
	cancel          context.CancelFunc
}

// NewSaveToParquet creates a new Parquet archival consumer
func NewSaveToParquet(config map[string]interface{}) (*SaveToParquet, error) {
	log.Printf("SaveToParquet: Initializing with config: %+v", config)
	var cfg SaveToParquetConfig
	
	// Extract configuration values with type assertions
	if storageType, ok := config["storage_type"].(string); ok {
		cfg.StorageType = storageType
	} else {
		return nil, fmt.Errorf("storage_type is required")
	}
	
	// Optional configurations with defaults
	if bucketName, ok := config["bucket_name"].(string); ok {
		cfg.BucketName = bucketName
	}
	
	if pathPrefix, ok := config["path_prefix"].(string); ok {
		cfg.PathPrefix = pathPrefix
	}
	
	if localPath, ok := config["local_path"].(string); ok {
		// Expand tilde to home directory
		if strings.HasPrefix(localPath, "~") {
			home, err := os.UserHomeDir()
			if err != nil {
				return nil, fmt.Errorf("failed to get home directory: %w", err)
			}
			localPath = strings.Replace(localPath, "~", home, 1)
		}
		cfg.LocalPath = localPath
		log.Printf("SaveToParquet: Using local path: %s", cfg.LocalPath)
	}
	
	if compression, ok := config["compression"].(string); ok {
		cfg.Compression = compression
	} else {
		cfg.Compression = "snappy"
	}
	
	if maxFileSizeMB, ok := config["max_file_size_mb"].(float64); ok {
		cfg.MaxFileSizeMB = int(maxFileSizeMB)
	} else {
		cfg.MaxFileSizeMB = 128
	}
	
	if bufferSize, ok := config["buffer_size"].(float64); ok {
		cfg.BufferSize = int(bufferSize)
		log.Printf("SaveToParquet: Buffer size set to %d", cfg.BufferSize)
	} else if bufferSize, ok := config["buffer_size"].(int); ok {
		cfg.BufferSize = bufferSize
		log.Printf("SaveToParquet: Buffer size set to %d", cfg.BufferSize)
	} else {
		cfg.BufferSize = 10000
		log.Printf("SaveToParquet: Using default buffer size: %d", cfg.BufferSize)
	}
	
	if rotationIntervalMinutes, ok := config["rotation_interval_minutes"].(float64); ok {
		cfg.RotationIntervalMinutes = int(rotationIntervalMinutes)
	} else {
		cfg.RotationIntervalMinutes = 60
	}
	
	if partitionBy, ok := config["partition_by"].(string); ok {
		cfg.PartitionBy = partitionBy
	} else {
		cfg.PartitionBy = "ledger_day"
	}
	
	if ledgersPerFile, ok := config["ledgers_per_file"].(float64); ok {
		cfg.LedgersPerFile = uint32(ledgersPerFile)
	} else {
		cfg.LedgersPerFile = 10000
	}
	
	if schemaEvolution, ok := config["schema_evolution"].(bool); ok {
		cfg.SchemaEvolution = schemaEvolution
	}
	
	if includeMetadata, ok := config["include_metadata"].(bool); ok {
		cfg.IncludeMetadata = includeMetadata
	}
	
	if region, ok := config["region"].(string); ok {
		cfg.Region = region
	}
	
	if debug, ok := config["debug"].(bool); ok {
		cfg.Debug = debug
	}
	
	if dryRun, ok := config["dry_run"].(bool); ok {
		cfg.DryRun = dryRun
	}
	
	// Validate required fields based on storage type
	switch cfg.StorageType {
	case "FS":
		if cfg.LocalPath == "" {
			return nil, fmt.Errorf("local_path is required for FS storage type")
		}
	case "GCS":
		if cfg.BucketName == "" {
			return nil, fmt.Errorf("bucket_name is required for GCS storage type")
		}
	case "S3":
		if cfg.BucketName == "" {
			return nil, fmt.Errorf("bucket_name is required for S3 storage type")
		}
		if cfg.Region == "" {
			cfg.Region = "us-east-1"
		}
	default:
		return nil, fmt.Errorf("unsupported storage_type: %s", cfg.StorageType)
	}
	
	// Initialize storage client
	storageClient, err := createStorageClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}
	
	// Wrap with retry logic
	storageClient = NewRetryableStorageClient(storageClient, 3)
	
	ctx, cancel := context.WithCancel(context.Background())
	
	consumer := &SaveToParquet{
		config:         cfg,
		storageClient:  storageClient,
		processors:     []processor.Processor{},
		allocator:      memory.NewGoAllocator(),
		buffer:         make([]processor.Message, 0, cfg.BufferSize),
		lastFlush:      time.Now(),
		schemaRegistry: NewParquetSchemaRegistry(),
		ctx:           ctx,
		cancel:        cancel,
	}
	
	log.Printf("SaveToParquet: Buffer initialized with capacity %d", cfg.BufferSize)
	
	// Start periodic flush goroutine
	go consumer.periodicFlush()
	
	log.Printf("SaveToParquet: Consumer initialized successfully with config: %+v", cfg)
	
	return consumer, nil
}

// Subscribe adds a processor to the subscription list
func (s *SaveToParquet) Subscribe(p processor.Processor) {
	s.processors = append(s.processors, p)
}

// Process handles incoming messages
func (s *SaveToParquet) Process(ctx context.Context, msg processor.Message) error {
	log.Printf("SaveToParquet: Processing message, type: %T", msg.Payload)
	
	s.bufferMu.Lock()
	defer s.bufferMu.Unlock()
	
	// Handle different payload types
	var processedMsg processor.Message
	switch payload := msg.Payload.(type) {
	case []byte:
		// Try to unmarshal JSON bytes into a map
		var data map[string]interface{}
		if err := json.Unmarshal(payload, &data); err != nil {
			log.Printf("SaveToParquet: Failed to unmarshal JSON payload: %v", err)
			return fmt.Errorf("failed to unmarshal JSON payload: %w", err)
		}
		processedMsg = processor.Message{
			Payload:  data,
			Metadata: msg.Metadata,
		}
		
		// Extract ledger info from the data if available
		if ledgerSeq, ok := data["ledger_sequence"].(float64); ok {
			s.currentLedger = uint32(ledgerSeq)
			if s.startLedger == 0 {
				s.startLedger = s.currentLedger
			}
			log.Printf("SaveToParquet: Extracted ledger sequence from ledger_sequence: %d", s.currentLedger)
		}
		
		// Extract closed_at from nested contract_data
		// Only update ledgerCloseTime if it's not already set (keep first in buffer)
		if s.ledgerCloseTime.IsZero() {
			if contractData, ok := data["contract_data"].(map[string]interface{}); ok {
				if closedAtStr, ok := contractData["closed_at"].(string); ok {
					if parsedTime, err := time.Parse(time.RFC3339, closedAtStr); err == nil {
						s.ledgerCloseTime = parsedTime
						if s.config.Debug {
							log.Printf("SaveToParquet: Extracted closed_at: %s", s.ledgerCloseTime.Format(time.RFC3339))
						}
					}
				}
			}
		}
		
		if s.config.Debug {
			log.Printf("SaveToParquet: Processed JSON message with %d fields", len(data))
		}
		
	case map[string]interface{}:
		// Already in the expected format
		processedMsg = msg
		
		// Extract ledger info from payload
		if ledgerSeq, ok := payload["LedgerSeq"].(float64); ok {
			s.currentLedger = uint32(ledgerSeq)
			if s.startLedger == 0 {
				s.startLedger = s.currentLedger
			}
		} else if ledgerSeq, ok := payload["ledger_sequence"].(float64); ok {
			s.currentLedger = uint32(ledgerSeq)
			if s.startLedger == 0 {
				s.startLedger = s.currentLedger
			}
		}
		
	default:
		log.Printf("SaveToParquet: Unsupported payload type: %T", payload)
		return fmt.Errorf("unsupported payload type: %T", payload)
	}
	
	// Extract ledger info from metadata if available and not already set
	if s.currentLedger == 0 && msg.Metadata != nil {
		if ledgerData, ok := msg.Metadata["ledger"].(map[string]interface{}); ok {
			if ledgerNum, ok := ledgerData["sequence"].(float64); ok {
				s.currentLedger = uint32(ledgerNum)
				if s.startLedger == 0 {
					s.startLedger = s.currentLedger
				}
			}
		}
	}
	
	// Add message to buffer
	s.buffer = append(s.buffer, processedMsg)
	
	if s.config.Debug {
		log.Printf("Buffered message. Buffer size: %d/%d, Current ledger: %d",
			len(s.buffer), s.config.BufferSize, s.currentLedger)
	}
	
	// Check if we should flush
	shouldFlush := false
	
	// Check buffer size
	if len(s.buffer) >= s.config.BufferSize {
		shouldFlush = true
		if s.config.Debug {
			log.Printf("Buffer full, triggering flush")
		}
	}
	
	// Check ledger range for ledger-based partitioning
	if s.config.PartitionBy == "ledger_range" && s.startLedger > 0 {
		ledgerRange := s.currentLedger - s.startLedger
		if ledgerRange >= s.config.LedgersPerFile {
			shouldFlush = true
			if s.config.Debug {
				log.Printf("Ledger range reached %d, triggering flush", ledgerRange)
			}
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
	
	log.Printf("Flushing %d records to Parquet", len(s.buffer))
	
	if s.config.DryRun {
		filePath := s.generateFilePath()
		log.Printf("[DRY RUN] Would write %d records to %s", len(s.buffer), filePath)
		// Clear buffer and reset state
		s.buffer = s.buffer[:0]
		s.startLedger = s.currentLedger
		s.ledgerCloseTime = time.Time{}
		s.lastFlush = time.Now()
		return nil
	}
	
	// Determine schema from first batch if needed
	if s.currentSchema == nil {
		schema, err := s.schemaRegistry.InferSchema(s.buffer)
		if err != nil {
			return fmt.Errorf("failed to infer schema: %w", err)
		}
		s.currentSchema = schema
		if s.config.Debug {
			log.Printf("Inferred schema with %d fields", len(schema.Fields()))
		}
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
	
	log.Printf("Written Parquet file: %s (%d records, %d bytes)", filePath, len(s.buffer), len(parquetData))
	
	// Clear buffer and reset state
	s.buffer = s.buffer[:0]
	s.startLedger = s.currentLedger
	s.ledgerCloseTime = time.Time{} // Reset for next batch
	s.lastFlush = time.Now()
	s.currentFileSize = 0
	
	return nil
}

// generateFilePath generates the output file path based on partitioning strategy
func (s *SaveToParquet) generateFilePath() string {
	now := time.Now().UTC()
	var partPath string
	
	switch s.config.PartitionBy {
	case "ledger_day":
		// Partition by the day of the ledger close time
		// Use actual ledger close time if available, otherwise use current time
		partitionTime := s.ledgerCloseTime
		if partitionTime.IsZero() {
			partitionTime = now
			if s.config.Debug {
				log.Printf("SaveToParquet: No ledger close time available, using current time for partitioning")
			}
		}
		partPath = fmt.Sprintf("year=%d/month=%02d/day=%02d",
			partitionTime.Year(), partitionTime.Month(), partitionTime.Day())
			
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
	var pathComponents []string
	if s.config.PathPrefix != "" {
		pathComponents = append(pathComponents, s.config.PathPrefix)
	}
	pathComponents = append(pathComponents, partPath, filename)
	
	// Join with "/" and clean up
	fullPath := strings.Join(pathComponents, "/")
	
	// Clean up double slashes and remove leading slash
	for strings.Contains(fullPath, "//") {
		fullPath = strings.ReplaceAll(fullPath, "//", "/")
	}
	fullPath = strings.TrimPrefix(fullPath, "/")
	
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
				if s.config.Debug {
					log.Printf("Periodic flush triggered after %v", timeSinceFlush)
				}
				if err := s.flush(); err != nil {
					log.Printf("Error in periodic flush: %v", err)
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
	
	log.Printf("Parquet archival consumer closed. Files written: %d, Records: %d, Bytes: %d",
		s.filesWritten, s.recordsWritten, s.bytesWritten)
	
	return nil
}

// convertToArrowBatch converts messages to Arrow record batch
func (s *SaveToParquet) convertToArrowBatch(messages []processor.Message) (arrow.Record, error) {
	return s.schemaRegistry.BuildRecordBatch(s.currentSchema, messages)
}

// writeParquet writes Arrow record to Parquet format
func (s *SaveToParquet) writeParquet(record arrow.Record) ([]byte, error) {
	var buf bytes.Buffer
	
	// Configure Parquet writer properties
	props := parquet.NewWriterProperties(
		parquet.WithCompression(s.getCompressionType()),
		parquet.WithDataPageSize(1024*1024), // 1MB data pages
	)
	
	// Create Parquet writer
	writer, err := pqarrow.NewFileWriter(record.Schema(), &buf, props, pqarrow.NewArrowWriterProperties())
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet writer: %w", err)
	}
	defer writer.Close()
	
	// Write record
	if err := writer.Write(record); err != nil {
		return nil, fmt.Errorf("failed to write record: %w", err)
	}
	
	// Close writer to finalize
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
	case "brotli":
		return compress.Codecs.Brotli
	case "none":
		return compress.Codecs.Uncompressed
	default:
		return compress.Codecs.Snappy
	}
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

