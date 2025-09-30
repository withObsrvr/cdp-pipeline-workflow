package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stellar/go/support/log"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// SaveToLedgerParquet is a consumer that saves ledger data to parquet files
// in AWS-compatible nested format
type SaveToLedgerParquet struct {
	OutputPath            string
	BufferSize            int
	MaxFileSize           int64 // in bytes
	CompressionType       string
	RotationInterval      time.Duration
	
	schema                *LedgerParquetSchema
	recordBuilder         *LedgerRecordBuilder
	currentRecords        []arrow.Record
	currentFileSize       int64
	lastRotation          time.Time
	fileCounter           int
	mutex                 sync.Mutex
	logger                *log.Entry
}

// NewSaveToLedgerParquet creates a new SaveToLedgerParquet consumer
func NewSaveToLedgerParquet(config map[string]interface{}) (processor.Processor, error) {
	consumer := &SaveToLedgerParquet{
		OutputPath:       "./ledger-parquet",
		BufferSize:       100, // Number of ledgers to buffer before writing
		MaxFileSize:      1 << 30, // 1GB default
		CompressionType:  "zstd",
		RotationInterval: 1 * time.Hour,
		lastRotation:     time.Now(),
		logger:           log.DefaultLogger.WithField("consumer", "SaveToLedgerParquet"),
	}

	// Parse configuration
	if outputPath, ok := config["output_path"].(string); ok {
		consumer.OutputPath = outputPath
	}

	if bufferSize, ok := config["buffer_size"].(float64); ok {
		consumer.BufferSize = int(bufferSize)
	}

	if maxFileSizeMB, ok := config["max_file_size_mb"].(float64); ok {
		consumer.MaxFileSize = int64(maxFileSizeMB * 1024 * 1024)
	}

	if compression, ok := config["compression"].(string); ok {
		consumer.CompressionType = compression
	}

	if rotationMinutes, ok := config["rotation_interval_minutes"].(float64); ok {
		consumer.RotationInterval = time.Duration(rotationMinutes) * time.Minute
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(consumer.OutputPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	// Initialize schema and builder
	consumer.schema = NewLedgerParquetSchema()
	consumer.recordBuilder = NewLedgerRecordBuilder(consumer.schema)
	consumer.currentRecords = make([]arrow.Record, 0, consumer.BufferSize)

	consumer.logger.Info("SaveToLedgerParquet consumer initialized",
		"output_path", consumer.OutputPath,
		"buffer_size", consumer.BufferSize,
		"max_file_size", consumer.MaxFileSize,
		"compression", consumer.CompressionType,
	)

	return consumer, nil
}

// Subscribe is not used for consumers
func (c *SaveToLedgerParquet) Subscribe(processor.Processor) {}

// Process handles incoming ledger JSON data
func (c *SaveToLedgerParquet) Process(ctx context.Context, msg processor.Message) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Parse JSON payload
	var ledgerData map[string]interface{}
	if err := json.Unmarshal(msg.Payload.([]byte), &ledgerData); err != nil {
		return fmt.Errorf("failed to unmarshal ledger JSON: %w", err)
	}

	// Build the ledger record
	if err := c.buildLedgerRecord(ledgerData); err != nil {
		return fmt.Errorf("failed to build ledger record: %w", err)
	}

	// Check if we should flush
	if len(c.currentRecords) >= c.BufferSize || c.shouldRotate() {
		if err := c.flush(); err != nil {
			return fmt.Errorf("failed to flush records: %w", err)
		}
	}

	return nil
}

// buildLedgerRecord builds an Arrow record from ledger JSON data
func (c *SaveToLedgerParquet) buildLedgerRecord(ledgerData map[string]interface{}) error {
	c.logger.Debug("Building ledger record", "sequence", ledgerData["sequence"])

	// Top-level fields
	// Sequence (field 0)
	if seq, ok := getLedgerInt64Value(ledgerData, "sequence"); ok {
		c.recordBuilder.SequenceBuilder().Append(seq)
	} else {
		c.recordBuilder.SequenceBuilder().AppendNull()
	}

	// Ledger hash (field 1)
	if hash, ok := getLedgerStringValue(ledgerData, "ledger_hash"); ok {
		c.recordBuilder.LedgerHashBuilder().Append(hash)
	} else {
		c.recordBuilder.LedgerHashBuilder().AppendNull()
	}

	// Previous ledger hash (field 2)
	if prevHash, ok := getLedgerStringValue(ledgerData, "previous_ledger_hash"); ok {
		c.recordBuilder.PreviousLedgerHashBuilder().Append(prevHash)
	} else {
		c.recordBuilder.PreviousLedgerHashBuilder().AppendNull()
	}

	// Closed at (field 3) - convert to milliseconds timestamp
	if closedAt, ok := getLedgerInt64Value(ledgerData, "closed_at"); ok {
		// AWS format uses milliseconds, our data might be in seconds
		timestamp := arrow.Timestamp(closedAt)
		c.recordBuilder.ClosedAtBuilder().Append(timestamp)
	} else {
		c.recordBuilder.ClosedAtBuilder().AppendNull()
	}

	// Protocol version (field 4)
	if version, ok := getLedgerInt32Value(ledgerData, "protocol_version"); ok {
		c.recordBuilder.ProtocolVersionBuilder().Append(version)
	} else {
		c.recordBuilder.ProtocolVersionBuilder().AppendNull()
	}

	// Total coins (field 5)
	if coins, ok := getLedgerInt64Value(ledgerData, "total_coins"); ok {
		c.recordBuilder.TotalCoinsBuilder().Append(coins)
	} else {
		c.recordBuilder.TotalCoinsBuilder().AppendNull()
	}

	// Fee pool (field 6)
	if pool, ok := getLedgerInt64Value(ledgerData, "fee_pool"); ok {
		c.recordBuilder.FeePoolBuilder().Append(pool)
	} else {
		c.recordBuilder.FeePoolBuilder().AppendNull()
	}

	// Base fee (field 7)
	if fee, ok := getLedgerInt32Value(ledgerData, "base_fee"); ok {
		c.recordBuilder.BaseFeeBuilder().Append(fee)
	} else {
		c.recordBuilder.BaseFeeBuilder().AppendNull()
	}

	// Base reserve (field 8)
	if reserve, ok := getLedgerInt32Value(ledgerData, "base_reserve"); ok {
		c.recordBuilder.BaseReserveBuilder().Append(reserve)
	} else {
		c.recordBuilder.BaseReserveBuilder().AppendNull()
	}

	// Max tx set size (field 9)
	if size, ok := getLedgerInt32Value(ledgerData, "max_tx_set_size"); ok {
		c.recordBuilder.MaxTxSetSizeBuilder().Append(size)
	} else {
		c.recordBuilder.MaxTxSetSizeBuilder().AppendNull()
	}

	// Successful transaction count (field 10)
	if count, ok := getLedgerInt32Value(ledgerData, "successful_transaction_count"); ok {
		c.recordBuilder.SuccessfulTransactionCountBuilder().Append(count)
	} else {
		c.recordBuilder.SuccessfulTransactionCountBuilder().Append(0)
	}

	// Failed transaction count (field 11)
	if count, ok := getLedgerInt32Value(ledgerData, "failed_transaction_count"); ok {
		c.recordBuilder.FailedTransactionCountBuilder().Append(count)
	} else {
		c.recordBuilder.FailedTransactionCountBuilder().Append(0)
	}

	// Soroban fee write 1kb (field 12)
	if fee, ok := getLedgerInt64Value(ledgerData, "soroban_fee_write_1kb"); ok {
		c.recordBuilder.SorobanFeeWrite1KbBuilder().Append(fee)
	} else {
		c.recordBuilder.SorobanFeeWrite1KbBuilder().AppendNull()
	}

	// Node ID (field 13)
	if nodeId, ok := getLedgerStringValue(ledgerData, "node_id"); ok {
		c.recordBuilder.NodeIdBuilder().Append(nodeId)
	} else {
		c.recordBuilder.NodeIdBuilder().AppendNull()
	}

	// Signature (field 14)
	if sig, ok := getLedgerStringValue(ledgerData, "signature"); ok {
		c.recordBuilder.SignatureBuilder().Append(sig)
	} else {
		c.recordBuilder.SignatureBuilder().AppendNull()
	}

	// Transactions (field 15)
	transactionsBuilder := c.recordBuilder.TransactionsBuilder()
	if transactions, ok := ledgerData["transactions"].([]interface{}); ok {
		transactionsBuilder.Append(true)
		txBuilder := NewTransactionBuilder(c.schema.MemoryPool, transactionsBuilder)
		for _, tx := range transactions {
			if txData, ok := tx.(map[string]interface{}); ok {
				if err := txBuilder.AppendTransaction(txData); err != nil {
					return fmt.Errorf("failed to append transaction: %w", err)
				}
			}
		}
	} else {
		transactionsBuilder.AppendNull()
	}

	// Create a record once we have accumulated enough
	record := c.recordBuilder.NewRecord()
	c.currentRecords = append(c.currentRecords, record)

	return nil
}

// shouldRotate checks if we should rotate the output file
func (c *SaveToLedgerParquet) shouldRotate() bool {
	return time.Since(c.lastRotation) > c.RotationInterval
}

// flush writes buffered records to a parquet file
func (c *SaveToLedgerParquet) flush() error {
	if len(c.currentRecords) == 0 {
		return nil
	}

	// Generate filename
	filename := c.generateFilename()
	filepath := filepath.Join(c.OutputPath, filename)

	c.logger.Info("Flushing records to parquet file",
		"filename", filename,
		"record_count", len(c.currentRecords),
	)

	// Write parquet file
	if err := c.writeParquetFile(filepath); err != nil {
		return fmt.Errorf("failed to write parquet file: %w", err)
	}

	// Reset state
	c.currentRecords = c.currentRecords[:0]
	c.currentFileSize = 0
	c.lastRotation = time.Now()
	c.fileCounter++

	return nil
}

// generateFilename generates a unique filename for the parquet file
func (c *SaveToLedgerParquet) generateFilename() string {
	timestamp := time.Now().Format("20060102_150405")
	return fmt.Sprintf("ledger_%s_%d.parquet", timestamp, c.fileCounter)
}

// writeParquetFile writes the Arrow records to a parquet file
func (c *SaveToLedgerParquet) writeParquetFile(filepath string) error {
	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Create parquet writer properties
	props := parquet.NewWriterProperties(
		parquet.WithCompression(c.getCompressionType()),
		parquet.WithDictionaryDefault(false),
	)

	// Create Arrow writer properties
	arrowProps := pqarrow.DefaultWriterProps()

	// Write records to parquet file
	writer, err := pqarrow.NewFileWriter(c.schema.Schema, file, props, arrowProps)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	defer writer.Close()

	// Write all records
	for _, record := range c.currentRecords {
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	return nil
}

// getCompressionType returns the Parquet compression type based on configuration
func (c *SaveToLedgerParquet) getCompressionType() compress.Compression {
	switch c.CompressionType {
	case "zstd":
		return compress.Codecs.Zstd
	case "snappy":
		return compress.Codecs.Snappy
	case "gzip":
		return compress.Codecs.Gzip
	case "lz4":
		return compress.Codecs.Lz4
	default:
		return compress.Codecs.Zstd
	}
}

// Close flushes any remaining records and cleans up resources
func (c *SaveToLedgerParquet) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Flush any remaining records
	if err := c.flush(); err != nil {
		return fmt.Errorf("failed to flush on close: %w", err)
	}

	// Release resources
	if c.recordBuilder != nil {
		c.recordBuilder.Release()
	}

	c.logger.Info("SaveToLedgerParquet consumer closed")
	return nil
}

// Ensure we implement the io.Closer interface
var _ io.Closer = (*SaveToLedgerParquet)(nil)