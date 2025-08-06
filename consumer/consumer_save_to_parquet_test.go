package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/parquet/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	processor "github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

func TestNewSaveToParquet(t *testing.T) {
	tests := []struct {
		name    string
		config  map[string]interface{}
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid FS configuration",
			config: map[string]interface{}{
				"storage_type": "FS",
				"local_path":   "/tmp/parquet",
				"buffer_size":  5000.0,
			},
			wantErr: false,
		},
		{
			name: "valid GCS configuration",
			config: map[string]interface{}{
				"storage_type": "GCS",
				"bucket_name":  "test-bucket",
				"path_prefix":  "stellar-data",
			},
			wantErr: false,
		},
		{
			name: "valid S3 configuration",
			config: map[string]interface{}{
				"storage_type": "S3",
				"bucket_name":  "test-bucket",
				"region":       "us-west-2",
			},
			wantErr: false,
		},
		{
			name: "missing storage_type",
			config: map[string]interface{}{
				"bucket_name": "test-bucket",
			},
			wantErr: true,
			errMsg:  "storage_type is required",
		},
		{
			name: "invalid storage_type",
			config: map[string]interface{}{
				"storage_type": "INVALID",
			},
			wantErr: true,
			errMsg:  "unsupported storage_type: INVALID",
		},
		{
			name: "FS missing local_path",
			config: map[string]interface{}{
				"storage_type": "FS",
			},
			wantErr: true,
			errMsg:  "local_path is required for FS storage type",
		},
		{
			name: "GCS missing bucket_name",
			config: map[string]interface{}{
				"storage_type": "GCS",
			},
			wantErr: true,
			errMsg:  "bucket_name is required for GCS storage type",
		},
		{
			name: "S3 missing bucket_name",
			config: map[string]interface{}{
				"storage_type": "S3",
			},
			wantErr: true,
			errMsg:  "bucket_name is required for S3 storage type",
		},
		{
			name: "all configuration options",
			config: map[string]interface{}{
				"storage_type":               "FS",
				"local_path":                 "/tmp/parquet",
				"path_prefix":                "data",
				"compression":                "zstd",
				"max_file_size_mb":           256.0,
				"buffer_size":                25000.0,
				"rotation_interval_minutes":  30.0,
				"partition_by":               "ledger_range",
				"ledgers_per_file":           5000.0,
				"schema_evolution":           true,
				"include_metadata":           true,
				"debug":                      true,
				"dry_run":                    true,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			consumer, err := NewSaveToParquet(tt.config)
			
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewSaveToParquet() error = nil, wantErr %v", tt.wantErr)
				} else if tt.errMsg != "" && err.Error() != tt.errMsg {
					t.Errorf("NewSaveToParquet() error = %v, want %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("NewSaveToParquet() error = %v, wantErr %v", err, tt.wantErr)
				}
				if consumer != nil {
					// Verify cleanup
					consumer.Close()
				}
			}
		})
	}
}

func TestSaveToParquetConfigDefaults(t *testing.T) {
	config := map[string]interface{}{
		"storage_type": "FS",
		"local_path":   "/tmp/parquet",
	}
	
	consumer, err := NewSaveToParquet(config)
	if err != nil {
		t.Fatalf("NewSaveToParquet() error = %v", err)
	}
	defer consumer.Close()
	
	// Check default values
	if consumer.config.Compression != "snappy" {
		t.Errorf("Default compression = %v, want snappy", consumer.config.Compression)
	}
	if consumer.config.MaxFileSizeMB != 128 {
		t.Errorf("Default MaxFileSizeMB = %v, want 128", consumer.config.MaxFileSizeMB)
	}
	if consumer.config.BufferSize != 10000 {
		t.Errorf("Default BufferSize = %v, want 10000", consumer.config.BufferSize)
	}
	if consumer.config.RotationIntervalMinutes != 60 {
		t.Errorf("Default RotationIntervalMinutes = %v, want 60", consumer.config.RotationIntervalMinutes)
	}
	if consumer.config.PartitionBy != "ledger_day" {
		t.Errorf("Default PartitionBy = %v, want ledger_day", consumer.config.PartitionBy)
	}
	if consumer.config.LedgersPerFile != 10000 {
		t.Errorf("Default LedgersPerFile = %v, want 10000", consumer.config.LedgersPerFile)
	}
}

func TestGenerateFilePath(t *testing.T) {
	tests := []struct {
		name           string
		partitionBy    string
		pathPrefix     string
		startLedger    uint32
		currentLedger  uint32
		expectedPrefix string
	}{
		{
			name:           "ledger_day partitioning",
			partitionBy:    "ledger_day",
			pathPrefix:     "data",
			startLedger:    1000000,
			currentLedger:  1001000,
			expectedPrefix: "data/year=",
		},
		{
			name:           "ledger_range partitioning",
			partitionBy:    "ledger_range",
			pathPrefix:     "archive",
			startLedger:    15000,
			currentLedger:  25000,
			expectedPrefix: "archive/ledgers/10000-19999",
		},
		{
			name:           "hour partitioning",
			partitionBy:    "hour",
			pathPrefix:     "hourly",
			startLedger:    1000,
			currentLedger:  2000,
			expectedPrefix: "hourly/year=",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := map[string]interface{}{
				"storage_type":     "FS",
				"local_path":       "/tmp",
				"partition_by":     tt.partitionBy,
				"path_prefix":      tt.pathPrefix,
				"ledgers_per_file": 10000.0,
			}
			
			consumer, err := NewSaveToParquet(config)
			if err != nil {
				t.Fatalf("NewSaveToParquet() error = %v", err)
			}
			defer consumer.Close()
			
			consumer.startLedger = tt.startLedger
			consumer.currentLedger = tt.currentLedger
			
			path := consumer.generateFilePath()
			
			if !strings.HasPrefix(path, tt.expectedPrefix) {
				t.Errorf("generateFilePath() = %v, want prefix %v", path, tt.expectedPrefix)
			}
			
			// Check filename format
			if !strings.Contains(path, ".parquet") {
				t.Errorf("generateFilePath() = %v, missing .parquet extension", path)
			}
			
			// Check no double slashes
			if strings.Contains(path, "//") {
				t.Errorf("generateFilePath() = %v, contains double slashes", path)
			}
		})
	}
}

func TestParquetWriting(t *testing.T) {
	t.Run("write contract data", func(t *testing.T) {
		config := map[string]interface{}{
			"storage_type": "FS",
			"local_path":   t.TempDir(),
			"buffer_size":  2.0,
			"compression":  "snappy",
		}
		
		consumer, err := NewSaveToParquet(config)
		if err != nil {
			t.Fatalf("NewSaveToParquet() error = %v", err)
		}
		defer consumer.Close()
		
		ctx := context.Background()
		
		// Send contract data messages
		messages := []processor.Message{
			{
				Payload: map[string]interface{}{
					"contract_id":         "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
					"contract_key_type":   "ContractData",
					"contract_durability": "persistent",
					"asset_code":         "USDC",
					"asset_issuer":       "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
					"deleted":            false,
					"ledger_sequence":    uint32(123456),
					"last_modified_ledger": uint32(123456),
					"ledger_entry_change": uint32(1),
					"closed_at":          float64(1704067200), // Unix timestamp
				},
				Metadata: map[string]interface{}{
					"ledger": map[string]interface{}{
						"sequence": float64(123456),
					},
				},
			},
			{
				Payload: map[string]interface{}{
					"contract_id":         "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
					"contract_key_type":   "ContractData",
					"contract_durability": "temporary",
					"asset_code":         "XLM",
					"deleted":            false,
					"ledger_sequence":    uint32(123457),
					"last_modified_ledger": uint32(123457),
					"ledger_entry_change": uint32(2),
					"closed_at":          float64(1704067206),
				},
				Metadata: map[string]interface{}{
					"ledger": map[string]interface{}{
						"sequence": float64(123457),
					},
				},
			},
		}
		
		// Process messages
		for _, msg := range messages {
			if err := consumer.Process(ctx, msg); err != nil {
				t.Fatalf("Process() error = %v", err)
			}
		}
		
		// Check metrics
		metrics := consumer.GetMetrics()
		if metrics["files_written"].(int64) != 1 {
			t.Errorf("files_written = %v, want 1", metrics["files_written"])
		}
		if metrics["records_written"].(int64) != 2 {
			t.Errorf("records_written = %v, want 2", metrics["records_written"])
		}
	})
	
	t.Run("compression types", func(t *testing.T) {
		compressionTypes := []string{"snappy", "gzip", "zstd", "none"}
		
		for _, compression := range compressionTypes {
			t.Run(compression, func(t *testing.T) {
				config := map[string]interface{}{
					"storage_type": "FS",
					"local_path":   t.TempDir(),
					"buffer_size":  1.0,
					"compression":  compression,
				}
				
				consumer, err := NewSaveToParquet(config)
				if err != nil {
					t.Fatalf("NewSaveToParquet() error = %v", err)
				}
				defer consumer.Close()
				
				ctx := context.Background()
				
				// Send a test message
				msg := processor.Message{
					Payload: map[string]interface{}{
						"test_field": "test_value",
						"number":     float64(42),
					},
				}
				
				if err := consumer.Process(ctx, msg); err != nil {
					t.Fatalf("Process() error = %v", err)
				}
				
				// Verify file was written
				metrics := consumer.GetMetrics()
				if metrics["files_written"].(int64) != 1 {
					t.Errorf("files_written = %v, want 1", metrics["files_written"])
				}
			})
		}
	})
	
	t.Run("schema evolution", func(t *testing.T) {
		config := map[string]interface{}{
			"storage_type":     "FS",
			"local_path":       t.TempDir(),
			"buffer_size":      10.0,
			"schema_evolution": true,
		}
		
		consumer, err := NewSaveToParquet(config)
		if err != nil {
			t.Fatalf("NewSaveToParquet() error = %v", err)
		}
		defer consumer.Close()
		
		ctx := context.Background()
		
		// Send messages with evolving schema
		messages := []processor.Message{
			{
				Payload: map[string]interface{}{
					"field1": "value1",
					"field2": float64(100),
				},
			},
			{
				Payload: map[string]interface{}{
					"field1": "value2",
					"field2": float64(200),
					"field3": "new_field", // New field
				},
			},
		}
		
		for _, msg := range messages {
			if err := consumer.Process(ctx, msg); err != nil {
				t.Fatalf("Process() error = %v", err)
			}
		}
	})
}

// TestSaveToParquet_JSONMessages tests processing actual JSON messages (critical path)
func TestSaveToParquet_JSONMessages(t *testing.T) {
	tests := []struct {
		name          string
		config        map[string]interface{}
		jsonMessages  []string
		expectedCount int
		validateFunc  func(t *testing.T, dir string)
	}{
		{
			name: "processes JSON contract data messages",
			config: map[string]interface{}{
				"storage_type": "FS",
				"local_path":   t.TempDir(),
				"buffer_size":  2.0,
			},
			jsonMessages: []string{
				`{"contract_id": "CONTRACT1", "key": "balance", "value": "1000", "closed_at": 1704067200}`,
				`{"contract_id": "CONTRACT2", "key": "owner", "value": "GABC123", "closed_at": 1704067210}`,
			},
			expectedCount: 2,
			validateFunc: func(t *testing.T, dir string) {
				// Verify Parquet file was created
				files := findParquetFiles(t, dir)
				require.Len(t, files, 1, "expected exactly one parquet file")
				
				// Read and validate content
				reader, err := file.OpenParquetFile(files[0], true)
				require.NoError(t, err)
				defer reader.Close()
				
				assert.Equal(t, int64(2), reader.NumRows())
			},
		},
		{
			name: "handles malformed JSON gracefully",
			config: map[string]interface{}{
				"storage_type": "FS",
				"local_path":   t.TempDir(),
				"buffer_size":  10.0,
			},
			jsonMessages: []string{
				`{"valid": "json", "closed_at": 1704067200}`,
				`{invalid json}`,
				`{"another": "valid", "closed_at": 1704067210}`,
			},
			expectedCount: 2, // Only valid messages
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			consumer, err := NewSaveToParquet(tt.config)
			require.NoError(t, err)
			defer consumer.Close()

			ctx := context.Background()
			successCount := 0

			// Process JSON messages
			for _, jsonMsg := range tt.jsonMessages {
				msg := processor.Message{
					Payload: []byte(jsonMsg),
				}
				err := consumer.Receive(ctx, msg)
				if err == nil {
					successCount++
				}
			}

			// Force flush
			consumer.Close()

			assert.Equal(t, tt.expectedCount, successCount)

			if tt.validateFunc != nil {
				tt.validateFunc(t, tt.config["local_path"].(string))
			}
		})
	}
}

// TestSaveToParquet_ConcurrentAccess tests thread safety (critical for production)
func TestSaveToParquet_ConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	config := map[string]interface{}{
		"storage_type": "FS",
		"local_path":   dir,
		"buffer_size":  100.0,
		"compression":  "snappy",
	}

	consumer, err := NewSaveToParquet(config)
	require.NoError(t, err)
	defer consumer.Close()

	ctx := context.Background()
	numGoroutines := 10
	messagesPerGoroutine := 100

	var wg sync.WaitGroup
	errors := make([]error, numGoroutines)

	// Launch concurrent writers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < messagesPerGoroutine; j++ {
				msg := map[string]interface{}{
					"worker_id":   workerID,
					"message_id":  j,
					"timestamp":   time.Now().Unix(),
					"closed_at":   time.Now().Unix(),
				}
				jsonBytes, _ := json.Marshal(msg)
				
				err := consumer.Receive(ctx, processor.Message{
					Payload: jsonBytes,
				})
				if err != nil {
					errors[workerID] = err
					return
				}
			}
		}(i)
	}

	wg.Wait()

	// Check for errors
	for i, err := range errors {
		assert.NoError(t, err, "worker %d encountered error", i)
	}

	// Verify all messages were processed
	consumer.Close()
	
	metrics := consumer.GetMetrics()
	expectedRecords := int64(numGoroutines * messagesPerGoroutine)
	assert.Equal(t, expectedRecords, metrics["records_written"].(int64))
}

// TestSaveToParquet_ErrorRecovery tests error handling and recovery
func TestSaveToParquet_ErrorRecovery(t *testing.T) {
	t.Run("recovers from temporary write failures", func(t *testing.T) {
		// This test would require mocking the storage layer
		// For now, we test JSON parsing errors
		config := map[string]interface{}{
			"storage_type": "FS",
			"local_path":   t.TempDir(),
			"buffer_size":  5.0,
		}

		consumer, err := NewSaveToParquet(config)
		require.NoError(t, err)
		defer consumer.Close()

		ctx := context.Background()
		
		// Mix of valid and invalid messages
		messages := []processor.Message{
			{Payload: []byte(`{"id": 1, "closed_at": 1704067200}`)},
			{Payload: []byte(`{invalid}`)},
			{Payload: []byte(`{"id": 2, "closed_at": 1704067210}`)},
			{Payload: []byte(`{"id": 3, "closed_at": 1704067220}`)},
		}

		validCount := 0
		for _, msg := range messages {
			if err := consumer.Receive(ctx, msg); err == nil {
				validCount++
			}
		}

		assert.Equal(t, 3, validCount, "should process 3 valid messages")
	})
}

// TestSaveToParquet_PartitioningLogic tests date-based partitioning
func TestSaveToParquet_PartitioningLogic(t *testing.T) {
	dir := t.TempDir()
	config := map[string]interface{}{
		"storage_type": "FS",
		"local_path":   dir,
		"buffer_size":  1.0, // Write immediately
		"partition_by": "ledger_day",
	}

	consumer, err := NewSaveToParquet(config)
	require.NoError(t, err)
	defer consumer.Close()

	ctx := context.Background()

	// Messages from different days
	messages := []struct {
		timestamp int64
		date      string
	}{
		{timestamp: 1704067200, date: "2024/01/01"}, // Jan 1, 2024
		{timestamp: 1704153600, date: "2024/01/02"}, // Jan 2, 2024
		{timestamp: 1704240000, date: "2024/01/03"}, // Jan 3, 2024
	}

	for _, msg := range messages {
		payload := map[string]interface{}{
			"id":        fmt.Sprintf("msg_%d", msg.timestamp),
			"closed_at": float64(msg.timestamp),
		}
		jsonBytes, _ := json.Marshal(payload)
		
		err := consumer.Receive(ctx, processor.Message{
			Payload: jsonBytes,
		})
		require.NoError(t, err)
	}

	consumer.Close()

	// Verify partition directories were created
	var partitionDirs []string
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() && strings.Contains(path, "year=") {
			partitionDirs = append(partitionDirs, path)
		}
		return nil
	})

	assert.GreaterOrEqual(t, len(partitionDirs), 1, "should create date-based partitions")
}

// BenchmarkSaveToParquet_Throughput benchmarks message processing speed
func BenchmarkSaveToParquet_Throughput(b *testing.B) {
	dir := b.TempDir()
	config := map[string]interface{}{
		"storage_type": "FS",
		"local_path":   dir,
		"buffer_size":  1000.0,
		"compression":  "snappy",
	}

	consumer, err := NewSaveToParquet(config)
	require.NoError(b, err)
	defer consumer.Close()

	ctx := context.Background()

	// Prepare test message
	testData := map[string]interface{}{
		"contract_id": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
		"key":         "balance",
		"value":       "1000000",
		"closed_at":   float64(time.Now().Unix()),
		"metadata": map[string]string{
			"field1": "value1",
			"field2": "value2",
			"field3": "value3",
		},
	}
	jsonBytes, _ := json.Marshal(testData)
	msg := processor.Message{Payload: jsonBytes}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := consumer.Receive(ctx, msg)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()

	// Report metrics
	b.ReportMetric(float64(b.N), "messages")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "messages/sec")
	b.ReportMetric(float64(len(jsonBytes)*b.N)/(1024*1024), "MB_processed")
}

// TestSaveToParquet_LargeFile tests handling of large files
func TestSaveToParquet_LargeFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large file test in short mode")
	}

	dir := t.TempDir()
	config := map[string]interface{}{
		"storage_type":     "FS",
		"local_path":       dir,
		"buffer_size":      10000.0,
		"max_file_size_mb": 10.0, // Small limit to trigger rotation
		"compression":      "snappy",
	}

	consumer, err := NewSaveToParquet(config)
	require.NoError(t, err)
	defer consumer.Close()

	ctx := context.Background()

	// Generate many messages
	const numMessages = 50000
	for i := 0; i < numMessages; i++ {
		msg := map[string]interface{}{
			"id":          i,
			"data":        fmt.Sprintf("This is message number %d with some padding text", i),
			"timestamp":   time.Now().Unix(),
			"closed_at":   float64(time.Now().Unix()),
		}
		jsonBytes, _ := json.Marshal(msg)
		
		err := consumer.Receive(ctx, processor.Message{
			Payload: jsonBytes,
		})
		require.NoError(t, err)
		
		if i%10000 == 0 {
			t.Logf("Processed %d messages", i)
		}
	}

	consumer.Close()

	// Should have multiple files due to size limit
	files := findParquetFiles(t, dir)
	assert.Greater(t, len(files), 1, "should create multiple files due to size limit")

	// Verify total row count
	totalRows := countTotalRows(t, dir)
	assert.Equal(t, int64(numMessages), totalRows)
}

// TestSaveToParquet_BufferManagement tests buffer behavior in detail
func TestSaveToParquet_BufferManagement(t *testing.T) {
	t.Run("flushes at exact buffer size", func(t *testing.T) {
		dir := t.TempDir()
		config := map[string]interface{}{
			"storage_type": "FS",
			"local_path":   dir,
			"buffer_size":  5.0,
		}

		consumer, err := NewSaveToParquet(config)
		require.NoError(t, err)
		defer consumer.Close()

		ctx := context.Background()

		// Send exactly buffer_size messages
		for i := 0; i < 5; i++ {
			msg := map[string]interface{}{
				"id":        i,
				"closed_at": float64(time.Now().Unix()),
			}
			jsonBytes, _ := json.Marshal(msg)
			err := consumer.Receive(ctx, processor.Message{Payload: jsonBytes})
			require.NoError(t, err)
		}

		// File should be written immediately after 5th message
		files := findParquetFiles(t, dir)
		assert.Len(t, files, 1, "should write file when buffer is full")

		// Verify exactly 5 records
		totalRows := countTotalRows(t, dir)
		assert.Equal(t, int64(5), totalRows)
	})

	t.Run("handles buffer overflow correctly", func(t *testing.T) {
		dir := t.TempDir()
		config := map[string]interface{}{
			"storage_type": "FS",
			"local_path":   dir,
			"buffer_size":  3.0,
		}

		consumer, err := NewSaveToParquet(config)
		require.NoError(t, err)

		ctx := context.Background()

		// Send more than buffer size without closing
		for i := 0; i < 10; i++ {
			msg := map[string]interface{}{
				"id":        i,
				"closed_at": float64(time.Now().Unix()),
			}
			jsonBytes, _ := json.Marshal(msg)
			err := consumer.Receive(ctx, processor.Message{Payload: jsonBytes})
			require.NoError(t, err)
		}

		// Close to flush remaining
		consumer.Close()

		// Should have multiple files
		files := findParquetFiles(t, dir)
		assert.GreaterOrEqual(t, len(files), 3, "should create multiple files")

		// Verify all records written
		totalRows := countTotalRows(t, dir)
		assert.Equal(t, int64(10), totalRows)
	})

	t.Run("time-based rotation", func(t *testing.T) {
		dir := t.TempDir()
		config := map[string]interface{}{
			"storage_type":               "FS",
			"local_path":                 dir,
			"buffer_size":                1000.0, // Large buffer
			"rotation_interval_minutes":  0.01,   // 0.6 seconds
		}

		consumer, err := NewSaveToParquet(config)
		require.NoError(t, err)
		defer consumer.Close()

		ctx := context.Background()

		// Send messages over time
		for i := 0; i < 5; i++ {
			msg := map[string]interface{}{
				"id":        i,
				"closed_at": float64(time.Now().Unix()),
			}
			jsonBytes, _ := json.Marshal(msg)
			err := consumer.Receive(ctx, processor.Message{Payload: jsonBytes})
			require.NoError(t, err)

			if i == 2 {
				// Wait for rotation interval
				time.Sleep(700 * time.Millisecond)
			}
		}

		consumer.Close()

		// Should have 2 files due to time rotation
		files := findParquetFiles(t, dir)
		assert.GreaterOrEqual(t, len(files), 2, "should rotate based on time")
	})

	t.Run("handles empty flush gracefully", func(t *testing.T) {
		dir := t.TempDir()
		config := map[string]interface{}{
			"storage_type": "FS",
			"local_path":   dir,
			"buffer_size":  10.0,
		}

		consumer, err := NewSaveToParquet(config)
		require.NoError(t, err)

		// Close immediately without sending messages
		consumer.Close()

		// Should not create any files
		files := findParquetFiles(t, dir)
		assert.Len(t, files, 0, "should not create files when buffer is empty")
	})

	t.Run("memory usage stays bounded", func(t *testing.T) {
		dir := t.TempDir()
		config := map[string]interface{}{
			"storage_type": "FS",
			"local_path":   dir,
			"buffer_size":  100.0,
		}

		consumer, err := NewSaveToParquet(config)
		require.NoError(t, err)
		defer consumer.Close()

		ctx := context.Background()

		// Track memory before
		var m1 runtime.MemStats
		runtime.ReadMemStats(&m1)

		// Send many messages that should flush periodically
		for i := 0; i < 1000; i++ {
			msg := map[string]interface{}{
				"id":        i,
				"data":      strings.Repeat("x", 1000), // 1KB per message
				"closed_at": float64(time.Now().Unix()),
			}
			jsonBytes, _ := json.Marshal(msg)
			err := consumer.Receive(ctx, processor.Message{Payload: jsonBytes})
			require.NoError(t, err)
		}

		// Track memory after
		var m2 runtime.MemStats
		runtime.ReadMemStats(&m2)

		// Memory growth should be reasonable (not storing all 1000 messages)
		memGrowthMB := float64(m2.Alloc-m1.Alloc) / (1024 * 1024)
		assert.Less(t, memGrowthMB, 50.0, "memory growth should be bounded")
	})
}

// TestSaveToParquet_SchemaEvolution tests schema changes
func TestSaveToParquet_SchemaEvolution(t *testing.T) {
	t.Run("handles new fields gracefully", func(t *testing.T) {
		dir := t.TempDir()
		config := map[string]interface{}{
			"storage_type":     "FS",
			"local_path":       dir,
			"buffer_size":      10.0,
			"schema_evolution": true,
		}

		consumer, err := NewSaveToParquet(config)
		require.NoError(t, err)
		defer consumer.Close()

		ctx := context.Background()

		// First batch with initial schema
		for i := 0; i < 5; i++ {
			msg := map[string]interface{}{
				"id":        i,
				"name":      fmt.Sprintf("user%d", i),
				"closed_at": float64(time.Now().Unix()),
			}
			jsonBytes, _ := json.Marshal(msg)
			err := consumer.Receive(ctx, processor.Message{Payload: jsonBytes})
			require.NoError(t, err)
		}

		// Second batch with additional field
		for i := 5; i < 10; i++ {
			msg := map[string]interface{}{
				"id":        i,
				"name":      fmt.Sprintf("user%d", i),
				"email":     fmt.Sprintf("user%d@example.com", i), // New field
				"closed_at": float64(time.Now().Unix()),
			}
			jsonBytes, _ := json.Marshal(msg)
			err := consumer.Receive(ctx, processor.Message{Payload: jsonBytes})
			require.NoError(t, err)
		}

		consumer.Close()

		// Verify all records written
		totalRows := countTotalRows(t, dir)
		assert.Equal(t, int64(10), totalRows)
	})

	t.Run("handles type changes", func(t *testing.T) {
		dir := t.TempDir()
		config := map[string]interface{}{
			"storage_type":     "FS",
			"local_path":       dir,
			"buffer_size":      5.0,
			"schema_evolution": true,
		}

		consumer, err := NewSaveToParquet(config)
		require.NoError(t, err)
		defer consumer.Close()

		ctx := context.Background()

		// Send messages with different types for same field
		messages := []map[string]interface{}{
			{"id": 1, "value": "100", "closed_at": float64(time.Now().Unix())},       // string
			{"id": 2, "value": 200, "closed_at": float64(time.Now().Unix())},         // int
			{"id": 3, "value": 300.5, "closed_at": float64(time.Now().Unix())},       // float
			{"id": 4, "value": true, "closed_at": float64(time.Now().Unix())},        // bool
			{"id": 5, "value": []int{1, 2, 3}, "closed_at": float64(time.Now().Unix())}, // array
		}

		successCount := 0
		for _, msg := range messages {
			jsonBytes, _ := json.Marshal(msg)
			err := consumer.Receive(ctx, processor.Message{Payload: jsonBytes})
			if err == nil {
				successCount++
			}
		}

		consumer.Close()

		// Should handle at least some messages gracefully
		assert.Greater(t, successCount, 0, "should process some messages despite type variations")
	})
}

// TestSaveToParquet_ContextCancellation tests graceful shutdown
func TestSaveToParquet_ContextCancellation(t *testing.T) {
	dir := t.TempDir()
	config := map[string]interface{}{
		"storage_type": "FS",
		"local_path":   dir,
		"buffer_size":  1000.0,
	}

	consumer, err := NewSaveToParquet(config)
	require.NoError(t, err)
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())

	// Start sending messages
	go func() {
		for i := 0; i < 100; i++ {
			msg := map[string]interface{}{
				"id":        i,
				"closed_at": float64(time.Now().Unix()),
			}
			jsonBytes, _ := json.Marshal(msg)
			consumer.Receive(ctx, processor.Message{Payload: jsonBytes})
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Cancel after some messages
	time.Sleep(200 * time.Millisecond)
	cancel()

	// Close should flush remaining messages
	consumer.Close()

	// Should have written some messages
	totalRows := countTotalRows(t, dir)
	assert.Greater(t, totalRows, int64(0), "should write some messages before cancellation")
	assert.Less(t, totalRows, int64(100), "should not write all messages due to cancellation")
}

// Helper functions
func findParquetFiles(t *testing.T, dir string) []string {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".parquet") {
			files = append(files, path)
		}
		return nil
	})
	require.NoError(t, err)
	return files
}

func countTotalRows(t *testing.T, dir string) int64 {
	files := findParquetFiles(t, dir)
	var total int64
	for _, f := range files {
		reader, err := file.OpenParquetFile(f, true)
		require.NoError(t, err)
		total += reader.NumRows()
		reader.Close()
	}
	return total
}