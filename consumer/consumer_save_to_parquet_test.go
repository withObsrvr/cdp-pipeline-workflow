package consumer

import (
	"context"
	"strings"
	"testing"

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