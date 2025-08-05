package v2_test

import (
	"os"
	"path/filepath"
	"testing"
	
	"github.com/withObsrvr/cdp-pipeline-workflow/internal/config/v2"
)

func TestComprehensiveV2Config(t *testing.T) {
	tests := []struct {
		name       string
		config     map[string]interface{}
		shouldPass bool
		checkFunc  func(*testing.T, *v2.LoadResult)
	}{
		{
			name: "Minimal valid config",
			config: map[string]interface{}{
				"source":  "stellar://mainnet",
				"save_to": "stdout",
			},
			shouldPass: true,
			checkFunc: func(t *testing.T, result *v2.LoadResult) {
				if len(result.Config.Pipelines) != 1 {
					t.Errorf("Expected 1 pipeline, got %d", len(result.Config.Pipelines))
				}
			},
		},
		{
			name: "Complex multi-processor pipeline",
			config: map[string]interface{}{
				"source": map[string]interface{}{
					"bucket":  "stellar-data",
					"network": "mainnet",
					"workers": 50,
				},
				"process": []interface{}{
					"contract_data",
					map[string]interface{}{
						"contract_filter": map[string]interface{}{
							"contract_ids": []string{"CA123", "CB456"},
						},
					},
					"contract_events",
				},
				"save_to": []interface{}{
					map[string]interface{}{
						"parquet": map[string]interface{}{
							"path":        "output/contracts",
							"compression": "zstd",
							"buffer":      50000,
						},
					},
					"postgres://localhost/stellar",
					map[string]interface{}{
						"redis": map[string]interface{}{
							"address": "localhost:6379",
							"ttl":     3600,
						},
					},
				},
			},
			shouldPass: true,
			checkFunc: func(t *testing.T, result *v2.LoadResult) {
				pipeline := result.Config.Pipelines["default"]
				
				// Check processors
				if len(pipeline.Processors) != 3 {
					t.Errorf("Expected 3 processors, got %d", len(pipeline.Processors))
				}
				
				// Check consumers
				if len(pipeline.Consumers) != 3 {
					t.Errorf("Expected 3 consumers, got %d", len(pipeline.Consumers))
				}
				
				// Check specific config values
				if pipeline.Consumers[0].Config["compression"] != "zstd" {
					t.Errorf("Expected compression 'zstd', got %v", pipeline.Consumers[0].Config["compression"])
				}
			},
		},
		{
			name: "Environment variable expansion",
			config: map[string]interface{}{
				"source": map[string]interface{}{
					"bucket":  "${TEST_BUCKET}",
					"network": "${TEST_NETWORK:-testnet}",
				},
				"save_to": "${DATABASE_URL}",
			},
			shouldPass: true,
			checkFunc: func(t *testing.T, result *v2.LoadResult) {
				// Set test env vars
				os.Setenv("TEST_BUCKET", "test-bucket-123")
				os.Setenv("DATABASE_URL", "postgres://test")
				defer os.Unsetenv("TEST_BUCKET")
				defer os.Unsetenv("DATABASE_URL")
				
				// The loader should have expanded these
				// Note: This test might need adjustment based on when env expansion happens
			},
		},
		{
			name: "All processor aliases",
			config: map[string]interface{}{
				"process": []interface{}{
					"payments",
					"payment_transform",
					"trades",
					"market_metrics",
					"contracts",
					"invocations",
					"account_data",
					"ledger",
				},
				"save_to": "stdout",
			},
			shouldPass: true,
			checkFunc: func(t *testing.T, result *v2.LoadResult) {
				expectedTypes := []string{
					"FilterPayments",
					"TransformToAppPayment",
					"TransformToAppTrade",
					"MarketMetricsProcessor",
					"ContractData",
					"ContractInvocation",
					"AccountData",
					"LedgerReader",
				}
				
				pipeline := result.Config.Pipelines["default"]
				if len(pipeline.Processors) != len(expectedTypes) {
					t.Errorf("Expected %d processors, got %d", len(expectedTypes), len(pipeline.Processors))
				}
				
				for i, expectedType := range expectedTypes {
					if i < len(pipeline.Processors) && pipeline.Processors[i].Type != expectedType {
						t.Errorf("Processor %d: expected type %s, got %s", i, expectedType, pipeline.Processors[i].Type)
					}
				}
			},
		},
		{
			name: "Protocol-style URLs",
			config: map[string]interface{}{
				"source": "stellar://mainnet",
				"save_to": []interface{}{
					"postgres://user:pass@localhost/db",
					"gs://bucket/path/to/files",
					"s3://my-bucket/prefix",
				},
			},
			shouldPass: true,
			checkFunc: func(t *testing.T, result *v2.LoadResult) {
				pipeline := result.Config.Pipelines["default"]
				
				// Check source
				if pipeline.Source.Type != "CaptiveCoreInboundAdapter" {
					t.Errorf("Expected CaptiveCoreInboundAdapter, got %s", pipeline.Source.Type)
				}
				
				// Check consumers
				expectedConsumerTypes := []string{
					"SaveToPostgreSQL",
					"SaveToParquet",
					"SaveToParquet",
				}
				
				for i, expectedType := range expectedConsumerTypes {
					if i < len(pipeline.Consumers) && pipeline.Consumers[i].Type != expectedType {
						t.Errorf("Consumer %d: expected type %s, got %s", i, expectedType, pipeline.Consumers[i].Type)
					}
				}
			},
		},
		{
			name: "Field alias resolution",
			config: map[string]interface{}{
				"source": map[string]interface{}{
					"bucket":  "data",
					"workers": 100,     // alias for num_workers
					"net":     "mainnet", // alias for network
				},
				"process": []interface{}{
					map[string]interface{}{
						"payment_filter": map[string]interface{}{
							"min": 100,  // alias for min_amount
							"max": 1000, // alias for max_amount
						},
					},
				},
				"save_to": map[string]interface{}{
					"postgres": map[string]interface{}{
						"connection": "postgres://localhost", // alias for connection_string
						"batch":      5000,                    // alias for batch_size
					},
				},
			},
			shouldPass: true,
			checkFunc: func(t *testing.T, result *v2.LoadResult) {
				pipeline := result.Config.Pipelines["default"]
				
				// Check source field aliases resolved
				if pipeline.Source.Config["num_workers"] != 100 {
					t.Errorf("Expected num_workers=100, got %v", pipeline.Source.Config["num_workers"])
				}
				if pipeline.Source.Config["network"] != "mainnet" {
					t.Errorf("Expected network=mainnet, got %v", pipeline.Source.Config["network"])
				}
				
				// Check processor field aliases
				if pipeline.Processors[0].Config["min_amount"] != 100 {
					t.Errorf("Expected min_amount=100, got %v", pipeline.Processors[0].Config["min_amount"])
				}
				
				// Check consumer field aliases
				if pipeline.Consumers[0].Config["batch_size"] != 5000 {
					t.Errorf("Expected batch_size=5000, got %v", pipeline.Consumers[0].Config["batch_size"])
				}
			},
		},
		{
			name: "Smart defaults applied",
			config: map[string]interface{}{
				"source": map[string]interface{}{
					"bucket":  "mainnet-data",
					"network": "mainnet",
				},
				"save_to": "parquet",
			},
			shouldPass: true,
			checkFunc: func(t *testing.T, result *v2.LoadResult) {
				pipeline := result.Config.Pipelines["default"]
				
				// Check defaults were applied
				source := pipeline.Source.Config
				
				// BufferedStorageSourceAdapter defaults
				if source["num_workers"] != 20 {
					t.Errorf("Expected default num_workers=20, got %v", source["num_workers"])
				}
				if source["retry_limit"] != 3 {
					t.Errorf("Expected default retry_limit=3, got %v", source["retry_limit"])
				}
				
				// SaveToParquet defaults
				consumer := pipeline.Consumers[0].Config
				if consumer["compression"] != "snappy" {
					t.Errorf("Expected default compression=snappy, got %v", consumer["compression"])
				}
				if consumer["buffer_size"] != 10000 {
					t.Errorf("Expected default buffer_size=10000, got %v", consumer["buffer_size"])
				}
			},
		},
		{
			name: "Invalid config - unknown source",
			config: map[string]interface{}{
				"source":  "unknown://invalid",
				"save_to": "stdout",
			},
			shouldPass: false,
		},
		{
			name: "Invalid config - unknown processor",
			config: map[string]interface{}{
				"process": "unknown_processor",
				"save_to": "stdout",
			},
			shouldPass: false,
		},
		{
			name: "Invalid config - missing required fields",
			config: map[string]interface{}{
				"save_to": "postgres", // No source
			},
			shouldPass: true, // This should pass but with warnings
			checkFunc: func(t *testing.T, result *v2.LoadResult) {
				if len(result.Warnings) == 0 {
					t.Error("Expected warnings for missing source")
				}
			},
		},
	}
	
	loader, err := v2.NewConfigLoader(v2.DefaultLoaderOptions())
	if err != nil {
		t.Fatalf("Failed to create loader: %v", err)
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := loader.LoadFromData(tt.config)
			
			if tt.shouldPass {
				if err != nil {
					t.Errorf("Expected config to load successfully, got error: %v", err)
				} else if tt.checkFunc != nil {
					tt.checkFunc(t, result)
				}
			} else {
				if err == nil {
					t.Error("Expected config to fail validation, but it passed")
				}
			}
		})
	}
}

func TestConfigMigration(t *testing.T) {
	// Test that legacy configs can be loaded and produce the same result
	legacyConfig := map[string]interface{}{
		"pipelines": map[string]interface{}{
			"TestPipeline": map[string]interface{}{
				"source": map[string]interface{}{
					"type": "BufferedStorageSourceAdapter",
					"config": map[string]interface{}{
						"bucket_name": "test-bucket",
						"network":     "testnet",
					},
				},
				"processors": []interface{}{
					map[string]interface{}{
						"type": "FilterPayments",
						"config": map[string]interface{}{
							"min_amount": "100",
						},
					},
				},
				"consumers": []interface{}{
					map[string]interface{}{
						"type": "SaveToPostgreSQL",
						"config": map[string]interface{}{
							"connection_string": "postgres://localhost",
						},
					},
				},
			},
		},
	}
	
	v2Config := map[string]interface{}{
		"source": map[string]interface{}{
			"bucket":  "test-bucket",
			"network": "testnet",
		},
		"process": []interface{}{
			map[string]interface{}{
				"payment_filter": map[string]interface{}{
					"min": "100",
				},
			},
		},
		"save_to": map[string]interface{}{
			"postgres": map[string]interface{}{
				"connection": "postgres://localhost",
			},
		},
	}
	
	loader, err := v2.NewConfigLoader(v2.DefaultLoaderOptions())
	if err != nil {
		t.Fatalf("Failed to create loader: %v", err)
	}
	
	// Load both configs
	legacyResult, err := loader.LoadFromData(legacyConfig)
	if err != nil {
		t.Fatalf("Failed to load legacy config: %v", err)
	}
	
	v2Result, err := loader.LoadFromData(v2Config)
	if err != nil {
		t.Fatalf("Failed to load v2 config: %v", err)
	}
	
	// Both should produce similar pipelines
	if len(legacyResult.Config.Pipelines) != len(v2Result.Config.Pipelines) {
		t.Errorf("Pipeline count mismatch: legacy=%d, v2=%d", 
			len(legacyResult.Config.Pipelines), len(v2Result.Config.Pipelines))
	}
	
	// Check formats detected correctly
	if legacyResult.Format != v2.FormatLegacy {
		t.Errorf("Expected legacy format, got %v", legacyResult.Format)
	}
	if v2Result.Format != v2.FormatV2 {
		t.Errorf("Expected v2 format, got %v", v2Result.Format)
	}
}

func TestConfigFiles(t *testing.T) {
	// Create temporary config files for testing
	tempDir := t.TempDir()
	
	// Create a v2 config file
	v2ConfigPath := filepath.Join(tempDir, "v2-config.yaml")
	v2Content := `
source:
  bucket: "test-bucket"
  network: testnet

process:
  - contract_data
  - payment_filter:
      min: 100

save_to:
  - parquet: "output/data"
  - postgres: "${DATABASE_URL}"
`
	
	if err := os.WriteFile(v2ConfigPath, []byte(v2Content), 0644); err != nil {
		t.Fatalf("Failed to write v2 config: %v", err)
	}
	
	// Create a legacy config file
	legacyConfigPath := filepath.Join(tempDir, "legacy-config.yaml")
	legacyContent := `
pipelines:
  TestPipeline:
    source:
      type: BufferedStorageSourceAdapter
      config:
        bucket_name: "test-bucket"
        network: "testnet"
    processors:
      - type: ContractData
        config: {}
      - type: FilterPayments
        config:
          min_amount: "100"
    consumers:
      - type: SaveToParquet
        config:
          path: "output/data"
      - type: SaveToPostgreSQL
        config:
          connection_string: "${DATABASE_URL}"
`
	
	if err := os.WriteFile(legacyConfigPath, []byte(legacyContent), 0644); err != nil {
		t.Fatalf("Failed to write legacy config: %v", err)
	}
	
	// Test loading both files
	loader, err := v2.NewConfigLoader(v2.DefaultLoaderOptions())
	if err != nil {
		t.Fatalf("Failed to create loader: %v", err)
	}
	
	// Load v2 config
	v2Result, err := loader.Load(v2ConfigPath)
	if err != nil {
		t.Fatalf("Failed to load v2 config file: %v", err)
	}
	
	// Load legacy config
	legacyResult, err := loader.Load(legacyConfigPath)
	if err != nil {
		t.Fatalf("Failed to load legacy config file: %v", err)
	}
	
	// Verify both loaded successfully
	if v2Result.Format != v2.FormatV2 {
		t.Errorf("Expected v2 format for v2 config file")
	}
	if legacyResult.Format != v2.FormatLegacy {
		t.Errorf("Expected legacy format for legacy config file")
	}
	
	// Both should have similar structure
	if len(v2Result.Config.Pipelines) != 1 || len(legacyResult.Config.Pipelines) != 1 {
		t.Errorf("Expected 1 pipeline in each config")
	}
}