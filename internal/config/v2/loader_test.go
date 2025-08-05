package v2

import (
	"testing"
)

func TestNewConfigLoader(t *testing.T) {
	// Test that we can create a new config loader
	loader, err := NewConfigLoader(DefaultLoaderOptions())
	if err != nil {
		t.Fatalf("Failed to create config loader: %v", err)
	}
	
	if loader == nil {
		t.Fatal("Loader should not be nil")
	}
	
	// Verify components are initialized
	if loader.resolver == nil {
		t.Error("Resolver should be initialized")
	}
	if loader.inferencer == nil {
		t.Error("Inferencer should be initialized")
	}
	if loader.defaultsEngine == nil {
		t.Error("Defaults engine should be initialized")
	}
	if loader.validator == nil {
		t.Error("Validator should be initialized")
	}
	if loader.transformer == nil {
		t.Error("Transformer should be initialized")
	}
}

func TestDetectFormat(t *testing.T) {
	tests := []struct {
		name     string
		config   map[string]interface{}
		expected FormatVersion
	}{
		{
			name: "Legacy format",
			config: map[string]interface{}{
				"pipelines": map[string]interface{}{
					"test": map[string]interface{}{},
				},
			},
			expected: FormatLegacy,
		},
		{
			name: "V2 format with source",
			config: map[string]interface{}{
				"source": map[string]interface{}{
					"bucket": "test",
				},
			},
			expected: FormatV2,
		},
		{
			name: "V2 format with process",
			config: map[string]interface{}{
				"process": []interface{}{"payment_filter"},
			},
			expected: FormatV2,
		},
		{
			name: "V2 format with save_to",
			config: map[string]interface{}{
				"save_to": "postgres",
			},
			expected: FormatV2,
		},
		{
			name:     "Unknown format",
			config:   map[string]interface{}{},
			expected: FormatUnknown,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DetectFormat(tt.config)
			if result != tt.expected {
				t.Errorf("Expected format %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestAliasResolver(t *testing.T) {
	resolver, err := NewAliasResolver()
	if err != nil {
		t.Fatalf("Failed to create alias resolver: %v", err)
	}
	
	// Test source aliases
	sourceType, found := resolver.ResolveSourceType("bucket")
	if !found || sourceType != "BufferedStorageSourceAdapter" {
		t.Errorf("Expected 'bucket' to resolve to 'BufferedStorageSourceAdapter', got '%s' (found=%v)", sourceType, found)
	}
	
	// Test processor aliases
	processorType, found := resolver.ResolveProcessorType("payment_filter")
	if !found || processorType != "FilterPayments" {
		t.Errorf("Expected 'payment_filter' to resolve to 'FilterPayments', got '%s' (found=%v)", processorType, found)
	}
	
	// Test consumer aliases
	consumerType, found := resolver.ResolveConsumerType("postgres")
	if !found || consumerType != "SaveToPostgreSQL" {
		t.Errorf("Expected 'postgres' to resolve to 'SaveToPostgreSQL', got '%s' (found=%v)", consumerType, found)
	}
	
	// Test field aliases
	fieldName := resolver.ResolveFieldName("bucket")
	if fieldName != "bucket_name" {
		t.Errorf("Expected 'bucket' to resolve to 'bucket_name', got '%s'", fieldName)
	}
}

func TestSimplifiedConfigTransformation(t *testing.T) {
	loader, err := NewConfigLoader(DefaultLoaderOptions())
	if err != nil {
		t.Fatalf("Failed to create config loader: %v", err)
	}
	
	// Test simple v2 config
	config := map[string]interface{}{
		"source": map[string]interface{}{
			"bucket": "test-bucket",
			"network": "testnet",
		},
		"process": "contract_data",
		"save_to": "parquet",
	}
	
	result, err := loader.LoadFromData(config)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}
	
	// Verify format detection
	if result.Format != FormatV2 {
		t.Errorf("Expected V2 format, got %v", result.Format)
	}
	
	// Verify transformation
	if len(result.Config.Pipelines) != 1 {
		t.Errorf("Expected 1 pipeline, got %d", len(result.Config.Pipelines))
	}
	
	pipeline := result.Config.Pipelines["default"]
	
	// Check source
	if pipeline.Source.Type != "BufferedStorageSourceAdapter" {
		t.Errorf("Expected source type 'BufferedStorageSourceAdapter', got '%s'", pipeline.Source.Type)
	}
	
	// Check processor
	if len(pipeline.Processors) != 1 {
		t.Errorf("Expected 1 processor, got %d", len(pipeline.Processors))
	}
	if pipeline.Processors[0].Type != "ContractData" {
		t.Errorf("Expected processor type 'ContractData', got '%s'", pipeline.Processors[0].Type)
	}
	
	// Check consumer
	if len(pipeline.Consumers) != 1 {
		t.Errorf("Expected 1 consumer, got %d", len(pipeline.Consumers))
	}
	if pipeline.Consumers[0].Type != "SaveToParquet" {
		t.Errorf("Expected consumer type 'SaveToParquet', got '%s'", pipeline.Consumers[0].Type)
	}
}