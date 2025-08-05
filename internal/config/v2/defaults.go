package v2

import (
	"fmt"
	"strings"
)

// NetworkConfig holds network-specific default configurations
type NetworkConfig struct {
	Passphrase         string
	HistoryURLs        []string
	DefaultStartLedger uint32
	HorizonURL         string
	SorobanRPCURL      string
}

// DefaultsEngine provides smart defaults for configurations
type DefaultsEngine struct {
	networkDefaults   map[string]NetworkConfig
	componentDefaults map[string]map[string]interface{}
}

// NewDefaultsEngine creates a new defaults engine
func NewDefaultsEngine() *DefaultsEngine {
	return &DefaultsEngine{
		networkDefaults:   getNetworkDefaults(),
		componentDefaults: getComponentDefaults(),
	}
}

// ApplyDefaults applies smart defaults to a configuration
func (e *DefaultsEngine) ApplyDefaults(componentType string, config map[string]interface{}) map[string]interface{} {
	// Start with a copy of the config
	result := make(map[string]interface{})
	
	// First apply component-specific defaults
	if defaults, ok := e.componentDefaults[componentType]; ok {
		for key, value := range defaults {
			result[key] = value
		}
	}
	
	// Then overlay user config (user config takes precedence)
	for key, value := range config {
		result[key] = value
	}
	
	// Apply network-aware defaults if applicable
	if network, ok := getNetworkFromConfig(config); ok {
		e.applyNetworkDefaults(componentType, network, result)
	}
	
	// Apply smart inference based on other fields
	e.applySmartDefaults(componentType, result)
	
	return result
}

// applyNetworkDefaults applies network-specific defaults
func (e *DefaultsEngine) applyNetworkDefaults(componentType, network string, config map[string]interface{}) {
	networkConfig, ok := e.networkDefaults[strings.ToLower(network)]
	if !ok {
		return
	}
	
	// Apply based on component type
	switch componentType {
	case "CaptiveCoreInboundAdapter":
		if _, hasPassphrase := config["network_passphrase"]; !hasPassphrase {
			config["network_passphrase"] = networkConfig.Passphrase
		}
		if _, hasHistory := config["history_archive_urls"]; !hasHistory {
			config["history_archive_urls"] = networkConfig.HistoryURLs
		}
	case "RPCSourceAdapter", "SorobanSourceAdapter":
		if _, hasEndpoint := config["endpoint"]; !hasEndpoint && networkConfig.SorobanRPCURL != "" {
			config["endpoint"] = networkConfig.SorobanRPCURL
		}
	// Many processors need network passphrase
	case "LatestLedger", "ContractData", "FilterPayments", "TransformToAppPayment",
		 "AccountData", "CreateAccount", "AccountTransaction", "ContractInvocation",
		 "ContractEvent", "LedgerReader", "ContractLedgerReader":
		if _, hasPassphrase := config["network_passphrase"]; !hasPassphrase {
			config["network_passphrase"] = networkConfig.Passphrase
		}
	}
}

// applySmartDefaults applies intelligent defaults based on configuration patterns
func (e *DefaultsEngine) applySmartDefaults(componentType string, config map[string]interface{}) {
	switch componentType {
	case "SaveToParquet":
		// If saving to cloud storage, use better defaults
		if storage, ok := config["storage_type"].(string); ok {
			switch strings.ToLower(storage) {
			case "gcs", "s3":
				// Use larger buffers and file sizes for cloud storage
				if _, hasBuffer := config["buffer_size"]; !hasBuffer {
					config["buffer_size"] = 50000
				}
				if _, hasFileSize := config["max_file_size_mb"]; !hasFileSize {
					config["max_file_size_mb"] = 256
				}
			}
		}
		
	case "BufferedStorageSourceAdapter", "S3BufferedStorageSourceAdapter":
		// If processing mainnet, use more workers
		if network, ok := config["network"].(string); ok && strings.ToLower(network) == "mainnet" {
			if _, hasWorkers := config["num_workers"]; !hasWorkers {
				config["num_workers"] = 50
			}
		}
		
	case "SaveToPostgreSQL", "SaveToMongoDB":
		// Set connection pool size based on workers if specified
		if workers, ok := config["num_workers"].(int); ok {
			if _, hasPool := config["connection_pool_size"]; !hasPool {
				// Pool size = workers / 2, min 5, max 20
				poolSize := workers / 2
				if poolSize < 5 {
					poolSize = 5
				}
				if poolSize > 20 {
					poolSize = 20
				}
				config["connection_pool_size"] = poolSize
			}
		}
	}
}

// GetDefaultsForComponent returns the default configuration for a component type
func (e *DefaultsEngine) GetDefaultsForComponent(componentType string) map[string]interface{} {
	if defaults, ok := e.componentDefaults[componentType]; ok {
		// Return a copy to prevent modification
		result := make(map[string]interface{})
		for k, v := range defaults {
			result[k] = v
		}
		return result
	}
	return make(map[string]interface{})
}

// GetNetworkDefaults returns network-specific defaults
func (e *DefaultsEngine) GetNetworkDefaults(network string) (NetworkConfig, bool) {
	config, ok := e.networkDefaults[strings.ToLower(network)]
	return config, ok
}

// Helper functions

func getNetworkFromConfig(config map[string]interface{}) (string, bool) {
	// Check various possible network fields
	if network, ok := config["network"].(string); ok {
		return network, true
	}
	if network, ok := config["net"].(string); ok {
		return network, true
	}
	// Check if network is in the passphrase
	if passphrase, ok := config["network_passphrase"].(string); ok {
		if strings.Contains(passphrase, "Public Global Stellar Network") {
			return "mainnet", true
		}
		if strings.Contains(passphrase, "Test SDF Network") {
			return "testnet", true
		}
	}
	return "", false
}

// Default configurations

func getNetworkDefaults() map[string]NetworkConfig {
	return map[string]NetworkConfig{
		"mainnet": {
			Passphrase: "Public Global Stellar Network ; September 2015",
			HistoryURLs: []string{
				"https://history.stellar.org/prd/core-live-001",
				"https://history.stellar.org/prd/core-live-002",
				"https://history.stellar.org/prd/core-live-003",
			},
			DefaultStartLedger: 1,
			HorizonURL:         "https://horizon.stellar.org",
			SorobanRPCURL:      "https://soroban-rpc.stellar.org",
		},
		"testnet": {
			Passphrase: "Test SDF Network ; September 2015",
			HistoryURLs: []string{
				"https://history.stellar.org/prd/core-testnet-001",
				"https://history.stellar.org/prd/core-testnet-002",
			},
			DefaultStartLedger: 1,
			HorizonURL:         "https://horizon-testnet.stellar.org",
			SorobanRPCURL:      "https://soroban-testnet.stellar.org",
		},
		"futurenet": {
			Passphrase: "Test SDF Future Network ; October 2022",
			HistoryURLs: []string{
				"https://history-futurenet.stellar.org",
			},
			DefaultStartLedger: 1,
			HorizonURL:         "https://horizon-futurenet.stellar.org",
			SorobanRPCURL:      "https://rpc-futurenet.stellar.org",
		},
	}
}

func getComponentDefaults() map[string]map[string]interface{} {
	return map[string]map[string]interface{}{
		// Source defaults
		"BufferedStorageSourceAdapter": {
			"num_workers":         20,
			"retry_limit":         3,
			"retry_wait":          5,
			"ledgers_per_file":    1,
			"files_per_partition": 64000,
			"buffer_size":         100,
		},
		"S3BufferedStorageSourceAdapter": {
			"num_workers":         20,
			"retry_limit":         3,
			"retry_wait":          5,
			"ledgers_per_file":    1,
			"files_per_partition": 64000,
			"buffer_size":         100,
			"region":              "us-east-1",
		},
		"FSBufferedStorageSourceAdapter": {
			"num_workers":      10,
			"retry_limit":      3,
			"retry_wait":       5,
			"buffer_size":      100,
		},
		"CaptiveCoreInboundAdapter": {
			"binary_path":     "/usr/local/bin/stellar-core",
			"storage_path":    "/tmp/stellar-core",
			"log_level":       "info",
		},
		
		// Processor defaults
		"FilterPayments": {
			"include_failed":   false,
			"include_muxed":    true,
		},
		"ContractData": {
			"include_expired":  false,
			"include_temporary": true,
		},
		"AccountData": {
			"include_trustlines": true,
			"include_offers":     true,
		},
		
		// Consumer defaults
		"SaveToParquet": {
			"compression":               "snappy",
			"buffer_size":               10000,
			"max_file_size_mb":          128,
			"rotation_interval_minutes": 60,
			"schema_evolution":          true,
			"include_metadata":          true,
			"partition_by":              "date",
		},
		"SaveToPostgreSQL": {
			"batch_size":           1000,
			"connection_pool_size": 10,
			"retry_on_conflict":    true,
			"create_table":         true,
			"use_copy":             true,
		},
		"SaveToMongoDB": {
			"batch_size":           1000,
			"write_concern":        "majority",
			"ordered_bulk_write":   false,
			"create_indexes":       true,
		},
		"SaveToDuckDB": {
			"batch_size":        10000,
			"create_table":      true,
			"append_only":       false,
		},
		"SaveToClickHouse": {
			"batch_size":        10000,
			"async_insert":      true,
			"create_table":      true,
		},
		"SaveToRedis": {
			"ttl":               86400, // 24 hours
			"batch_size":        100,
			"pipeline_commands": true,
		},
		"SaveToZeroMQ": {
			"socket_type":       "PUB",
			"high_water_mark":   1000,
			"send_timeout_ms":   5000,
		},
		"SaveToWebSocket": {
			"port":              8080,
			"ping_interval":     30,
			"max_connections":   1000,
			"buffer_size":       100,
		},
		"SaveToGCS": {
			"buffer_size":               10000,
			"max_file_size_mb":          128,
			"rotation_interval_minutes": 60,
			"compression":               "gzip",
		},
	}
}

// ValidateDefaults checks if the defaults are valid for a component
func (e *DefaultsEngine) ValidateDefaults(componentType string, config map[string]interface{}) []error {
	var errors []error
	
	// Validate network-specific settings
	if network, ok := getNetworkFromConfig(config); ok {
		if _, validNetwork := e.networkDefaults[strings.ToLower(network)]; !validNetwork {
			errors = append(errors, fmt.Errorf("unknown network: %s (valid: mainnet, testnet, futurenet)", network))
		}
	}
	
	// Component-specific validation
	switch componentType {
	case "SaveToParquet":
		if compression, ok := config["compression"].(string); ok {
			validCompressions := []string{"snappy", "gzip", "lz4", "zstd", "uncompressed"}
			valid := false
			for _, v := range validCompressions {
				if compression == v {
					valid = true
					break
				}
			}
			if !valid {
				errors = append(errors, fmt.Errorf("invalid compression: %s (valid: %v)", compression, validCompressions))
			}
		}
		
	case "BufferedStorageSourceAdapter", "S3BufferedStorageSourceAdapter":
		if workers, ok := config["num_workers"].(int); ok {
			if workers < 1 || workers > 100 {
				errors = append(errors, fmt.Errorf("num_workers must be between 1 and 100, got %d", workers))
			}
		}
	}
	
	return errors
}