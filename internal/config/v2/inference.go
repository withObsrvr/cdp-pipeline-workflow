package v2

import (
	"fmt"
	"strings"
)

// InferenceEngine handles type inference for configurations
type InferenceEngine struct {
	rules []InferenceRule
	resolver *AliasResolver
}

// InferenceRule defines a rule for inferring component types
type InferenceRule interface {
	Matches(config map[string]interface{}) bool
	InferType() string
	Priority() int
	Description() string
}

// NewInferenceEngine creates a new type inference engine
func NewInferenceEngine(resolver *AliasResolver) *InferenceEngine {
	engine := &InferenceEngine{
		resolver: resolver,
		rules: []InferenceRule{
			// Source inference rules
			&BucketInferenceRule{},
			&StellarInferenceRule{},
			&RPCInferenceRule{},
			&S3InferenceRule{},
			&FileSystemInferenceRule{},
			
			// Processor inference rules
			&PaymentFilterInferenceRule{},
			&ContractInferenceRule{},
			&AccountInferenceRule{},
			&TransformInferenceRule{},
			
			// Consumer inference rules
			&DatabaseInferenceRule{},
			&FileOutputInferenceRule{},
			&MessagingInferenceRule{},
		},
	}
	
	// Sort rules by priority (higher priority first)
	// In production, we'd use sort.Slice here
	
	return engine
}

// InferSourceType infers the source type from configuration
func (e *InferenceEngine) InferSourceType(config map[string]interface{}) (string, error) {
	// First check if type is explicitly specified
	if typ, ok := config["type"].(string); ok {
		return typ, nil
	}
	
	// Check if it's a simple string (e.g., "stellar://mainnet")
	if len(config) == 1 {
		for key, value := range config {
			if str, ok := value.(string); ok {
				// Check for protocol handlers (future Phase 3)
				if strings.Contains(str, "://") {
					parts := strings.SplitN(str, "://", 2)
					protocol := parts[0]
					
					// Map protocol to source type
					switch protocol {
					case "stellar", "core":
						return "CaptiveCoreInboundAdapter", nil
					case "s3":
						return "S3BufferedStorageSourceAdapter", nil
					case "gs", "gcs":
						return "BufferedStorageSourceAdapter", nil
					case "file", "fs":
						return "FSBufferedStorageSourceAdapter", nil
					}
				}
				
				// Check aliases
				if resolved, found := e.resolver.ResolveSourceType(key); found {
					return resolved, nil
				}
			}
		}
	}
	
	// Apply inference rules
	for _, rule := range e.rules {
		if rule.Matches(config) {
			return rule.InferType(), nil
		}
	}
	
	return "", fmt.Errorf("cannot infer source type from configuration")
}

// InferProcessorType infers the processor type from configuration
func (e *InferenceEngine) InferProcessorType(name string, config map[string]interface{}) (string, error) {
	// Check if the name itself is an alias
	if resolved, found := e.resolver.ResolveProcessorType(name); found {
		return resolved, nil
	}
	
	// Check if type is explicitly specified
	if typ, ok := config["type"].(string); ok {
		return typ, nil
	}
	
	// Apply inference rules based on config content
	for _, rule := range e.rules {
		if rule.Matches(config) {
			return rule.InferType(), nil
		}
	}
	
	return "", fmt.Errorf("cannot infer processor type for '%s'", name)
}

// InferConsumerType infers the consumer type from configuration
func (e *InferenceEngine) InferConsumerType(name string, config map[string]interface{}) (string, error) {
	// Check if the name itself is an alias
	if resolved, found := e.resolver.ResolveConsumerType(name); found {
		return resolved, nil
	}
	
	// Check if it's a string value (e.g., "postgres://connection")
	if len(config) == 0 {
		// Simple string consumer like "save_to: postgres"
		if resolved, found := e.resolver.ResolveConsumerType(name); found {
			return resolved, nil
		}
	}
	
	// Check if type is explicitly specified
	if typ, ok := config["type"].(string); ok {
		return typ, nil
	}
	
	// Apply inference rules
	for _, rule := range e.rules {
		if rule.Matches(config) {
			return rule.InferType(), nil
		}
	}
	
	return "", fmt.Errorf("cannot infer consumer type for '%s'", name)
}

// Source Inference Rules

type BucketInferenceRule struct{}

func (r *BucketInferenceRule) Matches(config map[string]interface{}) bool {
	_, hasBucket := config["bucket"]
	_, hasBucketName := config["bucket_name"]
	_, hasPath := config["path"]
	_, hasPathPrefix := config["path_prefix"]
	
	return hasBucket || hasBucketName || (hasPath || hasPathPrefix)
}

func (r *BucketInferenceRule) InferType() string {
	return "BufferedStorageSourceAdapter"
}

func (r *BucketInferenceRule) Priority() int { return 100 }

func (r *BucketInferenceRule) Description() string {
	return "Infers cloud storage source from bucket configuration"
}

type S3InferenceRule struct{}

func (r *S3InferenceRule) Matches(config map[string]interface{}) bool {
	// Check for S3-specific fields
	_, hasRegion := config["region"]
	_, hasAwsKey := config["aws_access_key_id"]
	
	// Check if bucket name suggests S3
	if bucket, ok := config["bucket"].(string); ok {
		if strings.Contains(bucket, "s3") || strings.Contains(bucket, "aws") {
			return true
		}
	}
	
	return hasRegion || hasAwsKey
}

func (r *S3InferenceRule) InferType() string {
	return "S3BufferedStorageSourceAdapter"
}

func (r *S3InferenceRule) Priority() int { return 110 } // Higher than generic bucket

func (r *S3InferenceRule) Description() string {
	return "Infers S3 source from AWS-specific configuration"
}

type FileSystemInferenceRule struct{}

func (r *FileSystemInferenceRule) Matches(config map[string]interface{}) bool {
	// Check for file system indicators
	if path, ok := config["path"].(string); ok {
		// Local paths typically start with / or ./ or are relative
		return strings.HasPrefix(path, "/") || strings.HasPrefix(path, "./") || 
		       !strings.Contains(path, "://")
	}
	
	_, hasDirectory := config["directory"]
	return hasDirectory
}

func (r *FileSystemInferenceRule) InferType() string {
	return "FSBufferedStorageSourceAdapter"
}

func (r *FileSystemInferenceRule) Priority() int { return 90 }

func (r *FileSystemInferenceRule) Description() string {
	return "Infers local filesystem source from path configuration"
}

type StellarInferenceRule struct{}

func (r *StellarInferenceRule) Matches(config map[string]interface{}) bool {
	_, hasStellar := config["stellar"]
	_, hasCore := config["core"]
	_, hasCaptive := config["captive_core"]
	_, hasNetwork := config["network"]
	_, hasBinary := config["binary_path"]
	
	return hasStellar || hasCore || hasCaptive || (hasNetwork && hasBinary)
}

func (r *StellarInferenceRule) InferType() string {
	return "CaptiveCoreInboundAdapter"
}

func (r *StellarInferenceRule) Priority() int { return 100 }

func (r *StellarInferenceRule) Description() string {
	return "Infers Stellar Core source from configuration"
}

type RPCInferenceRule struct{}

func (r *RPCInferenceRule) Matches(config map[string]interface{}) bool {
	_, hasRPC := config["rpc"]
	_, hasEndpoint := config["endpoint"]
	_, hasSoroban := config["soroban"]
	
	// Check for RPC URL patterns
	if url, ok := config["url"].(string); ok {
		return strings.Contains(url, "rpc") || strings.Contains(url, "soroban")
	}
	
	return hasRPC || hasEndpoint || hasSoroban
}

func (r *RPCInferenceRule) InferType() string {
	return "RPCSourceAdapter"
}

func (r *RPCInferenceRule) Priority() int { return 100 }

func (r *RPCInferenceRule) Description() string {
	return "Infers RPC source from endpoint configuration"
}

// Processor Inference Rules

type PaymentFilterInferenceRule struct{}

func (r *PaymentFilterInferenceRule) Matches(config map[string]interface{}) bool {
	_, hasMin := config["min"]
	_, hasMinAmount := config["min_amount"]
	_, hasMax := config["max"]
	_, hasMaxAmount := config["max_amount"]
	_, hasAssetCode := config["asset_code"]
	
	return hasMin || hasMinAmount || hasMax || hasMaxAmount || hasAssetCode
}

func (r *PaymentFilterInferenceRule) InferType() string {
	return "FilterPayments"
}

func (r *PaymentFilterInferenceRule) Priority() int { return 100 }

func (r *PaymentFilterInferenceRule) Description() string {
	return "Infers payment filter from amount configuration"
}

type ContractInferenceRule struct{}

func (r *ContractInferenceRule) Matches(config map[string]interface{}) bool {
	_, hasContractID := config["contract_id"]
	_, hasContractIDs := config["contract_ids"]
	_, hasEventTypes := config["event_types"]
	
	return hasContractID || hasContractIDs || hasEventTypes
}

func (r *ContractInferenceRule) InferType() string {
	return "ContractFilter"
}

func (r *ContractInferenceRule) Priority() int { return 100 }

func (r *ContractInferenceRule) Description() string {
	return "Infers contract processor from contract configuration"
}

type AccountInferenceRule struct{}

func (r *AccountInferenceRule) Matches(config map[string]interface{}) bool {
	_, hasAccountID := config["account_id"]
	_, hasAccountIDs := config["account_ids"]
	_, hasIncludeMuxed := config["include_muxed"]
	
	return hasAccountID || hasAccountIDs || hasIncludeMuxed
}

func (r *AccountInferenceRule) InferType() string {
	return "AccountDataFilter"
}

func (r *AccountInferenceRule) Priority() int { return 100 }

func (r *AccountInferenceRule) Description() string {
	return "Infers account processor from account configuration"
}

type TransformInferenceRule struct{}

func (r *TransformInferenceRule) Matches(config map[string]interface{}) bool {
	_, hasFormat := config["format"]
	_, hasTransform := config["transform"]
	
	return hasFormat || hasTransform
}

func (r *TransformInferenceRule) InferType() string {
	return "TransformToAppPayment" // Default transform
}

func (r *TransformInferenceRule) Priority() int { return 50 }

func (r *TransformInferenceRule) Description() string {
	return "Infers transform processor from format configuration"
}

// Consumer Inference Rules

type DatabaseInferenceRule struct{}

func (r *DatabaseInferenceRule) Matches(config map[string]interface{}) bool {
	_, hasConnection := config["connection"]
	_, hasConnectionString := config["connection_string"]
	_, hasTable := config["table"]
	_, hasDatabase := config["database"]
	
	return hasConnection || hasConnectionString || hasTable || hasDatabase
}

func (r *DatabaseInferenceRule) InferType() string {
	// Default database type - specific type inference happens during transformation
	return "SaveToPostgreSQL"
}

func (r *DatabaseInferenceRule) Priority() int { return 100 }

func (r *DatabaseInferenceRule) Description() string {
	return "Infers database consumer from connection configuration"
}

type FileOutputInferenceRule struct{}

func (r *FileOutputInferenceRule) Matches(config map[string]interface{}) bool {
	_, hasCompression := config["compression"]
	_, hasPartition := config["partition_by"]
	_, hasRotation := config["rotation_interval_minutes"]
	
	return hasCompression || hasPartition || hasRotation
}

func (r *FileOutputInferenceRule) InferType() string {
	return "SaveToParquet" // Default file output
}

func (r *FileOutputInferenceRule) Priority() int { return 80 }

func (r *FileOutputInferenceRule) Description() string {
	return "Infers file output consumer from file configuration"
}

type MessagingInferenceRule struct{}

func (r *MessagingInferenceRule) Matches(config map[string]interface{}) bool {
	_, hasAddress := config["address"]
	_, hasTopic := config["topic"]
	_, hasChannel := config["channel"]
	
	return hasAddress || hasTopic || hasChannel
}

func (r *MessagingInferenceRule) InferType() string {
	return "SaveToZeroMQ" // Default messaging
}

func (r *MessagingInferenceRule) Priority() int { return 80 }

func (r *MessagingInferenceRule) Description() string {
	return "Infers messaging consumer from address configuration"
}