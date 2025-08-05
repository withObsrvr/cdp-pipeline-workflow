package v2

import (
	"fmt"
	"strings"
)

// FormatVersion represents the configuration format version
type FormatVersion int

const (
	FormatUnknown FormatVersion = iota
	FormatLegacy
	FormatV2
)

// DetectFormat determines whether a configuration is legacy or v2 format
func DetectFormat(config map[string]interface{}) FormatVersion {
	// Legacy format always has a top-level "pipelines" key
	if _, hasPipelines := config["pipelines"]; hasPipelines {
		return FormatLegacy
	}
	
	// V2 format has source/process/save_to at the top level
	_, hasSource := config["source"]
	_, hasProcess := config["process"]
	_, hasSaveTo := config["save_to"]
	
	if hasSource || hasProcess || hasSaveTo {
		return FormatV2
	}
	
	return FormatUnknown
}

// LegacyConfig represents the legacy configuration format
type LegacyConfig struct {
	Pipelines map[string]LegacyPipeline `yaml:"pipelines"`
}

// LegacyPipeline represents a legacy pipeline configuration
type LegacyPipeline struct {
	Source     LegacyComponent   `yaml:"source"`
	Processors []LegacyComponent `yaml:"processors"`
	Consumers  []LegacyComponent `yaml:"consumers"`
}

// LegacyComponent represents a legacy component configuration
type LegacyComponent struct {
	Type   string                 `yaml:"type"`
	Config map[string]interface{} `yaml:"config"`
}

// LoadLegacyConfig loads a legacy configuration without transformation
func LoadLegacyConfig(config map[string]interface{}) (*LegacyConfig, error) {
	// For now, just validate that it's a valid legacy config
	pipelines, ok := config["pipelines"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid legacy config: missing or invalid 'pipelines' key")
	}
	
	// Basic validation
	for name := range pipelines {
		if name == "" {
			return nil, fmt.Errorf("invalid legacy config: empty pipeline name")
		}
	}
	
	// Return a placeholder - in production, this would do full parsing
	return &LegacyConfig{}, nil
}

// CompatibilityLayer provides backward compatibility for legacy configs
type CompatibilityLayer struct {
	showWarnings bool
}

// NewCompatibilityLayer creates a new compatibility layer
func NewCompatibilityLayer(showWarnings bool) *CompatibilityLayer {
	return &CompatibilityLayer{
		showWarnings: showWarnings,
	}
}

// CheckDeprecations checks for deprecated patterns and returns warnings
func (c *CompatibilityLayer) CheckDeprecations(config map[string]interface{}) []string {
	var warnings []string
	
	if !c.showWarnings {
		return warnings
	}
	
	// Check for deprecated type names
	deprecatedTypes := map[string]string{
		"KafkaInboundAdapter": "Consider using the new Kafka source with protocol handler (Phase 3)",
		"SaveToKafka":         "Consider using the new Kafka consumer with protocol handler (Phase 3)",
	}
	
	// Check source
	if source, ok := config["source"].(map[string]interface{}); ok {
		if typ, ok := source["type"].(string); ok {
			if msg, deprecated := deprecatedTypes[typ]; deprecated {
				warnings = append(warnings, fmt.Sprintf("Source type '%s' is deprecated: %s", typ, msg))
			}
		}
	}
	
	// Check processors
	if processors, ok := config["processors"].([]interface{}); ok {
		for i, proc := range processors {
			if procMap, ok := proc.(map[string]interface{}); ok {
				if typ, ok := procMap["type"].(string); ok {
					if msg, deprecated := deprecatedTypes[typ]; deprecated {
						warnings = append(warnings, fmt.Sprintf("Processor[%d] type '%s' is deprecated: %s", i, typ, msg))
					}
				}
			}
		}
	}
	
	// Check for overly verbose configurations
	if CountConfigLines(config) > 50 {
		warnings = append(warnings, "This configuration could be simplified by 60-80% using the new v2 format. Run 'flowctl config upgrade' to convert.")
	}
	
	return warnings
}

// CountConfigLines estimates the number of lines in a configuration
func CountConfigLines(config map[string]interface{}) int {
	// Simple estimation based on number of keys and nesting
	return countConfigLinesRecursive(config, 0)
}

func countConfigLinesRecursive(v interface{}, depth int) int {
	switch val := v.(type) {
	case map[string]interface{}:
		lines := len(val) // Each key is roughly one line
		for _, v := range val {
			lines += countConfigLinesRecursive(v, depth+1)
		}
		return lines
	case []interface{}:
		lines := len(val)
		for _, item := range val {
			lines += countConfigLinesRecursive(item, depth+1)
		}
		return lines
	default:
		return 0
	}
}

// MigrationReport provides information about config migration
type MigrationReport struct {
	OriginalLines   int
	SimplifiedLines int
	ReductionPct    float64
	Warnings        []string
	Suggestions     []string
}

// AnalyzeForMigration analyzes a legacy config for migration opportunities
func (c *CompatibilityLayer) AnalyzeForMigration(config map[string]interface{}) *MigrationReport {
	report := &MigrationReport{
		OriginalLines: CountConfigLines(config),
	}
	
	// Estimate simplified lines (very rough estimation)
	// V2 configs are typically 60-80% smaller
	report.SimplifiedLines = int(float64(report.OriginalLines) * 0.3)
	report.ReductionPct = float64(report.OriginalLines-report.SimplifiedLines) / float64(report.OriginalLines) * 100
	
	// Add suggestions based on config patterns
	if pipelines, ok := config["pipelines"].(map[string]interface{}); ok {
		if len(pipelines) == 1 {
			report.Suggestions = append(report.Suggestions, "Single pipeline configs can omit the pipeline name in v2 format")
		}
		
		for _, pipeline := range pipelines {
			if pipelineMap, ok := pipeline.(map[string]interface{}); ok {
				// Check for default values that could be omitted
				if source, ok := pipelineMap["source"].(map[string]interface{}); ok {
					if sourceConfig, ok := source["config"].(map[string]interface{}); ok {
						if workers, ok := sourceConfig["num_workers"].(float64); ok && workers == 20 {
							report.Suggestions = append(report.Suggestions, "Default values like num_workers=20 can be omitted")
						}
					}
				}
			}
		}
	}
	
	report.Warnings = c.CheckDeprecations(config)
	
	return report
}

// IsKnownLegacyType checks if a type string is a known legacy component type
func IsKnownLegacyType(typ string) bool {
	// List of all known legacy types
	knownTypes := []string{
		// Sources
		"BufferedStorageSourceAdapter",
		"S3BufferedStorageSourceAdapter",
		"FSBufferedStorageSourceAdapter",
		"CaptiveCoreInboundAdapter",
		"RPCSourceAdapter",
		"SorobanSourceAdapter",
		// Processors
		"FilterPayments",
		"TransformToAppPayment",
		"AccountData",
		"CreateAccount",
		"AccountTransaction",
		"ContractData",
		"ContractInvocation",
		"ContractEvent",
		// ... add more as needed
		// Consumers
		"SaveToParquet",
		"SaveToPostgreSQL",
		"SaveToMongoDB",
		"SaveToDuckDB",
		"SaveToRedis",
		"SaveToZeroMQ",
	}
	
	for _, known := range knownTypes {
		if typ == known {
			return true
		}
	}
	
	return false
}

// SuggestV2Equivalent suggests the v2 equivalent for a legacy type
func SuggestV2Equivalent(legacyType string) string {
	// This would use the alias resolver to find the best alias
	suggestions := map[string]string{
		"BufferedStorageSourceAdapter": "bucket",
		"CaptiveCoreInboundAdapter":    "stellar",
		"FilterPayments":               "payment_filter",
		"SaveToPostgreSQL":             "postgres",
		"SaveToParquet":                "parquet",
	}
	
	if v2, ok := suggestions[legacyType]; ok {
		return v2
	}
	
	// Try to simplify the name
	simplified := strings.TrimPrefix(legacyType, "SaveTo")
	simplified = strings.TrimPrefix(simplified, "TransformTo")
	simplified = strings.TrimSuffix(simplified, "Adapter")
	simplified = strings.ToLower(simplified)
	
	return simplified
}