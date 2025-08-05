package v2

import (
	"fmt"
	"strings"
)

// ValidationError represents a configuration validation error with helpful information
type ValidationError struct {
	Field       string
	Value       interface{}
	Problem     string
	Suggestion  string
	ValidValues []string
}

// Error implements the error interface
func (e ValidationError) Error() string {
	var msg strings.Builder
	msg.WriteString(fmt.Sprintf("\nError in configuration:\n\n"))
	msg.WriteString(fmt.Sprintf("  %s: %v  # <-- %s\n\n", e.Field, e.Value, e.Problem))
	
	if e.Suggestion != "" {
		msg.WriteString(fmt.Sprintf("Did you mean '%s'?\n\n", e.Suggestion))
	}
	
	if len(e.ValidValues) > 0 {
		msg.WriteString("Valid options:\n")
		for _, v := range e.ValidValues {
			msg.WriteString(fmt.Sprintf("  - %s\n", v))
		}
		msg.WriteString("\n")
	}
	
	return msg.String()
}

// ValidationResult holds multiple validation errors
type ValidationResult struct {
	Errors   []error
	Warnings []string
}

// HasErrors returns true if there are any errors
func (r *ValidationResult) HasErrors() bool {
	return len(r.Errors) > 0
}

// AddError adds a validation error
func (r *ValidationResult) AddError(err error) {
	r.Errors = append(r.Errors, err)
}

// AddWarning adds a validation warning
func (r *ValidationResult) AddWarning(warning string) {
	r.Warnings = append(r.Warnings, warning)
}

// Validator provides configuration validation with helpful error messages
type Validator struct {
	resolver       *AliasResolver
	defaultsEngine *DefaultsEngine
}

// NewValidator creates a new configuration validator
func NewValidator(resolver *AliasResolver, defaultsEngine *DefaultsEngine) *Validator {
	return &Validator{
		resolver:       resolver,
		defaultsEngine: defaultsEngine,
	}
}

// Validate validates a complete configuration
func (v *Validator) Validate(config map[string]interface{}) *ValidationResult {
	result := &ValidationResult{}
	
	// Check format
	format := DetectFormat(config)
	if format == FormatUnknown {
		result.AddError(fmt.Errorf("unknown configuration format: expected 'pipelines' key (legacy) or 'source'/'process'/'save_to' keys (v2)"))
		return result
	}
	
	if format == FormatLegacy {
		v.validateLegacyConfig(config, result)
	} else {
		v.validateV2Config(config, result)
	}
	
	return result
}

// validateV2Config validates a v2 format configuration
func (v *Validator) validateV2Config(config map[string]interface{}, result *ValidationResult) {
	// Validate source
	if source, ok := config["source"].(map[string]interface{}); ok {
		v.validateSource(source, result)
	} else if source, ok := config["source"].(string); ok {
		// Simple string source
		v.validateStringSource(source, result)
	} else if _, hasSource := config["source"]; !hasSource {
		result.AddWarning("No source specified - pipeline will need external data input")
	}
	
	// Validate processors
	if process, ok := config["process"]; ok {
		v.validateProcessors(process, result)
	}
	
	// Validate consumers
	if saveTo, ok := config["save_to"]; ok {
		v.validateConsumers(saveTo, result)
	} else {
		result.AddWarning("No 'save_to' specified - pipeline output will not be persisted")
	}
}

// validateLegacyConfig validates a legacy format configuration
func (v *Validator) validateLegacyConfig(config map[string]interface{}, result *ValidationResult) {
	pipelines, ok := config["pipelines"].(map[string]interface{})
	if !ok {
		result.AddError(fmt.Errorf("invalid legacy config: 'pipelines' must be a map"))
		return
	}
	
	for name, pipeline := range pipelines {
		v.validateLegacyPipeline(name, pipeline, result)
	}
}

// validateLegacyPipeline validates a single legacy pipeline
func (v *Validator) validateLegacyPipeline(name string, pipeline interface{}, result *ValidationResult) {
	pipelineMap, ok := pipeline.(map[string]interface{})
	if !ok {
		result.AddError(fmt.Errorf("pipeline '%s' must be a map", name))
		return
	}
	
	// Validate source
	if source, ok := pipelineMap["source"].(map[string]interface{}); ok {
		if typ, ok := source["type"].(string); ok {
			if !IsKnownLegacyType(typ) {
				result.AddWarning(fmt.Sprintf("Pipeline '%s': Unknown source type '%s'", name, typ))
			}
		} else {
			result.AddError(fmt.Errorf("pipeline '%s': source must have a 'type' field", name))
		}
	}
}

// validateSource validates a source configuration
func (v *Validator) validateSource(source map[string]interface{}, result *ValidationResult) {
	// Check for unknown fields
	knownFields := getKnownFieldsForComponent("source")
	for field := range source {
		if !isKnownField(field, knownFields) {
			// Check if it's a typo
			suggestion := v.findSimilarField(field, knownFields)
			result.AddError(ValidationError{
				Field:      fmt.Sprintf("source.%s", field),
				Value:      source[field],
				Problem:    "unknown field",
				Suggestion: suggestion,
			})
		}
	}
	
	// Validate specific source types
	if sourceType, err := v.inferSourceType(source); err == nil {
		v.validateSourceType(sourceType, source, result)
	}
}

// validateStringSource validates a simple string source
func (v *Validator) validateStringSource(source string, result *ValidationResult) {
	// Check if it's a protocol URL (future Phase 3)
	if strings.Contains(source, "://") {
		parts := strings.SplitN(source, "://", 2)
		protocol := parts[0]
		
		validProtocols := []string{"stellar", "core", "s3", "gs", "gcs", "file", "fs"}
		valid := false
		for _, p := range validProtocols {
			if protocol == p {
				valid = true
				break
			}
		}
		
		if !valid {
			result.AddError(ValidationError{
				Field:       "source",
				Value:       source,
				Problem:     fmt.Sprintf("unknown protocol '%s'", protocol),
				ValidValues: validProtocols,
			})
		}
	} else {
		// Check if it's a valid alias
		if _, found := v.resolver.ResolveSourceType(source); !found {
			suggestions := v.resolver.GetSimilarSource(source)
			result.AddError(ValidationError{
				Field:      "source",
				Value:      source,
				Problem:    "unknown source type",
				Suggestion: strings.Join(suggestions, " or "),
			})
		}
	}
}

// validateProcessors validates processor configurations
func (v *Validator) validateProcessors(process interface{}, result *ValidationResult) {
	switch p := process.(type) {
	case string:
		// Single processor as string
		if _, found := v.resolver.ResolveProcessorType(p); !found {
			suggestions := v.resolver.GetSimilarProcessor(p)
			result.AddError(ValidationError{
				Field:      "process",
				Value:      p,
				Problem:    "unknown processor type",
				Suggestion: strings.Join(suggestions, " or "),
			})
		}
		
	case []interface{}:
		// List of processors
		for i, proc := range p {
			v.validateProcessor(i, proc, result)
		}
		
	case map[string]interface{}:
		// Single processor as map
		v.validateProcessor(0, p, result)
		
	default:
		result.AddError(fmt.Errorf("'process' must be a string, list, or map"))
	}
}

// validateProcessor validates a single processor
func (v *Validator) validateProcessor(index int, processor interface{}, result *ValidationResult) {
	switch p := processor.(type) {
	case string:
		// Simple processor name
		if _, found := v.resolver.ResolveProcessorType(p); !found {
			suggestions := v.resolver.GetSimilarProcessor(p)
			result.AddError(ValidationError{
				Field:      fmt.Sprintf("process[%d]", index),
				Value:      p,
				Problem:    "unknown processor type",
				Suggestion: strings.Join(suggestions, " or "),
			})
		}
		
	case map[string]interface{}:
		// Processor with config
		for name, config := range p {
			if _, found := v.resolver.ResolveProcessorType(name); !found {
				suggestions := v.resolver.GetSimilarProcessor(name)
				result.AddError(ValidationError{
					Field:      fmt.Sprintf("process[%d]", index),
					Value:      name,
					Problem:    "unknown processor type",
					Suggestion: strings.Join(suggestions, " or "),
				})
			}
			
			// Validate processor config
			if configMap, ok := config.(map[string]interface{}); ok {
				v.validateProcessorConfig(name, configMap, result)
			}
		}
	}
}

// validateConsumers validates consumer configurations  
func (v *Validator) validateConsumers(saveTo interface{}, result *ValidationResult) {
	switch s := saveTo.(type) {
	case string:
		// Single consumer as string
		v.validateStringConsumer(s, result)
		
	case []interface{}:
		// List of consumers
		for i, consumer := range s {
			v.validateConsumer(i, consumer, result)
		}
		
	case map[string]interface{}:
		// Single consumer as map
		v.validateConsumer(0, s, result)
		
	default:
		result.AddError(fmt.Errorf("'save_to' must be a string, list, or map"))
	}
}

// validateStringConsumer validates a string consumer (e.g., "postgres://...")
func (v *Validator) validateStringConsumer(consumer string, result *ValidationResult) {
	// Handle environment variables
	if strings.HasPrefix(consumer, "${") && strings.HasSuffix(consumer, "}") {
		// This is an environment variable - it's valid
		return
	}
	
	// Check for protocol-style consumer
	if strings.Contains(consumer, "://") {
		parts := strings.SplitN(consumer, "://", 2)
		consumerType := parts[0]
		
		if _, found := v.resolver.ResolveConsumerType(consumerType); !found {
			suggestions := v.resolver.GetSimilarConsumer(consumerType)
			result.AddError(ValidationError{
				Field:      "save_to",
				Value:      consumer,
				Problem:    fmt.Sprintf("unknown consumer type '%s'", consumerType),
				Suggestion: strings.Join(suggestions, " or "),
			})
		}
	} else {
		// Simple consumer name
		if _, found := v.resolver.ResolveConsumerType(consumer); !found {
			suggestions := v.resolver.GetSimilarConsumer(consumer)
			result.AddError(ValidationError{
				Field:      "save_to",
				Value:      consumer,
				Problem:    "unknown consumer type",
				Suggestion: strings.Join(suggestions, " or "),
			})
		}
	}
}

// validateConsumer validates a single consumer
func (v *Validator) validateConsumer(index int, consumer interface{}, result *ValidationResult) {
	switch c := consumer.(type) {
	case string:
		v.validateStringConsumer(c, result)
		
	case map[string]interface{}:
		// Consumer with config
		for name, config := range c {
			if _, found := v.resolver.ResolveConsumerType(name); !found {
				suggestions := v.resolver.GetSimilarConsumer(name)
				result.AddError(ValidationError{
					Field:      fmt.Sprintf("save_to[%d]", index),
					Value:      name,
					Problem:    "unknown consumer type",
					Suggestion: strings.Join(suggestions, " or "),
				})
			}
			
			// Validate consumer config
			if configMap, ok := config.(map[string]interface{}); ok {
				v.validateConsumerConfig(name, configMap, result)
			}
		}
	}
}

// Helper methods

func (v *Validator) inferSourceType(source map[string]interface{}) (string, error) {
	// This would use the inference engine in production
	if typ, ok := source["type"].(string); ok {
		return typ, nil
	}
	return "", fmt.Errorf("cannot infer source type")
}

func (v *Validator) validateSourceType(sourceType string, config map[string]interface{}, result *ValidationResult) {
	// Apply component-specific validation
	errors := v.defaultsEngine.ValidateDefaults(sourceType, config)
	for _, err := range errors {
		result.AddError(err)
	}
}

func (v *Validator) validateProcessorConfig(processorType string, config map[string]interface{}, result *ValidationResult) {
	// Resolve field aliases
	resolved := v.resolver.ResolveFieldNames(config)
	
	// Check for processor-specific required fields
	switch processorType {
	case "payment_filter", "FilterPayments":
		// No required fields, but validate amounts if present
		if min, ok := resolved["min_amount"]; ok {
			v.validateAmount(min, "min_amount", result)
		}
		if max, ok := resolved["max_amount"]; ok {
			v.validateAmount(max, "max_amount", result)
		}
	}
}

func (v *Validator) validateConsumerConfig(consumerType string, config map[string]interface{}, result *ValidationResult) {
	// Resolve field aliases
	resolved := v.resolver.ResolveFieldNames(config)
	
	// Apply component-specific validation
	errors := v.defaultsEngine.ValidateDefaults(consumerType, resolved)
	for _, err := range errors {
		result.AddError(err)
	}
}

func (v *Validator) validateAmount(value interface{}, field string, result *ValidationResult) {
	switch value.(type) {
	case int, int64, float64:
		// Valid numeric amount
	case string:
		// Should be parseable as number
		// In production, we'd parse and validate
	default:
		result.AddError(fmt.Errorf("%s must be a number or numeric string, got %T", field, value))
	}
}

func (v *Validator) findSimilarField(field string, knownFields []string) string {
	// Simple similarity check
	field = strings.ToLower(field)
	for _, known := range knownFields {
		knownLower := strings.ToLower(known)
		if strings.Contains(knownLower, field) || strings.Contains(field, knownLower) {
			return known
		}
	}
	return ""
}

// Helper functions

func getKnownFieldsForComponent(componentType string) []string {
	// In production, this would be more comprehensive
	switch componentType {
	case "source":
		return []string{
			"type", "bucket", "bucket_name", "path", "path_prefix",
			"network", "net", "workers", "num_workers", "region",
			"endpoint", "binary_path", "history_archive_urls",
			"stellar", "ledgers", "start_ledger", "end_ledger",
			"stellar_core", "captive_core", "core", "rpc", 
			"soroban", "s3", "gcs", "file", "fs",
		}
	default:
		return []string{}
	}
}

func isKnownField(field string, knownFields []string) bool {
	for _, known := range knownFields {
		if field == known {
			return true
		}
	}
	return false
}