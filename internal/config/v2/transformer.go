package v2

import (
	"fmt"
	"os"
	"strings"
)

// TransformedConfig represents the internal configuration format
type TransformedConfig struct {
	Pipelines map[string]TransformedPipeline
}

// TransformedPipeline represents an internal pipeline configuration
type TransformedPipeline struct {
	Source     TransformedComponent
	Processors []TransformedComponent
	Consumers  []TransformedComponent
}

// TransformedComponent represents an internal component configuration
type TransformedComponent struct {
	Type   string
	Config map[string]interface{}
}

// Transformer transforms v2 configurations to internal format
type Transformer struct {
	resolver    *AliasResolver
	inferencer  *InferenceEngine
	defaults    *DefaultsEngine
}

// NewTransformer creates a new configuration transformer
func NewTransformer(resolver *AliasResolver, inferencer *InferenceEngine, defaults *DefaultsEngine) *Transformer {
	return &Transformer{
		resolver:   resolver,
		inferencer: inferencer,
		defaults:   defaults,
	}
}

// Transform transforms a v2 configuration to internal format
func (t *Transformer) Transform(config map[string]interface{}) (*TransformedConfig, error) {
	// Expand environment variables first
	expanded := t.expandEnvVars(config)
	
	// Type assert to map[string]interface{}
	expandedMap, ok := expanded.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expanded configuration is not a map")
	}
	
	// Detect format
	format := DetectFormat(expandedMap)
	
	switch format {
	case FormatLegacy:
		// Pass through legacy format with minimal changes
		return t.transformLegacy(expandedMap)
	case FormatV2:
		// Transform v2 format
		return t.transformV2(expandedMap)
	default:
		return nil, fmt.Errorf("unknown configuration format")
	}
}

// transformLegacy handles legacy format configurations
func (t *Transformer) transformLegacy(config map[string]interface{}) (*TransformedConfig, error) {
	result := &TransformedConfig{
		Pipelines: make(map[string]TransformedPipeline),
	}
	
	pipelines, ok := config["pipelines"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid legacy configuration: missing pipelines")
	}
	
	for name, pipeline := range pipelines {
		transformed, err := t.transformLegacyPipeline(pipeline)
		if err != nil {
			return nil, fmt.Errorf("pipeline '%s': %w", name, err)
		}
		result.Pipelines[name] = transformed
	}
	
	return result, nil
}

// transformLegacyPipeline transforms a single legacy pipeline
func (t *Transformer) transformLegacyPipeline(pipeline interface{}) (TransformedPipeline, error) {
	pipelineMap, ok := pipeline.(map[string]interface{})
	if !ok {
		return TransformedPipeline{}, fmt.Errorf("pipeline must be a map")
	}
	
	var result TransformedPipeline
	
	// Transform source
	if source, ok := pipelineMap["source"].(map[string]interface{}); ok {
		transformedSource, err := t.transformLegacyComponent(source)
		if err != nil {
			return result, fmt.Errorf("source: %w", err)
		}
		result.Source = transformedSource
	}
	
	// Transform processors
	if processors, ok := pipelineMap["processors"].([]interface{}); ok {
		for i, proc := range processors {
			if procMap, ok := proc.(map[string]interface{}); ok {
				transformed, err := t.transformLegacyComponent(procMap)
				if err != nil {
					return result, fmt.Errorf("processor[%d]: %w", i, err)
				}
				result.Processors = append(result.Processors, transformed)
			}
		}
	}
	
	// Transform consumers
	if consumers, ok := pipelineMap["consumers"].([]interface{}); ok {
		for i, cons := range consumers {
			if consMap, ok := cons.(map[string]interface{}); ok {
				transformed, err := t.transformLegacyComponent(consMap)
				if err != nil {
					return result, fmt.Errorf("consumer[%d]: %w", i, err)
				}
				result.Consumers = append(result.Consumers, transformed)
			}
		}
	}
	
	return result, nil
}

// transformLegacyComponent transforms a single legacy component
func (t *Transformer) transformLegacyComponent(component map[string]interface{}) (TransformedComponent, error) {
	typ, ok := component["type"].(string)
	if !ok {
		return TransformedComponent{}, fmt.Errorf("missing type field")
	}
	
	config, _ := component["config"].(map[string]interface{})
	if config == nil {
		config = make(map[string]interface{})
	}
	
	// Apply environment variable expansion to config
	expandedConfig := t.expandEnvVars(config).(map[string]interface{})
	
	return TransformedComponent{
		Type:   typ,
		Config: expandedConfig,
	}, nil
}

// transformV2 transforms a v2 format configuration
func (t *Transformer) transformV2(config map[string]interface{}) (*TransformedConfig, error) {
	// V2 configs have an implicit single pipeline
	pipeline, err := t.transformV2Pipeline(config)
	if err != nil {
		return nil, err
	}
	
	// Use a default pipeline name
	return &TransformedConfig{
		Pipelines: map[string]TransformedPipeline{
			"default": pipeline,
		},
	}, nil
}

// transformV2Pipeline transforms a v2 pipeline configuration
func (t *Transformer) transformV2Pipeline(config map[string]interface{}) (TransformedPipeline, error) {
	var result TransformedPipeline
	
	// Transform source
	var sourceNetwork string
	if source, ok := config["source"]; ok {
		transformed, err := t.transformV2Source(source)
		if err != nil {
			return result, fmt.Errorf("source: %w", err)
		}
		result.Source = transformed
		
		// Extract network from source for processors
		if network, ok := result.Source.Config["network"].(string); ok {
			sourceNetwork = network
		}
	}
	
	// Transform processors with network context
	if process, ok := config["process"]; ok {
		processors, err := t.transformV2ProcessorsWithNetwork(process, sourceNetwork)
		if err != nil {
			return result, fmt.Errorf("process: %w", err)
		}
		result.Processors = processors
	}
	
	// Transform consumers
	if saveTo, ok := config["save_to"]; ok {
		consumers, err := t.transformV2Consumers(saveTo)
		if err != nil {
			return result, fmt.Errorf("save_to: %w", err)
		}
		result.Consumers = consumers
	}
	
	return result, nil
}

// transformV2Source transforms a v2 source configuration
func (t *Transformer) transformV2Source(source interface{}) (TransformedComponent, error) {
	switch s := source.(type) {
	case string:
		// Simple string source (e.g., "stellar://mainnet")
		return t.transformStringSource(s)
		
	case map[string]interface{}:
		// Map source configuration
		return t.transformMapSource(s)
		
	default:
		return TransformedComponent{}, fmt.Errorf("source must be a string or map")
	}
}

// transformStringSource transforms a string source specification
func (t *Transformer) transformStringSource(source string) (TransformedComponent, error) {
	// Check for protocol-style source
	if strings.Contains(source, "://") {
		parts := strings.SplitN(source, "://", 2)
		protocol := parts[0]
		path := parts[1]
		
		// Map protocol to source type
		var sourceType string
		config := make(map[string]interface{})
		
		switch protocol {
		case "stellar", "core":
			sourceType = "CaptiveCoreInboundAdapter"
			config["network"] = path
			
		case "s3":
			sourceType = "S3BufferedStorageSourceAdapter"
			config["bucket_name"] = path
			
		case "gs", "gcs":
			sourceType = "BufferedStorageSourceAdapter"
			config["bucket_name"] = path
			
		case "file", "fs":
			sourceType = "FSBufferedStorageSourceAdapter"
			config["path"] = path
			
		default:
			return TransformedComponent{}, fmt.Errorf("unknown source protocol: %s", protocol)
		}
		
		// Apply defaults
		config = t.defaults.ApplyDefaults(sourceType, config)
		
		return TransformedComponent{
			Type:   sourceType,
			Config: config,
		}, nil
	}
	
	// Check if it's an alias
	sourceType, _ := t.resolver.ResolveSourceType(source)
	config := t.defaults.ApplyDefaults(sourceType, make(map[string]interface{}))
	
	return TransformedComponent{
		Type:   sourceType,
		Config: config,
	}, nil
}

// transformMapSource transforms a map source configuration
func (t *Transformer) transformMapSource(source map[string]interface{}) (TransformedComponent, error) {
	// Special handling for simplified stellar source
	if network, ok := source["stellar"].(string); ok {
		config := map[string]interface{}{
			"network": network,
		}
		// Handle ledger range if specified
		if ledgers, ok := source["ledgers"].(string); ok {
			// Parse ledger range (e.g., "1000000-2000000")
			if strings.Contains(ledgers, "-") {
				parts := strings.Split(ledgers, "-")
				if len(parts) == 2 {
					config["start_ledger"] = parts[0]
					config["end_ledger"] = parts[1]
				}
			} else {
				config["start_ledger"] = ledgers
			}
		}
		
		// Apply defaults
		config = t.defaults.ApplyDefaults("CaptiveCoreInboundAdapter", config)
		
		return TransformedComponent{
			Type:   "CaptiveCoreInboundAdapter",
			Config: config,
		}, nil
	}
	
	// Resolve field aliases
	resolved := t.resolver.ResolveFieldNames(source)
	
	// Infer source type
	sourceType, err := t.inferencer.InferSourceType(resolved)
	if err != nil {
		return TransformedComponent{}, err
	}
	
	// Apply defaults
	config := t.defaults.ApplyDefaults(sourceType, resolved)
	
	// Remove 'type' field if present (not needed in config)
	delete(config, "type")
	
	return TransformedComponent{
		Type:   sourceType,
		Config: config,
	}, nil
}

// transformV2ProcessorsWithNetwork transforms v2 processor configurations with network context
func (t *Transformer) transformV2ProcessorsWithNetwork(process interface{}, network string) ([]TransformedComponent, error) {
	components, err := t.transformV2Processors(process)
	if err != nil {
		return nil, err
	}
	
	// Apply network to processors that need it
	for i := range components {
		if network != "" {
			// Add network to config so defaults engine can use it
			if _, hasNetwork := components[i].Config["network"]; !hasNetwork {
				components[i].Config["network"] = network
			}
			// Apply network-aware defaults (this will inject network_passphrase)
			components[i].Config = t.defaults.ApplyDefaults(components[i].Type, components[i].Config)
		}
	}
	
	return components, nil
}

// transformV2Processors transforms v2 processor configurations
func (t *Transformer) transformV2Processors(process interface{}) ([]TransformedComponent, error) {
	var result []TransformedComponent
	
	switch p := process.(type) {
	case string:
		// Single processor as string
		component, err := t.transformStringProcessor(p)
		if err != nil {
			return nil, err
		}
		result = append(result, component)
		
	case []interface{}:
		// List of processors
		for i, proc := range p {
			component, err := t.transformV2Processor(proc)
			if err != nil {
				return nil, fmt.Errorf("processor[%d]: %w", i, err)
			}
			result = append(result, component)
		}
		
	case map[string]interface{}:
		// Single processor as map
		component, err := t.transformMapProcessor(p)
		if err != nil {
			return nil, err
		}
		result = append(result, component)
		
	default:
		return nil, fmt.Errorf("process must be a string, list, or map")
	}
	
	return result, nil
}

// transformV2Processor transforms a single v2 processor
func (t *Transformer) transformV2Processor(processor interface{}) (TransformedComponent, error) {
	switch p := processor.(type) {
	case string:
		return t.transformStringProcessor(p)
		
	case map[string]interface{}:
		// Processor with config (e.g., {payment_filter: {min: 100}})
		if len(p) == 1 {
			for name, config := range p {
				return t.transformNamedProcessor(name, config)
			}
		}
		// Direct map processor
		return t.transformMapProcessor(p)
		
	default:
		return TransformedComponent{}, fmt.Errorf("processor must be a string or map")
	}
}

// transformStringProcessor transforms a string processor specification
func (t *Transformer) transformStringProcessor(name string) (TransformedComponent, error) {
	// Resolve alias
	processorType, _ := t.resolver.ResolveProcessorType(name)
	config := t.defaults.ApplyDefaults(processorType, make(map[string]interface{}))
	
	return TransformedComponent{
		Type:   processorType,
		Config: config,
	}, nil
}

// transformNamedProcessor transforms a named processor with config
func (t *Transformer) transformNamedProcessor(name string, config interface{}) (TransformedComponent, error) {
	// Resolve processor type
	processorType, _ := t.resolver.ResolveProcessorType(name)
	
	// Handle config
	configMap := make(map[string]interface{})
	if config != nil {
		if cm, ok := config.(map[string]interface{}); ok {
			// Resolve field aliases
			configMap = t.resolver.ResolveFieldNames(cm)
		}
	}
	
	// Apply defaults
	configMap = t.defaults.ApplyDefaults(processorType, configMap)
	
	return TransformedComponent{
		Type:   processorType,
		Config: configMap,
	}, nil
}

// transformMapProcessor transforms a map processor configuration
func (t *Transformer) transformMapProcessor(processor map[string]interface{}) (TransformedComponent, error) {
	// Resolve field aliases
	resolved := t.resolver.ResolveFieldNames(processor)
	
	// Try to get type from 'type' field or infer it
	var processorType string
	if typ, ok := resolved["type"].(string); ok {
		processorType = typ
		delete(resolved, "type")
	} else {
		// Try to infer type
		inferred, err := t.inferencer.InferProcessorType("", resolved)
		if err != nil {
			return TransformedComponent{}, err
		}
		processorType = inferred
	}
	
	// Apply defaults
	config := t.defaults.ApplyDefaults(processorType, resolved)
	
	return TransformedComponent{
		Type:   processorType,
		Config: config,
	}, nil
}

// transformV2Consumers transforms v2 consumer configurations
func (t *Transformer) transformV2Consumers(saveTo interface{}) ([]TransformedComponent, error) {
	var result []TransformedComponent
	
	switch s := saveTo.(type) {
	case string:
		// Single consumer as string
		component, err := t.transformStringConsumer(s)
		if err != nil {
			return nil, err
		}
		result = append(result, component)
		
	case []interface{}:
		// List of consumers
		for i, consumer := range s {
			component, err := t.transformV2Consumer(consumer)
			if err != nil {
				return nil, fmt.Errorf("consumer[%d]: %w", i, err)
			}
			result = append(result, component)
		}
		
	case map[string]interface{}:
		// Check if this is a named consumer map (e.g., {parquet: "path"})
		// or a single consumer config map (e.g., {type: "SaveToParquet", config: {...}})
		if len(s) == 1 && !hasConfigFields(s) {
			// This looks like a named consumer map
			component, err := t.transformV2Consumer(s)
			if err != nil {
				return nil, err
			}
			result = append(result, component)
		} else {
			// This is a direct config map
			component, err := t.transformMapConsumer(s)
			if err != nil {
				return nil, err
			}
			result = append(result, component)
		}
		
	default:
		return nil, fmt.Errorf("save_to must be a string, list, or map")
	}
	
	return result, nil
}

// hasConfigFields checks if a map contains typical configuration fields
func hasConfigFields(m map[string]interface{}) bool {
	configFields := []string{"type", "connection_string", "path", "bucket_name", "address", "table"}
	for _, field := range configFields {
		if _, ok := m[field]; ok {
			return true
		}
	}
	return false
}

// transformV2Consumer transforms a single v2 consumer
func (t *Transformer) transformV2Consumer(consumer interface{}) (TransformedComponent, error) {
	switch c := consumer.(type) {
	case string:
		return t.transformStringConsumer(c)
		
	case map[string]interface{}:
		// Consumer with config
		if len(c) == 1 {
			for name, config := range c {
				// Handle empty string key case
				if name == "" {
					// Try to infer from config content
					if configMap, ok := config.(map[string]interface{}); ok {
						return t.transformMapConsumer(configMap)
					}
					return TransformedComponent{}, fmt.Errorf("empty consumer name with non-map config")
				}
				return t.TransformNamedConsumer(name, config)
			}
		}
		// Direct map consumer
		return t.transformMapConsumer(c)
		
	default:
		return TransformedComponent{}, fmt.Errorf("consumer must be a string or map")
	}
}

// transformStringConsumer transforms a string consumer specification
func (t *Transformer) transformStringConsumer(spec string) (TransformedComponent, error) {
	// Handle environment variable that might resolve to a connection string
	if strings.HasPrefix(spec, "${") && strings.HasSuffix(spec, "}") {
		// This is an environment variable - we can't know the type until runtime
		// Default to PostgreSQL for database URLs
		return TransformedComponent{
			Type: "SaveToPostgreSQL",
			Config: map[string]interface{}{
				"connection_string": spec,
			},
		}, nil
	}
	
	// Check for protocol-style consumer (e.g., "postgres://connection")
	if strings.Contains(spec, "://") {
		parts := strings.SplitN(spec, "://", 2)
		consumerAlias := parts[0]
		connectionInfo := parts[1]
		
		// Resolve consumer type
		consumerType, _ := t.resolver.ResolveConsumerType(consumerAlias)
		
		// Build config based on consumer type
		config := make(map[string]interface{})
		switch consumerType {
		case "SaveToPostgreSQL", "SaveToMongoDB":
			config["connection_string"] = spec // Use full URL
		case "SaveToParquet", "SaveToGCS":
			// Parse GCS path
			if strings.HasPrefix(spec, "gs://") {
				pathParts := strings.SplitN(strings.TrimPrefix(spec, "gs://"), "/", 2)
				config["storage_type"] = "GCS"
				config["bucket_name"] = pathParts[0]
				if len(pathParts) > 1 {
					config["path_prefix"] = pathParts[1]
				}
			} else {
				config["path"] = connectionInfo
			}
		default:
			config["connection"] = connectionInfo
		}
		
		// Apply defaults
		config = t.defaults.ApplyDefaults(consumerType, config)
		
		return TransformedComponent{
			Type:   consumerType,
			Config: config,
		}, nil
	}
	
	// Simple consumer name
	consumerType, _ := t.resolver.ResolveConsumerType(spec)
	config := t.defaults.ApplyDefaults(consumerType, make(map[string]interface{}))
	
	return TransformedComponent{
		Type:   consumerType,
		Config: config,
	}, nil
}

// TransformNamedConsumer transforms a named consumer with config (exported for testing)
func (t *Transformer) TransformNamedConsumer(name string, config interface{}) (TransformedComponent, error) {
	// Resolve consumer type
	consumerType, _ := t.resolver.ResolveConsumerType(name)
	
	// Handle config
	configMap := make(map[string]interface{})
	if config != nil {
		switch c := config.(type) {
		case map[string]interface{}:
			// Resolve field aliases
			configMap = t.resolver.ResolveFieldNames(c)
		case string:
			// String config (e.g., parquet: "output/path")
			if strings.HasPrefix(c, "gs://") {
				// GCS path
				pathParts := strings.SplitN(strings.TrimPrefix(c, "gs://"), "/", 2)
				configMap["storage_type"] = "GCS"
				configMap["bucket_name"] = pathParts[0]
				if len(pathParts) > 1 {
					configMap["path_prefix"] = pathParts[1]
				}
			} else if consumerType == "SaveToParquet" || consumerType == "SaveToGCS" {
				configMap["path"] = c
			} else {
				configMap["connection_string"] = c
			}
		}
	}
	
	// Apply defaults
	configMap = t.defaults.ApplyDefaults(consumerType, configMap)
	
	return TransformedComponent{
		Type:   consumerType,
		Config: configMap,
	}, nil
}

// transformMapConsumer transforms a map consumer configuration
func (t *Transformer) transformMapConsumer(consumer map[string]interface{}) (TransformedComponent, error) {
	// Resolve field aliases
	resolved := t.resolver.ResolveFieldNames(consumer)
	
	// Try to get type from 'type' field or infer it
	var consumerType string
	if typ, ok := resolved["type"].(string); ok {
		consumerType = typ
		delete(resolved, "type")
	} else {
		// Try to infer type from map content
		inferred, err := t.inferencer.InferConsumerType("unknown", resolved)
		if err != nil {
			// If inference fails, try to determine from content
			if _, hasPath := resolved["path"]; hasPath {
				consumerType = "SaveToParquet" // Default for file-like consumers
			} else if _, hasConnection := resolved["connection_string"]; hasConnection {
				consumerType = "SaveToPostgreSQL" // Default for database-like consumers
			} else {
				return TransformedComponent{}, fmt.Errorf("cannot determine consumer type from configuration")
			}
		} else {
			consumerType = inferred
		}
	}
	
	// Apply defaults
	config := t.defaults.ApplyDefaults(consumerType, resolved)
	
	return TransformedComponent{
		Type:   consumerType,
		Config: config,
	}, nil
}

// expandEnvVars recursively expands environment variables in the configuration
func (t *Transformer) expandEnvVars(v interface{}) interface{} {
	switch val := v.(type) {
	case string:
		return os.ExpandEnv(val)
	case map[string]interface{}:
		result := make(map[string]interface{})
		for k, v := range val {
			result[k] = t.expandEnvVars(v)
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(val))
		for i, v := range val {
			result[i] = t.expandEnvVars(v)
		}
		return result
	default:
		return v
	}
}