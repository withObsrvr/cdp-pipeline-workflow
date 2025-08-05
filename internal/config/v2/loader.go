package v2

import (
	"fmt"
	"io/ioutil"
	"strings"
	
	"gopkg.in/yaml.v3"
)

// ConfigLoader is the main entry point for loading configurations
type ConfigLoader struct {
	resolver       *AliasResolver
	inferencer     *InferenceEngine
	defaultsEngine *DefaultsEngine
	validator      *Validator
	transformer    *Transformer
	compatibility  *CompatibilityLayer
}

// LoaderOptions configures the config loader behavior
type LoaderOptions struct {
	ShowWarnings     bool
	StrictValidation bool
	ExpandEnvVars    bool
}

// DefaultLoaderOptions returns default loader options
func DefaultLoaderOptions() LoaderOptions {
	return LoaderOptions{
		ShowWarnings:     true,
		StrictValidation: false,
		ExpandEnvVars:    true,
	}
}

// NewConfigLoader creates a new configuration loader
func NewConfigLoader(options LoaderOptions) (*ConfigLoader, error) {
	// Initialize components
	resolver, err := NewAliasResolver()
	if err != nil {
		return nil, fmt.Errorf("initializing alias resolver: %w", err)
	}
	
	defaultsEngine := NewDefaultsEngine()
	inferencer := NewInferenceEngine(resolver)
	validator := NewValidator(resolver, defaultsEngine)
	transformer := NewTransformer(resolver, inferencer, defaultsEngine)
	compatibility := NewCompatibilityLayer(options.ShowWarnings)
	
	return &ConfigLoader{
		resolver:       resolver,
		inferencer:     inferencer,
		defaultsEngine: defaultsEngine,
		validator:      validator,
		transformer:    transformer,
		compatibility:  compatibility,
	}, nil
}

// LoadResult contains the loaded configuration and metadata
type LoadResult struct {
	Config       *TransformedConfig
	Format       FormatVersion
	Warnings     []string
	SourceFile   string
	WasUpgraded  bool
}

// Load loads a configuration from a file path
func (l *ConfigLoader) Load(path string) (*LoadResult, error) {
	// Read the file
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}
	
	// Parse YAML
	var raw map[string]interface{}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parsing YAML: %w", err)
	}
	
	// Load from parsed data
	result, err := l.LoadFromData(raw)
	if err != nil {
		return nil, err
	}
	
	result.SourceFile = path
	return result, nil
}

// LoadFromData loads a configuration from parsed data
func (l *ConfigLoader) LoadFromData(data map[string]interface{}) (*LoadResult, error) {
	result := &LoadResult{
		Warnings: []string{},
	}
	
	// Detect format
	result.Format = DetectFormat(data)
	
	// Validate configuration
	validationResult := l.validator.Validate(data)
	if validationResult.HasErrors() {
		// Return the first error with context
		return nil, l.enhanceError(validationResult.Errors[0], data)
	}
	
	// Add validation warnings
	result.Warnings = append(result.Warnings, validationResult.Warnings...)
	
	// Check for deprecations and migration opportunities
	if result.Format == FormatLegacy {
		// Add compatibility warnings
		deprecations := l.compatibility.CheckDeprecations(data)
		result.Warnings = append(result.Warnings, deprecations...)
		
		// Add migration suggestion if beneficial
		migrationReport := l.compatibility.AnalyzeForMigration(data)
		if migrationReport.ReductionPct > 50 {
			result.Warnings = append(result.Warnings, 
				fmt.Sprintf("This configuration could be simplified by %.0f%% using the new format. Run 'flowctl config upgrade %s' to convert.",
					migrationReport.ReductionPct, result.SourceFile))
		}
	}
	
	// Transform to internal format
	transformed, err := l.transformer.Transform(data)
	if err != nil {
		return nil, fmt.Errorf("transforming configuration: %w", err)
	}
	
	result.Config = transformed
	return result, nil
}

// ValidateFile validates a configuration file without loading it
func (l *ConfigLoader) ValidateFile(path string) (*ValidationResult, error) {
	// Read the file
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}
	
	// Parse YAML
	var raw map[string]interface{}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parsing YAML: %w", err)
	}
	
	// Validate
	return l.validator.Validate(raw), nil
}

// ExplainConfig provides detailed information about a configuration
func (l *ConfigLoader) ExplainConfig(path string) (*ExplanationResult, error) {
	// Load the configuration
	loadResult, err := l.Load(path)
	if err != nil {
		return nil, err
	}
	
	explanation := &ExplanationResult{
		Format:      loadResult.Format,
		Pipelines:   make(map[string]PipelineExplanation),
	}
	
	// Explain each pipeline
	for name, pipeline := range loadResult.Config.Pipelines {
		pipelineExpl := PipelineExplanation{
			Name: name,
		}
		
		// Explain source
		if pipeline.Source.Type != "" {
			pipelineExpl.Source = ComponentExplanation{
				Type:        pipeline.Source.Type,
				Aliases:     l.resolver.GetAllSourceAliases(pipeline.Source.Type),
				Description: l.getComponentDescription(pipeline.Source.Type),
				Defaults:    l.getAppliedDefaults(pipeline.Source.Type, pipeline.Source.Config),
			}
		}
		
		// Explain processors
		for _, proc := range pipeline.Processors {
			procExpl := ComponentExplanation{
				Type:        proc.Type,
				Aliases:     l.resolver.GetAllProcessorAliases(proc.Type),
				Description: l.getComponentDescription(proc.Type),
				Defaults:    l.getAppliedDefaults(proc.Type, proc.Config),
			}
			pipelineExpl.Processors = append(pipelineExpl.Processors, procExpl)
		}
		
		// Explain consumers
		for _, cons := range pipeline.Consumers {
			consExpl := ComponentExplanation{
				Type:        cons.Type,
				Aliases:     l.resolver.GetAllConsumerAliases(cons.Type),
				Description: l.getComponentDescription(cons.Type),
				Defaults:    l.getAppliedDefaults(cons.Type, cons.Config),
			}
			pipelineExpl.Consumers = append(pipelineExpl.Consumers, consExpl)
		}
		
		explanation.Pipelines[name] = pipelineExpl
	}
	
	return explanation, nil
}

// ExplanationResult contains detailed configuration explanation
type ExplanationResult struct {
	Format    FormatVersion
	Pipelines map[string]PipelineExplanation
}

// PipelineExplanation explains a single pipeline
type PipelineExplanation struct {
	Name       string
	Source     ComponentExplanation
	Processors []ComponentExplanation
	Consumers  []ComponentExplanation
}

// ComponentExplanation explains a single component
type ComponentExplanation struct {
	Type        string
	Aliases     []string
	Description string
	Defaults    map[string]DefaultInfo
}

// DefaultInfo contains information about a default value
type DefaultInfo struct {
	Value      interface{}
	IsDefault  bool
	DefaultValue interface{}
}

// enhanceError adds context to configuration errors
func (l *ConfigLoader) enhanceError(err error, config map[string]interface{}) error {
	// If it's already a ValidationError, return as-is
	if _, ok := err.(ValidationError); ok {
		return err
	}
	
	// Try to add context based on error message
	errMsg := err.Error()
	
	// Check for common issues
	if strings.Contains(errMsg, "unknown") {
		// Try to provide suggestions
		if strings.Contains(errMsg, "source") {
			sources := []string{}
			for alias := range l.resolver.definitions.Aliases.Sources {
				sources = append(sources, alias)
			}
			if len(sources) > 10 {
				sources = sources[:10]
				sources = append(sources, "...")
			}
			return fmt.Errorf("%w\n\nAvailable source types: %s", err, strings.Join(sources, ", "))
		}
	}
	
	return err
}

// getComponentDescription returns a description for a component type
func (l *ConfigLoader) getComponentDescription(componentType string) string {
	descriptions := map[string]string{
		// Sources
		"BufferedStorageSourceAdapter":   "Reads from cloud storage (GCS/S3)",
		"S3BufferedStorageSourceAdapter": "Reads from Amazon S3",
		"FSBufferedStorageSourceAdapter": "Reads from local filesystem",
		"CaptiveCoreInboundAdapter":      "Connects to Stellar Core",
		"RPCSourceAdapter":               "Connects to Stellar RPC",
		"SorobanSourceAdapter":           "Connects to Soroban RPC",
		
		// Processors
		"FilterPayments":         "Filters payment operations",
		"TransformToAppPayment":  "Transforms payments to app format",
		"ContractData":           "Processes contract data",
		"AccountData":            "Processes account data",
		
		// Consumers
		"SaveToParquet":     "Saves to Parquet files",
		"SaveToPostgreSQL":  "Saves to PostgreSQL database",
		"SaveToMongoDB":     "Saves to MongoDB database",
		"SaveToDuckDB":      "Saves to DuckDB database",
		"SaveToRedis":       "Saves to Redis cache",
		"SaveToZeroMQ":      "Publishes to ZeroMQ",
		"SaveToWebSocket":   "Streams via WebSocket",
		"SaveToGCS":         "Saves to Google Cloud Storage",
	}
	
	if desc, ok := descriptions[componentType]; ok {
		return desc
	}
	
	return "No description available"
}

// getAppliedDefaults identifies which values are defaults
func (l *ConfigLoader) getAppliedDefaults(componentType string, config map[string]interface{}) map[string]DefaultInfo {
	defaults := l.defaultsEngine.GetDefaultsForComponent(componentType)
	result := make(map[string]DefaultInfo)
	
	for key, value := range config {
		info := DefaultInfo{
			Value: value,
		}
		
		if defaultValue, hasDefault := defaults[key]; hasDefault {
			info.DefaultValue = defaultValue
			info.IsDefault = fmt.Sprintf("%v", value) == fmt.Sprintf("%v", defaultValue)
		}
		
		result[key] = info
	}
	
	return result
}

// Helper function to check if a config uses simplified syntax
func isSimplifiedConfig(config map[string]interface{}) bool {
	_, hasSource := config["source"]
	_, hasProcess := config["process"] 
	_, hasSaveTo := config["save_to"]
	
	return hasSource || hasProcess || hasSaveTo
}

// GetSimplifiedExample returns an example of simplified configuration
func GetSimplifiedExample() string {
	return `# Simplified v2 configuration example
source:
  bucket: "stellar-mainnet-data"
  network: mainnet

process:
  - contract_data
  - payment_filter:
      min: 100

save_to:
  - parquet: "gs://output-bucket/contracts"
  - postgres: "${DATABASE_URL}"`
}

// GetLegacyExample returns an example of legacy configuration
func GetLegacyExample() string {
	return `# Legacy configuration example
pipelines:
  MyPipeline:
    source:
      type: BufferedStorageSourceAdapter
      config:
        bucket_name: "stellar-mainnet-data"
        network: "mainnet"
        num_workers: 20
        retry_limit: 3
    processors:
      - type: ContractData
        config:
          network_passphrase: "Public Global Stellar Network ; September 2015"
      - type: FilterPayments
        config:
          min_amount: "100"
    consumers:
      - type: SaveToParquet
        config:
          storage_type: "GCS"
          bucket_name: "output-bucket"
          path_prefix: "contracts"
      - type: SaveToPostgreSQL
        config:
          connection_string: "${DATABASE_URL}"`
}