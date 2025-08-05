package v2

import (
	_ "embed"
	"fmt"
	"gopkg.in/yaml.v3"
	"strings"
)

//go:embed aliases.yaml
var aliasesYAML string

// AliasDefinitions holds all alias mappings
type AliasDefinitions struct {
	Version int                              `yaml:"version"`
	Aliases struct {
		Sources    map[string]string         `yaml:"sources"`
		Processors map[string]string         `yaml:"processors"`
		Consumers  map[string]string         `yaml:"consumers"`
		Fields     map[string]string         `yaml:"fields"`
	} `yaml:"aliases"`
}

// AliasResolver handles alias resolution for configurations
type AliasResolver struct {
	definitions *AliasDefinitions
	// Reverse mappings for finding aliases from types
	sourceAliases    map[string][]string
	processorAliases map[string][]string
	consumerAliases  map[string][]string
}

// NewAliasResolver creates a new alias resolver
func NewAliasResolver() (*AliasResolver, error) {
	var defs AliasDefinitions
	if err := yaml.Unmarshal([]byte(aliasesYAML), &defs); err != nil {
		return nil, fmt.Errorf("parsing alias definitions: %w", err)
	}

	resolver := &AliasResolver{
		definitions:      &defs,
		sourceAliases:    make(map[string][]string),
		processorAliases: make(map[string][]string),
		consumerAliases:  make(map[string][]string),
	}

	// Build reverse mappings
	for alias, typ := range defs.Aliases.Sources {
		resolver.sourceAliases[typ] = append(resolver.sourceAliases[typ], alias)
	}
	for alias, typ := range defs.Aliases.Processors {
		resolver.processorAliases[typ] = append(resolver.processorAliases[typ], alias)
	}
	for alias, typ := range defs.Aliases.Consumers {
		resolver.consumerAliases[typ] = append(resolver.consumerAliases[typ], alias)
	}

	return resolver, nil
}

// ResolveSourceType resolves a source alias to its actual type
func (r *AliasResolver) ResolveSourceType(alias string) (string, bool) {
	if typ, ok := r.definitions.Aliases.Sources[alias]; ok {
		return typ, true
	}
	// If not an alias, return as-is (might be the actual type)
	return alias, false
}

// ResolveProcessorType resolves a processor alias to its actual type
func (r *AliasResolver) ResolveProcessorType(alias string) (string, bool) {
	if typ, ok := r.definitions.Aliases.Processors[alias]; ok {
		return typ, true
	}
	return alias, false
}

// ResolveConsumerType resolves a consumer alias to its actual type
func (r *AliasResolver) ResolveConsumerType(alias string) (string, bool) {
	if typ, ok := r.definitions.Aliases.Consumers[alias]; ok {
		return typ, true
	}
	return alias, false
}

// ResolveFieldName resolves a field alias to its actual field name
func (r *AliasResolver) ResolveFieldName(field string) string {
	if actual, ok := r.definitions.Aliases.Fields[field]; ok {
		return actual
	}
	return field
}

// ResolveFieldNames resolves all field aliases in a configuration map
func (r *AliasResolver) ResolveFieldNames(config map[string]interface{}) map[string]interface{} {
	resolved := make(map[string]interface{})
	for key, value := range config {
		resolvedKey := r.ResolveFieldName(key)
		
		// Recursively resolve nested maps
		if nestedMap, ok := value.(map[string]interface{}); ok {
			resolved[resolvedKey] = r.ResolveFieldNames(nestedMap)
		} else {
			resolved[resolvedKey] = value
		}
	}
	return resolved
}

// GetSimilarSource finds similar source aliases for error suggestions
func (r *AliasResolver) GetSimilarSource(input string) []string {
	return r.getSimilar(input, r.definitions.Aliases.Sources)
}

// GetSimilarProcessor finds similar processor aliases for error suggestions
func (r *AliasResolver) GetSimilarProcessor(input string) []string {
	return r.getSimilar(input, r.definitions.Aliases.Processors)
}

// GetSimilarConsumer finds similar consumer aliases for error suggestions
func (r *AliasResolver) GetSimilarConsumer(input string) []string {
	return r.getSimilar(input, r.definitions.Aliases.Consumers)
}

// GetSimilarField finds similar field aliases for error suggestions
func (r *AliasResolver) GetSimilarField(input string) []string {
	return r.getSimilar(input, r.definitions.Aliases.Fields)
}

// getSimilar finds aliases similar to the input (for typo suggestions)
func (r *AliasResolver) getSimilar(input string, aliases map[string]string) []string {
	input = strings.ToLower(input)
	var similar []string
	
	for alias := range aliases {
		aliasLower := strings.ToLower(alias)
		// Simple similarity check - could be enhanced with Levenshtein distance
		if strings.Contains(aliasLower, input) || strings.Contains(input, aliasLower) {
			similar = append(similar, alias)
		} else if len(input) > 2 && strings.HasPrefix(aliasLower, input[:2]) {
			similar = append(similar, alias)
		}
	}
	
	// Limit suggestions
	if len(similar) > 5 {
		similar = similar[:5]
	}
	
	return similar
}

// GetAllSourceAliases returns all aliases for a given source type
func (r *AliasResolver) GetAllSourceAliases(sourceType string) []string {
	return r.sourceAliases[sourceType]
}

// GetAllProcessorAliases returns all aliases for a given processor type
func (r *AliasResolver) GetAllProcessorAliases(processorType string) []string {
	return r.processorAliases[processorType]
}

// GetAllConsumerAliases returns all aliases for a given consumer type
func (r *AliasResolver) GetAllConsumerAliases(consumerType string) []string {
	return r.consumerAliases[consumerType]
}