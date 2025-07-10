package base

import (
	"fmt"
	"plugin"

	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/common/types"
)

// ProcessorRegistry is a map of processor types to factory functions
var ProcessorRegistry = make(map[string]ProcessorFactory)

// ProcessorFactory is a function that creates a processor
type ProcessorFactory func(config map[string]interface{}) (types.Processor, error)

// RegisterProcessor registers a processor factory
func RegisterProcessor(typeName string, factory ProcessorFactory) {
	ProcessorRegistry[typeName] = factory
}

// NewProcessor creates a processor based on the provided configuration.
func NewProcessor(config types.ProcessorConfig) (types.Processor, error) {
	// First check if we have a registered factory for this processor type
	if factory, exists := ProcessorRegistry[config.Type]; exists {
		return factory(config.Config)
	}

	// If not registered, check if we have a plugin for this processor type
	proc, err := loadProcessorPlugin(config.Type, config.Config)
	if err == nil {
		return proc, nil
	}

	return nil, fmt.Errorf("unknown processor type: %s", config.Type)
}

// loadProcessorPlugin loads a processor from a plugin
func loadProcessorPlugin(processorType string, config map[string]interface{}) (types.Processor, error) {
	// Try to load the plugin
	pluginPath := fmt.Sprintf("plugins/%s/plugin.so", processorType)
	p, err := plugin.Open(pluginPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open processor plugin: %w", err)
	}

	// Look up the processor constructor
	newSym, err := p.Lookup("New")
	if err != nil {
		return nil, fmt.Errorf("processor plugin does not export 'New' function: %w", err)
	}

	// Call the constructor
	newFunc, ok := newSym.(func() interface{})
	if !ok {
		return nil, fmt.Errorf("processor plugin 'New' is not a function")
	}

	instance := newFunc()

	// Check if the instance has an Initialize method
	initializer, ok := instance.(interface {
		Initialize(map[string]interface{}) error
	})
	if !ok {
		return nil, fmt.Errorf("processor plugin instance does not implement Initialize method")
	}

	// Initialize the processor
	if err := initializer.Initialize(config); err != nil {
		return nil, fmt.Errorf("failed to initialize processor plugin: %w", err)
	}

	// Check if the instance implements the Processor interface
	processor, ok := instance.(types.Processor)
	if !ok {
		return nil, fmt.Errorf("processor plugin instance does not implement Processor interface")
	}

	return processor, nil
}
