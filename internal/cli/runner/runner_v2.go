package runner

import (
	"context"
	"fmt"
	"log"
	"os"
	
	"github.com/withObsrvr/cdp-pipeline-workflow/consumer"
	v2config "github.com/withObsrvr/cdp-pipeline-workflow/internal/config/v2"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
	"gopkg.in/yaml.v3"
)

// RunWithV2Config runs pipelines using the v2 configuration loader
func (r *Runner) RunWithV2Config(ctx context.Context) error {
	// Create v2 config loader
	loader, err := v2config.NewConfigLoader(v2config.DefaultLoaderOptions())
	if err != nil {
		return fmt.Errorf("creating config loader: %w", err)
	}
	
	// Load configuration
	result, err := loader.Load(r.opts.ConfigFile)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}
	
	// Log any warnings
	for _, warning := range result.Warnings {
		log.Printf("Warning: %s", warning)
	}
	
	// Log format info
	formatName := "unknown"
	switch result.Format {
	case v2config.FormatLegacy:
		formatName = "legacy"
	case v2config.FormatV2:
		formatName = "v2"
	}
	log.Printf("Loaded %s format configuration with %d pipeline(s)", formatName, len(result.Config.Pipelines))
	
	// Run each pipeline
	for name, pipeline := range result.Config.Pipelines {
		log.Printf("Starting pipeline: %s", name)
		
		// Convert v2 config to runner format
		pipelineConfig := r.convertV2Pipeline(pipeline)
		
		err := r.setupPipeline(ctx, pipelineConfig)
		if err != nil {
			// Handle errors as before
			errorMsg := err.Error()
			if r.opts.Verbose {
				log.Printf("DEBUG: Error message: '%s'", errorMsg)
			}
			log.Printf("Pipeline error: error in pipeline %s: %v", name, err)
		}
	}
	
	log.Printf("All pipelines finished.")
	return nil
}

// convertV2Pipeline converts a v2 pipeline to the runner's expected format
func (r *Runner) convertV2Pipeline(pipeline v2config.TransformedPipeline) PipelineConfig {
	config := PipelineConfig{
		Source: SourceConfig{
			Type:   pipeline.Source.Type,
			Config: pipeline.Source.Config,
		},
	}
	
	// Convert processors
	for _, proc := range pipeline.Processors {
		config.Processors = append(config.Processors, processor.ProcessorConfig{
			Type:   proc.Type,
			Config: proc.Config,
		})
	}
	
	// Convert consumers
	for _, cons := range pipeline.Consumers {
		config.Consumers = append(config.Consumers, consumer.ConsumerConfig{
			Type:   cons.Type,
			Config: cons.Config,
		})
	}
	
	return config
}

// LoadConfigForInspection loads a config file for inspection without running it
func LoadConfigForInspection(configFile string) (*v2config.LoadResult, error) {
	loader, err := v2config.NewConfigLoader(v2config.DefaultLoaderOptions())
	if err != nil {
		return nil, fmt.Errorf("creating config loader: %w", err)
	}
	
	return loader.Load(configFile)
}

// DetectConfigFormat detects whether a config file is legacy or v2 format
func DetectConfigFormat(configFile string) (v2config.FormatVersion, error) {
	// Read the file
	data, err := os.ReadFile(configFile)
	if err != nil {
		return v2config.FormatUnknown, fmt.Errorf("reading config file: %w", err)
	}
	
	// Parse YAML
	var raw map[string]interface{}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return v2config.FormatUnknown, fmt.Errorf("parsing YAML: %w", err)
	}
	
	return v2config.DetectFormat(raw), nil
}