package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	
	v2config "github.com/withObsrvr/cdp-pipeline-workflow/internal/config/v2"
	"github.com/withObsrvr/cdp-pipeline-workflow/consumer"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// loadConfigWithV2 loads a configuration file using the v2 loader
func loadConfigWithV2(configFile string) (*v2config.LoadResult, error) {
	loader, err := v2config.NewConfigLoader(v2config.DefaultLoaderOptions())
	if err != nil {
		return nil, fmt.Errorf("creating v2 config loader: %w", err)
	}
	
	result, err := loader.Load(configFile)
	if err != nil {
		return nil, fmt.Errorf("loading config with v2 loader: %w", err)
	}
	
	// Log warnings
	for _, warning := range result.Warnings {
		log.Printf("Config warning: %s", warning)
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
	
	return result, nil
}

// runPipelinesFromV2Config runs pipelines from a v2 config load result
func runPipelinesFromV2Config(ctx context.Context, result *v2config.LoadResult) {
	for name, pipeline := range result.Config.Pipelines {
		log.Printf("Starting pipeline: %s", name)
		
		// Convert v2 pipeline to legacy format
		pipelineConfig := convertV2Pipeline(pipeline)
		pipelineConfig.Name = name // Set the pipeline name
		
		err := setupPipeline(ctx, pipelineConfig)
		log.Printf("DEBUG: setupPipeline returned error: %v", err)
		if err != nil {
			// Check if this is a rolling window completion (EOF from callback)
			errorMsg := err.Error()
			log.Printf("DEBUG: Error message: '%s'", errorMsg)
			log.Printf("DEBUG: errors.Is(err, io.EOF): %v", errors.Is(err, io.EOF))
			log.Printf("DEBUG: strings.Contains check: %v", strings.Contains(errorMsg, "received an error from callback invocation: EOF"))
			if errors.Is(err, io.EOF) || strings.Contains(errorMsg, "received an error from callback invocation: EOF") {
				log.Printf("Pipeline %s completed successfully (rolling window phase finished)", name)
			} else {
				log.Printf("Pipeline error: error in pipeline %s: %v", name, err)
			}
		}
	}
	
	log.Printf("All pipelines finished.")
}

// convertV2Pipeline converts a v2 pipeline to the legacy format
func convertV2Pipeline(pipeline v2config.TransformedPipeline) PipelineConfig {
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

