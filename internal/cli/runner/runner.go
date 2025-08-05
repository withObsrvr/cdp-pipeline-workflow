package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	
	"github.com/withObsrvr/cdp-pipeline-workflow/consumer"
	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/pipeline"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
	"gopkg.in/yaml.v2"
)

type Options struct {
	ConfigFile string
	Verbose    bool
}

// Factory functions for creating pipeline components
type Factories struct {
	CreateSourceAdapter func(SourceConfig) (SourceAdapter, error)
	CreateProcessor     func(processor.ProcessorConfig) (processor.Processor, error)
	CreateConsumer      func(consumer.ConsumerConfig) (processor.Processor, error)
}

type Runner struct {
	opts      Options
	factories Factories
}

// Config structures - mirroring from main.go
type Config struct {
	Pipelines map[string]PipelineConfig `yaml:"pipelines"`
}

type PipelineConfig struct {
	Name       string                      `yaml:"name"`
	Source     SourceConfig                `yaml:"source"`
	Processors []processor.ProcessorConfig `yaml:"processors"`
	Consumers  []consumer.ConsumerConfig   `yaml:"consumers"`
}

type SourceConfig struct {
	Type   string                 `yaml:"type"`
	Config map[string]interface{} `yaml:"config"`
}

type SourceAdapter interface {
	Run(context.Context) error
	Subscribe(processor.Processor)
}

func New(opts Options, factories Factories) *Runner {
	return &Runner{
		opts:      opts,
		factories: factories,
	}
}

func (r *Runner) Run(ctx context.Context) error {
	// Read configuration from specified file
	configBytes, err := os.ReadFile(r.opts.ConfigFile)
	if err != nil {
		return fmt.Errorf("error reading config file %s: %w", r.opts.ConfigFile, err)
	}

	var config Config
	if err := yaml.Unmarshal(configBytes, &config); err != nil {
		return fmt.Errorf("error parsing config: %w", err)
	}

	// Run each pipeline
	for name, pipelineConfig := range config.Pipelines {
		log.Printf("Starting pipeline: %s", name)
		err := r.setupPipeline(ctx, pipelineConfig)
		if r.opts.Verbose {
			log.Printf("DEBUG: setupPipeline returned error: %v", err)
		}
		if err != nil {
			// Check if this is a rolling window completion (EOF from callback)
			errorMsg := err.Error()
			if r.opts.Verbose {
				log.Printf("DEBUG: Error message: '%s'", errorMsg)
				log.Printf("DEBUG: errors.Is(err, io.EOF): %v", errors.Is(err, io.EOF))
				log.Printf("DEBUG: strings.Contains check: %v", strings.Contains(errorMsg, "received an error from callback invocation: EOF"))
			}
			if errors.Is(err, io.EOF) || strings.Contains(errorMsg, "received an error from callback invocation: EOF") {
				log.Printf("Pipeline %s completed successfully (rolling window phase finished)", name)
			} else {
				log.Printf("Pipeline error: error in pipeline %s: %v", name, err)
			}
		}
	}

	log.Printf("All pipelines finished.")
	return nil
}

func (r *Runner) setupPipeline(ctx context.Context, pipelineConfig PipelineConfig) error {
	// Create source
	source, err := r.factories.CreateSourceAdapter(pipelineConfig.Source)
	if err != nil {
		return fmt.Errorf("error creating source: %w", err)
	}

	// Create processors
	processors := make([]processor.Processor, len(pipelineConfig.Processors))
	for i, procConfig := range pipelineConfig.Processors {
		proc, err := r.factories.CreateProcessor(procConfig)
		if err != nil {
			return fmt.Errorf("error creating processor %s: %w", procConfig.Type, err)
		}
		processors[i] = proc
	}

	// Create consumers
	consumers := make([]processor.Processor, len(pipelineConfig.Consumers))
	for i, consConfig := range pipelineConfig.Consumers {
		cons, err := r.factories.CreateConsumer(consConfig)
		if err != nil {
			return fmt.Errorf("error creating consumer %s: %w", consConfig.Type, err)
		}
		consumers[i] = cons
	}

	// Build the chain
	pipeline.BuildProcessorChain(processors, consumers)

	// Connect source to the first processor
	if len(processors) > 0 {
		source.Subscribe(processors[0])
	} else if len(consumers) > 0 {
		// If no processors, subscribe source directly to consumers
		source.Subscribe(consumers[0])
	}

	// Run the source with context
	err = source.Run(ctx)
	
	// Flush any remaining data in consumers
	log.Printf("Pipeline source completed, flushing consumers...")
	for _, cons := range consumers {
		if closer, ok := cons.(interface{ Close() error }); ok {
			if closeErr := closer.Close(); closeErr != nil {
				log.Printf("Error closing consumer %T: %v", cons, closeErr)
			}
		}
	}
	
	return err
}


