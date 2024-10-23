package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Pipelines map[string]PipelineConfig `yaml:"pipelines"`
}

type PipelineConfig struct {
	Name       string            `yaml:"name"`
	Source     SourceConfig      `yaml:"source"`
	Processors []ProcessorConfig `yaml:"processors"`
	Consumers  []ConsumerConfig  `yaml:"consumers"`
}

type SourceConfig struct {
	Type   string                 `yaml:"type"`
	Config map[string]interface{} `yaml:"config"`
}

type ProcessorConfig struct {
	Type   string                 `yaml:"type"`
	Config map[string]interface{} `yaml:"config"`
}

type ConsumerConfig struct {
	Type   string                 `yaml:"type"`
	Config map[string]interface{} `yaml:"config"`
}

type SourceAdapter interface {
	Run(context.Context) error
	Subscribe(Processor)
}

type Processor interface {
	Process(context.Context, Message) error
	Subscribe(Processor)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	configFile := "pipeline_config_buffered.secret.yaml"
	config, err := loadConfig(configFile)
	if err != nil {
		log.Fatalf("Error loading configuration: %v", err)
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(config.Pipelines))

	for name, pipelineConfig := range config.Pipelines {
		wg.Add(1)
		go func(name string, config PipelineConfig) {
			defer wg.Done()
			if err := runPipeline(ctx, name, config); err != nil {
				errChan <- fmt.Errorf("error in pipeline %s: %v", name, err)
			}
		}(name, pipelineConfig)
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	for err := range errChan {
		log.Printf("Pipeline error: %v", err)
	}

	log.Println("All pipelines finished.")
}

func loadConfig(configFile string) (*Config, error) {
	file, err := os.Open(configFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	defer file.Close()
	var config Config
	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return nil, err
	}
	return &config, nil
}

func createSourceAdapter(sourceConfig SourceConfig) (SourceAdapter, error) {
	switch sourceConfig.Type {
	case "CaptiveCoreInboundAdapter":
		return NewCaptiveCoreInboundAdapter(sourceConfig.Config)
	case "BufferedStorageSourceAdapter":
		return NewBufferedStorageSourceAdapter(sourceConfig.Config)
	// Add more source types as needed
	default:
		return nil, fmt.Errorf("unsupported source type: %s", sourceConfig.Type)
	}
}

func createProcessors(processorConfigs []ProcessorConfig) ([]Processor, error) {
	processors := make([]Processor, len(processorConfigs))
	for i, config := range processorConfigs {
		processor, err := createProcessor(config)
		if err != nil {
			return nil, err
		}
		processors[i] = processor
	}
	return processors, nil
}

func createProcessor(processorConfig ProcessorConfig) (Processor, error) {
	switch processorConfig.Type {
	case "FilterPayments":
		return NewFilterPayments(processorConfig.Config)
	case "TransformToAppPayment":
		return NewTransformToAppPayment(processorConfig.Config)
	case "CreateAccountTransformer":
		return NewCreateAccount(processorConfig.Config)
	default:
		return nil, fmt.Errorf("unsupported processor type: %s", processorConfig.Type)
	}
}

func createConsumer(consumerConfig ConsumerConfig) (Processor, error) {
	switch consumerConfig.Type {
	case "SaveToExcel":
		return NewSaveToExcel(consumerConfig.Config)
	case "SaveToMongoDB":
		return NewSaveToMongoDB(consumerConfig.Config)
	case "SaveToZeroMQ":
		return NewSaveToZeroMQ(consumerConfig.Config)
	case "SaveToGCS":
		return NewSaveToGCS(consumerConfig.Config)
	default:
		return nil, fmt.Errorf("unsupported consumer type: %s", consumerConfig.Type)
	}
}

func createConsumers(consumerConfigs []ConsumerConfig) ([]Processor, error) {
	consumers := make([]Processor, len(consumerConfigs))
	for i, config := range consumerConfigs {
		consumer, err := createConsumer(config)
		if err != nil {
			return nil, err
		}
		consumers[i] = consumer
	}
	return consumers, nil
}

func subscribeComponents(source SourceAdapter, processors []Processor, consumers []Processor) {
	for _, processor := range processors {
		source.Subscribe(processor)
		for _, consumer := range consumers {
			processor.Subscribe(consumer)
		}
	}
}

// NewCaptiveCoreInboundAdapter, NewFilterPayments, and NewSaveToExcel functions need to be implemented

func runPipeline(ctx context.Context, name string, config PipelineConfig) error {
	log.Printf("Starting pipeline: %s", name)

	source, err := createSourceAdapter(config.Source)
	if err != nil {
		return fmt.Errorf("error creating source adapter: %v", err)
	}

	processors, err := createProcessors(config.Processors)
	if err != nil {
		return fmt.Errorf("error creating processors: %v", err)
	}

	consumers, err := createConsumers(config.Consumers)
	if err != nil {
		return fmt.Errorf("error creating consumers: %v", err)
	}

	subscribeComponents(source, processors, consumers)

	if err := source.Run(ctx); err != nil {
		return fmt.Errorf("error running source adapter: %v", err)
	}

	log.Printf("Pipeline %s finished.", name)
	return nil
}
