package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/withObsrvr/cdp-pipeline-workflow/consumer"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
	"gopkg.in/yaml.v2"
)

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

func main() {
	// Define command line flags
	configFile := flag.String("config", "pipeline_config.yaml", "Path to pipeline configuration file")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	// Read configuration from specified file
	configBytes, err := os.ReadFile(*configFile)
	if err != nil {
		log.Fatalf("Error reading config file %s: %v", *configFile, err)
	}

	var config Config
	if err := yaml.Unmarshal(configBytes, &config); err != nil {
		log.Fatalf("Error parsing config: %v", err)
	}

	// Run each pipeline
	for name, pipelineConfig := range config.Pipelines {
		log.Printf("Starting pipeline: %s", name)
		if err := setupPipeline(ctx, pipelineConfig); err != nil {
			log.Printf("Pipeline error: error in pipeline %s: %v", name, err)
		}
	}

	log.Printf("All pipelines finished.")
}

func createSourceAdapter(sourceConfig SourceConfig) (SourceAdapter, error) {
	switch sourceConfig.Type {
	case "CaptiveCoreInboundAdapter":
		return NewCaptiveCoreInboundAdapter(sourceConfig.Config)
	case "BufferedStorageSourceAdapter":
		return NewBufferedStorageSourceAdapter(sourceConfig.Config)
	case "SorobanSourceAdapter":
		return NewSorobanSourceAdapter(sourceConfig.Config)
	case "GCSBufferedStorageSourceAdapter":
		return NewGCSBufferedStorageSourceAdapter(sourceConfig.Config)
	case "FSBufferedStorageSourceAdapter":
		return NewFSBufferedStorageSourceAdapter(sourceConfig.Config)
	case "S3BufferedStorageSourceAdapter":
		return NewS3BufferedStorageSourceAdapter(sourceConfig.Config)
	case "RPCSourceAdapter":
		return NewRPCSourceAdapter(sourceConfig.Config)
	// Add more source types as needed
	default:
		return nil, fmt.Errorf("unsupported source type: %s", sourceConfig.Type)
	}
}

func createProcessors(processorConfigs []processor.ProcessorConfig) ([]processor.Processor, error) {
	processors := make([]processor.Processor, len(processorConfigs))
	for i, config := range processorConfigs {
		processor, err := createProcessor(config)
		if err != nil {
			return nil, err
		}
		processors[i] = processor
	}
	return processors, nil
}

func createProcessor(processorConfig processor.ProcessorConfig) (processor.Processor, error) {
	switch processorConfig.Type {
	case "CreateAccount":
		return processor.NewCreateAccount(processorConfig.Config)
	case "AccountData":
		return processor.NewProcessAccountData(processorConfig.Config)
	case "ProcessAccountDataFull":
		return processor.NewProcessAccountData(processorConfig.Config)
	case "TransformToAppPayment":
		return processor.NewTransformToAppPayment(processorConfig.Config)
	case "FilterPayments":
		return processor.NewFilterPayments(processorConfig.Config)
	case "TransformToAppTrade":
		return processor.NewTransformToAppTrade(processorConfig.Config)
	case "TransformToAppTrustline":
		return processor.NewTransformToAppTrustline(processorConfig.Config)
	case "TransformToAppMetrics":
		return processor.NewTransformToAppMetrics(processorConfig.Config)
	case "FilterEventsProcessor":
		return processor.NewFilterEventsProcessor(processorConfig.Config)
	case "TransformToAssetStats":
		return processor.NewTransformToAssetStats(processorConfig.Config)
	case "TransformToTokenPrice":
		return processor.NewTransformToTokenPrice(processorConfig.Config)
	case "TransformToTickerAsset":
		return processor.NewTransformToTickerAssetProcessor(processorConfig.Config)
	case "TransformToTickerOrderbook":
		return processor.NewTransformToTickerOrderbookProcessor(processorConfig.Config)
	case "MarketMetricsProcessor":
		return processor.NewMarketMetricsProcessor(processorConfig.Config)
	case "TransformToMarketCapProcessor":
		return processor.NewTransformToMarketCapProcessor(processorConfig.Config)
	case "TransformToMarketAnalytics":
		return processor.NewTransformToMarketAnalytics(processorConfig.Config)
	case "TransformToAppAccount":
		return processor.NewTransformToAppAccount(processorConfig.Config)
	case "BlankProcessor":
		return processor.NewBlankProcessor(processorConfig.Config)
	case "LedgerReader":
		return processor.NewLedgerReader(processorConfig.Config)
	case "AssetEnrichment":
		return processor.NewAssetEnrichmentProcessor(processorConfig.Config)
	case "ContractLedgerReader":
		return processor.NewContractLedgerReader(processorConfig.Config)
	case "ContractInvocation":
		return processor.NewContractInvocationProcessor(processorConfig.Config)
	case "ContractCreation":
		return processor.NewContractCreationProcessor(processorConfig.Config)
	case "ContractEvent":
		return processor.NewContractEventProcessor(processorConfig.Config)
	case "LatestLedger":
		return processor.NewLatestLedgerProcessor(processorConfig.Config)
	case "AccountTransaction":
		return processor.NewAccountTransactionProcessor(processorConfig.Config)
	case "FilteredContractInvocation":
		return processor.NewFilteredContractInvocationProcessor(processorConfig.Config)
	case "AccountYearAnalytics":
		return processor.NewAccountYearAnalytics(processorConfig.Config)
	case "ContractFilter":
		return processor.NewContractFilterProcessor(processorConfig.Config)
	case "AccountEffect":
		return processor.NewAccountEffectProcessor(processorConfig.Config)
	case "LedgerChanges":
		return processor.NewLedgerChangeProcessor(processorConfig.Config)
	case "LatestLedgerRPC":
		return processor.NewLatestLedgerRPCProcessor(processorConfig.Config)
	case "GetEventsRPC":
		return processor.NewGetEventsRPCProcessor(processorConfig.Config)
	case "StdoutSink":
		return processor.NewStdoutSink(), nil
	case "SoroswapRouter":
		return processor.NewSoroswapRouterProcessor(processorConfig.Config)
	case "Soroswap":
		return processor.NewSoroswapProcessor(processorConfig.Config)
	default:
		return nil, fmt.Errorf("unsupported processor type: %s", processorConfig.Type)
	}
}

func createConsumer(consumerConfig consumer.ConsumerConfig) (processor.Processor, error) {
	switch consumerConfig.Type {
	case "SaveToExcel":
		return consumer.NewSaveToExcel(consumerConfig.Config)
	case "SaveToMongoDB":
		return consumer.NewSaveToMongoDB(consumerConfig.Config)
	case "SaveToZeroMQ":
		return consumer.NewSaveToZeroMQ(consumerConfig.Config)
	case "SaveToGCS":
		return consumer.NewSaveToGCS(consumerConfig.Config)
	case "SaveToDuckDB":
		return consumer.NewSaveToDuckDB(consumerConfig.Config)
	case "SaveContractToDuckDB":
		return consumer.NewSaveContractToDuckDB(consumerConfig.Config)
	case "SaveToTimescaleDB":
		return consumer.NewSaveToTimescaleDB(consumerConfig.Config)
	case "SaveToRedis":
		return consumer.NewSaveToRedis(consumerConfig.Config)
	case "NotificationDispatcher":
		return consumer.NewNotificationDispatcher(consumerConfig.Config)
	case "SaveToWebSocket":
		return consumer.NewSaveToWebSocket(consumerConfig.Config)
	case "SaveToPostgreSQL":
		return consumer.NewSaveToPostgreSQL(consumerConfig.Config)
	case "SaveToClickHouse":
		return consumer.NewSaveToClickHouse(consumerConfig.Config)
	case "SaveToMarketAnalytics":
		return consumer.NewSaveToMarketAnalyticsConsumer(consumerConfig.Config)
	case "SaveToRedisOrderbook":
		return consumer.NewSaveToRedisOrderbookConsumer(consumerConfig.Config)
	case "SaveAssetToPostgreSQL":
		return consumer.NewSaveAssetToPostgreSQL(consumerConfig.Config)
	case "SaveAssetEnrichment":
		return consumer.NewSaveAssetEnrichmentConsumer(consumerConfig.Config)
	case "SavePaymentToPostgreSQL":
		return consumer.NewSavePaymentToPostgreSQL(consumerConfig.Config)
	case "SavePaymentsToRedis":
		return consumer.NewSavePaymentsToRedis(consumerConfig.Config)
	case "SaveLatestLedgerToRedis":
		return consumer.NewSaveLatestLedgerRedis(consumerConfig.Config)
	case "SaveLatestLedgerToExcel":
		return consumer.NewSaveLatestLedgerToExcel(consumerConfig.Config)
	case "AnthropicClaude":
		return consumer.NewAnthropicClaudeConsumer(consumerConfig.Config)
	case "StdoutConsumer":
		return consumer.NewStdoutConsumer(), nil
	case "SaveSoroswapPairsToDuckDB":
		return consumer.NewSaveSoroswapPairsToDuckDB(consumerConfig.Config)
	case "SaveSoroswapRouterToDuckDB":
		return consumer.NewSaveSoroswapRouterToDuckDB(consumerConfig.Config)
	case "SaveAccountDataToPostgreSQL":
		return consumer.NewSaveAccountDataToPostgreSQL(consumerConfig.Config)
	case "SaveAccountDataToDuckDB":
		return consumer.NewSaveAccountDataToDuckDB(consumerConfig.Config)
	case "SaveContractEventsToPostgreSQL":
		return consumer.NewSaveContractEventsToPostgreSQL(consumerConfig.Config)
	case "SaveSoroswapToPostgreSQL":
		return consumer.NewSaveSoroswapToPostgreSQL(consumerConfig.Config)
	case "SaveContractInvocationsToPostgreSQL":
		return consumer.NewSaveContractInvocationsToPostgreSQL(consumerConfig.Config)
	default:
		return nil, fmt.Errorf("unsupported consumer type: %s", consumerConfig.Type)
	}
}

func createConsumers(consumerConfigs []consumer.ConsumerConfig) ([]processor.Processor, error) {
	consumers := make([]processor.Processor, len(consumerConfigs))
	for i, config := range consumerConfigs {
		consumer, err := createConsumer(config)
		if err != nil {
			return nil, err
		}
		consumers[i] = consumer
	}
	return consumers, nil
}

// buildProcessorChain chains processors sequentially and subscribes all consumers to the last processor
func buildProcessorChain(processors []processor.Processor, consumers []processor.Processor) {
	var lastProcessor processor.Processor

	// Chain all processors sequentially
	for _, p := range processors {
		if lastProcessor != nil {
			lastProcessor.Subscribe(p)
			log.Printf("Chained processor %T -> %T", lastProcessor, p)
		}
		lastProcessor = p
	}

	// If any consumers are provided, subscribe them to the last processor
	if lastProcessor != nil {
		for _, c := range consumers {
			lastProcessor.Subscribe(c)
			log.Printf("Chained processor %T -> consumer %T", lastProcessor, c)
		}
	} else if len(consumers) > 0 {
		// If no processors but multiple consumers, chain the consumers
		for i := 1; i < len(consumers); i++ {
			consumers[0].Subscribe(consumers[i])
			log.Printf("Chained consumer %T -> consumer %T", consumers[0], consumers[i])
		}
	}
}

func setupPipeline(ctx context.Context, pipelineConfig PipelineConfig) error {
	// Create source
	source, err := createSourceAdapter(pipelineConfig.Source)
	if err != nil {
		return fmt.Errorf("error creating source: %w", err)
	}

	// Create processors
	processors := make([]processor.Processor, len(pipelineConfig.Processors))
	for i, procConfig := range pipelineConfig.Processors {
		proc, err := createProcessor(procConfig)
		if err != nil {
			return fmt.Errorf("error creating processor %s: %w", procConfig.Type, err)
		}
		processors[i] = proc
	}

	// Create consumers
	consumers := make([]processor.Processor, len(pipelineConfig.Consumers))
	for i, consConfig := range pipelineConfig.Consumers {
		cons, err := createConsumer(consConfig)
		if err != nil {
			return fmt.Errorf("error creating consumer %s: %w", consConfig.Type, err)
		}
		consumers[i] = cons
	}

	// Build the chain
	buildProcessorChain(processors, consumers)

	// Connect source to the first processor
	if len(processors) > 0 {
		source.Subscribe(processors[0])
	} else if len(consumers) > 0 {
		// If no processors, subscribe source directly to consumers
		source.Subscribe(consumers[0])
	}

	// Run the source with context
	return source.Run(ctx)
}
