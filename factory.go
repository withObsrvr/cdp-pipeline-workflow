package main

import (
	"errors"
	"fmt"
	"log"
	
	"github.com/withObsrvr/cdp-pipeline-workflow/consumer"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// Factory functions exported for use by the new CLI runner

func CreateSourceAdapterFunc(sourceConfig SourceConfig) (SourceAdapter, error) {
	switch sourceConfig.Type {
	case "CaptiveCoreInboundAdapter":
		return NewCaptiveCoreInboundAdapter(sourceConfig.Config)
	case "BufferedStorageSourceAdapter":
		// Try enhanced version first, fall back to legacy
		adapter, err := NewBufferedStorageSourceAdapterEnhanced(sourceConfig.Config)
		if err == nil {
			return adapter, nil
		}
		log.Printf("Enhanced BufferedStorageSourceAdapter failed with: %v, falling back to legacy", err)
		return NewBufferedStorageSourceAdapter(sourceConfig.Config)
	case "SorobanSourceAdapter":
		return NewSorobanSourceAdapter(sourceConfig.Config)
	case "GCSBufferedStorageSourceAdapter":
		return NewGCSBufferedStorageSourceAdapter(sourceConfig.Config)
	case "FSBufferedStorageSourceAdapter":
		return nil, errors.New("FSBufferedStorageSourceAdapter is deprecated and no longer supported")
	case "S3BufferedStorageSourceAdapter":
		// Try enhanced version first, fall back to legacy
		if adapter, err := NewS3BufferedStorageSourceAdapterEnhanced(sourceConfig.Config); err == nil {
			return adapter, nil
		} else {
			log.Printf("Enhanced S3BufferedStorageSourceAdapter failed with: %v, falling back to legacy", err)
		}
		return NewS3BufferedStorageSourceAdapter(sourceConfig.Config)
	case "RPCSourceAdapter":
		return NewRPCSourceAdapter(sourceConfig.Config)
	case "BronzeSourceAdapter":
		return NewBronzeSourceAdapter(sourceConfig.Config)
	case "DuckLakeSourceAdapter":
		return NewDuckLakeSourceAdapter(sourceConfig.Config)
	// Add more source types as needed
	default:
		return nil, fmt.Errorf("unsupported source type: %s", sourceConfig.Type)
	}
}

func CreateProcessorFunc(processorConfig processor.ProcessorConfig) (processor.Processor, error) {
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
	case "StellarContractEvent":
		return processor.NewStellarContractEventProcessor(processorConfig.Config)
	case "LatestLedger":
		return processor.NewLatestLedgerProcessor(processorConfig.Config)
	case "AccountTransaction":
		return processor.NewAccountTransactionProcessor(processorConfig.Config)
	case "AccountDataFilter":
		return processor.NewAccountDataFilter(processorConfig.Config)
	case "FilteredContractInvocation":
		return processor.NewFilteredContractInvocationProcessor(processorConfig.Config)
	case "AccountYearAnalytics":
		return processor.NewAccountYearAnalytics(processorConfig.Config)
	case "ContractFilter":
		return processor.NewContractFilterProcessor(processorConfig.Config)
	case "ContractInvocationExtractor":
		return processor.NewContractInvocationExtractor(processorConfig.Config)
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
	case "Kale":
		return processor.NewKaleProcessor(processorConfig.Config)
	case "ContractData":
		return processor.NewContractDataProcessor(processorConfig.Config)
	case "TransactionXDRExtractor":
		return processor.ProcessorTransactionXDRExtractor(processorConfig.Config), nil
	case "WalletBackend":
		return processor.ProcessorWalletBackend(processorConfig.Config), nil
	case "ParticipantExtractor":
		return processor.ProcessorParticipantExtractor(processorConfig.Config), nil
	case "StellarEffects":
		return processor.ProcessorStellarEffects(processorConfig.Config), nil
	case "BronzeExtractors":
		return processor.NewBronzeExtractorsProcessor(processorConfig.Config), nil
	case "SilverLedgerReader": // Deprecated: use BronzeExtractors instead
		log.Println("WARNING: SilverLedgerReader is deprecated, use BronzeExtractors instead")
		return processor.NewBronzeExtractorsProcessor(processorConfig.Config), nil
	case "BronzeToContractInvocation":
		return processor.NewBronzeToContractInvocationProcessor(processorConfig.Config)
	default:
		return nil, fmt.Errorf("unsupported processor type: %s", processorConfig.Type)
	}
}

func CreateConsumerFunc(consumerConfig consumer.ConsumerConfig) (processor.Processor, error) {
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
	case "SaveToDuckLakeEnhanced":
		return consumer.NewSaveToDuckLakeEnhanced(consumerConfig.Config)
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
	case "SaveToPostgreSQLBronze":
		return consumer.NewSaveToPostgreSQLBronze(consumerConfig.Config)
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
	case "SaveExtractedContractInvocationsToPostgreSQL":
		return consumer.NewSaveExtractedContractInvocationsToPostgreSQL(consumerConfig.Config)
	case "SaveContractDataToPostgreSQL":
		return consumer.NewSaveContractDataToPostgreSQL(consumerConfig.Config)
	case "SaveToParquet":
		return consumer.NewSaveToParquet(consumerConfig.Config)
	case "DebugLogger":
		return consumer.NewDebugLogger(consumerConfig.Config)
	case "LogDebug":
		return consumer.NewLogDebug(consumerConfig.Config)
	case "WalletBackendPostgreSQL":
		return consumer.ConsumerWalletBackendPostgreSQL(consumerConfig.Config)
	case "SilverIngester":
		return consumer.NewSilverIngesterConsumer(consumerConfig.Config)
	case "BronzeToDuckDB":
		return consumer.NewBronzeToDuckDB(consumerConfig.Config)
	default:
		return nil, fmt.Errorf("unsupported consumer type: %s", consumerConfig.Type)
	}
}