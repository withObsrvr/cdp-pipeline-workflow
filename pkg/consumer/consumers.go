package consumer

import (
	"context"

	"github.com/withObsrvr/cdp-pipeline-workflow/pkg/common/types"
)

// BaseConsumer provides a base implementation for consumers
type BaseConsumer struct {
	processors []types.Processor
	name       string
}

// Process handles the incoming message
func (c *BaseConsumer) Process(ctx context.Context, msg types.Message) error {
	// Placeholder implementation
	return nil
}

// Subscribe adds a processor to this consumer
func (c *BaseConsumer) Subscribe(processor types.Processor) {
	c.processors = append(c.processors, processor)
}

// SaveToExcel is a consumer that saves data to Excel
type SaveToExcel struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveToExcel creates a new SaveToExcel consumer
func NewSaveToExcel(config map[string]interface{}) (types.Processor, error) {
	return &SaveToExcel{
		BaseConsumer: BaseConsumer{
			name: "SaveToExcel",
		},
		config: config,
	}, nil
}

// SaveToMongoDB is a consumer that saves data to MongoDB
type SaveToMongoDB struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveToMongoDB creates a new SaveToMongoDB consumer
func NewSaveToMongoDB(config map[string]interface{}) (types.Processor, error) {
	return &SaveToMongoDB{
		BaseConsumer: BaseConsumer{
			name: "SaveToMongoDB",
		},
		config: config,
	}, nil
}

// SaveToZeroMQ is a consumer that saves data to ZeroMQ
type SaveToZeroMQ struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveToZeroMQ creates a new SaveToZeroMQ consumer
func NewSaveToZeroMQ(config map[string]interface{}) (types.Processor, error) {
	return &SaveToZeroMQ{
		BaseConsumer: BaseConsumer{
			name: "SaveToZeroMQ",
		},
		config: config,
	}, nil
}

// SaveToGCS is a consumer that saves data to GCS
type SaveToGCS struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveToGCS creates a new SaveToGCS consumer
func NewSaveToGCS(config map[string]interface{}) (types.Processor, error) {
	return &SaveToGCS{
		BaseConsumer: BaseConsumer{
			name: "SaveToGCS",
		},
		config: config,
	}, nil
}

// SaveToDuckDB is a consumer that saves data to DuckDB
type SaveToDuckDB struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveToDuckDB creates a new SaveToDuckDB consumer
func NewSaveToDuckDB(config map[string]interface{}) (types.Processor, error) {
	return &SaveToDuckDB{
		BaseConsumer: BaseConsumer{
			name: "SaveToDuckDB",
		},
		config: config,
	}, nil
}

// SaveContractToDuckDB is a consumer that saves contract data to DuckDB
type SaveContractToDuckDB struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveContractToDuckDB creates a new SaveContractToDuckDB consumer
func NewSaveContractToDuckDB(config map[string]interface{}) (types.Processor, error) {
	return &SaveContractToDuckDB{
		BaseConsumer: BaseConsumer{
			name: "SaveContractToDuckDB",
		},
		config: config,
	}, nil
}

// SaveToTimescaleDB is a consumer that saves data to TimescaleDB
type SaveToTimescaleDB struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveToTimescaleDB creates a new SaveToTimescaleDB consumer
func NewSaveToTimescaleDB(config map[string]interface{}) (types.Processor, error) {
	return &SaveToTimescaleDB{
		BaseConsumer: BaseConsumer{
			name: "SaveToTimescaleDB",
		},
		config: config,
	}, nil
}

// SaveToRedis is a consumer that saves data to Redis
type SaveToRedis struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveToRedis creates a new SaveToRedis consumer
func NewSaveToRedis(config map[string]interface{}) (types.Processor, error) {
	return &SaveToRedis{
		BaseConsumer: BaseConsumer{
			name: "SaveToRedis",
		},
		config: config,
	}, nil
}

// NotificationDispatcher is a consumer that dispatches notifications
type NotificationDispatcher struct {
	BaseConsumer
	config map[string]interface{}
}

// NewNotificationDispatcher creates a new NotificationDispatcher consumer
func NewNotificationDispatcher(config map[string]interface{}) (types.Processor, error) {
	return &NotificationDispatcher{
		BaseConsumer: BaseConsumer{
			name: "NotificationDispatcher",
		},
		config: config,
	}, nil
}

// SaveToWebSocket is a consumer that saves data to WebSocket
type SaveToWebSocket struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveToWebSocket creates a new SaveToWebSocket consumer
func NewSaveToWebSocket(config map[string]interface{}) (types.Processor, error) {
	return &SaveToWebSocket{
		BaseConsumer: BaseConsumer{
			name: "SaveToWebSocket",
		},
		config: config,
	}, nil
}

// SaveToPostgreSQL is a consumer that saves data to PostgreSQL
type SaveToPostgreSQL struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveToPostgreSQL creates a new SaveToPostgreSQL consumer
func NewSaveToPostgreSQL(config map[string]interface{}) (types.Processor, error) {
	return &SaveToPostgreSQL{
		BaseConsumer: BaseConsumer{
			name: "SaveToPostgreSQL",
		},
		config: config,
	}, nil
}

// SaveToClickHouse is a consumer that saves data to ClickHouse
type SaveToClickHouse struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveToClickHouse creates a new SaveToClickHouse consumer
func NewSaveToClickHouse(config map[string]interface{}) (types.Processor, error) {
	return &SaveToClickHouse{
		BaseConsumer: BaseConsumer{
			name: "SaveToClickHouse",
		},
		config: config,
	}, nil
}

// SaveToMarketAnalyticsConsumer is a consumer that saves data to market analytics
type SaveToMarketAnalyticsConsumer struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveToMarketAnalyticsConsumer creates a new SaveToMarketAnalyticsConsumer
func NewSaveToMarketAnalyticsConsumer(config map[string]interface{}) (types.Processor, error) {
	return &SaveToMarketAnalyticsConsumer{
		BaseConsumer: BaseConsumer{
			name: "SaveToMarketAnalyticsConsumer",
		},
		config: config,
	}, nil
}

// SaveToRedisOrderbookConsumer is a consumer that saves orderbook data to Redis
type SaveToRedisOrderbookConsumer struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveToRedisOrderbookConsumer creates a new SaveToRedisOrderbookConsumer
func NewSaveToRedisOrderbookConsumer(config map[string]interface{}) (types.Processor, error) {
	return &SaveToRedisOrderbookConsumer{
		BaseConsumer: BaseConsumer{
			name: "SaveToRedisOrderbookConsumer",
		},
		config: config,
	}, nil
}

// SaveAssetToPostgreSQL is a consumer that saves asset data to PostgreSQL
type SaveAssetToPostgreSQL struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveAssetToPostgreSQL creates a new SaveAssetToPostgreSQL
func NewSaveAssetToPostgreSQL(config map[string]interface{}) (types.Processor, error) {
	return &SaveAssetToPostgreSQL{
		BaseConsumer: BaseConsumer{
			name: "SaveAssetToPostgreSQL",
		},
		config: config,
	}, nil
}

// StdoutConsumer is a consumer that outputs to stdout
type StdoutConsumer struct {
	BaseConsumer
}

// NewStdoutConsumer creates a new StdoutConsumer
func NewStdoutConsumer() types.Processor {
	return &StdoutConsumer{
		BaseConsumer: BaseConsumer{
			name: "StdoutConsumer",
		},
	}
}

// SaveAssetEnrichmentConsumer is a consumer that saves asset enrichment data
type SaveAssetEnrichmentConsumer struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveAssetEnrichmentConsumer creates a new SaveAssetEnrichmentConsumer
func NewSaveAssetEnrichmentConsumer(config map[string]interface{}) (types.Processor, error) {
	return &SaveAssetEnrichmentConsumer{
		BaseConsumer: BaseConsumer{
			name: "SaveAssetEnrichmentConsumer",
		},
		config: config,
	}, nil
}

// SavePaymentToPostgreSQL is a consumer that saves payment data to PostgreSQL
type SavePaymentToPostgreSQL struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSavePaymentToPostgreSQL creates a new SavePaymentToPostgreSQL
func NewSavePaymentToPostgreSQL(config map[string]interface{}) (types.Processor, error) {
	return &SavePaymentToPostgreSQL{
		BaseConsumer: BaseConsumer{
			name: "SavePaymentToPostgreSQL",
		},
		config: config,
	}, nil
}

// SavePaymentsToRedis is a consumer that saves payments to Redis
type SavePaymentsToRedis struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSavePaymentsToRedis creates a new SavePaymentsToRedis
func NewSavePaymentsToRedis(config map[string]interface{}) (types.Processor, error) {
	return &SavePaymentsToRedis{
		BaseConsumer: BaseConsumer{
			name: "SavePaymentsToRedis",
		},
		config: config,
	}, nil
}

// SaveLatestLedgerRedis is a consumer that saves the latest ledger to Redis
type SaveLatestLedgerRedis struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveLatestLedgerRedis creates a new SaveLatestLedgerRedis
func NewSaveLatestLedgerRedis(config map[string]interface{}) (types.Processor, error) {
	return &SaveLatestLedgerRedis{
		BaseConsumer: BaseConsumer{
			name: "SaveLatestLedgerRedis",
		},
		config: config,
	}, nil
}

// SaveLatestLedgerToExcel is a consumer that saves the latest ledger to Excel
type SaveLatestLedgerToExcel struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveLatestLedgerToExcel creates a new SaveLatestLedgerToExcel
func NewSaveLatestLedgerToExcel(config map[string]interface{}) (types.Processor, error) {
	return &SaveLatestLedgerToExcel{
		BaseConsumer: BaseConsumer{
			name: "SaveLatestLedgerToExcel",
		},
		config: config,
	}, nil
}

// AnthropicClaudeConsumer is a consumer that uses Anthropic Claude
type AnthropicClaudeConsumer struct {
	BaseConsumer
	config map[string]interface{}
}

// NewAnthropicClaudeConsumer creates a new AnthropicClaudeConsumer
func NewAnthropicClaudeConsumer(config map[string]interface{}) (types.Processor, error) {
	return &AnthropicClaudeConsumer{
		BaseConsumer: BaseConsumer{
			name: "AnthropicClaudeConsumer",
		},
		config: config,
	}, nil
}

// SaveSoroswapPairsToDuckDB is a consumer that saves Soroswap pairs to DuckDB
type SaveSoroswapPairsToDuckDB struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveSoroswapPairsToDuckDB creates a new SaveSoroswapPairsToDuckDB
func NewSaveSoroswapPairsToDuckDB(config map[string]interface{}) (types.Processor, error) {
	return &SaveSoroswapPairsToDuckDB{
		BaseConsumer: BaseConsumer{
			name: "SaveSoroswapPairsToDuckDB",
		},
		config: config,
	}, nil
}

// SaveSoroswapRouterToDuckDB is a consumer that saves Soroswap router data to DuckDB
type SaveSoroswapRouterToDuckDB struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveSoroswapRouterToDuckDB creates a new SaveSoroswapRouterToDuckDB
func NewSaveSoroswapRouterToDuckDB(config map[string]interface{}) (types.Processor, error) {
	return &SaveSoroswapRouterToDuckDB{
		BaseConsumer: BaseConsumer{
			name: "SaveSoroswapRouterToDuckDB",
		},
		config: config,
	}, nil
}

// SaveAccountDataToPostgreSQL is a consumer that saves account data to PostgreSQL
type SaveAccountDataToPostgreSQL struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveAccountDataToPostgreSQL creates a new SaveAccountDataToPostgreSQL
func NewSaveAccountDataToPostgreSQL(config map[string]interface{}) (types.Processor, error) {
	return &SaveAccountDataToPostgreSQL{
		BaseConsumer: BaseConsumer{
			name: "SaveAccountDataToPostgreSQL",
		},
		config: config,
	}, nil
}

// SaveAccountDataToDuckDB is a consumer that saves account data to DuckDB
type SaveAccountDataToDuckDB struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveAccountDataToDuckDB creates a new SaveAccountDataToDuckDB
func NewSaveAccountDataToDuckDB(config map[string]interface{}) (types.Processor, error) {
	return &SaveAccountDataToDuckDB{
		BaseConsumer: BaseConsumer{
			name: "SaveAccountDataToDuckDB",
		},
		config: config,
	}, nil
}

// SaveContractEventsToPostgreSQL is a consumer that saves contract events to PostgreSQL
type SaveContractEventsToPostgreSQL struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveContractEventsToPostgreSQL creates a new SaveContractEventsToPostgreSQL
func NewSaveContractEventsToPostgreSQL(config map[string]interface{}) (types.Processor, error) {
	return &SaveContractEventsToPostgreSQL{
		BaseConsumer: BaseConsumer{
			name: "SaveContractEventsToPostgreSQL",
		},
		config: config,
	}, nil
}

// SaveSoroswapToPostgreSQL is a consumer that saves Soroswap data to PostgreSQL
type SaveSoroswapToPostgreSQL struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveSoroswapToPostgreSQL creates a new SaveSoroswapToPostgreSQL
func NewSaveSoroswapToPostgreSQL(config map[string]interface{}) (types.Processor, error) {
	return &SaveSoroswapToPostgreSQL{
		BaseConsumer: BaseConsumer{
			name: "SaveSoroswapToPostgreSQL",
		},
		config: config,
	}, nil
}

// SaveContractInvocationsToPostgreSQL is a consumer that saves contract invocations to PostgreSQL
type SaveContractInvocationsToPostgreSQL struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveContractInvocationsToPostgreSQL creates a new SaveContractInvocationsToPostgreSQL
func NewSaveContractInvocationsToPostgreSQL(config map[string]interface{}) (types.Processor, error) {
	return &SaveContractInvocationsToPostgreSQL{
		BaseConsumer: BaseConsumer{
			name: "SaveContractInvocationsToPostgreSQL",
		},
		config: config,
	}, nil
}

// SaveContractDataToPostgreSQL is a consumer that saves contract data to PostgreSQL
type SaveContractDataToPostgreSQL struct {
	BaseConsumer
	config map[string]interface{}
}

// NewSaveContractDataToPostgreSQL creates a new SaveContractDataToPostgreSQL
func NewSaveContractDataToPostgreSQL(config map[string]interface{}) (types.Processor, error) {
	return &SaveContractDataToPostgreSQL{
		BaseConsumer: BaseConsumer{
			name: "SaveContractDataToPostgreSQL",
		},
		config: config,
	}, nil
}
