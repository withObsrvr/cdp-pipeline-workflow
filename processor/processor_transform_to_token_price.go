package processor

import (
	"context"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stellar/go/amount"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledger"
	"github.com/stellar/go/xdr"
)

type TokenPrice struct {
	Timestamp         time.Time `json:"timestamp"`
	BaseAsset         string    `json:"base_asset"`
	BaseIssuer        string    `json:"base_issuer"`
	QuoteAsset        string    `json:"quote_asset"`
	QuoteIssuer       string    `json:"quote_issuer"`
	Price             float64   `json:"price"`
	Volume24h         float64   `json:"volume_24h"`
	NumTrades24h      int64     `json:"num_trades_24h"`
	HighPrice24h      float64   `json:"high_price_24h"`
	LowPrice24h       float64   `json:"low_price_24h"`
	PriceChange24h    float64   `json:"price_change_24h"`
	Type              string    `json:"type"`
	MarketCap         float64   `json:"market_cap,omitempty"`
	CirculatingSupply float64   `json:"circulating_supply,omitempty"`
	Rank              int       `json:"rank,omitempty"`
	PriceChange1h     float64   `json:"price_change_1h,omitempty"`
	PriceChange7d     float64   `json:"price_change_7d,omitempty"`
	SparklineData     []float64 `json:"sparkline_data,omitempty"`
}

type RedisConfig struct {
	Address     string
	Password    string
	DB          int
	MaxRetries  int
	PoolSize    int
	BatchSize   int
	KeyPrefix   string
	ReadTimeout time.Duration
}

type TransformToTokenPrice struct {
	networkPassphrase string
	redisClient       *redis.Client
	processors        []Processor
	batchSize         int
	tradeBatch        []TokenPrice
	stats             struct {
		messagesReceived int64
		batchesProcessed int64
		lastProcessedAt  time.Time
	}
	metrics ProcessorMetrics
}

type ProcessorMetrics struct {
	TradesProcessed   int64
	PricesCalculated  int64
	RedisErrors       int64
	ProcessingTime    time.Duration
	LastProcessedTime time.Time
	BatchSize         int
}

// NewTransformToTokenPrice creates a new TransformToTokenPrice processor.
// It uses the provided config to initialize Redis and other settings.
func NewTransformToTokenPrice(config map[string]interface{}) (*TransformToTokenPrice, error) {
	redisConfig, err := parseRedisConfig(config)
	if err != nil {
		return nil, err
	}

	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok || networkPassphrase == "" {
		return nil, fmt.Errorf("missing network_passphrase in configuration")
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:        redisConfig.Address,
		Password:    redisConfig.Password,
		DB:          redisConfig.DB,
		MaxRetries:  redisConfig.MaxRetries,
		PoolSize:    redisConfig.PoolSize,
		ReadTimeout: redisConfig.ReadTimeout,
	})

	ctx := context.Background()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %v", err)
	}

	return &TransformToTokenPrice{
		networkPassphrase: networkPassphrase,
		redisClient:       redisClient,
	}, nil
}

// Subscribe adds a downstream processor to which the token price messages will be forwarded.
func (t *TransformToTokenPrice) Subscribe(processor Processor) {
	t.processors = append(t.processors, processor)
}

// Process reads ledger transactions, extracts trade data from offer management operations,
// calculates the token price metrics, and forwards the price information to downstream processors.
func (t *TransformToTokenPrice) Process(ctx context.Context, msg Message) error {
	log.Printf("Processing message in TransformToTokenPrice")

	ledgerCloseMeta, err := ExtractLedgerCloseMeta(msg)
	if err != nil {
		return fmt.Errorf("failed to extract ledger close meta: %w", err)
	}

	closeTime := ledger.ClosedAt(ledgerCloseMeta)

	ledgerTxReader, err := CreateTransactionReader(ledgerCloseMeta, t.networkPassphrase)
	if err != nil {
		return fmt.Errorf("failed to create transaction reader: %w", err)
	}
	defer ledgerTxReader.Close()

	// Loop over each transaction in the ledger.
	for {
		transaction, err := ledgerTxReader.Read()
		if err == io.EOF {
			break
		}
		// Instead of aborting on an "unknown tx hash" error, log and skip the offending transaction.
		if err != nil {
			if strings.Contains(err.Error(), "unknown tx hash") {
				log.Printf("Skipping transaction due to unknown tx hash error: %v", err)
				continue
			}
			return fmt.Errorf("error reading transaction: %w", err)
		}

		// Iterate over operations to find trade offers.
		for _, op := range transaction.Envelope.Operations() {
			switch op.Body.Type {
			case xdr.OperationTypeManageBuyOffer, xdr.OperationTypeManageSellOffer:
				trade, err := t.extractTradeFromOperation(op, transaction, closeTime)
				if err != nil {
					log.Printf("Error extracting trade: %v", err)
					continue
				}

				if trade.SellAsset == "" || trade.BuyAsset == "" ||
					trade.SellAmount == "" || trade.BuyAmount == "" {
					log.Printf("Skipping trade due to missing required fields")
					continue
				}

				timestamp := closeTime

				// Extract asset details
				baseAsset, baseIssuer := parseAsset(trade.SellAsset)
				quoteAsset, quoteIssuer := parseAsset(trade.BuyAsset)

				log.Printf("Processing trade for price calculation: %+v", trade)

				// Build the Redis key for historical prices and query the 24h window.
				tsKey := fmt.Sprintf("%sprice:%s:%s:history", "stellar:", baseAsset, quoteAsset)
				now := time.Now()
				start := now.Add(-24 * time.Hour).Unix()

				redisCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				prices, err := t.redisClient.ZRangeByScoreWithScores(redisCtx, tsKey, &redis.ZRangeBy{
					Min: fmt.Sprintf("%d", start),
					Max: fmt.Sprintf("%d", now.Unix()),
				}).Result()
				cancel()
				if err != nil {
					t.metrics.RedisErrors++
					log.Printf("Error fetching historical prices from Redis key %s: %v", tsKey, err)
				}

				// Calculate simple metrics: volume, high, and low prices.
				var volume24h float64
				highPrice24h := trade.Price
				lowPrice24h := trade.Price

				sellAmount, _ := strconv.ParseFloat(trade.SellAmount, 64)
				volume24h = sellAmount

				for _, p := range prices {
					priceStr, ok := p.Member.(string)
					if !ok {
						continue
					}
					priceFloat, err := strconv.ParseFloat(priceStr, 64)
					if err != nil {
						continue
					}
					if priceFloat > highPrice24h {
						highPrice24h = priceFloat
					}
					if priceFloat < lowPrice24h {
						lowPrice24h = priceFloat
					}
				}

				// Prepare the token price payload.
				payload := &TokenPrice{
					Timestamp:    timestamp,
					BaseAsset:    baseAsset,
					BaseIssuer:   baseIssuer,
					QuoteAsset:   quoteAsset,
					QuoteIssuer:  quoteIssuer,
					Price:        trade.Price,
					Volume24h:    volume24h,
					HighPrice24h: highPrice24h,
					LowPrice24h:  lowPrice24h,
					Type:         "token_price",
				}

				// Forward the token price to downstream processors.
				if err := ForwardToProcessors(ctx, payload, t.processors); err != nil {
					return fmt.Errorf("error forwarding token price: %w", err)
				}
				log.Printf("Successfully forwarded trade: %+v", trade)
			}
		}
	}

	return nil
}

// extractTradeFromOperation converts offer management operations into a standardized AppTrade.
// It supports both ManageBuyOffer and ManageSellOffer types.
func (t *TransformToTokenPrice) extractTradeFromOperation(op xdr.Operation, tx ingest.LedgerTransaction, closeTime time.Time) (*AppTrade, error) {
	trade := &AppTrade{
		Timestamp: fmt.Sprintf("%d", closeTime.Unix()),
		Type:      "trade",
		SellerId:  tx.Envelope.SourceAccount().ToAccountId().Address(),
	}
	switch op.Body.Type {
	case xdr.OperationTypeManageBuyOffer:
		offer := op.Body.MustManageBuyOfferOp()
		trade.SellAsset = offer.Selling.StringCanonical()
		trade.BuyAsset = offer.Buying.StringCanonical()
		trade.SellAmount = amount.String(offer.BuyAmount)
		trade.BuyAmount = amount.String(offer.BuyAmount)
		if offer.Price.D == 0 {
			return nil, fmt.Errorf("invalid price denominator is 0")
		}
		trade.Price = float64(offer.Price.N) / float64(offer.Price.D)
		trade.OrderbookID = fmt.Sprintf("%d", offer.OfferId)
	case xdr.OperationTypeManageSellOffer:
		offer := op.Body.MustManageSellOfferOp()
		trade.SellAsset = offer.Selling.StringCanonical()
		trade.BuyAsset = offer.Buying.StringCanonical()
		trade.SellAmount = amount.String(offer.Amount)
		trade.BuyAmount = amount.String(offer.Amount)
		if offer.Price.D == 0 {
			return nil, fmt.Errorf("invalid price denominator is 0")
		}
		trade.Price = float64(offer.Price.N) / float64(offer.Price.D)
		trade.OrderbookID = fmt.Sprintf("%d", offer.OfferId)
	default:
		return nil, fmt.Errorf("unsupported operation type: %v", op.Body.Type)
	}
	if trade.SellAsset == "" || trade.BuyAsset == "" {
		return nil, fmt.Errorf("trade asset details missing")
	}
	return trade, nil
}

// parseAsset splits an asset string into its code and issuer parts.
// It expects the asset string to be formatted as "CODE:ISSUER". If no colon is found, the entire string is returned as the asset code.
func parseAsset(assetStr string) (string, string) {
	parts := strings.Split(assetStr, ":")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return assetStr, ""
}

// Cleanup closes the Redis connection.
func (t *TransformToTokenPrice) Cleanup(ctx context.Context) error {
	if t.redisClient != nil {
		if err := t.redisClient.Close(); err != nil {
			return fmt.Errorf("error closing Redis connection: %w", err)
		}
	}
	return nil
}

func parseRedisConfig(config map[string]interface{}) (RedisConfig, error) {
	var redisConfig RedisConfig

	addr, ok := config["redis_address"].(string)
	if !ok {
		return redisConfig, fmt.Errorf("missing redis_address in config")
	}
	redisConfig.Address = addr

	if password, ok := config["redis_password"].(string); ok {
		redisConfig.Password = password
	}

	if db, ok := config["redis_db"].(float64); ok {
		redisConfig.DB = int(db)
	}

	if maxRetries, ok := config["redis_max_retries"].(float64); ok {
		redisConfig.MaxRetries = int(maxRetries)
	} else {
		redisConfig.MaxRetries = 3 // default
	}

	if poolSize, ok := config["redis_pool_size"].(float64); ok {
		redisConfig.PoolSize = int(poolSize)
	} else {
		redisConfig.PoolSize = 10 // default
	}

	if batchSize, ok := config["batch_size"].(float64); ok {
		redisConfig.BatchSize = int(batchSize)
	} else {
		redisConfig.BatchSize = 100 // default
	}

	if prefix, ok := config["redis_key_prefix"].(string); ok {
		redisConfig.KeyPrefix = prefix
	} else {
		redisConfig.KeyPrefix = "stellar:" // default
	}

	redisConfig.ReadTimeout = 5 * time.Second // default timeout

	return redisConfig, nil
}
