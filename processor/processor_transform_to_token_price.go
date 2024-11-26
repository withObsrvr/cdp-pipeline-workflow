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
	MarketCap         float64   `json:"market_cap"`
	CirculatingSupply float64   `json:"circulating_supply"`
	Rank              int       `json:"rank"`
	PriceChange1h     float64   `json:"price_change_1h"`
	PriceChange7d     float64   `json:"price_change_7d"`
	SparklineData     []float64 `json:"sparkline_data"`
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

type TradeValidationError struct {
	Field string
	Error error
}

type PriceMetrics struct {
	Volume24h      float64
	NumTrades24h   int64
	HighPrice24h   float64
	LowPrice24h    float64
	PriceChange24h float64
	FirstPrice     float64
	LastPrice      float64
}

func NewTransformToTokenPrice(config map[string]interface{}) (*TransformToTokenPrice, error) {
	redisConfig, err := parseRedisConfig(config)
	if err != nil {
		return nil, err
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:        redisConfig.Address,
		Password:    redisConfig.Password,
		DB:          redisConfig.DB,
		MaxRetries:  redisConfig.MaxRetries,
		PoolSize:    redisConfig.PoolSize,
		ReadTimeout: redisConfig.ReadTimeout,
	})

	// Test connection
	ctx := context.Background()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %v", err)
	}

	return &TransformToTokenPrice{
		networkPassphrase: redisConfig.KeyPrefix,
		redisClient:       redisClient,
	}, nil
}

func (t *TransformToTokenPrice) Subscribe(processor Processor) {
	t.processors = append(t.processors, processor)
}

func (t *TransformToTokenPrice) Process(ctx context.Context, msg Message) error {
	log.Printf("Processing message in TransformToTokenPrice")
	ledgerCloseMeta, err := ExtractLedgerCloseMeta(msg)
	if err != nil {
		return fmt.Errorf("failed to extract ledger close meta: %w", err)
	}

	ledgerTxReader, err := CreateTransactionReader(ledgerCloseMeta, t.networkPassphrase)
	if err != nil {
		return fmt.Errorf("failed to create transaction reader: %w", err)
	}

	closeTime := uint32(ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime)

	var transaction ingest.LedgerTransaction
	for {
		transaction, err = ledgerTxReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %w", err)
		}

		for _, op := range transaction.Envelope.Operations() {
			switch op.Body.Type {
			case xdr.OperationTypeManageBuyOffer, xdr.OperationTypeManageSellOffer:
				trade, err := t.extractTradeFromOperation(op, transaction, closeTime)
				if err != nil {
					log.Printf("Error extracting trade: %v", err)
					continue
				}

				// Validate trade data
				if trade.SellAsset == "" || trade.BuyAsset == "" ||
					trade.SellAmount == "" || trade.BuyAmount == "" {
					return fmt.Errorf("invalid trade data: missing required fields")
				}

				// Calculate price from trade
				ts, err := parseTimestamp(trade.Timestamp)
				if err != nil {
					return fmt.Errorf("error parsing timestamp: %w", err)
				}

				// Extract asset details
				baseAsset, baseIssuer := parseAsset(trade.SellAsset)
				quoteAsset, quoteIssuer := parseAsset(trade.BuyAsset)

				log.Printf("Processing trade for price calculation: %+v", trade)

				// Get 24h metrics from Redis
				tsKey := fmt.Sprintf("%sprice:%s:%s:history", "stellar:", baseAsset, quoteAsset)

				// Get last 24h window
				now := time.Now()
				start := now.Add(-24 * time.Hour).Unix()

				// Get all prices in last 24h
				prices, err := t.redisClient.ZRangeByScoreWithScores(ctx, tsKey, &redis.ZRangeBy{
					Min: fmt.Sprintf("%d", start),
					Max: fmt.Sprintf("%d", now.Unix()),
				}).Result()

				var volume24h float64
				var highPrice24h float64 = trade.Price // Start with current price
				var lowPrice24h float64 = trade.Price  // Start with current price

				// Get current trade volume
				sellAmount, _ := strconv.ParseFloat(trade.SellAmount, 64)
				volume24h = sellAmount // Initialize with current trade volume

				// Add historical volumes
				if len(prices) > 0 {
					// Iterate through prices to find high/low
					for _, p := range prices {
						price := p.Member.(string)
						priceFloat, _ := strconv.ParseFloat(price, 64)

						if priceFloat > highPrice24h {
							highPrice24h = priceFloat
						}
						if priceFloat < lowPrice24h {
							lowPrice24h = priceFloat
						}
					}
				}

				payload := &TokenPrice{
					Timestamp:    ts,
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

				if err := ForwardToProcessors(ctx, payload, t.processors); err != nil {
					return fmt.Errorf("error forwarding token price: %w", err)
				}
				log.Printf("Successfully forwarded trade: %+v", trade)

			}
		}
	}

	return nil
}

func (t *TransformToTokenPrice) extractTradeFromOperation(op xdr.Operation, tx ingest.LedgerTransaction, closeTime uint32) (*AppTrade, error) {
	trade := &AppTrade{
		Timestamp: fmt.Sprintf("%d", closeTime),
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
		trade.Price = float64(offer.Price.N) / float64(offer.Price.D)
		trade.OrderbookID = fmt.Sprintf("%d", offer.OfferId)

	case xdr.OperationTypeManageSellOffer:
		offer := op.Body.MustManageSellOfferOp()
		trade.SellAsset = offer.Selling.StringCanonical()
		trade.BuyAsset = offer.Buying.StringCanonical()
		trade.SellAmount = amount.String(offer.Amount)
		trade.BuyAmount = amount.String(offer.Amount)
		trade.Price = float64(offer.Price.N) / float64(offer.Price.D)
		trade.OrderbookID = fmt.Sprintf("%d", offer.OfferId)

	default:
		return nil, nil
	}

	// Validate trade data
	if trade.SellAsset == "" || trade.BuyAsset == "" ||
		trade.SellAmount == "" || trade.BuyAmount == "" {
		return nil, nil // Skip invalid trades without error
	}

	// Parse asset details
	sellAssetDetails, err := t.parseAssetDetails(trade.SellAsset)
	if err != nil {
		return nil, fmt.Errorf("error parsing sell asset: %w", err)
	}
	trade.SellAssetDetails = sellAssetDetails

	buyAssetDetails, err := t.parseAssetDetails(trade.BuyAsset)
	if err != nil {
		return nil, fmt.Errorf("error parsing buy asset: %w", err)
	}
	trade.BuyAssetDetails = buyAssetDetails

	return trade, nil
}

func (t *TransformToTokenPrice) storeAssetInfo(ctx context.Context, asset AssetDetails) error {
	key := fmt.Sprintf("stellar:asset:%s:%s", asset.Code, asset.Issuer)

	err := t.redisClient.HSet(ctx, key,
		map[string]interface{}{
			"code":         asset.Code,
			"issuer":       asset.Issuer,
			"type":         asset.Type,
			"last_updated": time.Now().Format(time.RFC3339),
		}).Err()
	if err != nil {
		return err
	}

	// Update assets set
	assetKey := fmt.Sprintf("%s:%s", asset.Code, asset.Issuer)
	return t.redisClient.SAdd(ctx, "stellar:assets", assetKey).Err()
}

func (t *TransformToTokenPrice) getAssetStats(ctx context.Context, code, issuer string) (*AssetStats, error) {
	key := fmt.Sprintf("stellar:asset:%s:%s", code, issuer)

	data, err := t.redisClient.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	stats := &AssetStats{}
	stats.CirculatingSupply, _ = strconv.ParseFloat(data["circulating_supply"], 64)
	stats.NumHolders, _ = strconv.ParseUint(data["num_holders"], 10, 64)
	stats.TotalSupply, _ = strconv.ParseFloat(data["total_supply"], 64)
	stats.BalanceAuthorized, _ = strconv.ParseFloat(data["balance_authorized"], 64)
	stats.BalanceAuthLiabilities, _ = strconv.ParseFloat(data["balance_auth_liabilities"], 64)

	return stats, nil
}

func (t *TransformToTokenPrice) parseAssetDetails(assetStr string) (AssetDetails, error) {
	parts := strings.Split(assetStr, ":")

	details := AssetDetails{}

	if len(parts) == 1 && parts[0] == "native" {
		details.Code = "XLM"
		details.Type = "native"
	} else if len(parts) == 2 {
		details.Code = parts[0]
		details.Issuer = parts[1]
		details.Type = "credit_alphanum4"
		if len(parts[0]) > 4 {
			details.Type = "credit_alphanum12"
		}
	} else {
		return details, fmt.Errorf("invalid asset format: %s", assetStr)
	}

	return details, nil
}

func (t *TransformToTokenPrice) GetMetrics() ProcessorMetrics {
	return t.metrics
}

func (t *TransformToTokenPrice) get24hMetrics(ctx context.Context, baseAsset, quoteAsset string, tradeTime time.Time) (*PriceMetrics, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	tsKey := fmt.Sprintf("%sprice:%s:%s:history", "stellar:", baseAsset, quoteAsset)
	start := tradeTime.Add(-24 * time.Hour).Unix()
	end := tradeTime.Unix()

	prices, err := t.redisClient.ZRangeByScoreWithScores(timeoutCtx, tsKey, &redis.ZRangeBy{
		Min: fmt.Sprintf("%d", start),
		Max: fmt.Sprintf("%d", end),
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("failed to get 24h metrics: %w", err)
	}

	// Process prices and return metrics
	return calculatePriceMetrics(prices), nil
}

func (t *TransformToTokenPrice) validateTrade(trade *AppTrade) error {
	var validationErrors []TradeValidationError

	if trade.SellAsset == "" {
		validationErrors = append(validationErrors, TradeValidationError{
			Field: "SellAsset",
			Error: fmt.Errorf("sell asset is required"),
		})
	}
	// Add other validations...

	if len(validationErrors) > 0 {
		return fmt.Errorf("trade validation failed: %v", validationErrors)
	}
	return nil
}

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

func calculatePriceMetrics(prices []redis.Z) *PriceMetrics {
	metrics := &PriceMetrics{
		NumTrades24h: int64(len(prices)),
	}

	if len(prices) == 0 {
		return metrics
	}

	// Initialize with first price
	firstPrice, _ := strconv.ParseFloat(prices[0].Member.(string), 64)
	metrics.FirstPrice = firstPrice
	metrics.HighPrice24h = firstPrice
	metrics.LowPrice24h = firstPrice

	// Process all prices
	for _, p := range prices {
		price, _ := strconv.ParseFloat(p.Member.(string), 64)

		// Update high/low
		if price > metrics.HighPrice24h {
			metrics.HighPrice24h = price
		}
		if price < metrics.LowPrice24h {
			metrics.LowPrice24h = price
		}

		// Add to volume (assuming score is volume)
		metrics.Volume24h += p.Score
	}

	// Get last price and calculate change
	lastPrice, _ := strconv.ParseFloat(prices[len(prices)-1].Member.(string), 64)
	metrics.LastPrice = lastPrice

	if metrics.FirstPrice > 0 {
		metrics.PriceChange24h = ((lastPrice - metrics.FirstPrice) / metrics.FirstPrice) * 100
	}

	return metrics
}
