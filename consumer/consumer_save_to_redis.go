package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

type SaveToRedis struct {
	client     *redis.Client
	processors []processor.Processor
	keyPrefix  string
	ttl        time.Duration
}

type RedisConfig struct {
	Address   string
	Password  string
	DB        int
	KeyPrefix string
	TTLHours  int
}

func NewSaveToRedis(config map[string]interface{}) (*SaveToRedis, error) {
	// Parse configuration
	redisConfig, err := parseRedisConfig(config)
	if err != nil {
		return nil, err
	}

	// Create Redis client
	client := redis.NewClient(&redis.Options{
		Addr:     redisConfig.Address,
		Password: redisConfig.Password,
		DB:       redisConfig.DB,
	})

	// Test connection
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %v", err)
	}

	ttl := time.Duration(redisConfig.TTLHours) * time.Hour

	return &SaveToRedis{
		client:    client,
		keyPrefix: redisConfig.KeyPrefix,
		ttl:       ttl,
	}, nil
}

func parseRedisConfig(config map[string]interface{}) (RedisConfig, error) {
	var redisConfig RedisConfig

	addr, ok := config["address"].(string)
	if !ok {
		return redisConfig, fmt.Errorf("missing Redis address")
	}
	redisConfig.Address = addr

	if password, ok := config["password"].(string); ok {
		redisConfig.Password = password
	}

	if db, ok := config["db"].(float64); ok {
		redisConfig.DB = int(db)
	}

	if prefix, ok := config["key_prefix"].(string); ok {
		redisConfig.KeyPrefix = prefix
	} else {
		redisConfig.KeyPrefix = "stellar:"
	}

	if ttl, ok := config["ttl_hours"].(float64); ok {
		redisConfig.TTLHours = int(ttl)
	} else {
		redisConfig.TTLHours = 24 // Default 24 hours TTL
	}

	return redisConfig, nil
}

func (r *SaveToRedis) Subscribe(processor processor.Processor) {
	r.processors = append(r.processors, processor)
}

func (r *SaveToRedis) Process(ctx context.Context, msg processor.Message) error {
	log.Printf("Processing message in SaveToRedis")
	// Parse message payload
	var data map[string]interface{}
	if err := json.Unmarshal(msg.Payload.([]byte), &data); err != nil {
		return fmt.Errorf("error unmarshaling payload: %w", err)
	}

	// Get operation type from operation_type field
	operationType, ok := data["operation_type"].(string)
	if !ok || operationType == "" {
		// Fallback to type field for backward compatibility
		operationType, ok = data["type"].(string)
		if !ok || operationType == "" {
			return fmt.Errorf("missing operation type in payload")
		}
	}

	// Process based on operation type
	switch operationType {
	case "change_trust":
		return r.processChangeTrust(ctx, data)
	case "payment":
		return r.processPayment(ctx, data)
	case "create_account":
		return r.processCreateAccount(ctx, data)
	case "token_price":
		return r.processTokenPrice(ctx, data)
	case "manage_buy_offer", "manage_sell_offer":
		return r.processOffer(ctx, data)
	case "set_trust_line_flags":
		return r.processSetTrustLineFlags(ctx, data)
	case "asset_stats":
		return r.handleAssetStats(ctx, data)
	default:
		return fmt.Errorf("unsupported operation type: %s", operationType)
	}
}

func (r *SaveToRedis) processChangeTrust(ctx context.Context, data map[string]interface{}) error {
	// Extract required fields
	code, ok := data["code"].(string)
	if !ok {
		return fmt.Errorf("missing code in data")
	}
	issuer, ok := data["issuer"].(string)
	if !ok {
		return fmt.Errorf("missing issuer in data")
	}

	// Clean asset code by removing null bytes
	code = strings.TrimRight(code, "\x00")

	// Create Redis key
	key := fmt.Sprintf("%sasset:%s:%s", r.keyPrefix, code, issuer)

	// Prepare data for Redis
	redisData := make(map[string]interface{})

	// Required fields
	redisData["asset_code"] = code
	redisData["asset_issuer"] = issuer
	redisData["type"] = "change_trust"
	redisData["operation_type"] = "change_trust"
	redisData["last_updated"] = time.Now().Format(time.RFC3339)

	// Optional fields - only add if they exist and are non-zero
	if assetType, ok := data["asset_type"].(string); ok && assetType != "" {
		redisData["asset_type"] = assetType
	}
	if authRequired, ok := data["auth_required"].(bool); ok {
		redisData["auth_required"] = authRequired
	}
	if authRevocable, ok := data["auth_revocable"].(bool); ok {
		redisData["auth_revocable"] = authRevocable
	}

	// Store in Redis
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, key, redisData)

	// Set TTL (30 days)
	pipe.Expire(ctx, key, 30*24*time.Hour)

	// Execute pipeline
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("error executing Redis pipeline: %w", err)
	}

	return nil
}

func (r *SaveToRedis) processPayment(ctx context.Context, data map[string]interface{}) error {
	// Extract required fields with nil checks
	code, ok := data["code"].(string)
	if !ok || code == "" {
		return fmt.Errorf("missing or invalid code in payment data")
	}

	// For native assets (XLM), issuer should be empty
	assetType, _ := data["asset_type"].(string)
	if assetType == "native" {
		// Create Redis key for native asset
		key := fmt.Sprintf("%sasset:%s", r.keyPrefix, code)

		// Prepare data for Redis
		redisData := map[string]interface{}{
			"asset_code":     code,
			"asset_type":     "native",
			"operation_type": "payment",
			"last_updated":   time.Now().Format(time.RFC3339),
		}

		if amount, ok := data["amount"].(string); ok {
			redisData["amount"] = amount
		}

		// Store in Redis
		pipe := r.client.Pipeline()
		pipe.HSet(ctx, key, redisData)
		if r.ttl > 0 {
			pipe.Expire(ctx, key, r.ttl)
		}

		_, err := pipe.Exec(ctx)
		if err != nil {
			return fmt.Errorf("error executing Redis pipeline: %w", err)
		}

		return nil
	}

	// For non-native assets, require issuer
	issuer, ok := data["issuer"].(string)
	if !ok || issuer == "" {
		return fmt.Errorf("missing or invalid issuer in payment data")
	}

	amount, ok := data["amount"].(string)
	if !ok {
		// Set default amount if missing
		amount = "0"
	}

	// Clean asset code
	code = strings.TrimRight(code, "\x00")

	// Create Redis key
	key := fmt.Sprintf("%sasset:%s:%s", r.keyPrefix, code, issuer)

	// Prepare data for Redis
	redisData := map[string]interface{}{
		"asset_code":     code,
		"asset_issuer":   issuer,
		"amount":         amount,
		"operation_type": "payment",
		"last_updated":   time.Now().Format(time.RFC3339),
	}

	// Optional fields
	if assetType, ok := data["asset_type"].(string); ok && assetType != "" {
		redisData["asset_type"] = assetType
	}

	// Store in Redis
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, key, redisData)
	pipe.Expire(ctx, key, r.ttl)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("error executing Redis pipeline: %w", err)
	}

	return nil
}

func (r *SaveToRedis) processCreateAccount(ctx context.Context, data map[string]interface{}) error {
	// Store the full account creation data
	accountKey := fmt.Sprintf("%saccount_creation:%s:%s",
		r.keyPrefix, data["timestamp"], data["account"])

	if err := r.storeJSON(ctx, accountKey, data); err != nil {
		return err
	}

	// Update account creation metrics
	statsKey := fmt.Sprintf("%sstats:account_creation", r.keyPrefix)
	pipe := r.client.Pipeline()
	pipe.HIncrBy(ctx, statsKey, "total_accounts", 1)
	if balance, ok := data["starting_balance"].(string); ok {
		pipe.HIncrByFloat(ctx, statsKey, "total_initial_balance",
			mustParseFloat(balance))
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("error updating account creation metrics: %w", err)
	}

	// Store account metadata
	metaKey := fmt.Sprintf("%saccount:%s:meta", r.keyPrefix, data["account"])
	metadata := map[string]interface{}{
		"created_at":       data["timestamp"],
		"funder":           data["funder"],
		"starting_balance": data["starting_balance"],
	}
	return r.storeJSON(ctx, metaKey, metadata)
}

func (r *SaveToRedis) processTokenPrice(ctx context.Context, data map[string]interface{}) error {
	// Store latest price
	priceKey := fmt.Sprintf("%sprice:%s:%s:%s:%s:latest",
		r.keyPrefix, data["base_asset"], data["base_issuer"],
		data["quote_asset"], data["quote_issuer"])

	pipe := r.client.Pipeline()

	// Store price in Redis
	pipe.HSet(ctx, priceKey, map[string]interface{}{
		"price":      data["price"],
		"timestamp":  data["timestamp"],
		"volume_24h": data["volume_24h"],
		"high_24h":   data["high_price_24h"],
		"low_24h":    data["low_price_24h"],
	})

	// Set TTL for cleanup
	pipe.Expire(ctx, priceKey, 24*time.Hour)

	// Store in time series for short-term history
	tsKey := fmt.Sprintf("%sprice:%s:%s:%s:%s:history",
		r.keyPrefix, data["base_asset"], data["base_issuer"],
		data["quote_asset"], data["quote_issuer"])
	pipe.ZAdd(ctx, tsKey, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: data["price"],
	})

	// Keep only last 24 hours of data
	pipe.ZRemRangeByScore(ctx, tsKey,
		"-inf",
		fmt.Sprintf("%d", time.Now().Add(-24*time.Hour).Unix()),
	)

	// Store 1h data
	hourKey := fmt.Sprintf("%sprice:%s:%s:%s:%s:1h",
		r.keyPrefix, data["base_asset"], data["base_issuer"],
		data["quote_asset"], data["quote_issuer"])
	pipe.ZAdd(ctx, hourKey, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: data["price"],
	})
	pipe.ZRemRangeByScore(ctx, hourKey, "-inf",
		fmt.Sprintf("%d", time.Now().Add(-time.Hour).Unix()))

	// Store 24h data
	dayKey := fmt.Sprintf("%sprice:%s:%s:%s:%s:24h",
		r.keyPrefix, data["base_asset"], data["base_issuer"],
		data["quote_asset"], data["quote_issuer"])
	// Similar ZAdd/ZRemRangeByScore for 24h
	pipe.ZAdd(ctx, dayKey, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: data["price"],
	})
	pipe.ZRemRangeByScore(ctx, dayKey, "-inf",
		fmt.Sprintf("%d", time.Now().Add(-24*time.Hour).Unix()))

	// Store 7d data
	weekKey := fmt.Sprintf("%sprice:%s:%s:%s:%s:7d",
		r.keyPrefix, data["base_asset"], data["base_issuer"],
		data["quote_asset"], data["quote_issuer"])
	// Similar for 7d
	pipe.ZAdd(ctx, weekKey, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: data["price"],
	})
	pipe.ZRemRangeByScore(ctx, weekKey, "-inf",
		fmt.Sprintf("%d", time.Now().Add(-7*24*time.Hour).Unix()))

	// Store market cap data
	marketCapKey := fmt.Sprintf("%smarket_cap:%s:%s",
		r.keyPrefix, data["base_asset"], data["base_issuer"])
	pipe.Set(ctx, marketCapKey, data["market_cap"], 24*time.Hour)

	_, err := pipe.Exec(ctx)
	return err
}

func (r *SaveToRedis) processOffer(ctx context.Context, data map[string]interface{}) error {
	// Extract base and quote assets from trade_pair
	tradePair, ok := data["trade_pair"].(string)
	if !ok {
		return fmt.Errorf("missing trade_pair in data")
	}

	// Split trade pair into base and quote assets
	assets := strings.Split(tradePair, "/")
	if len(assets) != 2 {
		return fmt.Errorf("invalid trade pair format: %s", tradePair)
	}

	// Store the offer data
	offerKey := fmt.Sprintf("%soffer:%s:%s",
		r.keyPrefix,
		data["timestamp"],
		data["trade_pair"])

	if err := r.storeJSON(ctx, offerKey, data); err != nil {
		return err
	}

	// Update market metrics
	marketKey := fmt.Sprintf("%smarket:%s:stats",
		r.keyPrefix, tradePair)

	pipe := r.client.Pipeline()

	// Update trade count
	if tradeCount, ok := data["trade_count"].(float64); ok {
		pipe.HSet(ctx, marketKey, "num_trades", tradeCount)
	}

	// Update volume
	if volume, ok := data["volume"].(float64); ok {
		pipe.HSet(ctx, marketKey, "volume", volume)
	}

	// Store latest price
	if closePrice, ok := data["close_price"].(float64); ok {
		pipe.HSet(ctx, marketKey, "last_price", closePrice)
		pipe.HSet(ctx, marketKey, "last_updated", time.Now().Format(time.RFC3339))
	}

	// Store price history
	if timestamp, ok := data["timestamp"].(string); ok {
		ts, err := time.Parse(time.RFC3339, timestamp)
		if err == nil {
			priceHistoryKey := fmt.Sprintf("%smarket:%s:price_history",
				r.keyPrefix, tradePair)

			if closePrice, ok := data["close_price"].(float64); ok {
				pipe.ZAdd(ctx, priceHistoryKey, redis.Z{
					Score:  float64(ts.Unix()),
					Member: closePrice,
				})
			}
		}
	}

	// Set TTL (30 days)
	ttl := 30 * 24 * time.Hour
	pipe.Expire(ctx, marketKey, ttl)
	pipe.Expire(ctx, offerKey, ttl)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("error updating market metrics: %w", err)
	}

	return nil
}

func (r *SaveToRedis) processSetTrustLineFlags(ctx context.Context, data map[string]interface{}) error {
	// Extract required fields
	code, ok := data["code"].(string)
	if !ok || code == "" {
		return fmt.Errorf("missing or invalid code in trust line flags data")
	}

	// Handle native assets
	assetType, _ := data["asset_type"].(string)
	if assetType == "native" {
		key := fmt.Sprintf("%sasset:%s", r.keyPrefix, code)
		redisData := map[string]interface{}{
			"asset_code":     code,
			"asset_type":     "native",
			"operation_type": "set_trust_line_flags",
			"last_updated":   time.Now().Format(time.RFC3339),
		}

		// Store in Redis
		pipe := r.client.Pipeline()
		pipe.HSet(ctx, key, redisData)
		if r.ttl > 0 {
			pipe.Expire(ctx, key, r.ttl)
		}
		_, err := pipe.Exec(ctx)
		return err
	}

	// For non-native assets
	issuer, ok := data["issuer"].(string)
	if !ok || issuer == "" {
		return fmt.Errorf("missing or invalid issuer in trust line flags data")
	}

	key := fmt.Sprintf("%sasset:%s:%s", r.keyPrefix, code, issuer)
	redisData := map[string]interface{}{
		"asset_code":     code,
		"asset_issuer":   issuer,
		"asset_type":     assetType,
		"operation_type": "set_trust_line_flags",
		"last_updated":   time.Now().Format(time.RFC3339),
	}

	// Store in Redis
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, key, redisData)
	if r.ttl > 0 {
		pipe.Expire(ctx, key, r.ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (r *SaveToRedis) storeJSON(ctx context.Context, key string, data interface{}) error {
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("error marshaling data: %w", err)
	}

	if err := r.client.Set(ctx, key, jsonBytes, r.ttl).Err(); err != nil {
		return fmt.Errorf("error storing data in Redis: %w", err)
	}

	return nil
}

// Helper function to parse float values
func mustParseFloat(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

func (r *SaveToRedis) Close() error {
	if r.client != nil {
		return r.client.Close()
	}
	return nil
}

// Utility methods for retrieving data

func (r *SaveToRedis) GetAccountStats(ctx context.Context, accountID string) (map[string]float64, error) {
	// Get sent amounts
	sentKey := fmt.Sprintf("%saccount:%s:sent", r.keyPrefix, accountID)
	sent, err := r.client.ZRangeWithScores(ctx, sentKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	// Get received amounts
	receivedKey := fmt.Sprintf("%saccount:%s:received", r.keyPrefix, accountID)
	received, err := r.client.ZRangeWithScores(ctx, receivedKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	stats := make(map[string]float64)
	for _, z := range sent {
		stats["sent_"+z.Member.(string)] = z.Score
	}
	for _, z := range received {
		stats["received_"+z.Member.(string)] = z.Score
	}

	return stats, nil
}

func (r *SaveToRedis) GetAssetStats(ctx context.Context, assetCode string) (map[string]string, error) {
	key := fmt.Sprintf("%sasset:%s:stats", r.keyPrefix, assetCode)
	return r.client.HGetAll(ctx, key).Result()
}

func (r *SaveToRedis) GetAccountCreationStats(ctx context.Context) (map[string]string, error) {
	key := fmt.Sprintf("%sstats:account_creation", r.keyPrefix)
	return r.client.HGetAll(ctx, key).Result()
}

func (r *SaveToRedis) handleAssetStats(ctx context.Context, payload map[string]interface{}) error {
	assetCode, ok := payload["asset_code"].(string)
	if !ok {
		return fmt.Errorf("missing asset_code in asset_stats payload")
	}

	assetIssuer, ok := payload["asset_issuer"].(string)
	if !ok {
		return fmt.Errorf("missing asset_issuer in asset_stats payload")
	}

	key := fmt.Sprintf("stellar:asset:%s:%s", assetCode, assetIssuer)

	// Store all relevant fields
	data := make(map[string]interface{})
	for k, v := range payload {
		switch k {
		case "circulating_supply", "total_supply", "num_holders",
			"market_cap", "last_price", "balance_authorized",
			"balance_auth_liabilities", "balance_unauthorized",
			"accounts_authorized", "accounts_unauthorized",
			"num_claimable_balances", "num_contracts",
			"num_liquidity_pools":
			data[k] = v
		}
	}

	// Add timestamp
	data["last_updated"] = time.Now().Format(time.RFC3339)

	pipe := r.client.Pipeline()
	pipe.HSet(ctx, key, data)
	pipe.Expire(ctx, key, 30*24*time.Hour) // 30 days TTL

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("error storing asset stats in redis: %w", err)
	}

	return nil
}
