package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type SaveToRedis struct {
	client     *redis.Client
	processors []Processor
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

func (r *SaveToRedis) Subscribe(processor Processor) {
	r.processors = append(r.processors, processor)
}

func (r *SaveToRedis) Process(ctx context.Context, msg Message) error {
	payloadBytes, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected Message type, got %T", msg)
	}

	// Parse the operation type
	var data map[string]interface{}
	if err := json.Unmarshal(payloadBytes, &data); err != nil {
		return fmt.Errorf("error unmarshaling payload: %w", err)
	}

	operationType, ok := data["type"].(string)
	if !ok {
		return fmt.Errorf("missing operation type in payload")
	}

	// Process based on operation type
	switch operationType {
	case "payment":
		return r.processPayment(ctx, data)
	case "create_account":
		return r.processCreateAccount(ctx, data)
	default:
		return fmt.Errorf("unsupported operation type: %s", operationType)
	}
}

func (r *SaveToRedis) processPayment(ctx context.Context, data map[string]interface{}) error {
	// Store the full payment data
	paymentKey := fmt.Sprintf("%spayment:%s:%s",
		r.keyPrefix, data["timestamp"], data["buyer_account_id"])

	if err := r.storeJSON(ctx, paymentKey, data); err != nil {
		return err
	}

	// Update account balances in sorted sets
	if amount, ok := data["amount"].(string); ok {
		amountFloat, err := strconv.ParseFloat(amount, 64)
		if err == nil {
			// Update sender's total sent
			senderKey := fmt.Sprintf("%saccount:%s:sent",
				r.keyPrefix, data["seller_account_id"])
			r.client.ZIncrBy(ctx, senderKey, amountFloat, data["asset_code"].(string))

			// Update receiver's total received
			receiverKey := fmt.Sprintf("%saccount:%s:received",
				r.keyPrefix, data["buyer_account_id"])
			r.client.ZIncrBy(ctx, receiverKey, amountFloat, data["asset_code"].(string))
		}
	}

	// Update asset metrics
	if assetCode, ok := data["asset_code"].(string); ok {
		assetKey := fmt.Sprintf("%sasset:%s:stats", r.keyPrefix, assetCode)
		pipe := r.client.Pipeline()
		pipe.HIncrBy(ctx, assetKey, "total_transactions", 1)
		if amount, ok := data["amount"].(string); ok {
			pipe.HIncrByFloat(ctx, assetKey, "total_volume",
				mustParseFloat(amount))
		}
		_, err := pipe.Exec(ctx)
		if err != nil {
			return fmt.Errorf("error updating asset metrics: %w", err)
		}
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
