package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

type SavePaymentsToRedis struct {
	client     *redis.Client
	processors []processor.Processor
	keyPrefix  string
	ttl        time.Duration
}

type RedisPaymentConfig struct {
	ConnectionString string
	KeyPrefix        string
	TTLHours         int
}

func NewSavePaymentsToRedis(config map[string]interface{}) (*SavePaymentsToRedis, error) {
	// Parse configuration
	connStr, ok := config["connection_string"].(string)
	if !ok || connStr == "" {
		return nil, fmt.Errorf("missing or empty connection_string in config")
	}

	keyPrefix, _ := config["key_prefix"].(string)
	if keyPrefix == "" {
		keyPrefix = "stellar:payments:"
	}

	ttlHours := 24 // Default 24 hours TTL
	if ttl, ok := config["ttl_hours"].(float64); ok {
		ttlHours = int(ttl)
	}

	// Create Redis client
	opt, err := redis.ParseURL(connStr)
	if err != nil {
		return nil, fmt.Errorf("invalid connection string: %w", err)
	}

	client := redis.NewClient(opt)

	// Test connection
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &SavePaymentsToRedis{
		client:    client,
		keyPrefix: keyPrefix,
		ttl:       time.Duration(ttlHours) * time.Hour,
	}, nil
}

func (r *SavePaymentsToRedis) Subscribe(processor processor.Processor) {
	r.processors = append(r.processors, processor)
}

func (r *SavePaymentsToRedis) Process(ctx context.Context, msg processor.Message) error {
	log.Printf("Processing message in SavePaymentsToRedis")

	var payment processor.AppPayment
	if err := json.Unmarshal(msg.Payload.([]byte), &payment); err != nil {
		return fmt.Errorf("error unmarshaling payment: %w", err)
	}

	// Create Redis key using timestamp and account IDs
	key := fmt.Sprintf("%s%s:%s:%s",
		r.keyPrefix,
		payment.Timestamp,
		payment.BuyerAccountId[:8],
		payment.SellerAccountId[:8])

	// Prepare data for Redis
	redisData := map[string]interface{}{
		"timestamp":         payment.Timestamp,
		"buyer_account_id":  payment.BuyerAccountId,
		"seller_account_id": payment.SellerAccountId,
		"asset_code":        payment.AssetCode,
		"amount":            payment.Amount,
		"type":              payment.Type,
		"memo":              payment.Memo,
		"ledger_sequence":   payment.LedgerSequence,
		"stored_at":         time.Now().UTC().Format(time.RFC3339),
	}

	// Store in Redis using pipeline
	pipe := r.client.Pipeline()

	// Store payment data
	pipe.HSet(ctx, key, redisData)

	// Set TTL
	pipe.Expire(ctx, key, r.ttl)

	// Add to account indices for faster lookups
	buyerKey := fmt.Sprintf("%sbuyer:%s", r.keyPrefix, payment.BuyerAccountId)
	sellerKey := fmt.Sprintf("%sseller:%s", r.keyPrefix, payment.SellerAccountId)

	pipe.ZAdd(ctx, buyerKey, redis.Z{
		Score:  float64(payment.LedgerSequence),
		Member: key,
	})
	pipe.ZAdd(ctx, sellerKey, redis.Z{
		Score:  float64(payment.LedgerSequence),
		Member: key,
	})

	// Set TTL on indices
	pipe.Expire(ctx, buyerKey, r.ttl)
	pipe.Expire(ctx, sellerKey, r.ttl)

	// Execute pipeline
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("error executing Redis pipeline: %w", err)
	}

	log.Printf("Successfully stored payment in Redis: %s", key)
	return nil
}
