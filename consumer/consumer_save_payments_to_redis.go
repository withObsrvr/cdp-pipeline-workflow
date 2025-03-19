package consumer

import (
	"context"
	"crypto/tls"
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
	var client *redis.Client

	// Check if we're using a Redis URL or individual connection parameters
	connStr, hasConnStr := config["connection_string"].(string)
	redisURL, hasRedisURL := config["redis_url"].(string)

	// Set default key prefix
	keyPrefix, _ := config["key_prefix"].(string)
	if keyPrefix == "" {
		keyPrefix = "stellar:payments:"
	}

	// Set default TTL
	ttlHours := 24 // Default 24 hours TTL
	if ttl, ok := config["ttl_hours"].(float64); ok {
		ttlHours = int(ttl)
	}

	// Handle connection based on provided parameters
	if hasRedisURL {
		// Use Redis URL (supports both redis:// and rediss://)
		opt, err := redis.ParseURL(redisURL)
		if err != nil {
			return nil, fmt.Errorf("invalid Redis URL: %w", err)
		}
		client = redis.NewClient(opt)
		log.Printf("Connecting to Redis using URL: %s", redisURL)
	} else if hasConnStr {
		// Legacy connection string support
		opt, err := redis.ParseURL(connStr)
		if err != nil {
			return nil, fmt.Errorf("invalid connection string: %w", err)
		}
		client = redis.NewClient(opt)
		log.Printf("Connecting to Redis using connection string: %s", connStr)
	} else {
		// Use individual connection parameters
		address, ok := config["redis_address"].(string)
		if !ok {
			return nil, fmt.Errorf("missing redis_address in config")
		}

		password, _ := config["redis_password"].(string)

		dbNum := 0
		if db, ok := config["redis_db"].(float64); ok {
			dbNum = int(db)
		}

		// Check if TLS is enabled
		useTLS, _ := config["use_tls"].(bool)

		// Create Redis options
		opts := &redis.Options{
			Addr:      address,
			Password:  password,
			DB:        dbNum,
			TLSConfig: nil, // Default to no TLS
		}

		// Enable TLS if specified
		if useTLS {
			opts.TLSConfig = &tls.Config{
				MinVersion: tls.VersionTLS12,
			}
		}

		client = redis.NewClient(opts)
		log.Printf("Connecting to Redis at %s (TLS: %v)...", address, useTLS)
	}

	// Test connection
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	log.Printf("Successfully connected to Redis")

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

	// Generate a unique key for this payment
	key := fmt.Sprintf("%spayment:%s:%d",
		r.keyPrefix,
		payment.SourceAccountId,
		payment.LedgerSequence,
	)

	// Prepare data for Redis
	redisData := map[string]interface{}{
		"timestamp":              payment.Timestamp,
		"source_account_id":      payment.SourceAccountId,
		"destination_account_id": payment.DestinationAccountId,
		"asset_code":             payment.AssetCode,
		"amount":                 payment.Amount,
		"type":                   payment.Type,
		"memo":                   payment.Memo,
		"ledger_sequence":        payment.LedgerSequence,
		"stored_at":              time.Now().UTC().Format(time.RFC3339),
	}

	// Store in Redis using pipeline
	pipe := r.client.Pipeline()

	// Store payment data
	pipe.HSet(ctx, key, redisData)

	// Set TTL
	pipe.Expire(ctx, key, r.ttl)

	// Add to account indices for faster lookups
	sourceKey := fmt.Sprintf("%ssource:%s", r.keyPrefix, payment.SourceAccountId)
	destinationKey := fmt.Sprintf("%sdestination:%s", r.keyPrefix, payment.DestinationAccountId)

	pipe.ZAdd(ctx, sourceKey, redis.Z{
		Score:  float64(payment.LedgerSequence),
		Member: key,
	})
	pipe.ZAdd(ctx, destinationKey, redis.Z{
		Score:  float64(payment.LedgerSequence),
		Member: key,
	})

	// Set TTL on indices
	pipe.Expire(ctx, sourceKey, r.ttl)
	pipe.Expire(ctx, destinationKey, r.ttl)

	// Execute pipeline
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("error executing Redis pipeline: %w", err)
	}

	log.Printf("Successfully stored payment in Redis: %s", key)
	return nil
}
