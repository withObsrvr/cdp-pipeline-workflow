package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

type SaveToRedisOrderbookConsumer struct {
	client     *redis.Client
	processors []processor.Processor
	keyPrefix  string
}

func NewSaveToRedisOrderbookConsumer(config map[string]interface{}) (*SaveToRedisOrderbookConsumer, error) {
	address, ok := config["redis_address"].(string)
	if !ok {
		return nil, fmt.Errorf("missing redis_address in config")
	}

	password, _ := config["redis_password"].(string)
	dbNum, _ := config["redis_db"].(int)
	keyPrefix, _ := config["key_prefix"].(string)
	if keyPrefix == "" {
		keyPrefix = "orderbook:"
	}

	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       dbNum,
	})

	// Test connection
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %v", err)
	}

	return &SaveToRedisOrderbookConsumer{
		client:    client,
		keyPrefix: keyPrefix,
	}, nil
}

func (c *SaveToRedisOrderbookConsumer) Subscribe(processor processor.Processor) {
	c.processors = append(c.processors, processor)
}

func (c *SaveToRedisOrderbookConsumer) Process(ctx context.Context, msg processor.Message) error {
	var orderbook TickerOrderbook

	switch payload := msg.Payload.(type) {
	case []byte:
		if err := json.Unmarshal(payload, &orderbook); err != nil {
			return fmt.Errorf("failed to unmarshal orderbook data: %w", err)
		}
	default:
		return fmt.Errorf("unexpected payload type: %T", msg.Payload)
	}

	if err := c.storeOrderbook(ctx, orderbook); err != nil {
		return fmt.Errorf("failed to store orderbook: %w", err)
	}

	return nil
}

func (c *SaveToRedisOrderbookConsumer) storeOrderbook(ctx context.Context, orderbook TickerOrderbook) error {
	pipe := c.client.Pipeline()

	// Create market identifier
	marketID := fmt.Sprintf("%s:%s", orderbook.BaseAsset, orderbook.QuoteAsset)

	// Store current orderbook state
	currentKey := fmt.Sprintf("%s%s:current", c.keyPrefix, marketID)
	data := map[string]interface{}{
		"bid_count":     orderbook.BidCount,
		"bid_volume":    orderbook.BidVolume,
		"bid_max":       orderbook.BidMax,
		"ask_count":     orderbook.AskCount,
		"ask_volume":    orderbook.AskVolume,
		"ask_min":       orderbook.AskMin,
		"spread":        orderbook.Spread,
		"spread_mid":    orderbook.SpreadMid,
		"last_modified": orderbook.LastModified.Format(time.RFC3339),
	}
	pipe.HMSet(ctx, currentKey, data)

	// Store time series data
	timestamp := float64(orderbook.LastModified.Unix())

	// Store bid/ask prices history
	bidKey := fmt.Sprintf("%s%s:bids", c.keyPrefix, marketID)
	askKey := fmt.Sprintf("%s%s:asks", c.keyPrefix, marketID)
	spreadKey := fmt.Sprintf("%s%s:spread", c.keyPrefix, marketID)
	volumeKey := fmt.Sprintf("%s%s:volume", c.keyPrefix, marketID)

	pipe.ZAdd(ctx, bidKey, redis.Z{Score: timestamp, Member: orderbook.BidMax})
	pipe.ZAdd(ctx, askKey, redis.Z{Score: timestamp, Member: orderbook.AskMin})
	pipe.ZAdd(ctx, spreadKey, redis.Z{Score: timestamp, Member: orderbook.Spread})
	pipe.ZAdd(ctx, volumeKey, redis.Z{
		Score:  timestamp,
		Member: orderbook.BidVolume + orderbook.AskVolume,
	})

	// Set TTL (30 days)
	ttl := 30 * 24 * time.Hour
	pipe.Expire(ctx, currentKey, ttl)
	pipe.Expire(ctx, bidKey, ttl)
	pipe.Expire(ctx, askKey, ttl)
	pipe.Expire(ctx, spreadKey, ttl)
	pipe.Expire(ctx, volumeKey, ttl)

	// Execute pipeline
	_, err := pipe.Exec(ctx)
	return err
}

func (c *SaveToRedisOrderbookConsumer) GetCurrentOrderbook(ctx context.Context, baseAsset, quoteAsset string) (map[string]string, error) {
	marketID := fmt.Sprintf("%s:%s", baseAsset, quoteAsset)
	key := fmt.Sprintf("%s%s:current", c.keyPrefix, marketID)
	return c.client.HGetAll(ctx, key).Result()
}

func (c *SaveToRedisOrderbookConsumer) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

// TickerOrderbook represents the orderbook data for a market
type TickerOrderbook struct {
	BaseAsset    string    `json:"base_asset"`
	QuoteAsset   string    `json:"quote_asset"`
	BidCount     int       `json:"bid_count"`
	BidVolume    float64   `json:"bid_volume"`
	BidMax       float64   `json:"bid_max"`
	AskCount     int       `json:"ask_count"`
	AskVolume    float64   `json:"ask_volume"`
	AskMin       float64   `json:"ask_min"`
	Spread       float64   `json:"spread"`
	SpreadMid    float64   `json:"spread_mid"`
	LastModified time.Time `json:"last_modified"`
}
