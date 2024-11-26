package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

// AnalyticsPeriod represents different time periods for analytics
type AnalyticsPeriod string

const (
	Hour  AnalyticsPeriod = "1h"
	Day   AnalyticsPeriod = "24h"
	Week  AnalyticsPeriod = "7d"
	Month AnalyticsPeriod = "30d"
)

// MarketAnalytics represents aggregated market data for a trading pair
type MarketAnalytics struct {
	TradePair      string    `json:"trade_pair"`
	Period         string    `json:"period"`
	BaseAsset      string    `json:"base_asset"`
	CounterAsset   string    `json:"counter_asset"`
	OpenPrice      float64   `json:"open_price"`
	ClosePrice     float64   `json:"close_price"`
	HighPrice      float64   `json:"high_price"`
	LowPrice       float64   `json:"low_price"`
	Volume         float64   `json:"volume"`
	TradeCount     int64     `json:"trade_count"`
	PriceChange    float64   `json:"price_change"`
	Timestamp      time.Time `json:"timestamp"`
	BidVolume      float64   `json:"bid_volume"`
	AskVolume      float64   `json:"ask_volume"`
	Spread         float64   `json:"spread"`
	SpreadMidPoint float64   `json:"spread_mid_point"`
	Type           string    `json:"type"`
}

type SaveToMarketAnalyticsConsumer struct {
	client     *redis.Client
	processors []processor.Processor
	keyPrefix  string
}

func NewSaveToMarketAnalyticsConsumer(config map[string]interface{}) (*SaveToMarketAnalyticsConsumer, error) {
	address, ok := config["redis_address"].(string)
	if !ok {
		return nil, fmt.Errorf("missing redis_address in config")
	}

	password, _ := config["redis_password"].(string)
	dbNum, _ := config["redis_db"].(int)
	keyPrefix, _ := config["key_prefix"].(string)
	if keyPrefix == "" {
		keyPrefix = "market:analytics:"
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

	return &SaveToMarketAnalyticsConsumer{
		client:    client,
		keyPrefix: keyPrefix,
	}, nil
}

func (c *SaveToMarketAnalyticsConsumer) Subscribe(processor processor.Processor) {
	c.processors = append(c.processors, processor)
}

func (c *SaveToMarketAnalyticsConsumer) Process(ctx context.Context, msg processor.Message) error {
	// Convert message payload to MarketAnalytics
	var analytics MarketAnalytics

	switch payload := msg.Payload.(type) {
	case []byte:
		if err := json.Unmarshal(payload, &analytics); err != nil {
			return fmt.Errorf("failed to unmarshal analytics data: %w", err)
		}
	default:
		return fmt.Errorf("unexpected payload type: %T", msg.Payload)
	}

	// Store analytics data in Redis
	if err := c.storeMarketAnalytics(ctx, analytics); err != nil {
		return fmt.Errorf("failed to store market analytics: %w", err)
	}

	return nil
}

func (c *SaveToMarketAnalyticsConsumer) storeMarketAnalytics(ctx context.Context, analytics MarketAnalytics) error {
	pipe := c.client.Pipeline()

	// Key formats
	baseKey := fmt.Sprintf("%s%s:%s", c.keyPrefix, analytics.TradePair, analytics.Period)
	priceKey := fmt.Sprintf("%sprice", baseKey)
	volumeKey := fmt.Sprintf("%svolume", baseKey)
	tradesKey := fmt.Sprintf("%strades", baseKey)
	spreadKey := fmt.Sprintf("%sspread", baseKey)

	timestamp := float64(analytics.Timestamp.Unix())

	// Store time series data
	pipe.ZAdd(ctx, priceKey, redis.Z{
		Score:  timestamp,
		Member: analytics.ClosePrice,
	})
	pipe.ZAdd(ctx, volumeKey, redis.Z{
		Score:  timestamp,
		Member: analytics.Volume,
	})
	pipe.ZAdd(ctx, tradesKey, redis.Z{
		Score:  timestamp,
		Member: analytics.TradeCount,
	})
	pipe.ZAdd(ctx, spreadKey, redis.Z{
		Score:  timestamp,
		Member: analytics.Spread,
	})

	// Store latest market stats
	statsKey := fmt.Sprintf("%s%s:latest", c.keyPrefix, analytics.TradePair)
	stats := map[string]interface{}{
		"open_price":      analytics.OpenPrice,
		"close_price":     analytics.ClosePrice,
		"high_price":      analytics.HighPrice,
		"low_price":       analytics.LowPrice,
		"volume":          analytics.Volume,
		"trade_count":     analytics.TradeCount,
		"price_change":    analytics.PriceChange,
		"bid_volume":      analytics.BidVolume,
		"ask_volume":      analytics.AskVolume,
		"spread":          analytics.Spread,
		"spread_midpoint": analytics.SpreadMidPoint,
		"timestamp":       analytics.Timestamp.Format(time.RFC3339),
	}
	pipe.HMSet(ctx, statsKey, stats)

	// Set expiration for data cleanup (30 days)
	pipe.Expire(ctx, priceKey, 30*24*time.Hour)
	pipe.Expire(ctx, volumeKey, 30*24*time.Hour)
	pipe.Expire(ctx, tradesKey, 30*24*time.Hour)
	pipe.Expire(ctx, spreadKey, 30*24*time.Hour)
	pipe.Expire(ctx, statsKey, 30*24*time.Hour)

	// Execute pipeline
	_, err := pipe.Exec(ctx)
	return err
}

// Helper functions for analytics retrieval
func (c *SaveToMarketAnalyticsConsumer) GetMarketStats(ctx context.Context, tradePair string) (map[string]string, error) {
	statsKey := fmt.Sprintf("%s%s:latest", c.keyPrefix, tradePair)
	return c.client.HGetAll(ctx, statsKey).Result()
}

func (c *SaveToMarketAnalyticsConsumer) GetPriceHistory(ctx context.Context, tradePair string, period AnalyticsPeriod, from, to time.Time) ([]redis.Z, error) {
	priceKey := fmt.Sprintf("%s%s:%s:price", c.keyPrefix, tradePair, period)
	return c.client.ZRangeByScoreWithScores(ctx, priceKey, &redis.ZRangeBy{
		Min: strconv.FormatInt(from.Unix(), 10),
		Max: strconv.FormatInt(to.Unix(), 10),
	}).Result()
}

func (c *SaveToMarketAnalyticsConsumer) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}
