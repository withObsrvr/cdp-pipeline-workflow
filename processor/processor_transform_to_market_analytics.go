package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type MarketAnalyticsCombined struct {
	// Basic Asset Info (from TickerAsset)
	Code        string `json:"code"`
	Issuer      string `json:"issuer"`
	AssetType   string `json:"asset_type"`
	Name        string `json:"name"`
	Description string `json:"description"`
	IsAnchored  bool   `json:"is_anchored"`

	// Market Data (from AssetStats)
	TotalSupply       float64 `json:"total_supply"`
	CirculatingSupply float64 `json:"circulating_supply"`
	NumHolders        uint64  `json:"num_holders"`

	// Price Data (from TokenPrice)
	Price          float64   `json:"price"`
	PriceChange24h float64   `json:"price_change_24h"`
	Volume24h      float64   `json:"volume_24h"`
	MarketCap      float64   `json:"market_cap"`
	HighPrice24h   float64   `json:"high_24h"`
	LowPrice24h    float64   `json:"low_24h"`
	NumTrades24h   int64     `json:"num_trades_24h"`
	Timestamp      time.Time `json:"timestamp"`

	// Rankings
	MarketCapRank int       `json:"market_cap_rank"`
	VolumeRank    int       `json:"volume_rank"`
	LastUpdated   time.Time `json:"last_updated"`
}

type TransformToMarketAnalytics struct {
	processors  []Processor
	redisClient *redis.Client
	keyPrefix   string
}

func NewTransformToMarketAnalytics(config map[string]interface{}) (*TransformToMarketAnalytics, error) {
	redisAddr, ok := config["redis_address"].(string)
	if !ok {
		return nil, fmt.Errorf("missing redis_address in config")
	}

	redisPassword, _ := config["redis_password"].(string)
	redisDB := 0
	if db, ok := config["redis_db"].(float64); ok {
		redisDB = int(db)
	}

	keyPrefix, _ := config["key_prefix"].(string)
	if keyPrefix == "" {
		keyPrefix = "stellar:"
	}

	client := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})

	// Test connection
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %v", err)
	}

	return &TransformToMarketAnalytics{
		redisClient: client,
		keyPrefix:   keyPrefix,
		processors:  make([]Processor, 0),
	}, nil
}

func (p *TransformToMarketAnalytics) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *TransformToMarketAnalytics) Process(ctx context.Context, msg Message) error {
	// Parse incoming message
	var assetStats map[string]interface{}
	if err := json.Unmarshal(msg.Payload.([]byte), &assetStats); err != nil {
		return fmt.Errorf("error unmarshaling payload: %w", err)
	}

	// Check if this is an asset_stats message
	msgType, ok := assetStats["type"].(string)
	if !ok || msgType != "asset_stats" {
		return nil // Skip non-asset_stats messages
	}

	// Create analytics object from incoming stats
	analytics := &MarketAnalyticsCombined{
		// Basic Asset Info
		Code:      assetStats["asset_code"].(string),
		Issuer:    assetStats["asset_issuer"].(string),
		AssetType: assetStats["asset_type"].(string),
		Timestamp: assetStats["timestamp"].(time.Time),

		// Supply Data
		TotalSupply:       parseFloat64(fmt.Sprintf("%v", assetStats["total_supply"])),
		CirculatingSupply: parseFloat64(fmt.Sprintf("%v", assetStats["circulating_supply"])),
		NumHolders:        parseUint64(fmt.Sprintf("%v", assetStats["num_holders"])),

		// Price Data
		Price:       parseFloat64(fmt.Sprintf("%v", assetStats["last_price"])),
		MarketCap:   parseFloat64(fmt.Sprintf("%v", assetStats["market_cap"])),
		LastUpdated: time.Now(),
	}

	// Forward to next processors
	jsonBytes, err := json.Marshal(analytics)
	if err != nil {
		return fmt.Errorf("error marshaling analytics: %w", err)
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

func parseUint64(s string) uint64 {
	u, _ := strconv.ParseUint(s, 10, 64)
	return u
}

func parseInt64(s string) int64 {
	i, _ := strconv.ParseInt(s, 10, 64)
	return i
}
