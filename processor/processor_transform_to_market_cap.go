package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

// MarketCapAnalytics represents market cap and supply data for an asset
type MarketCapAnalytics struct {
	Asset             string    `json:"asset"`
	Issuer            string    `json:"issuer"`
	MarketCap         float64   `json:"market_cap"`
	Price             float64   `json:"price"`
	CirculatingSupply float64   `json:"circulating_supply"`
	TotalSupply       float64   `json:"total_supply"`
	NumHolders        int64     `json:"num_holders"`
	Volume24h         float64   `json:"volume_24h"`
	PriceChange24h    float64   `json:"price_change_24h"`
	LastUpdated       time.Time `json:"last_updated"`
	Rank              int       `json:"rank"`
	Type              string    `json:"type"`
}

type TransformToMarketCapProcessor struct {
	processors  []Processor
	redisClient *redis.Client
	keyPrefix   string
}

func NewTransformToMarketCapProcessor(config map[string]interface{}) (*TransformToMarketCapProcessor, error) {
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

	return &TransformToMarketCapProcessor{
		redisClient: client,
		keyPrefix:   keyPrefix,
		processors:  make([]Processor, 0),
	}, nil
}

func (p *TransformToMarketCapProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *TransformToMarketCapProcessor) Process(ctx context.Context, msg Message) error {
	var payload map[string]interface{}
	if err := json.Unmarshal(msg.Payload.([]byte), &payload); err != nil {
		return fmt.Errorf("error unmarshaling payload: %w", err)
	}

	// Get supply data from payload
	circulatingSupply := parseFloat64(fmt.Sprintf("%v", payload["circulating_supply"]))
	totalSupply := parseFloat64(fmt.Sprintf("%v", payload["total_supply"]))
	numHolders := parseUint64(fmt.Sprintf("%v", payload["num_holders"]))
	price := parseFloat64(fmt.Sprintf("%v", payload["last_price"]))

	// Calculate market cap
	marketCap := 0.0
	if circulatingSupply > 0 && price > 0 {
		marketCap = circulatingSupply * price
	}

	// Add market cap to payload
	payload["market_cap"] = marketCap
	payload["price"] = price
	payload["circulating_supply"] = circulatingSupply
	payload["total_supply"] = totalSupply
	payload["num_holders"] = numHolders
	payload["last_updated"] = time.Now().Format(time.RFC3339)

	// Forward to next processors
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error marshaling payload: %w", err)
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

// Helper function to parse asset string into code and issuer
func parseAsset(asset string) (code string, issuer string) {
	parts := strings.Split(asset, ":")
	if len(parts) == 1 {
		return parts[0], "" // Native asset
	}
	return parts[0], parts[1] // Issued asset
}

func (p *TransformToMarketCapProcessor) processFromMetrics(ctx context.Context, metrics *MarketMetrics) *MarketCapAnalytics {
	baseAsset, baseIssuer := parseAsset(metrics.BaseAsset)

	return &MarketCapAnalytics{
		Asset:          baseAsset,
		Issuer:         baseIssuer,
		Price:          metrics.Close24h,
		Volume24h:      metrics.BaseVolume24h,
		PriceChange24h: metrics.Change24h,
		LastUpdated:    metrics.LastTradeAt,
		Type:           metrics.Type,
	}
}

func (p *TransformToMarketCapProcessor) processFromPrice(ctx context.Context, price *TokenPrice) *MarketCapAnalytics {
	return &MarketCapAnalytics{
		Asset:          price.BaseAsset,
		Issuer:         price.BaseIssuer,
		Price:          price.Price,
		Volume24h:      price.Volume24h,
		PriceChange24h: price.PriceChange24h,
		LastUpdated:    price.Timestamp,
		Type:           price.Type,
	}
}

func (p *TransformToMarketCapProcessor) storeMarketCap(ctx context.Context, marketCap *MarketCapAnalytics) error {
	key := fmt.Sprintf("%smarket_cap:%s:%s", p.keyPrefix, marketCap.Asset, marketCap.Issuer)

	data := map[string]interface{}{
		"price":              marketCap.Price,
		"market_cap":         marketCap.MarketCap,
		"circulating_supply": marketCap.CirculatingSupply,
		"total_supply":       marketCap.TotalSupply,
		"num_holders":        marketCap.NumHolders,
		"volume_24h":         marketCap.Volume24h,
		"price_change_24h":   marketCap.PriceChange24h,
		"last_updated":       marketCap.LastUpdated.Format(time.RFC3339),
		"type":               marketCap.Type,
	}

	pipe := p.redisClient.Pipeline()

	// Store current state
	pipe.HMSet(ctx, key, data)

	// Store time series data
	tsKey := fmt.Sprintf("%smarket_cap:%s:%s:history", p.keyPrefix, marketCap.Asset, marketCap.Issuer)
	pipe.ZAdd(ctx, tsKey, &redis.Z{
		Score:  float64(marketCap.LastUpdated.Unix()),
		Member: marketCap.MarketCap,
	})

	// Set TTL (30 days)
	pipe.Expire(ctx, key, 30*24*time.Hour)
	pipe.Expire(ctx, tsKey, 30*24*time.Hour)

	_, err := pipe.Exec(ctx)
	return err
}

func (p *TransformToMarketCapProcessor) getAssetStats(ctx context.Context, code, issuer string) (*AssetStats, error) {
	key := fmt.Sprintf("%sasset:%s:%s", p.keyPrefix, code, issuer)

	data, err := p.redisClient.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	stats := &AssetStats{}
	stats.CirculatingSupply, _ = strconv.ParseFloat(data["circulating_supply"], 64)
	stats.TotalSupply, _ = strconv.ParseFloat(data["total_supply"], 64)
	stats.NumHolders, _ = strconv.ParseUint(data["num_holders"], 10, 64)

	return stats, nil
}
